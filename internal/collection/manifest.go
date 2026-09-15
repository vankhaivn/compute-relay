package collection

import (
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/vankhaivn/compute-relay/api/schemas"
	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

type manifest struct {
	Phase    string          `json:"phase"`
	Started  time.Time       `json:"started_at"`
	Finished time.Time       `json:"finished_at"`
	TimedOut bool            `json:"timed_out"`
	Error    json.RawMessage `json:"error"`
	Resource struct {
		Required bool `json:"gpu_required"`
		Verified bool `json:"gpu_verified"`
	} `json:"resource_check"`
	Artifacts []struct {
		Path      string              `json:"path"`
		Bytes     int64               `json:"bytes"`
		SHA256    domain.SHA256Digest `json:"sha256"`
		MediaType string              `json:"media_type"`
	} `json:"artifacts"`
}

var schemaOnce sync.Once
var resultSchema *jsonschema.Schema
var schemaErr error

type closedLoader struct{}

func (closedLoader) Load(string) (any, error) { return nil, ErrInvalid }
func compileManifestSchema() {
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	c.AssertFormat()
	c.UseLoader(closedLoader{})
	for _, name := range []string{"common.v1alpha1.schema.json", "result-manifest.v1alpha1.schema.json"} {
		raw, err := schemas.Files.ReadFile(name)
		if err != nil {
			schemaErr = ErrUnavailable
			return
		}
		var v any
		if json.Unmarshal(raw, &v) != nil {
			schemaErr = ErrUnavailable
			return
		}
		if c.AddResource("https://contracts.compute-relay.invalid/"+name, v) != nil {
			schemaErr = ErrUnavailable
			return
		}
	}
	resultSchema, schemaErr = c.Compile("https://contracts.compute-relay.invalid/result-manifest.v1alpha1.schema.json")
}
func parseManifest(w Work, raw []byte) (manifest, admission.Spec, error) {
	var m manifest
	var spec admission.Spec
	if !w.Valid() {
		return m, spec, ErrInvalid
	}
	value, err := strictValue(raw)
	if err != nil {
		return m, spec, failure(domain.CodeArtifactCollectionFailed, true)
	}
	schemaOnce.Do(compileManifestSchema)
	if schemaErr != nil {
		return m, spec, ErrUnavailable
	}
	if resultSchema.Validate(value) != nil {
		return m, spec, failure(domain.CodeArtifactCollectionFailed, true)
	}
	if provider.ValidateManifestIdentity(w.Plan.Job.Identity, raw) != nil {
		return m, spec, failure(domain.CodeRemoteIdentityMismatch, true)
	}
	if json.Unmarshal(raw, &m) != nil || json.Unmarshal(w.Plan.Job.Specification, &spec) != nil {
		return m, spec, ErrInvalid
	}
	if m.Finished.Before(m.Started) || m.TimedOut != (m.Phase == "timed_out") ||
		m.Resource.Required != (spec.Resources.Accelerator == "gpu") ||
		m.Phase == "completed" && m.Resource.Required && !m.Resource.Verified ||
		m.Phase != "completed" && (len(m.Error) == 0 || string(m.Error) == "null") {
		return m, spec, failure(domain.CodeArtifactCollectionFailed, true)
	}
	return m, spec, nil
}

// collisionFree applies portable case/prefix rules in O(n log n), not an unbounded
// pairwise comparison. No remote path is interpreted by a host filesystem.
func collisionFree(paths []string) bool {
	names := make([]string, len(paths))
	for i, path := range paths {
		if !provider.SafeArtifactPath(path) {
			return false
		}
		for _, c := range path {
			if c < 32 || c > 126 {
				return false
			}
		}
		names[i] = strings.ToLower(path)
	}
	sort.Strings(names)
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		if seen[name] {
			return false
		}
		for at := strings.IndexByte(name, '/'); at >= 0; {
			if seen[name[:at]] {
				return false
			}
			next := strings.IndexByte(name[at+1:], '/')
			if next < 0 {
				break
			}
			at += next + 1
		}
		seen[name] = true
	}
	return true
}

// BuildSnapshot verifies the complete runner manifest against a complete provider
// listing, then selects ONLY declared outputs and a fixed control-file allowlist.
func BuildSnapshot(w Work, raw []byte, catalog []provider.Artifact, cfg Config) (Snapshot, error) {
	var empty Snapshot
	if !cfg.Valid() || len(catalog) > cfg.MaxFiles {
		return empty, ErrInvalid
	}
	m, spec, err := parseManifest(w, raw)
	if err != nil {
		return empty, err
	}
	byPath := make(map[string]provider.Artifact, len(catalog))
	paths := make([]string, 0, len(catalog))
	for _, a := range catalog {
		if a.Validate(w.Observation.Remote) != nil {
			return empty, failure(domain.CodeRemoteIdentityMismatch, true)
		}
		paths = append(paths, a.Path)
		byPath[a.Path] = a
	}
	if !collisionFree(paths) {
		return empty, failure(domain.CodeArtifactCollectionFailed, true)
	}
	control, ok := byPath[ManifestPath]
	if !ok {
		return empty, failure(domain.CodeResultManifestMissing, false)
	}
	if control.Bytes != int64(len(raw)) || control.SHA256 != provider.Digest(raw) {
		return empty, failure(domain.CodeArtifactDigestMismatch, true)
	}
	result := Snapshot{Manifest: string(raw)}
	var total int64
	add := func(a provider.Artifact, role, media string) error {
		if a.Bytes > cfg.MaxBytes-total || len(result.Files) >= cfg.MaxFiles {
			return failure(domain.CodeArtifactCollectionFailed, false)
		}
		id := FileIdentity(w.Observation.Remote, a.Path, a.Bytes, a.SHA256)
		result.Files = append(result.Files, File{ID: id, Path: a.Path, Role: role, MediaType: media, Object: domain.ObjectMetadata{
			ID: domain.ObjectID(id), WorkspaceID: w.Lease.WorkspaceID, Bytes: a.Bytes, SHA256: a.SHA256,
		}})
		total += a.Bytes
		return nil
	}
	if err = add(control, "manifest", "application/json"); err != nil {
		return empty, err
	}
	outputPaths := make([]string, 0, len(m.Artifacts))
	counts, sizes := make([]int, len(spec.Outputs)), make([]int64, len(spec.Outputs))
	for _, a := range m.Artifacts {
		outputPaths = append(outputPaths, a.Path)
		matched := -1
		for i, output := range spec.Outputs {
			isDir := output.Kind == "directory"
			if !isDir && a.Path == output.Path || isDir && strings.HasPrefix(a.Path, output.Path+"/") {
				matched = i
				break
			}
		}
		if matched < 0 {
			return empty, failure(domain.CodeArtifactCollectionFailed, true)
		}
		output := spec.Outputs[matched]
		if output.MaxBytes > 0 && a.Bytes > output.MaxBytes-sizes[matched] {
			return empty, failure(domain.CodeArtifactCollectionFailed, false)
		}
		sizes[matched] += a.Bytes
		counts[matched]++
		listed, ok := byPath["outputs/"+a.Path]
		if !ok {
			return empty, failure(domain.CodeArtifactMissing, false)
		}
		if listed.Bytes != a.Bytes || listed.SHA256 != a.SHA256 {
			return empty, failure(domain.CodeArtifactDigestMismatch, true)
		}
		if err = add(listed, "output", a.MediaType); err != nil {
			return empty, err
		}
	}
	if !collisionFree(outputPaths) {
		return empty, failure(domain.CodeArtifactCollectionFailed, true)
	}
	// The v1 manifest has no directory-presence entries. Empty required directories
	// cannot establish verified presence; do not silently claim an absent output exists.
	if m.Phase == "completed" {
		for i, output := range spec.Outputs {
			if output.Required && counts[i] == 0 {
				return empty, failure(domain.CodeArtifactMissing, false)
			}
		}
	}
	for _, selected := range []struct {
		path, role, media string
		max               int64
	}{
		{"control/stdout.log", "log", "text/plain", 20 << 20},
		{"control/stderr.log", "log", "text/plain", 20 << 20},
		{"control/environment.json", "provenance", "application/json", 1 << 20},
	} {
		if a, ok := byPath[selected.path]; ok {
			if a.Bytes > selected.max {
				return empty, failure(domain.CodeArtifactCollectionFailed, false)
			}
			if err = add(a, selected.role, selected.media); err != nil {
				return empty, err
			}
		}
	}
	sort.Slice(result.Files, func(i, j int) bool { return result.Files[i].Path < result.Files[j].Path })
	if !result.Digest().Valid() {
		return empty, ErrInvalid
	}
	return result, nil
}

// ValidateSnapshot is also run when loading durable pins. It cannot assert that
// local bytes exist; only the engine can produce the sealed Verified value.
func ValidateSnapshot(w Work, s Snapshot) error {
	if len(s.Files) == 0 || len(s.Files) > MaxFiles || len(s.Manifest) > MaxManifestBytes || !s.Digest().Valid() {
		return ErrInvalid
	}
	catalog := make([]provider.Artifact, 0, len(s.Files))
	for _, f := range s.Files {
		if !f.Object.Valid() || f.Object.WorkspaceID != w.Lease.WorkspaceID {
			return ErrInvalid
		}
		catalog = append(catalog, provider.Artifact{Remote: w.Observation.Remote, Path: f.Path, Bytes: f.Object.Bytes, SHA256: f.Object.SHA256})
	}
	rebuilt, err := BuildSnapshot(w, []byte(s.Manifest), catalog, DefaultConfig())
	if err != nil || rebuilt.Digest() != s.Digest() {
		return ErrInvalid
	}
	return nil
}
