// Package staging builds and verifies immutable Kaggle staging snapshots.
// It has no provider mutation, credential, process or durable-store API. A plan is
// not a creation permit; integration with the existing M3 journal is required.
package staging

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

const (
	ManifestName             = "relay-staging.json"
	MaxInputs                = 64
	MaxBundleBytes     int64 = 100 << 20
	MaxInputBytes      int64 = 2 << 30
	MaxTotalInputBytes int64 = 4 << 30
	MaxManifestBytes         = 128 << 10
)

var (
	ErrPlan   = errors.New("invalid immutable staging plan")
	idRE      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	accountRE = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{1,49}$`)
	nameRE    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	digestRE  = regexp.MustCompile(`^[a-f0-9]{64}$`)
)

// Identity must be supplied from the existing frozen M3 plan and preparation
// journal, not generated at recovery time. These values are non-secret identity.
// ParentPlanSHA256 binds the entire original provider plan, not only its inputs.
type Identity struct {
	InstallationID        string `json:"installation_id"`
	WorkspaceID           string `json:"workspace_id"`
	JobID                 string `json:"job_id"`
	AttemptID             string `json:"attempt_id"`
	InstanceID            string `json:"instance_id"`
	ConfigurationRevision string `json:"configuration_revision"`
	PreparationID         string `json:"preparation_id"`
	Nonce                 string `json:"nonce"`
	ParentPlanSHA256      string `json:"parent_plan_sha256"`
	InputManifestSHA256   string `json:"input_manifest_sha256"`
}

// Object is adapter-local transfer metadata, not a replacement public object API.
// There are deliberately no URLs, arbitrary host paths or credential fields.
type Object struct {
	WorkspaceID string `json:"workspace_id"`
	ID          string `json:"object_id"`
	Bytes       int64  `json:"bytes"`
	SHA256      string `json:"sha256"`
}
type Input struct {
	Name   string `json:"name"`
	Target string `json:"target"`
	Object Object `json:"object"`
}
type File struct {
	Name   string `json:"name"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}
type payload struct {
	File      File   `json:"file"`
	Object    Object `json:"object"`
	InputName string `json:"input_name,omitempty"`
	Target    string `json:"target,omitempty"`
}
type manifest struct {
	Version  string    `json:"manifest_version"`
	Identity Identity  `json:"identity"`
	Files    []payload `json:"files"`
}

// Plan owns its bytes; accessors never return mutable internal slices. This
// checkpoint format is provisional until the staging transport/ledger review.
type Plan struct {
	account, slug, digest string
	identity              Identity
	manifest, metadata    []byte
	files                 []File
}

func NewPlan(account string, identity Identity, bundle Object, inputs []Input) (Plan, error) {
	var zero Plan
	if !accountRE.MatchString(account) || len(inputs) > MaxInputs {
		return zero, ErrPlan
	}
	for _, id := range []string{identity.InstallationID, identity.WorkspaceID, identity.JobID,
		identity.AttemptID, identity.InstanceID, identity.ConfigurationRevision, identity.PreparationID} {
		if !idRE.MatchString(id) {
			return zero, ErrPlan
		}
	}
	for _, d := range []string{identity.Nonce, identity.ParentPlanSHA256, identity.InputManifestSHA256} {
		if !digestRE.MatchString(d) {
			return zero, ErrPlan
		}
	}
	validObject := func(o Object, max int64) bool {
		return o.WorkspaceID == identity.WorkspaceID && idRE.MatchString(o.ID) &&
			o.Bytes >= 0 && o.Bytes <= max && digestRE.MatchString(o.SHA256)
	}
	if !validObject(bundle, MaxBundleBytes) {
		return zero, ErrPlan
	}
	ordered := append([]Input(nil), inputs...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Target < ordered[j].Target })
	names := map[string]bool{}
	objects := map[string]Object{bundle.ID: bundle}
	paths := []string{}
	var total int64
	m := manifest{Version: "compute-relay/kaggle-staging/v1", Identity: identity,
		Files: []payload{{File: File{"bundle.bin", bundle.Bytes, bundle.SHA256}, Object: bundle}}}
	// Input content identity follows the already published runner digest recipe:
	// sorted target order, alphabetical JSON keys and no HTML escaping.
	type digestEntry struct {
		Bytes  int64  `json:"bytes"`
		Name   string `json:"name"`
		SHA256 string `json:"sha256"`
		Target string `json:"target"`
	}
	entries := make([]digestEntry, 0, len(ordered))
	for i, in := range ordered {
		lowerName := strings.ToLower(in.Name)
		if !validObject(in.Object, MaxInputBytes) || in.Object.Bytes > MaxTotalInputBytes-total ||
			!nameRE.MatchString(in.Name) || names[lowerName] || !safeTarget(in.Target) {
			return zero, ErrPlan
		}
		if previous, ok := objects[in.Object.ID]; ok && previous != in.Object {
			return zero, ErrPlan
		}
		objects[in.Object.ID] = in.Object
		names[lowerName] = true
		p := strings.ToLower(in.Target)
		for _, old := range paths {
			if old == p || strings.HasPrefix(old, p+"/") || strings.HasPrefix(p, old+"/") {
				return zero, ErrPlan
			}
		}
		paths = append(paths, p)
		total += in.Object.Bytes
		file := File{fmt.Sprintf("input-%03d.bin", i), in.Object.Bytes, in.Object.SHA256}
		m.Files = append(m.Files, payload{file, in.Object, in.Name, in.Target})
		entries = append(entries, digestEntry{in.Object.Bytes, in.Name, in.Object.SHA256, in.Target})
	}
	entryBytes, err := canonicalJSON(entries)
	if err != nil || sum(entryBytes) != identity.InputManifestSHA256 {
		return zero, ErrPlan
	}
	raw, err := canonicalJSON(m)
	if err != nil || len(raw) > MaxManifestBytes {
		return zero, ErrPlan
	}
	// Include account/configuration/operation as well as complete manifest bytes.
	// Length-delimited JSON, rather than concatenated IDs, prevents ambiguity.
	seed, err := canonicalJSON(struct {
		Account  string
		Manifest string
	}{account, sum(raw)})
	if err != nil {
		return zero, ErrPlan
	}
	p := Plan{account: account, slug: "crs-" + sum(seed)[:40], digest: sum(raw), identity: identity, manifest: raw}
	for _, entry := range m.Files {
		p.files = append(p.files, entry.File)
	}
	p.files = append(p.files, File{ManifestName, int64(len(raw)), p.digest})
	// Privacy is NOT a Kaggle metadata field. A future create transport must
	// separately force private visibility and disable byte conversion/extraction.
	metadata := struct {
		Title    string `json:"title"`
		ID       string `json:"id"`
		Licenses []struct {
			Name string `json:"name"`
		} `json:"licenses"`
		Description string `json:"description"`
	}{Title: p.slug, ID: p.Reference(), Licenses: []struct {
		Name string `json:"name"`
	}{{"copyright-authors"}},
		Description: "Compute Relay private attempt staging. Existing licenses and ownership of all files remain applicable; this metadata does not relicense the contents."}
	p.metadata, err = canonicalJSON(metadata)
	if err != nil {
		return zero, ErrPlan
	}
	return p, nil
}

func safeTarget(p string) bool {
	if p == "" || len(p) > 1024 || strings.ContainsAny(p, "\\:") {
		return false
	}
	for _, c := range p {
		if c < 32 || c >= 127 {
			return false
		}
	}
	for _, segment := range strings.Split(p, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return false
		}
	}
	return true
}
func canonicalJSON(v any) ([]byte, error) {
	var b strings.Builder
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	if err := e.Encode(v); err != nil {
		return nil, err
	}
	return []byte(strings.TrimSuffix(b.String(), "\n")), nil
}
func sum(raw []byte) string { h := sha256.Sum256(raw); return hex.EncodeToString(h[:]) }
func (p Plan) valid() bool  { return p.slug != "" && len(p.manifest) > 0 && p.digest == sum(p.manifest) }
func (p Plan) Reference() string {
	if !p.validForReference() {
		return ""
	}
	return p.account + "/" + p.slug
}
func (p Plan) validForReference() bool { return p.account != "" && p.slug != "" }
func (p Plan) Digest() string          { return p.digest }
func (p Plan) Manifest() []byte        { return append([]byte(nil), p.manifest...) }
func (p Plan) Metadata() []byte        { return append([]byte(nil), p.metadata...) }
func (p Plan) Files() []File           { return append([]File(nil), p.files...) }
