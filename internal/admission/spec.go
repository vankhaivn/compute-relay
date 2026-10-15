// Package admission owns durable local job acceptance, not dispatch or input fetching.
package admission

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/vankhaivn/compute-relay/api/schemas"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

var (
	ErrUnsupportedInput = errors.New("HTTPS input sources are unsupported; upload inputs as objects")
	ErrSpec             = errors.New("invalid job specification")
	ErrKey              = errors.New("invalid idempotency key")
	ErrSelection        = errors.New("selected immutable profile revision is no longer available for new work")
	ErrConflict         = errors.New("idempotency key was used with a different request")
	ErrNotFound         = errors.New("job or admission not found")
	ErrInputs           = errors.New("referenced input is missing or not visible")
	ErrRequirements     = errors.New("job exceeds the resolved profile policy")
	ErrLimit            = errors.New("local outstanding job limit reached")
	ErrExpired          = errors.New("absolute execution deadline has expired")
	ErrUnavailable      = errors.New("durable admission unavailable")
)

type Source struct {
	Kind           string              `json:"kind"`
	ObjectID       domain.ObjectID     `json:"object_id,omitempty"`
	SHA256         domain.SHA256Digest `json:"sha256,omitempty"`
	Bytes          *int64              `json:"bytes,omitempty"`
	URL            string              `json:"url,omitempty"`
	ExpectedSHA256 domain.SHA256Digest `json:"expected_sha256,omitempty"`
}
type Input struct {
	Name   string `json:"name"`
	Source Source `json:"source"`
	Target string `json:"target"`
}
type Output struct {
	Path     string `json:"path"`
	Kind     string `json:"kind,omitempty"`
	Required bool   `json:"required"`
	MaxBytes int64  `json:"max_bytes,omitempty"`
}

// ApplicationImage names one immutable, workspace-owned Docker/OCI archive.
// Its digest and byte count are mandatory when the image is selected.
type ApplicationImage struct {
	ObjectID domain.ObjectID     `json:"object_id"`
	SHA256   domain.SHA256Digest `json:"sha256"`
	Bytes    int64               `json:"bytes"`
}

// Retention is frozen with the job. A zero after-collection delay requests
// cleanup after verified publication; an uncollected lifetime permits discard
// only after a terminal attempt with confirmed remote inactivity (or no start).
type Retention struct {
	AfterCollectionSeconds *int64 `json:"after_collection_seconds,omitempty"`
	UncollectedMaxSeconds  *int64 `json:"uncollected_max_seconds,omitempty"`
}

type Spec struct {
	WorkloadSecrets  []workloadsecret.Slot `json:"workload_secrets,omitempty"`
	ExecutionControl string                `json:"execution_control,omitempty"`
	ResultCollection string                `json:"result_collection,omitempty"`
	Retention        *Retention            `json:"retention,omitempty"`
	APIVersion       string                `json:"api_version"`
	Name             string                `json:"name"`
	Profile          string                `json:"profile"`
	Labels           map[string]string     `json:"labels,omitempty"`
	Bundle           struct {
		ObjectID domain.ObjectID     `json:"object_id"`
		SHA256   domain.SHA256Digest `json:"sha256,omitempty"`
		Bytes    *int64              `json:"bytes,omitempty"`
	} `json:"bundle"`
	ApplicationImage *ApplicationImage `json:"application_image,omitempty"`
	Execution        struct {
		Kind             string            `json:"kind"`
		Command          []string          `json:"command"`
		WorkingDirectory string            `json:"working_directory,omitempty"`
		Environment      map[string]string `json:"environment,omitempty"`
		Dependencies     struct {
			PythonRequirements string     `json:"python_requirements,omitempty"`
			ShellSetup         [][]string `json:"shell_setup,omitempty"`
		} `json:"dependencies,omitempty"`
	} `json:"execution"`
	Inputs    []Input  `json:"inputs"`
	Outputs   []Output `json:"outputs"`
	Resources struct {
		Accelerator           string `json:"accelerator"`
		MinimumGPUCount       int    `json:"minimum_gpu_count,omitempty"`
		MinimumGPUMemoryBytes int64  `json:"minimum_gpu_memory_bytes,omitempty"`
	} `json:"resources"`
	Network struct {
		RemoteInternet string `json:"remote_internet"`
	} `json:"network"`
	Timeouts struct {
		ExecutionDeadline        string `json:"execution_deadline,omitempty"`
		RemoteWallSeconds        int64  `json:"remote_wall_seconds"`
		SetupSeconds             int64  `json:"setup_seconds"`
		FinalizationGraceSeconds int64  `json:"finalization_grace_seconds"`
	} `json:"timeouts"`
}

// SeparateStart selects the one-shot prepare/ready/start contract. Omission keeps
// the original combined authorization path and its canonical request unchanged.
func (s Spec) SeparateStart() bool { return s.ExecutionControl == "prepare_then_start" }

// Deadline is an immutable UTC bound on setup and payload execution. Parse validates
// its canonical whole-second spelling; omission preserves relative-only jobs.
func (s Spec) Deadline() time.Time {
	at, _ := time.Parse(time.RFC3339, s.Timeouts.ExecutionDeadline)
	return at
}

func (s Spec) Expired(now time.Time) bool {
	at := s.Deadline()
	return !at.IsZero() && !now.Before(at)
}

// CollectionMode interprets omission without changing accepted canonical bytes.
func (s Spec) CollectionMode() string {
	if s.ResultCollection == "" {
		return "automatic"
	}
	return s.ResultCollection
}

// Request is constructed only after strict schema and semantic validation. Keep bytes
// private and return copies; caller mutations cannot change its request hash.
type Request struct {
	canonical string
	hash      string
}

func (r Request) Canonical() []byte         { return []byte(r.canonical) }
func (r Request) Hash() domain.SHA256Digest { return domain.SHA256Digest(r.hash) }
func (r Request) Valid() bool               { return r.canonical != "" && r.Hash().Valid() }
func (r Request) Spec() Spec                { var s Spec; _ = json.Unmarshal([]byte(r.canonical), &s); return s }
func (Request) String() string              { return "[job request redacted]" }
func (Request) GoString() string            { return "[job request redacted]" }

var schemaOnce sync.Once
var jobSchema *jsonschema.Schema
var schemaError error

type closedLoader struct{}

func (closedLoader) Load(string) (any, error) {
	return nil, errors.New("external schema loading disabled")
}
func compileSchema() {
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	c.AssertFormat()
	c.UseLoader(closedLoader{})
	for _, name := range []string{"common.v1alpha1.schema.json", "job-spec.v1alpha1.schema.json"} {
		b, err := schemas.Files.ReadFile(name)
		if err != nil {
			schemaError = ErrUnavailable
			return
		}
		var v any
		if json.Unmarshal(b, &v) != nil {
			schemaError = ErrUnavailable
			return
		}
		if c.AddResource("https://contracts.compute-relay.invalid/"+name, v) != nil {
			schemaError = ErrUnavailable
			return
		}
	}
	jobSchema, schemaError = c.Compile("https://contracts.compute-relay.invalid/job-spec.v1alpha1.schema.json")
}

func Parse(raw []byte) (Request, error) {
	b, v, err := canonicalJSON(raw)
	if err != nil {
		return Request{}, err
	}
	schemaOnce.Do(compileSchema)
	if schemaError != nil {
		return Request{}, ErrUnavailable
	}
	if jobSchema.Validate(v) != nil {
		return Request{}, ErrSpec
	}
	var spec Spec
	if json.Unmarshal(b, &spec) != nil {
		return Request{}, ErrSpec
	}
	if err := semantic(spec); err != nil {
		return Request{}, err
	}
	return Request{canonical: string(b), hash: digest(append([]byte(CanonicalVersion+"\x00"), b...))}, nil
}

func semantic(s Spec) error {
	if !validObjectExpectation(s.Bundle.SHA256, s.Bundle.Bytes) {
		return ErrSpec
	}
	if image := s.ApplicationImage; image != nil && (!image.ObjectID.Valid() || !image.SHA256.Valid() || image.Bytes < 1 || image.Bytes > 2<<30) {
		return ErrSpec
	}
	if !workloadsecret.ValidSlots(s.WorkloadSecrets) || len(s.WorkloadSecrets) > 0 && !s.SeparateStart() {
		return ErrSpec
	}
	if raw := s.Timeouts.ExecutionDeadline; raw != "" {
		at := s.Deadline()
		if at.Year() < 2 || at.UTC().Format(time.RFC3339) != raw {
			return ErrSpec
		}
	}
	if strings.TrimSpace(s.Name) == "" || s.Timeouts.SetupSeconds+s.Timeouts.FinalizationGraceSeconds >= s.Timeouts.RemoteWallSeconds {
		return ErrSpec
	}
	for k, v := range s.Execution.Environment {
		u := strings.ToUpper(k)
		if strings.HasPrefix(u, "CC_") || strings.HasPrefix(u, "LD_") || strings.HasPrefix(u, "DYLD_") || strings.ContainsRune(v, 0) {
			return ErrSpec
		}
		switch u {
		case "PATH", "HOME", "PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV", "KAGGLE_API_TOKEN", "KAGGLE_KEY", "KAGGLE_USERNAME":
			return ErrSpec
		}
	}
	for _, arg := range s.Execution.Command {
		if strings.ContainsRune(arg, 0) {
			return ErrSpec
		}
	}
	for _, cmd := range s.Execution.Dependencies.ShellSetup {
		for _, arg := range cmd {
			if strings.ContainsRune(arg, 0) {
				return ErrSpec
			}
		}
	}
	names := map[string]bool{}
	targets := []string{}
	for _, in := range s.Inputs {
		if in.Source.Kind == "object" && !validObjectExpectation(in.Source.SHA256, in.Source.Bytes) {
			return ErrSpec
		}
		name := strings.ToLower(in.Name)
		if names[name] {
			return ErrSpec
		}
		names[name] = true
		if overlaps(targets, in.Target) {
			return ErrSpec
		}
		targets = append(targets, in.Target)
	}
	targets = nil
	for _, out := range s.Outputs {
		if overlaps(targets, out.Path) {
			return ErrSpec
		}
		targets = append(targets, out.Path)
	}
	return nil
}

func validObjectExpectation(sha domain.SHA256Digest, size *int64) bool {
	if (sha == "") != (size == nil) {
		return false
	}
	return size == nil || sha.Valid() && *size >= 0
}
func overlaps(paths []string, next string) bool {
	n := strings.ToLower(next)
	for _, p := range paths {
		p = strings.ToLower(p)
		if p == n || strings.HasPrefix(p, n+"/") || strings.HasPrefix(n, p+"/") {
			return true
		}
	}
	return false
}

// CheckInputs applies the current admission policy without altering historical requests.
func (s Spec) CheckInputs() error {
	for _, input := range s.Inputs {
		if input.Source.Kind != "object" {
			return ErrUnsupportedInput
		}
	}
	return nil
}
