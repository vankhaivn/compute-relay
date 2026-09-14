// Package admission owns durable local job acceptance, not dispatch or input fetching.
package admission

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/vankhaivn/compute-relay/api/schemas"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/httpsinput"
)

var (
	ErrSpec         = errors.New("invalid job specification")
	ErrKey          = errors.New("invalid idempotency key")
	ErrConflict     = errors.New("idempotency key was used with a different request")
	ErrNotFound     = errors.New("job or admission not found")
	ErrInputs       = errors.New("referenced input is missing or not visible")
	ErrRequirements = errors.New("job exceeds the resolved profile policy")
	ErrLimit        = errors.New("local outstanding job limit reached")
	ErrUnavailable  = errors.New("durable admission unavailable")
)

type Source struct {
	Kind           string              `json:"kind"`
	ObjectID       domain.ObjectID     `json:"object_id,omitempty"`
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
type Spec struct {
	APIVersion string            `json:"api_version"`
	Name       string            `json:"name"`
	Profile    string            `json:"profile"`
	Labels     map[string]string `json:"labels,omitempty"`
	Bundle     struct {
		ObjectID domain.ObjectID `json:"object_id"`
	} `json:"bundle"`
	Execution struct {
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
		RemoteWallSeconds        int64 `json:"remote_wall_seconds"`
		SetupSeconds             int64 `json:"setup_seconds"`
		FinalizationGraceSeconds int64 `json:"finalization_grace_seconds"`
	} `json:"timeouts"`
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
		name := strings.ToLower(in.Name)
		if names[name] {
			return ErrSpec
		}
		names[name] = true
		if overlaps(targets, in.Target) {
			return ErrSpec
		}
		targets = append(targets, in.Target)
		if in.Source.Kind == "https" {
			if (httpsinput.Request{URL: in.Source.URL, SHA256: string(in.Source.ExpectedSHA256)}).Validate() != nil {
				return ErrSpec
			}
		}
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
