package runnerplan

import (
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/vankhaivn/compute-relay/api/schemas"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

const MaxManifestBytes = 1 << 20

var ErrResult = errors.New("invalid execution result manifest")

type Result struct {
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

func (closedLoader) Load(string) (any, error) { return nil, ErrResult }
func compileManifestSchema() {
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	c.AssertFormat()
	c.UseLoader(closedLoader{})
	for _, name := range []string{"common.v1alpha1.schema.json", "result-manifest.v1alpha1.schema.json"} {
		raw, err := schemas.Files.ReadFile(name)
		if err != nil {
			schemaErr = ErrResult
			return
		}
		var v any
		if json.Unmarshal(raw, &v) != nil {
			schemaErr = ErrResult
			return
		}
		if c.AddResource("https://contracts.compute-relay.invalid/"+name, v) != nil {
			schemaErr = ErrResult
			return
		}
	}
	resultSchema, schemaErr = c.Compile("https://contracts.compute-relay.invalid/result-manifest.v1alpha1.schema.json")
}

// ParseResult validates the bounded result wire contract before any output is trusted.
// Frozen output declarations and measured catalog checks remain the collector's responsibility.
func ParseResult(expected provider.Identity, raw []byte) (Result, error) {
	var m Result
	value, err := strictValue(raw)
	if err != nil {
		return m, ErrResult
	}
	schemaOnce.Do(compileManifestSchema)
	if schemaErr != nil || resultSchema.Validate(value) != nil {
		return m, ErrResult
	}
	if err := provider.ValidateManifestIdentity(expected, raw); err != nil {
		return m, err
	}
	if json.Unmarshal(raw, &m) != nil {
		return m, ErrResult
	}
	if m.Finished.Before(m.Started) || m.TimedOut != (m.Phase == "timed_out") ||
		m.Phase != "completed" && (len(m.Error) == 0 || string(m.Error) == "null") {
		return Result{}, ErrResult
	}
	paths := make([]string, 0, len(m.Artifacts))
	for _, artifact := range m.Artifacts {
		if !provider.SafeArtifactPath(artifact.Path) {
			return Result{}, ErrResult
		}
		for _, c := range artifact.Path {
			if c < 32 || c > 126 {
				return Result{}, ErrResult
			}
		}
		paths = append(paths, strings.ToLower(artifact.Path))
	}
	sort.Strings(paths)
	seen := make(map[string]bool, len(paths))
	for _, path := range paths {
		if seen[path] {
			return Result{}, ErrResult
		}
		parts := strings.Split(path, "/")
		for i := 1; i < len(parts); i++ {
			if seen[strings.Join(parts[:i], "/")] {
				return Result{}, ErrResult
			}
		}
		seen[path] = true
	}
	return m, nil
}
