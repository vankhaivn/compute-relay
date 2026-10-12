package runnerplan

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

const spec = `{"api_version":"compute-connector/v1alpha1","name":"PRIVATE_LABEL","profile":"profile","bundle":{"object_id":"code"},"execution":{"kind":"python","command":["python","main.py"]},"inputs":[{"name":"input","target":"data.txt","source":{"kind":"object","object_id":"input"}}],"outputs":[{"path":"answer.txt","required":true}],"resources":{"accelerator":"cpu"},"network":{"remote_internet":"disabled"},"timeouts":{"remote_wall_seconds":10,"setup_seconds":5,"finalization_grace_seconds":2}}`

func plan(t *testing.T) provider.Plan {
	t.Helper()
	snapshot := provider.InputSnapshot{Bundle: domain.ObjectMetadata{ID: "code", WorkspaceID: "workspace", Bytes: 4, SHA256: provider.Digest([]byte("code"))},
		Inputs: []provider.FrozenInput{{Name: "input", Target: "data.txt", Object: domain.ObjectMetadata{ID: "input", WorkspaceID: "workspace", Bytes: 3, SHA256: provider.Digest([]byte("abc"))}}}}
	job := provider.ResolvedJob{Identity: provider.Identity{InstallationID: "installation", WorkspaceID: "workspace", JobID: "job", AttemptID: "attempt",
		InstanceID: "instance", IntentID: "intent", ResourceKey: "resource", Nonce: strings.Repeat("a", 32), BundleSHA256: snapshot.Bundle.SHA256,
		InputManifestSHA256: snapshot.InputDigest()}, Binding: domain.ProviderBinding{Profile: "profile", ProviderInstanceID: "instance", ConfigurationRevision: "r1"},
		Specification: []byte(spec), SpecificationSHA256: provider.Digest([]byte(spec)), Required: []domain.CapabilityName{domain.CapabilityBatchExecution},
		WallSeconds: 10, Inputs: &snapshot}
	if err := job.Validate(); err != nil {
		t.Fatal(err)
	}
	return provider.Plan{Job: job}
}

func TestBuildCopiesOnlyExecutionFieldsAndStagedPaths(t *testing.T) {
	manifest, parsed, err := Build(plan(t))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(manifest)
	text := string(raw)
	for _, want := range []string{`"manifest_version":"compute-relay/runner/v1"`, `"path":"code.bin"`, `"path":"input-000.bin"`,
		`"attempt_nonce":"` + strings.Repeat("a", 32) + `"`, `"remote_wall_seconds":10`} {
		if !strings.Contains(text, want) {
			t.Errorf("manifest lacks %s: %s", want, text)
		}
	}
	for _, secret := range []string{"PRIVATE_LABEL", "profile", "object_id"} {
		if strings.Contains(text, secret) {
			t.Errorf("manifest copies %q: %s", secret, text)
		}
	}
	if parsed.Resources.Accelerator != "cpu" || parsed.Network.RemoteInternet != "disabled" {
		t.Fatalf("spec = %+v", parsed)
	}
}

func TestBuildRejectsPlansThatDisagreeWithTheirSpecification(t *testing.T) {
	for name, mutate := range map[string]func(*provider.Plan){
		"no inputs": func(p *provider.Plan) { p.Job.Inputs = nil },
		"wall":      func(p *provider.Plan) { p.Job.WallSeconds = 11 },
		"profile":   func(p *provider.Plan) { p.Job.Binding.Profile = "other" },
		"bundle": func(p *provider.Plan) {
			snapshot := p.Job.Inputs.Clone()
			snapshot.Bundle.ID = "other"
			p.Job.Inputs = &snapshot
		},
		"input object": func(p *provider.Plan) {
			snapshot := p.Job.Inputs.Clone()
			snapshot.Inputs[0].Object.ID = "other"
			p.Job.Inputs = &snapshot
		},
		"input target": func(p *provider.Plan) {
			snapshot := p.Job.Inputs.Clone()
			snapshot.Inputs[0].Target = "x.txt"
			p.Job.Inputs = &snapshot
		},
		"invalid spec": func(p *provider.Plan) { p.Job.Specification = []byte(`{}`) },
		"missing input": func(p *provider.Plan) {
			snapshot := p.Job.Inputs.Clone()
			snapshot.Inputs = nil
			p.Job.Inputs = &snapshot
		},
	} {
		p := plan(t)
		mutate(&p)
		if _, _, err := Build(p); !errors.Is(err, ErrPlan) {
			t.Errorf("%s: %v", name, err)
		}
	}
}
