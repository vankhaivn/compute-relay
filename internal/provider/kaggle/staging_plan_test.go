package kaggle

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func stagingFixture(t *testing.T) (Config, provider.Plan) {
	t.Helper()
	c := testConfig(t)
	data := []byte("synthetic-code")
	snapshot := provider.InputSnapshot{Bundle: domain.ObjectMetadata{ID: "code", WorkspaceID: "workspace", Bytes: int64(len(data)), SHA256: provider.Digest(data)}, Inputs: []provider.FrozenInput{
		{Name: "input", Target: "data.txt", Object: domain.ObjectMetadata{ID: "input", WorkspaceID: "workspace", Bytes: 3, SHA256: provider.Digest([]byte("abc"))}},
	}}
	spec := []byte(`{"name":"PRIVATE_JOB_LABEL","environment":{"TOKEN":"NEVER_STAGE_THIS_FIELD"}}`)
	job := provider.ResolvedJob{Identity: provider.Identity{InstallationID: "installation", WorkspaceID: "workspace", JobID: "job", AttemptID: "attempt", InstanceID: domain.ProviderInstanceID(c.InstanceID), IntentID: "intent", ResourceKey: "resource", Nonce: strings.Repeat("a", 32), BundleSHA256: snapshot.Bundle.SHA256, InputManifestSHA256: snapshot.InputDigest()}, Binding: domain.ProviderBinding{Profile: "profile", ProviderInstanceID: domain.ProviderInstanceID(c.InstanceID), ConfigurationRevision: c.Revision}, Specification: spec, SpecificationSHA256: provider.Digest(spec), Required: []domain.CapabilityName{domain.CapabilityBatchExecution}, WallSeconds: 10, Inputs: &snapshot}
	if err := job.Validate(); err != nil {
		t.Fatal(err)
	}
	return c, provider.Plan{Job: job}
}

func TestStagingPlanPinsIdentityAndExcludesSecrets(t *testing.T) {
	c, p := stagingFixture(t)
	one, err := buildStagingPlan(c, DefaultStagingPolicy(), p, "prep")
	if err != nil {
		t.Fatal(err)
	}
	two, err := buildStagingPlan(c, DefaultStagingPolicy(), p.Clone(), "prep")
	if err != nil || !bytes.Equal(one.marker, two.marker) || one.request.Slug != two.request.Slug {
		t.Fatal("unstable staging", err)
	}
	if len(one.request.Slug) != 44 || !strings.HasPrefix(one.request.Slug, "crs-") || len(one.request.Files) != 3 || len(one.objects) != 2 {
		t.Fatal("invalid catalog")
	}
	encoded, _ := json.Marshal(one.request)
	for _, secret := range []string{"PRIVATE_JOB_LABEL", "NEVER_STAGE_THIS_FIELD", string(c.CredentialRef), c.PythonExecutable} {
		if bytes.Contains(append(append([]byte{}, one.marker...), encoded...), []byte(secret)) {
			t.Fatal("private configuration copied")
		}
	}
	if one.request.MarkerSHA256 != provider.Digest(one.marker) || one.request.Files[0].SHA256 != one.request.MarkerSHA256 {
		t.Fatal("marker not bound")
	}
	// Same intent with changed content conflicts at the remote marker; it must not
	// silently choose another dataset and orphan the first one.
	changed := p.Clone()
	changed.Job.Specification = []byte(`{"changed":true}`)
	changed.Job.SpecificationSHA256 = provider.Digest(changed.Job.Specification)
	other, err := buildStagingPlan(c, DefaultStagingPolicy(), changed, "prep")
	if err != nil || other.request.Slug != one.request.Slug || other.request.MarkerSHA256 == one.request.MarkerSHA256 {
		t.Fatal("changed plan lost intent identity", err)
	}
	changed.Job.Identity.AttemptID = "another_attempt"
	other, err = buildStagingPlan(c, DefaultStagingPolicy(), changed, "prep")
	if err != nil || other.request.Slug == one.request.Slug {
		t.Fatal("attempts share a dataset", err)
	}
}

func TestStagingPlanRejectsIncompleteOrRemappedInputs(t *testing.T) {
	for _, mode := range []string{"no-inputs", "workspace", "digest", "revision", "license", "limit"} {
		t.Run(mode, func(t *testing.T) {
			c, p := stagingFixture(t)
			policy := DefaultStagingPolicy()
			switch mode {
			case "no-inputs":
				p.Job.Inputs = nil
			case "workspace":
				p.Job.Inputs.Bundle.WorkspaceID = "foreign"
			case "digest":
				p.Job.Identity.InputManifestSHA256 = provider.Digest(nil)
			case "revision":
				c.Revision = "remapped"
			case "license":
				policy.License = "CC0-1.0"
			case "limit":
				policy.MaxBytes = 1
			}
			if _, err := buildStagingPlan(c, policy, p, "prep"); err == nil {
				t.Fatal("unsafe plan accepted")
			}
		})
	}
}
