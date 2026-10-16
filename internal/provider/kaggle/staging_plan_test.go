package kaggle

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle/kaggleapi"
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
	for _, secret := range []string{"PRIVATE_JOB_LABEL", "NEVER_STAGE_THIS_FIELD", string(c.CredentialRef)} {
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

func TestDefaultStagingBudgetFitsLimitOnOrdinaryUplink(t *testing.T) {
	policy := DefaultStagingPolicy()
	upload := time.Duration(policy.MaxBytes*8/10_000_000) * time.Second // 10 Mbit/s
	if !policy.valid() || policy.Timeout < upload {
		t.Fatalf("staging budget %s cannot upload the %d-byte limit in %s", policy.Timeout, policy.MaxBytes, upload)
	}
	if dispatch.DefaultConfig().PreparationTimeout < policy.Timeout {
		t.Fatal("dispatch preparation budget cuts staging before its own policy timeout")
	}
}

// Worst case: 128-byte identities, 64-byte names and 1024-byte targets whose characters all
// expand six times under JSON escaping.
func TestStagingPlanFitsMaximalInputMarker(t *testing.T) {
	maximal := func(t *testing.T, count int) (Config, provider.Plan, domain.OperationID) {
		c, p := stagingFixture(t)
		long := strings.Repeat("i", 128)
		job := &p.Job
		job.Identity.InstallationID, job.Identity.WorkspaceID, job.Identity.JobID, job.Identity.AttemptID = domain.RuntimeInstallationID(long), domain.WorkspaceID(long), domain.JobID(long), domain.AttemptID(long)
		job.Identity.IntentID, job.Identity.ResourceKey, job.Identity.Nonce = domain.SubmissionIntentID(long), long, long
		snapshot := job.Inputs.Clone()
		snapshot.Bundle.WorkspaceID = job.Identity.WorkspaceID
		snapshot.Inputs = nil
		for i := 0; i < count; i++ {
			snapshot.Inputs = append(snapshot.Inputs, provider.FrozenInput{
				Name: fmt.Sprintf("n%03d", i) + strings.Repeat("x", 60), Target: fmt.Sprintf("%03d", i) + strings.Repeat("<", 1021),
				Object: domain.ObjectMetadata{ID: "input", WorkspaceID: job.Identity.WorkspaceID, Bytes: 0, SHA256: provider.Digest(nil)}})
		}
		job.Inputs, job.Identity.InputManifestSHA256 = &snapshot, snapshot.InputDigest()
		return c, p, domain.OperationID(long)
	}
	c, p, operation := maximal(t, 256)
	if err := p.Job.Validate(); err != nil {
		t.Fatal(err)
	}
	stage, err := buildStagingPlan(c, DefaultStagingPolicy(), p, operation)
	if err != nil {
		t.Fatal("maximal 256-input plan refused", err)
	}
	if len(stage.marker) > maxMarkerBytes || len(stage.marker) < 3*maxMarkerBytes/4 || len(stage.request.Files) != kaggleapi.MaxStagingFiles {
		t.Fatalf("marker %d bytes, %d files", len(stage.marker), len(stage.request.Files))
	}
	if maxStagingBytes != kaggleapi.MaxStagingBytes {
		t.Fatal("staging byte bounds disagree")
	}
	c, p, operation = maximal(t, 257)
	if _, err := buildStagingPlan(c, DefaultStagingPolicy(), p, operation); err == nil {
		t.Fatal("257-input plan accepted")
	}
}
