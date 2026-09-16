package kaggle

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

type acceptanceTestClock struct{}

func (acceptanceTestClock) Now() time.Time { return time.Now().UTC() }

func acceptanceAdapterFixture(t *testing.T, allow bool) (*AcceptanceAdapter, *provider.Plan, *provider.Prepared, *int) {
	t.Helper()
	fixture := newExecutionFixture(t)
	plan := fixture.plan.Clone()
	plan.Job.Specification = []byte(strings.ReplaceAll(strings.ReplaceAll(executionSpec, `"accelerator":"cpu"`, `"accelerator":"gpu","minimum_gpu_count":1`), `"remote_wall_seconds":10`, `"remote_wall_seconds":120`))
	plan.Job.SpecificationSHA256 = provider.Digest(plan.Job.Specification)
	plan.Job.WallSeconds = 120
	plan.Job.Required = []domain.CapabilityName{domain.CapabilityBatchExecution, domain.CapabilityGPU, domain.CapabilityPython}
	plan.VerifyAfterStart = []domain.CapabilityName{domain.CapabilityGPU}
	c := fixture.stager.config
	stage, err := buildStagingPlan(c, fixture.stager.policy, plan, "prep")
	if err != nil {
		t.Fatal(err)
	}
	ref, _ := json.Marshal(stagingReference{"123", c.AccountName + "/" + stage.request.Slug, 1, stage.request.MarkerSHA256})
	prepared := provider.Prepared{Identity: plan.Job.Identity, PreparationID: "prep", PlanSHA256: plan.Digest(), Resource: string(ref), Ready: true, Private: true}
	scope := AcceptanceScope{Binding: provider.BindingSnapshot{Binding: plan.Job.Binding, AccountScope: c.AccountName, CredentialRef: string(c.CredentialRef)}, WorkspaceID: plan.Job.Identity.WorkspaceID, JobID: plan.Job.Identity.JobID, AttemptID: plan.Job.Identity.AttemptID, SpecificationSHA256: plan.Job.SpecificationSHA256}
	p, err := NewAcceptanceAdapter(c, scope, fixture.stager.credentials, executionUnreadBlobs{}, acceptanceTestClock{}, func(context.Context) (provider.Plan, provider.Prepared, error) {
		return plan.Clone(), prepared, nil
	}, "NvidiaTeslaT4", allow)
	if err != nil {
		t.Fatal(err)
	}
	p.stager.local, p.stager.run = fixture.stager.local, fixture.stager.run
	p.preflight.run = func(_ context.Context, _ Config, mode Mode, _ []byte) (Report, error) {
		if mode == Local {
			return baseline(Local), nil
		}
		return verified(), nil
	}
	saves := new(int)
	p.makeExecutor = func(stage *Stager, policy ExecutionPolicy, original provider.Plan, prior provider.Prepared, permit bool) (*Executor, error) {
		e, err := NewExecutor(stage, policy, original, prior, permit)
		if err != nil {
			return nil, err
		}
		e.run = func(_ context.Context, _ Config, mode string, _ []byte, request executionRequest) (executionResponse, error) {
			if mode == "submit" {
				*saves++
			} else if request.Source != "" {
				t.Error("recovery retransmitted private source")
			}
			return executionFound(request, "COMPLETE"), nil
		}
		return e, nil
	}
	return p, &plan, &prepared, saves
}

func TestAcceptanceAdapterBindsOneAttemptWithoutLiveCapabilityInflation(t *testing.T) {
	p, plan, prepared, saves := acceptanceAdapterFixture(t, true)
	registry := provider.NewSnapshotRegistry()
	if err := registry.Register(p.scope.Binding, p); err != nil {
		t.Fatal("real component composition failed registration", err)
	}
	for _, capability := range p.Describe().Capabilities {
		if capability.Evidence != domain.EvidenceImplementedOffline || capability.AccountChecked {
			t.Fatal("component advertised live evidence")
		}
	}
	if p.Describe().Support(domain.CapabilityGPU) != domain.CapabilitySupportUnknown {
		t.Fatal("GPU execution is not known before the probe")
	}
	if err := p.VerifyBinding(context.Background(), p.scope.Binding); err != nil {
		t.Fatal(err)
	}
	out := p.Submit(context.Background(), *prepared)
	if out.Status != provider.SubmissionAccepted || *saves != 1 {
		t.Fatal("new scoped submission failed", out)
	}
	if p.Submit(context.Background(), *prepared).Status != provider.SubmissionUnknown || *saves != 1 {
		t.Fatal("same instance repeated submission")
	}
	if _, err := p.ReconcileSubmission(context.Background(), plan.Job.Identity); err != nil || *saves != 1 {
		t.Fatal("reconciliation repeated submission", err)
	}
	if result, err := p.Cleanup(context.Background(), provider.CleanupRequest{}); err != ErrAcceptanceCleanup || result != (provider.CleanupOutcome{}) {
		t.Fatal("cleanup was represented as successful")
	}
}

func TestAcceptanceReadOnlyCompositionCannotCreateOrSubmit(t *testing.T) {
	p, plan, prepared, saves := acceptanceAdapterFixture(t, false)
	if _, err := p.Prepare(context.Background(), *plan, prepared.PreparationID); err == nil {
		t.Fatal("read-only mode created staging")
	}
	if p.Submit(context.Background(), *prepared).Status != provider.SubmissionUnknown || *saves != 0 {
		t.Fatal("read-only mode submitted compute")
	}
	found, err := p.ReconcileSubmission(context.Background(), plan.Job.Identity)
	if err != nil || found.Status != provider.ReconciliationFound || *saves != 0 {
		t.Fatal("read-only recovery failed", err)
	}
	if _, err := p.Observe(context.Background(), *found.Remote); err != nil || *saves != 0 {
		t.Fatal("read-only observation failed", err)
	}
}

func TestAcceptanceAdapterRetainsCatalogAndRejectsChangedOriginalEvidence(t *testing.T) {
	p, plan, prepared, _ := acceptanceAdapterFixture(t, false)
	data := map[string][]byte{artifactManifest: []byte(`{"fixture":true}`), "outputs/answer.txt": []byte("yes")}
	catalogs := 0
	p.makeArtifacts = func(e *Executor, policy ArtifactPolicy) (*ArtifactReader, error) {
		a, err := NewArtifactReader(e, policy)
		if err != nil {
			return nil, err
		}
		a.run = func(_ context.Context, _ Config, mode string, _ []byte, request artifactRequest, dst io.Writer) ([]byte, error) {
			if mode == "catalog" {
				catalogs++
				return artifactCatalog(data), nil
			}
			_, err := dst.Write(data[request.Target.Path])
			return nil, err
		}
		return a, nil
	}
	found, err := p.ReconcileSubmission(context.Background(), plan.Job.Identity)
	if err != nil {
		t.Fatal(err)
	}
	first, err := p.ListArtifacts(context.Background(), *found.Remote, provider.PageRequest{Limit: 1})
	if err != nil || first.NextCursor == "" {
		t.Fatal(err)
	}
	second, err := p.ListArtifacts(context.Background(), *found.Remote, provider.PageRequest{Cursor: first.NextCursor, Limit: 1})
	if err != nil || second.NextCursor != "" || catalogs != 1 {
		t.Fatal("adapter reconstructed a reader between pages", err, catalogs)
	}
	var dst bytes.Buffer
	file := second.Artifacts[0]
	if _, err := p.FetchArtifact(context.Background(), *found.Remote, file, &dst, file.Bytes); err != nil || dst.String() != "yes" {
		t.Fatal(err)
	}
	prepared.Resource = "changed"
	if _, err := p.ListArtifacts(context.Background(), *found.Remote, provider.PageRequest{Limit: 1}); err != ErrAcceptanceScope || catalogs != 1 {
		t.Fatal("replaced the original prepared resource", err)
	}
	foreign := *found.Remote
	foreign.Identity.WorkspaceID = "foreign"
	p.load = func(context.Context) (provider.Plan, provider.Prepared, error) {
		t.Fatal("foreign reference triggered journal lookup")
		return provider.Plan{}, provider.Prepared{}, nil
	}
	if _, err := p.Observe(context.Background(), foreign); err != ErrAcceptanceScope {
		t.Fatal(err)
	}
}
