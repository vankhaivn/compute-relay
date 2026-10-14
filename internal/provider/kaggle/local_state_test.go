package kaggle

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/worker"
)

type localStoreBusy struct{}

func (localStoreBusy) Error() string   { return "private store diagnostic" }
func (localStoreBusy) Temporary() bool { return true }

func localStoreFailures() []error {
	return []error{localStoreBusy{}, context.DeadlineExceeded, errors.New("private invariant diagnostic")}
}

func localCleanupRequest(e *Executor) provider.CleanupResourceRequest {
	return provider.CleanupResourceRequest{CleanupRequest: provider.CleanupRequest{
		Remote:   provider.RemoteReference{Identity: e.plan.Job.Identity, Resource: e.prepared.Resource, Version: "prepared"},
		LedgerID: "res_fixture", CreationOperationID: e.prepared.PreparationID, Mode: provider.CleanupApply},
		Purpose: "staging", OwnershipOperationID: e.prepared.PreparationID, PlanSHA256: e.plan.Digest()}
}

func assertLocalStoreFailure(t *testing.T, got, cause error) {
	t.Helper()
	if !provider.IsLocalStateError(got) || !errors.Is(got, cause) || worker.Classify(got) != worker.Classify(cause) {
		t.Fatal("local failure became provider evidence", got)
	}
	if strings.Contains(got.Error(), "private") {
		t.Fatal("local diagnostic escaped the safe wrapper")
	}
}

func TestRuntimeLoaderFailuresPreserveLocalOrigin(t *testing.T) {
	for _, cause := range localStoreFailures() {
		t.Run(cause.Error(), func(t *testing.T) {
			e := newExecutionFixture(t)
			p := &RuntimeAdapter{config: e.stager.config, binding: provider.BindingSnapshot{Binding: e.plan.Job.Binding}, stager: e.stager,
				load: func(context.Context, provider.Identity) (provider.Plan, provider.Prepared, error) {
					return provider.Plan{}, provider.Prepared{}, cause
				}, runCleanup: func(context.Context, Config, string, []byte, cleanupRequest) (cleanupResponse, error) {
					t.Fatal("failed local load reached cleanup transport")
					return cleanupResponse{}, nil
				}}
			remote, err := e.remote(executionFound(e.request, "COMPLETE"))
			if err != nil {
				t.Fatal(err)
			}
			calls := map[string]func() error{
				"reconcile": func() error { _, err := p.Reconcile(t.Context(), e.plan.Job.Identity); return err },
				"observe":   func() error { _, err := p.Observe(t.Context(), remote); return err },
				"catalog":   func() error { _, err := p.ListArtifacts(t.Context(), remote); return err },
				"fetch": func() error {
					_, err := p.Fetch(t.Context(), remote, provider.Artifact{}, io.Discard, 0, nil)
					return err
				},
				"logs": func() error { _, err := p.Logs(t.Context(), remote, provider.PageRequest{Limit: 1}); return err },
				"cleanup": func() error {
					_, err := p.CleanupResource(t.Context(), localCleanupRequest(e))
					return err
				},
			}
			for name, call := range calls {
				t.Run(name, func(t *testing.T) { assertLocalStoreFailure(t, call(), cause) })
			}
			// Start has no bare error channel after durable submission intent. Keep
			// its existing unknown outcome; later observation carries the local cause.
			out := p.Start(t.Context(), e.prepared, nil, nil)
			if out.Status != provider.SubmissionUnknown || out.Validate(e.plan.Job.Identity) != nil {
				t.Fatal("local state unavailability invented a submission refusal", out)
			}
		})
	}
}

func TestCredentialLocalFailuresSurviveKaggleSanitizers(t *testing.T) {
	for _, cause := range localStoreFailures() {
		t.Run(cause.Error(), func(t *testing.T) {
			resolver := stagingCredentialFunc(func(context.Context, ports.CredentialRef, func([]byte) error) error {
				return provider.LocalStateError{Err: cause}
			})
			a, remote, data := artifactFixture(t)
			e := a.executor
			e.stager.credentials = resolver
			preflight, err := New(e.stager.config, resolver)
			if err != nil {
				t.Fatal(err)
			}
			monitor := monitorFixture(t)
			monitor.config, monitor.credentials = e.stager.config, resolver
			p := &RuntimeAdapter{binding: provider.BindingSnapshot{Binding: e.plan.Job.Binding}, preflight: preflight}
			calls := map[string]func() error{
				"verify": func() error { return p.Verify(t.Context(), p.binding) },
				"staging": func() error {
					_, err := e.stager.ReconcilePrepare(t.Context(), e.plan, e.prepared.PreparationID)
					return err
				},
				"reconcile": func() error { _, err := e.Reconcile(t.Context(), e.plan.Job.Identity); return err },
				"observe":   func() error { _, err := e.Observe(t.Context(), remote); return err },
				"quota":     func() error { _, err := monitor.ReadQuota(t.Context()); return err },
				"catalog":   func() error { _, err := a.ListArtifacts(t.Context(), remote); return err },
				"fetch": func() error {
					file := artifactFor(remote, "outputs/answer.txt", data["outputs/answer.txt"])
					_, err := a.Fetch(t.Context(), remote, file, io.Discard, file.Bytes, nil)
					return err
				},
				"cleanup": func() error {
					p := &RuntimeAdapter{config: e.stager.config, binding: provider.BindingSnapshot{Binding: e.plan.Job.Binding}, stager: e.stager,
						load: func(context.Context, provider.Identity) (provider.Plan, provider.Prepared, error) {
							return e.plan, e.prepared, nil
						},
						runCleanup: func(context.Context, Config, string, []byte, cleanupRequest) (cleanupResponse, error) {
							t.Fatal("failed credential slot read reached cleanup transport")
							return cleanupResponse{}, nil
						}}
					_, err := p.CleanupResource(t.Context(), localCleanupRequest(e))
					return err
				},
			}
			for name, call := range calls {
				t.Run(name, func(t *testing.T) { assertLocalStoreFailure(t, call(), cause) })
			}
		})
	}
}

func TestStartLocalCredentialFailureStaysUnknownAndNeverResubmits(t *testing.T) {
	for _, afterStaging := range []bool{false, true} {
		t.Run(map[bool]string{false: "staging-read", true: "submission-read"}[afterStaging], func(t *testing.T) {
			e := newExecutionFixture(t)
			reads, submits := 0, 0
			cause := provider.LocalStateError{Err: localStoreBusy{}}
			e.stager.credentials = stagingCredentialFunc(func(_ context.Context, _ ports.CredentialRef, use func([]byte) error) error {
				reads++
				if afterStaging && reads == 1 {
					return use([]byte("SYNTHETIC_TOKEN"))
				}
				return cause
			})
			e.run = func(_ context.Context, _ Config, mode string, _ []byte, r executionRequest) (executionResponse, error) {
				if mode == "submit" {
					submits++
				}
				return executionFound(r, "RUNNING"), nil
			}
			out := e.Start(t.Context(), e.prepared, nil, nil)
			if out.Status != provider.SubmissionUnknown || out.Validate(e.plan.Job.Identity) != nil || submits != 0 {
				t.Fatal("unavailable local state invented rejection or submitted", out)
			}
			e.stager.credentials = stagingCredentialFunc(func(_ context.Context, _ ports.CredentialRef, use func([]byte) error) error {
				return use([]byte("SYNTHETIC_TOKEN"))
			})
			if out := e.Start(t.Context(), e.prepared, nil, nil); out.Status != provider.SubmissionUnknown || submits != 0 {
				t.Fatal("local recovery rearmed Start", out)
			}
			seen, err := e.Reconcile(t.Context(), e.plan.Job.Identity)
			if err != nil || seen.Status != provider.ReconciliationFound || submits != 0 {
				t.Fatal("local recovery did not remain observation-only", seen, err)
			}
		})
	}
}

func TestRuntimeQuotaLocalFailureIsNotAResourceRefusal(t *testing.T) {
	c, plan, _ := executionFixture(t)
	plan.Job.Specification = []byte(strings.Replace(executionSpec, `"accelerator":"cpu"`, `"accelerator":"gpu","minimum_gpu_count":1`, 1))
	plan.Job.SpecificationSHA256 = provider.Digest(plan.Job.Specification)
	plan.Job.Required = append(plan.Job.Required, domain.CapabilityGPU)
	monitor := monitorFixture(t)
	monitor.credentials = stagingCredentialFunc(func(context.Context, ports.CredentialRef, func([]byte) error) error {
		return provider.LocalStateError{Err: context.DeadlineExceeded}
	})
	policy := DefaultExecutionPolicy()
	policy.MachineShape = "NvidiaTeslaT4"
	p := &RuntimeAdapter{config: c, binding: provider.BindingSnapshot{Binding: plan.Job.Binding}, monitor: monitor, policy: policy}
	_, err := p.Validate(t.Context(), plan.Job)
	assertLocalStoreFailure(t, err, context.DeadlineExceeded)
}
