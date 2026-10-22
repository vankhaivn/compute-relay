package cleanup

import (
	"context"
	"errors"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

type reasonTarget struct {
	stubTarget
	descriptionErr   error
	descriptionPanic bool
}

func (s *reasonTarget) DescribeCleanup(q provider.CleanupResourceRequest) (provider.CleanupDescription, error) {
	if s.descriptionPanic {
		panic("private canary")
	}
	if s.descriptionErr != nil {
		return provider.CleanupDescription{}, s.descriptionErr
	}
	return s.stubTarget.DescribeCleanup(q)
}
func TestCleanupPersistsFailedStepOnResourceAndAggregate(t *testing.T) {
	cause := errors.New("private canary")
	for _, tc := range []struct {
		name   string
		target reasonTarget
		reason string
		state  operations.ResourceState
	}{
		{name: "description", target: reasonTarget{descriptionErr: cause}, reason: "cleanup_description_failed", state: operations.ResourceRetained},
		{name: "description_panic", target: reasonTarget{descriptionPanic: true}, reason: "cleanup_inspection_panic", state: operations.ResourceRetained},
		{name: "binding", target: reasonTarget{stubTarget: stubTarget{verify: provider.WithReason(cause, "host_unavailable")}}, reason: "host_unavailable", state: operations.ResourceRetained},
		{name: "delete", target: reasonTarget{stubTarget: stubTarget{err: provider.WithReason(cause, "cleanup_container_inspect_failed")}}, reason: "cleanup_container_inspect_failed", state: operations.ResourceUnknown},
		{name: "delete_panic", target: reasonTarget{stubTarget: stubTarget{panics: true}}, reason: "cleanup_delete_panic", state: operations.ResourceUnknown},
		{name: "no_outcome", reason: "cleanup_delete_unconfirmed", state: operations.ResourceUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := stubWork(t)
			item := w.Resources[0]
			result, failed, err := inspectCleanupResource(context.Background(), &tc.target, *w, item)
			if result.State == operations.ResourceRunning {
				result, failed, err = callCleanupResource(context.Background(), &tc.target, item)
			}
			if err != nil || result.Reason != tc.reason || result.State != tc.state || failed.Problem == nil || failed.Problem.PublicDetails()[domain.ReasonDetail] != tc.reason || failed.Problem.SafeOperationRetry {
				t.Fatalf("result=%#v failure=%#v err=%v", result, failed, err)
			}
		})
	}
}
func TestStorageCleanupPersistsStepWithoutChangingDeleteAuthority(t *testing.T) {
	cause := errors.New("private canary")
	for _, tc := range []struct {
		name, state string
		target      storageStubProvider
		reason      string
		outcome     hoststorage.DeleteOutcome
	}{
		{name: "verify", state: "pending", target: storageStubProvider{verifyErr: provider.WithReason(cause, "host_unavailable")}, reason: "host_unavailable", outcome: hoststorage.DeleteRetained},
		{name: "observe", state: "pending", target: storageStubProvider{observeErr: provider.WithReason(cause, "storage_labels_read_failed")}, reason: "storage_labels_read_failed", outcome: hoststorage.DeleteRetained},
		{name: "delete", state: "pending", target: storageStubProvider{deleteErr: provider.WithReason(cause, "storage_delete_failed")}, reason: "storage_delete_failed", outcome: hoststorage.DeleteUnknown},
		{name: "delete_panic", state: "pending", target: storageStubProvider{panicDelete: true}, reason: "storage_cleanup_delete_panic", outcome: hoststorage.DeleteUnknown},
		{name: "recovery_verify", state: "running", target: storageStubProvider{verifyErr: provider.WithReason(cause, "host_unavailable")}, reason: "host_unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := storageStubWork(t)
			w.State = tc.state
			repo := &storageStubRepository{work: w}
			engine, _ := New(&stubRepo{}, stubResolver{p: &tc.target}, DefaultConfig())
			engine.WithStorage(repo)
			_, err := engine.RunOnce(context.Background())
			if tc.state == "running" && !errors.Is(err, ErrDeferred) || tc.state == "pending" && err != nil {
				t.Fatal(err)
			}
			if len(repo.reasons) != 1 || repo.reasons[0] != tc.reason {
				t.Fatal("lost durable reason", repo.reasons)
			}
			if tc.outcome != "" && (len(repo.completed) != 1 || repo.completed[0] != tc.outcome) {
				t.Fatal("wrong outcome", repo.completed)
			}
			if tc.state == "running" && (repo.begins != 0 || tc.target.deletes != 0 || len(repo.completed) != 0) {
				t.Fatal("recovery repeated mutation")
			}
		})
	}
}

func TestCleanupVerificationPreservesTypedProblemSemantics(t *testing.T) {
	for _, code := range []domain.ErrorCode{domain.CodeProviderStateUnknown, domain.CodeRemoteIdentityMismatch, domain.CodeProviderAuthFailed} {
		stage := domain.FailureStageObservation
		if code == domain.CodeProviderAuthFailed {
			stage = domain.FailureStageAuthentication
		}
		p := provider.Problem(code, stage, "private canary").WithDetail(domain.ReasonDetail, "host_unavailable").WithRetrySemantics(true, false)
		if p.Validate() != nil {
			t.Fatal("invalid test problem", p.Validate())
		}
		w := stubWork(t)
		target := &reasonTarget{stubTarget: stubTarget{verify: &p}}
		result, failed, err := inspectCleanupResource(context.Background(), target, *w, w.Resources[0])
		if err != nil || result.State != operations.ResourceRetained || failed.Problem.Code != p.Code || failed.Problem.SafeOperationRetry != p.SafeOperationRetry || failed.Problem.ComputeMayHaveStarted != p.ComputeMayHaveStarted || !errors.Is(failed.Problem, &p) {
			t.Fatal("typed semantics changed", result, failed, err)
		}
	}
}
