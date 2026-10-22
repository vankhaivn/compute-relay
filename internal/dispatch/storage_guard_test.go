package dispatch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hostimages"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func TestStorageRefusalPreservesUnsubmittedAuthority(t *testing.T) {
	for _, reason := range []string{"host_storage_limit", "host_storage_unknown", "host_image_count_limit", "remote_staging_dataset_limit", "remote_kernel_limit", "remote_attempt_container_limit", "remote_resource_scope_unknown", "remote_shared_dataset_limit"} {
		t.Run(reason, func(t *testing.T) {
			w, _, _ := fixtureWork(t)
			j, state := apply(t, w.Journal, w.Job.Attempt.State, Action{Kind: RefuseNewWork, RefusalReason: reason, Cause: "storage_volumes_list_failed"})
			expected := w.Job.Attempt.State
			expected.Orchestration = domain.OrchestrationQueued
			if j.Phase != Local || state != expected || j.Plan != nil || j.SubmitStarted || j.Problem == nil || j.Problem.Details[domain.ReasonDetail] != reason || j.Problem.Details[domain.CauseDetail] != "storage_volumes_list_failed" || j.Problem.ComputeMayHaveStarted || j.Problem.SafeOperationRetry {
				t.Fatal("storage refusal changed queued authority or terminalized work")
			}
			j, state = readyFixture(t)
			plan, prepared := j.Plan.Digest(), *j.Prepared
			j, state = apply(t, j, state, Action{Kind: RefuseNewWork, RefusalReason: reason})
			if j.Phase != Ready || j.SubmitStarted || j.Plan.Digest() != plan || *j.Prepared != prepared || !scheduler.HoldsAccount(state, true) || state.Execution != domain.ExecutionNotSubmitted {
				t.Fatal("storage refusal discarded prepared ownership or started work")
			}
			j, state = apply(t, j, state, Action{Kind: BeginSubmission})
			if j.Problem != nil {
				t.Fatal("successful start retained stale storage refusal")
			}
			if _, _, _, err := Apply(j, state, Action{Kind: RefuseNewWork, RefusalReason: reason}, time.Now()); err == nil {
				t.Fatal("storage policy stopped possibly running work")
			}
		})
	}
}

func TestStorageRefusalDoesNotTerminateDispatchWorkers(t *testing.T) {
	for _, cause := range []error{hoststorage.ErrHardLimit, hoststorage.ErrUnknown, hostimages.ErrCountLimit, provider.WithReason(provider.ErrResourceCountLimit, "remote_staging_dataset_limit"), provider.WithReason(provider.ErrResourceCountLimit, "remote_shared_dataset_limit")} {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		repo := &workerFaultRepo{faults: []error{cause}, cancel: cancel}
		cfg := DefaultConfig()
		cfg.Workers = 1
		cfg.PollDelay = time.Millisecond
		e := &Engine{repo: repo, clock: systemClock{}, config: cfg, slots: make(chan struct{}, 1)}
		err := e.Run(ctx)
		cancel()
		if !errors.Is(err, context.Canceled) || repo.calls != 2 {
			t.Fatalf("storage refusal stopped worker: calls=%d err=%v", repo.calls, err)
		}
	}
}
