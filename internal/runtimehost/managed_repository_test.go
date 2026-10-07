package runtimehost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/objects"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

type advanceBeforeFinish struct {
	*sqlite.Store
	calls int
	first error
}

func (r *advanceBeforeFinish) FinishConnection(ctx context.Context, record connections.Record, result connections.Completion) error {
	r.calls++
	if r.calls == 1 {
		if _, err := r.Store.ClaimNextManaged(ctx, "clock_fixture", result.Now.Add(time.Millisecond)); err != nil {
			return err
		}
	}
	err := r.Store.FinishConnection(ctx, record, result)
	if r.calls == 1 {
		r.first = err
	}
	return err
}

func TestManagedConnectionCompletionRetriesClockWithoutReverification(t *testing.T) {
	f := newManagedTestFixture(t, true)
	repo := &advanceBeforeFinish{Store: f.host.store}
	service, err := connections.New(f.host.access, managedConnectionRepository{repo}, f.vault, []connections.Adapter{f.adapter}, clock{}.Now)
	if err != nil {
		t.Fatal(err)
	}
	f.services.connections = service
	connection := f.connection(t, "clock-verification-key", "SYNTHETIC_FIRST")
	if !errors.Is(repo.first, scheduler.ErrClock) || repo.calls != 2 || f.adapter.checks != 1 || connection.Selection == nil {
		t.Fatalf("completion did not recover one rolled-back write: calls=%d checks=%d first=%v", repo.calls, f.adapter.checks, repo.first)
	}
	// Capacity GET/list uses the same guard but never re-verifies the account.
	now := time.Now().UTC()
	if _, err := f.host.store.ClaimNextManaged(context.Background(), "clock_fixture", now); err != nil {
		t.Fatal(err)
	}
	stale := now.Add(-time.Second)
	wrapped := managedConnectionRepository{f.host.store}
	if _, err = wrapped.ReadConnection(context.Background(), "app", f.actor.TokenID(), connection.ID, stale); err != nil {
		t.Fatal("cached connection read rejected stale caller time", err)
	}
	if list, err := wrapped.ListConnections(context.Background(), "app", f.actor.TokenID(), stale); err != nil || len(list) != 1 || f.adapter.checks != 1 {
		t.Fatal("cached list failed or contacted provider", err)
	}
}

type advanceBeforeDispatchCommit struct {
	*sqlite.Store
	calls  int
	first  error
	handle dispatch.Handle
	action dispatch.Action
}

func (r *advanceBeforeDispatchCommit) CommitDispatch(ctx context.Context, handle dispatch.Handle, action dispatch.Action, now time.Time) (dispatch.Work, error) {
	r.calls++
	if r.calls == 1 {
		r.handle, r.action = handle, action
		if _, err := r.Store.ClaimNextManaged(ctx, "clock_fixture", now.Add(time.Millisecond)); err != nil {
			return dispatch.Work{}, err
		}
	} else if handle != r.handle || !reflect.DeepEqual(action, r.action) {
		return dispatch.Work{}, errors.New("clock retry changed claim or provider evidence")
	}
	result, err := r.Store.CommitDispatch(ctx, handle, action, now)
	if r.calls == 1 {
		r.first = err
	}
	return result, err
}

type rejectingManagedProvider struct {
	provider.Provider
	checks int
}

func (p *rejectingManagedProvider) VerifyBinding(context.Context, provider.BindingSnapshot) error {
	p.checks++
	return errors.New("synthetic provider rejection")
}

func (p *rejectingManagedProvider) Resolve(provider.BindingSnapshot) (provider.Provider, error) {
	return p, nil
}

func TestManagedPostProviderCommitRetriesClockWithoutRepeatingCallback(t *testing.T) {
	f := newManagedTestFixture(t, true)
	connection := f.connection(t, "clock-dispatch-key", "SYNTHETIC_FIRST")
	ctx := context.Background()
	objectService, err := objects.New(f.host.access, f.host.inputs, f.host.store)
	if err != nil {
		t.Fatal(err)
	}
	payload := "synthetic bundle never executed"
	bundle, err := objectService.Upload(ctx, f.actor, "app", int64(len(payload)), provider.Digest([]byte(payload)), strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := admission.New(f.host.access, f.host.store, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	raw := fmt.Sprintf(`{"api_version":"compute-connector/v1alpha1","name":"clock fixture","profile":%q,"bundle":{"object_id":%q},"execution":{"kind":"python","command":["python","MUST_NOT_EXECUTE.py"]},"inputs":[],"outputs":[{"path":"answer.json","required":true}],"resources":{"accelerator":"gpu","minimum_gpu_count":1},"network":{"remote_internet":"disabled"},"timeouts":{"remote_wall_seconds":10,"setup_seconds":5,"finalization_grace_seconds":2}}`, connection.Selection.Profile, bundle.ID)
	job, err := jobs.Submit(ctx, f.actor, "app", "clock-job-admission", []byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err = f.host.store.ClaimNextManaged(ctx, "clock_fixture", now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	key, _ := admission.KeyDigest("clock-job-authorization")
	request := executionauth.Request{AttemptID: job.AttemptID, MaxRemoteWallSeconds: 10, AuthorizePrivateStaging: true, AuthorizeCompute: true}
	command := executionauth.Command{WorkspaceID: "app", TokenID: f.actor.TokenID(), JobID: job.JobID, KeyDigest: key, Request: request, Now: now}
	if _, err = f.host.store.AuthorizeExecution(ctx, command); !errors.Is(err, scheduler.ErrClock) {
		t.Fatal("fixture did not cross authorization clock guard", err)
	}
	if _, err = (managedAuthorizationRepository{f.host.store}).AuthorizeExecution(ctx, command); err != nil {
		t.Fatal("grant did not retry its rolled-back capacity read", err)
	}
	repo := &advanceBeforeDispatchCommit{Store: f.host.store}
	p := &rejectingManagedProvider{}
	engine, err := dispatch.New(managedDispatchRepository{repo}, p, f.host.inputs, nil, clock{}, dispatch.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	worked, err := engine.RunOnce(ctx, "clock_worker")
	var problem *domain.Problem
	if !worked || !errors.As(err, &problem) || problem.Code != domain.CodeProviderAuthFailed {
		t.Fatal("provider rejection was lost behind clock error", worked, err)
	}
	if !errors.Is(repo.first, scheduler.ErrClock) || repo.calls != 2 || p.checks != 1 || repo.action.Kind != dispatch.Fault {
		t.Fatalf("post-provider commit repeated callback or changed evidence: calls=%d checks=%d first=%v", repo.calls, p.checks, repo.first)
	}
	stored, err := jobs.Get(ctx, f.actor, "app", job.JobID)
	if err != nil || stored.Attempt.State.Orchestration != domain.OrchestrationBlocked {
		state, _ := json.Marshal(stored.Attempt.State)
		t.Fatalf("provider failure was not persisted: %s %v", state, err)
	}
}

func TestManagedClockRetryRemainsBoundedAndRejectsUncertainAcknowledgement(t *testing.T) {
	calls := 0
	_, err := retryManagedClock(context.Background(), time.Now().UTC(), func(time.Time) (struct{}, error) {
		calls++
		return struct{}{}, scheduler.ErrClock
	})
	if !errors.Is(err, scheduler.ErrClock) || calls != managedClockAttempts {
		t.Fatal("persistent backwards clock did not surface", calls, err)
	}
	calls = 0
	uncertain := errors.New("uncertain SQLite acknowledgement")
	_, err = retryManagedClock(context.Background(), time.Now().UTC(), func(time.Time) (struct{}, error) {
		calls++
		return struct{}{}, uncertain
	})
	if err != uncertain || calls != 1 {
		t.Fatal("uncertain acknowledgement was retried", calls, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	_, err = retryManagedClock(ctx, time.Now().UTC(), func(time.Time) (struct{}, error) {
		cancel()
		return struct{}{}, scheduler.ErrClock
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatal("clock retry ignored cancellation", err)
	}
}
