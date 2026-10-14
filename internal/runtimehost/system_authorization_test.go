package runtimehost

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

func systemAuthorizationJob(t *testing.T, scenario fake.Scenario) (*managedTestFixture, admission.Receipt) {
	t.Helper()
	f := newSystemFixture(t, scenario)
	connection := f.connection(t, "authorization-connection", "SYNTHETIC_FIRST")
	return f, f.submitJob(t, "authorization-job", f.jobSpec(t, connection))
}
func systemAuthorize(t *testing.T, f *managedTestFixture, job admission.Receipt, key string) executionauth.Receipt {
	t.Helper()
	receipt, err := f.services.authorizations.Authorize(t.Context(), f.actor, "app", job.JobID, key, systemAuthorizationRequest(job.AttemptID, 10))
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}
func TestAuthorizationConcurrentReplayAndOneGrantAcrossKeys(t *testing.T) {
	f, job := systemAuthorizationJob(t, fake.DefaultScenario())
	const count = 12
	type result struct {
		receipt executionauth.Receipt
		err     error
	}
	results := make(chan result, count)
	var wg sync.WaitGroup
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := f.services.authorizations.Authorize(t.Context(), f.actor, "app", job.JobID, "same-grant-key", systemAuthorizationRequest(job.AttemptID, 10))
			results <- result{r, err}
		}()
	}
	wg.Wait()
	close(results)
	fresh := 0
	var id domain.OperationID
	for r := range results {
		if r.err != nil {
			t.Fatal(r.err)
		}
		if id == "" {
			id = r.receipt.AuthorizationID
		}
		if r.receipt.AuthorizationID != id {
			t.Fatal("concurrent replay changed receipt")
		}
		if !r.receipt.Replay {
			fresh++
		}
	}
	if fresh != 1 {
		t.Fatal("concurrent replay had multiple fresh grants", fresh)
	}
	for _, request := range []struct {
		key  string
		wall int64
	}{{"same-grant-key", 11}, {"another-grant-key", 10}} {
		_, err := f.services.authorizations.Authorize(t.Context(), f.actor, "app", job.JobID, request.key, systemAuthorizationRequest(job.AttemptID, request.wall))
		if !errors.Is(err, executionauth.ErrConflict) {
			t.Fatal("changed key/payload replaced permit", err)
		}
	}
}
func TestAuthorizationCurrentAuthorityAndExactTarget(t *testing.T) {
	f, job := systemAuthorizationJob(t, fake.DefaultScenario())
	for _, scope := range []auth.Scope{auth.Read, auth.Write, auth.Operate, auth.Manage} {
		actor, _ := f.principal(t, "app", scope)
		if _, err := f.services.authorizations.Authorize(t.Context(), actor, "app", job.JobID, "scope-grant-key", systemAuthorizationRequest(job.AttemptID, 10)); !errors.Is(err, auth.ErrForbidden) {
			t.Fatalf("scope %s granted: %v", scope, err)
		}
	}
	execute, _ := f.principal(t, "app", auth.Execute)
	for _, tc := range []struct {
		workspace domain.WorkspaceID
		job       domain.JobID
		attempt   domain.AttemptID
		wall      int64
		want      error
	}{
		{"other", job.JobID, job.AttemptID, 10, auth.ErrForbidden}, {"app", "missing", job.AttemptID, 10, executionauth.ErrNotFound}, {"app", job.JobID, "wrong", 10, executionauth.ErrConflict}, {"app", job.JobID, job.AttemptID, 11, executionauth.ErrRequest},
	} {
		_, err := f.services.authorizations.Authorize(t.Context(), execute, tc.workspace, tc.job, "wrong-target-key", systemAuthorizationRequest(tc.attempt, tc.wall))
		if !errors.Is(err, tc.want) {
			t.Fatalf("got %v want %v", err, tc.want)
		}
	}
	receipt, err := f.services.authorizations.Authorize(t.Context(), execute, "app", job.JobID, "valid-grant-key", systemAuthorizationRequest(job.AttemptID, 10))
	if err != nil {
		t.Fatal(err)
	}
	other, _ := f.principal(t, "other", auth.Execute)
	if _, err = f.services.authorizations.Get(t.Context(), other, "other", job.JobID, receipt.AuthorizationID); !errors.Is(err, executionauth.ErrNotFound) {
		t.Fatal("cross-workspace receipt visible", err)
	}
	if err = f.host.access.Revoke(t.Context(), execute.TokenID()); err != nil {
		t.Fatal(err)
	}
	if _, err = f.services.authorizations.Authorize(t.Context(), execute, "app", job.JobID, "valid-grant-key", systemAuthorizationRequest(job.AttemptID, 10)); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("public replay bypassed revoked authority", err)
	}
}
func TestAuthorizationReplayOriginalAndReadConsumedAfterRestart(t *testing.T) {
	f, job := systemAuthorizationJob(t, fake.DefaultScenario())
	original := systemAuthorize(t, f, job, "grant-original-key")
	f.reopen(t)
	// This test intentionally creates the acknowledged consume-before-intent boundary
	// through exported Store calls. Recovery must continue only the original attempt.
	claim, err := f.host.store.ClaimNext(t.Context(), "consume-before-restart")
	if err != nil || claim.Claim == nil {
		t.Fatal("authorized claim unavailable", err)
	}
	work, err := f.host.store.LoadDispatch(t.Context(), *claim.Claim)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.host.store.ConsumeAuthorization(t.Context(), work.Handle); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	f.advance(t, time.Minute)
	replay, err := f.services.authorizations.Authorize(t.Context(), f.actor, "app", job.JobID, "grant-original-key", systemAuthorizationRequest(job.AttemptID, 10))
	if err != nil {
		t.Fatal(err)
	}
	expected := original
	expected.Replay = true
	if !reflect.DeepEqual(replay, expected) {
		t.Fatal("POST replay returned current state", replay)
	}
	current, err := f.services.authorizations.Get(t.Context(), f.actor, "app", job.JobID, original.AuthorizationID)
	if err != nil || current.Status != executionauth.Consumed || current.ConsumedAt == nil {
		t.Fatal("consumption lost", current, err)
	}
	f.dispatchOnce(t)
	if f.backend.Stats().PrepareCalls != 1 {
		t.Fatal("consumed grant did not resume preparation exactly once")
	}
	f.reopen(t)
	f.dispatchOnce(t)
	if f.backend.Stats().PrepareCalls != 1 || f.backend.Stats().SubmitCalls != 1 {
		t.Fatal("same grant repeated a mutation")
	}
}
func TestAuthorizationAmbiguousSubmissionAndRetryNeedSeparateGrant(t *testing.T) {
	t.Run("ambiguous", func(t *testing.T) {
		scenario := fake.DefaultScenario()
		scenario.Mode = fake.AcceptLoseResponse
		f, job := systemAuthorizationJob(t, scenario)
		systemAuthorize(t, f, job, "grant-original-key")
		f.dispatchOnce(t)
		f.dispatchOnce(t)
		target, err := f.host.store.ReadLogAttempt(t.Context(), "app", f.actor.TokenID(), job.JobID, job.AttemptID)
		if err != nil || target.Remote != nil || f.backend.Stats().SubmitCalls != 1 {
			t.Fatal("ambiguity was hidden", err)
		}
		f.reopen(t)
		f.dispatchOnce(t)
		if stats := f.backend.Stats(); stats.SubmitCalls != 1 || stats.Executions != 1 {
			t.Fatal("ambiguous submission duplicated", stats)
		}
	})
	t.Run("retry", func(t *testing.T) {
		f, job := systemAuthorizationJob(t, fake.DefaultScenario())
		original := systemAuthorize(t, f, job, "grant-original-key")
		control := f.controls(t)
		raw := systemJSON(t, map[string]any{"attempt_id": job.AttemptID, "reason": "explicit fixture retry"})
		if _, err := control.Submit(t.Context(), f.actor, "app", job.JobID, domain.OperationCancel, "cancel-original", raw); err != nil {
			t.Fatal(err)
		}
		retry, err := control.Submit(t.Context(), f.actor, "app", job.JobID, domain.OperationRetryCompute, "retry-grant-key", raw)
		if err != nil || retry.Effect != operations.NewAttemptCreated {
			t.Fatal("retry missing", retry, err)
		}
		if worked, err := f.services.dispatcher.RunOnce(t.Context(), "ungranted-retry"); err != nil || worked || f.backend.Stats().PrepareCalls != 0 {
			t.Fatal("retry inherited old authorization", worked, err)
		}
		next := job
		next.AttemptID = retry.NewAttemptID
		grant := systemAuthorize(t, f, next, "retry-new-permit")
		if grant.AuthorizationID == original.AuthorizationID {
			t.Fatal("retry reused original grant")
		}
		f.dispatchOnce(t)
		if f.backend.Stats().PrepareCalls != 1 {
			t.Fatal("retry did not use a separate permit")
		}
	})
}
func TestAuthorizationConcurrentDifferentKeysHaveOneWinner(t *testing.T) {
	f, job := systemAuthorizationJob(t, fake.DefaultScenario())
	const count = 12
	results := make(chan error, count)
	var wg sync.WaitGroup
	for i := range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := f.services.authorizations.Authorize(t.Context(), f.actor, "app", job.JobID, fmt.Sprintf("distinct-permit-key-%d", i), systemAuthorizationRequest(job.AttemptID, 10))
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	winners := 0
	for err := range results {
		if err == nil {
			winners++
		} else if !errors.Is(err, executionauth.ErrConflict) {
			t.Fatal(err)
		}
	}
	if winners != 1 {
		t.Fatal("distinct keys multiplied grant", winners)
	}
}
