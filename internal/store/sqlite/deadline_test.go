package sqlite

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

func TestExpiredAdmissionRejectsNewWorkButReplaysOriginal(t *testing.T) {
	f := newAdmission(t)
	ctx := context.Background()
	deadline := time.Now().UTC().Add(2 * time.Second).Truncate(time.Second)
	raw := []byte(strings.Replace(admissionJSON, `"timeouts":{`, `"timeouts":{"execution_deadline":"`+deadline.Format(time.RFC3339)+`",`, 1))
	first, err := f.service.Submit(ctx, f.p, "a", "absolute-original", raw)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Until(deadline) + time.Millisecond)
	replay, err := f.service.Submit(ctx, f.p, "a", "absolute-original", raw)
	if err != nil || !replay.Replay || replay.JobID != first.JobID || replay.AttemptID != first.AttemptID {
		t.Fatal("expiry destroyed immutable replay", err)
	}
	if _, err = f.service.Submit(ctx, f.p, "a", "absolute-new-key", raw); !errors.Is(err, admission.ErrExpired) {
		t.Fatal("expired admission accepted", err)
	}
	if _, err = f.service.Validate(ctx, f.p, "a", raw); !errors.Is(err, admission.ErrExpired) {
		t.Fatal("expired validation accepted", err)
	}
	if tableCount(t, f.s, "jobs") != 1 || tableCount(t, f.s, "attempts") != 1 {
		t.Fatal("expiry created new history")
	}
}

func TestQueuedDeadlineExpiresWithoutProviderEffects(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	deadline := f.clock.Now().Add(time.Minute).UTC().Format(time.RFC3339)
	raw := strings.Replace(dispatchJSON, `"timeouts":{`, `"timeouts":{"execution_deadline":"`+deadline+`",`, 1)
	id := f.seedRaw(t, "a", 1, false, raw)
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	j, state, stats := f.journal(t, id), f.state(t, id), f.backend.Stats()
	if j.Phase != dispatch.Prevented || !state.DeadlineExceeded || state.Orchestration != domain.OrchestrationTimedOut || state.Execution != domain.ExecutionNotSubmitted {
		t.Fatal("queued expiry lost local-only facts", j.Phase, state)
	}
	if stats.PrepareCalls != 0 || stats.SubmitCalls != 0 {
		t.Fatal("expired queue mutated provider", stats)
	}
}

func TestPreparedExpiryAndLostStartKeepOneShotIdentity(t *testing.T) {
	for _, phase := range []string{"ready", "staging", "submitting"} {
		t.Run(phase, func(t *testing.T) {
			scenario := fake.DefaultScenario()
			scenario.AbsoluteDeadline = true
			scenario.Mode = fake.AcceptLoseResponse
			f := newDispatchFixture(t, scenario)
			minutes := 15
			if phase == "submitting" {
				minutes = 25
			}
			deadline := f.clock.Now().Add(time.Duration(minutes) * time.Minute).UTC().Format(time.RFC3339)
			raw := strings.Replace(dispatchJSON, `"timeouts":{`, `"timeouts":{"execution_deadline":"`+deadline+`",`, 1)
			id := f.seedRaw(t, "a", 1, false, raw)
			f.adapter.losePrepare = phase == "staging"
			err := f.step(t)
			if phase == "staging" {
				if err == nil || !strings.HasPrefix(err.Error(), "STAGING_FAILED:") {
					t.Fatal("lost preparation acknowledgement was not retained", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if phase == "submitting" {
				if err := f.step(t); err != nil {
					t.Fatal(err)
				}
				if f.journal(t, id).Phase != dispatch.Submitting {
					t.Fatal("fixture did not lose the start response")
				}
			}
			// Reopen the durable store before the next (now expired) worker pass.
			if err := f.s.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(context.Background(), f.root, DefaultOptions())
			if err != nil {
				t.Fatal(err)
			}
			f.attach(t, reopened, nil)
			if err := f.step(t); err != nil {
				t.Fatal(err)
			}
			j, state, stats := f.journal(t, id), f.state(t, id), f.backend.Stats()
			if stats.PrepareCalls != 1 {
				t.Fatal("expiry repeated preparation", stats)
			}
			if phase == "submitting" {
				if j.Phase != dispatch.Submitted || stats.SubmitCalls != 1 || stats.Executions != 1 || state.Orchestration.Terminal() || state.RemoteActivity != domain.RemoteActivityPossible {
					t.Fatal("expiry lost possible original execution", j.Phase, state, stats)
				}
			} else if j.Phase != dispatch.Prevented || !state.DeadlineExceeded || stats.SubmitCalls != 0 {
				t.Fatal("expired preparation became executable", j.Phase, state, stats)
			}
		})
	}
}

func TestDeadlineAuthorizationReplayCannotConsumeExpiredGrant(t *testing.T) {
	f := managedFixture(t, fake.DefaultScenario())
	deadline := f.clock.Now().Add(2 * time.Second).UTC().Format(time.RFC3339)
	raw := strings.Replace(dispatchJSON, `"timeouts":{`, `"timeouts":{"execution_deadline":"`+deadline+`",`, 1)
	id := f.seedRaw(t, "a", 1, false, raw)
	original, service, p := grantAuthorization(t, f, id)
	work := managedClaim(t, f)
	f.clock.advance(2 * time.Second)
	replay, err := service.Authorize(context.Background(), p, "a", id.JobID, "grant-original-key", authorizationRequest(id.AttemptID, 10))
	if err != nil || !replay.Replay || replay.AuthorizationID != original.AuthorizationID {
		t.Fatal("expired grant lost original receipt", err)
	}
	if _, err = f.s.ConsumeAuthorization(context.Background(), work.Handle, f.clock.Now()); !errors.Is(err, admission.ErrExpired) {
		t.Fatal("expired grant became a mutation permit", err)
	}
	if n := controlCount(t, f.s, "SELECT count(*) FROM execution_authorizations WHERE status='consumed'"); n != 0 {
		t.Fatal("expiry consumed the grant", n)
	}
}

func TestDeadlineRefusesNewAuthorizationAndRetry(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(map[bool]string{false: "authorization", true: "retry"}[retry], func(t *testing.T) {
			f := managedFixture(t, fake.DefaultScenario())
			deadline := f.clock.Now().Add(time.Hour).UTC().Format(time.RFC3339)
			raw := strings.Replace(dispatchJSON, `"timeouts":{`, `"timeouts":{"execution_deadline":"`+deadline+`",`, 1)
			id := f.seedRaw(t, "a", 1, false, raw)
			if retry {
				control, p, _ := controlService(t, f, "a", auth.Operate)
				cancelControl(t, f, control, p, id)
				f.clock.advance(2 * time.Hour)
				if _, err := control.Submit(context.Background(), p, "a", id.JobID, domain.OperationRetryCompute, "expired-retry", controlRequest(id.AttemptID)); !errors.Is(err, admission.ErrExpired) {
					t.Fatal("retry extended the immutable deadline", err)
				}
			} else {
				f.clock.advance(2 * time.Hour)
				service, p := authorizationService(t, f, "a", auth.Execute)
				if _, err := service.Authorize(context.Background(), p, "a", id.JobID, "expired-grant", authorizationRequest(id.AttemptID, 10)); !errors.Is(err, admission.ErrExpired) {
					t.Fatal("expired job received new authority", err)
				}
			}
			if controlCount(t, f.s, "SELECT count(*) FROM execution_authorizations") != 0 || controlCount(t, f.s, "SELECT count(*) FROM attempts") != 1 {
				t.Fatal("expiry created a grant or replacement attempt")
			}
		})
	}
}

func TestDeadlineExpiredPreparedAttemptCanBeCleanedWithoutResult(t *testing.T) {
	scenario := fake.DefaultScenario()
	scenario.AbsoluteDeadline = true
	f := newDispatchFixture(t, scenario)
	deadline := f.clock.Now().Add(15 * time.Minute).UTC().Format(time.RFC3339)
	raw := strings.Replace(dispatchJSON, `"timeouts":{`, `"timeouts":{"execution_deadline":"`+deadline+`",`, 1)
	id := f.seedRaw(t, "a", 1, false, raw)
	for range 2 {
		if err := f.step(t); err != nil {
			t.Fatal(err)
		}
	}
	j := f.journal(t, id)
	if j.Phase != dispatch.Prevented || j.Prepared == nil || !f.state(t, id).DeadlineExceeded {
		t.Fatal("prepared expiry was not established")
	}
	service, p, receipt := admitCleanup(t, f, id, "cleanup-expired-preparation", false)
	engine, probe := cleanupWorker(t, f)
	probe.result = &provider.CleanupOutcome{Deleted: true}
	runCleanup(t, engine)
	if len(probe.requests) != 1 || probe.requests[0].Remote.Resource != j.Prepared.Resource || probe.requests[0].Remote.Version != "prepared" || probe.requests[0].ResultsCollected {
		t.Fatal("expired cleanup lost exact never-started identity", probe.requests)
	}
	if got, err := service.Get(context.Background(), p, "a", receipt.Operation.ID); err != nil || got.Effect != cleanup.RemoteDeleted {
		t.Fatal("cleanup not confirmed", err)
	}
	if stats := f.backend.Stats(); stats.SubmitCalls != 0 || stats.Executions != 0 {
		t.Fatal("expired cleanup started compute", stats)
	}
}
