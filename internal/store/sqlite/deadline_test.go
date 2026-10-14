package sqlite

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
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
	if _, err = f.s.ConsumeAuthorization(context.Background(), work.Handle); !errors.Is(err, admission.ErrExpired) {
		t.Fatal("expired grant became a mutation permit", err)
	}
	if n := controlCount(t, f.s, "SELECT count(*) FROM attempt_grants WHERE kind='execution' AND status='consumed'"); n != 0 {
		t.Fatal("expiry consumed the grant", n)
	}
}

func TestDeadlineRejectedAuthorityAndRetryCreateNoRows(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(fmt.Sprint(retry), func(t *testing.T) {
			f := managedFixture(t, fake.DefaultScenario())
			deadline := f.clock.Now().Add(time.Hour).UTC().Format(time.RFC3339)
			id := f.seedRaw(t, "a", 1, false, strings.Replace(dispatchJSON, `"timeouts":{`, `"timeouts":{"execution_deadline":"`+deadline+`",`, 1))
			if retry {
				service, p, _ := controlService(t, f, "a", auth.Operate)
				cancelControl(t, f, service, p, id)
				f.clock.advance(2 * time.Hour)
				if _, err := service.Submit(t.Context(), p, "a", id.JobID, domain.OperationRetryCompute, "expired-retry", controlRequest(id.AttemptID)); !errors.Is(err, admission.ErrExpired) {
					t.Fatal(err)
				}
			} else {
				f.clock.advance(2 * time.Hour)
				service, p := authorizationService(t, f, "a", auth.Execute)
				if _, err := service.Authorize(t.Context(), p, "a", id.JobID, "expired-grant", authorizationRequest(id.AttemptID, 10)); !errors.Is(err, admission.ErrExpired) {
					t.Fatal(err)
				}
			}
			if controlCount(t, f.s, "SELECT count(*) FROM attempt_grants WHERE kind='execution'") != 0 || controlCount(t, f.s, "SELECT count(*) FROM attempts") != 1 {
				t.Fatal("expiry created grant or attempt rows")
			}
		})
	}
}
