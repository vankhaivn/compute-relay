package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

var schedulerNow = time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)

func schedulerStore(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state")
	s, err := Open(context.Background(), path, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err = s.ConfigureScheduler(context.Background(), scheduler.DefaultSettings()); err != nil {
		t.Fatal(err)
	}
	return s, path
}

// These are synthetic durable queue fixtures, not assertions that their payload ran.
// Existing admission/API tests exercise real canonicalization/auth/idempotent enqueue.
func schedulerSeed(t *testing.T, s *Store, id, w, instance, account string) scheduler.Identity {
	t.Helper()
	ctx := context.Background()
	identity := scheduler.Identity{WorkspaceID: domain.WorkspaceID(w), JobID: domain.JobID("job_" + id), AttemptID: domain.AttemptID("att_" + id)}
	profile := admission.DefaultProfile(domain.ProviderBinding{Profile: "profile-" + instance, ProviderInstanceID: domain.ProviderInstanceID(instance), ConfigurationRevision: "r1"}, account)
	p, _ := json.Marshal(profile)
	raw := `{"api_version":"compute-connector/v1alpha1","name":"fixture","profile":"` + profile.Binding.Profile + `","bundle":{"object_id":"bundle"},"execution":{"kind":"python","command":["python","main.py"]},"inputs":[],"outputs":[{"path":"out.txt","required":true}],"resources":{"accelerator":"gpu","minimum_gpu_count":1},"network":{"remote_internet":"disabled"},"timeouts":{"remote_wall_seconds":120,"setup_seconds":30,"finalization_grace_seconds":15}}`
	sum := sha256.Sum256([]byte(raw))
	state, _ := json.Marshal(domain.InitialAttemptState())
	at := schedulerNow.Add(-time.Hour).Format(time.RFC3339Nano)
	err := withTx(ctx, s.db, func(tx *sql.Tx) error {
		for _, q := range []struct {
			sql  string
			args []any
		}{
			{`INSERT INTO workspaces VALUES(?,1,?) ON CONFLICT(workspace_id) DO NOTHING`, []any{w, `["` + profile.Binding.Profile + `"]`}},
			{`INSERT INTO profile_revisions VALUES(?,?,?) ON CONFLICT(profile,revision) DO NOTHING`, []any{profile.Binding.Profile, "r1", string(p)}},
			{`INSERT INTO profiles VALUES(?,?,1) ON CONFLICT(profile) DO NOTHING`, []any{profile.Binding.Profile, "r1"}},
			{`INSERT INTO jobs VALUES(?,?,?,?,?,?,?,?,?)`, []any{w, string(identity.JobID), raw, admission.CanonicalVersion, hex.EncodeToString(sum[:]), profile.Binding.Profile, "r1", string(identity.AttemptID), at}},
			{`INSERT INTO attempts VALUES(?,?,?,?,?,?,?,?,?,?)`, []any{w, string(identity.JobID), string(identity.AttemptID), 1, strings.Repeat("a", 64), string(state), "queued", 1, at, at}},
			{`INSERT INTO events VALUES(?,?,?,?,?,?,?,?)`, []any{w, string(identity.JobID), "evt_" + id, 1, string(identity.AttemptID), "", "job.accepted", at}},
		} {
			if _, err := tx.ExecContext(ctx, q.sql, q.args...); err != nil {
				return dbError(err)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return identity
}
func schedulerClaim(t *testing.T, s *Store, owner string, at time.Time) scheduler.Claim {
	t.Helper()
	r, err := s.ClaimNext(context.Background(), owner, at)
	if err != nil || r.Claim == nil {
		t.Fatalf("claim=%+v err=%v", r, err)
	}
	if !r.Claim.Valid() {
		t.Fatal("invalid claim")
	}
	return *r.Claim
}
func schedulerCount(t *testing.T, s *Store, query string) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(query).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
func TestSchedulerFIFOAndCursorSurviveRestart(t *testing.T) {
	s, path := schedulerStore(t)
	settings := scheduler.DefaultSettings()
	settings.MaxActivePerAccount = 4
	if err := s.ConfigureScheduler(context.Background(), settings); err != nil {
		t.Fatal(err)
	}
	a1 := schedulerSeed(t, s, "a1", "a", "first", "shared")
	a2 := schedulerSeed(t, s, "a2", "a", "first", "shared")
	b1 := schedulerSeed(t, s, "b1", "b", "second", "shared")
	b2 := schedulerSeed(t, s, "b2", "b", "second", "shared")
	first := schedulerClaim(t, s, "worker1", schedulerNow)
	if first.Identity != a1 {
		t.Fatal("FIFO first", first)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	s, err = Open(context.Background(), path, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i, want := range []scheduler.Identity{b1, a2, b2} {
		c := schedulerClaim(t, s, fmt.Sprintf("worker%d", i+2), schedulerNow)
		if c.Identity != want {
			t.Fatal("persisted round robin/FIFO", c, want)
		}
	}
	r, err := s.ClaimNext(context.Background(), "extra", schedulerNow)
	if err != nil || r.Claim != nil || r.View.LiveWorkers != 4 || r.View.AccountReservations["shared"] != 4 {
		t.Fatal(r, err)
	}
}
func TestSchedulerConcurrentClaimsShareAccountAcrossInstances(t *testing.T) {
	s, _ := schedulerStore(t)
	schedulerSeed(t, s, "a", "a", "first", "same-account")
	schedulerSeed(t, s, "b", "b", "second", "same-account")
	var wg sync.WaitGroup
	claims := make(chan scheduler.Claim, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := s.ClaimNext(context.Background(), fmt.Sprintf("worker%d", i), schedulerNow)
			if err != nil {
				t.Error(err)
				return
			}
			if r.Claim != nil {
				claims <- *r.Claim
			}
		}(i)
	}
	wg.Wait()
	close(claims)
	if len(claims) != 1 || schedulerCount(t, s, "SELECT count(*) FROM scheduler_leases WHERE held=1") != 1 {
		t.Fatal("account overcommitted")
	}
	// Alias remapping cannot move an accepted attempt into another account.
	p := admission.DefaultProfile(domain.ProviderBinding{Profile: "profile-second", ProviderInstanceID: "third", ConfigurationRevision: "r2"}, "different-account")
	if err := s.PutProfile(context.Background(), p, true); err != nil {
		t.Fatal(err)
	}
	r, err := s.ClaimNext(context.Background(), "remap", schedulerNow)
	if err != nil || r.Claim != nil || r.View.AccountReservations["same-account"] != 1 {
		t.Fatal("frozen binding lost", r, err)
	}
}
func TestSchedulerExpiredClaimFencesEveryMutation(t *testing.T) {
	s, _ := schedulerStore(t)
	id := schedulerSeed(t, s, "one", "a", "p", "account")
	old := schedulerClaim(t, s, "old", schedulerNow)
	fresh := schedulerClaim(t, s, "new", schedulerNow.Add(31*time.Second))
	if fresh.Identity != id || fresh.Generation != old.Generation+1 || fresh.Fence == old.Fence || fresh.AttemptRevision != old.AttemptRevision {
		t.Fatal("reclaim changed execution identity", old, fresh)
	}
	for _, f := range []func() error{
		func() error {
			_, err := s.RenewClaim(context.Background(), old, schedulerNow.Add(31*time.Second))
			return err
		},
		func() error { return s.ReleaseClaim(context.Background(), old, schedulerNow.Add(31*time.Second)) },
		func() error { return s.DeferClaim(context.Background(), old, schedulerNow.Add(31*time.Second)) },
	} {
		if err := f(); !errors.Is(err, scheduler.ErrLeaseLost) {
			t.Fatal("stale fence accepted", err)
		}
	}
	a, nonce, err := loadAttempt(context.Background(), s.db, id.WorkspaceID, id.JobID, id.AttemptID)
	if err != nil || a.Number != 1 || nonce != strings.Repeat("a", 64) {
		t.Fatal("attempt identity lost", err)
	}
	next := a.State
	next.Orchestration = domain.OrchestrationBlocked
	after, err := a.Transition(next, schedulerNow.Add(32*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	event := domain.Event{ID: "evt_stale", Sequence: 4, WorkspaceID: id.WorkspaceID, JobID: id.JobID, AttemptID: id.AttemptID, Type: domain.EventSchedulerDeferred, OccurredAt: after.UpdatedAt}
	if err = s.CommitAttempt(context.Background(), ports.AttemptChange{WorkspaceID: id.WorkspaceID, Before: a, After: after, Event: event}); !errors.Is(err, scheduler.ErrLeaseLost) {
		t.Fatal("legacy CAS bypasses fence", err)
	}
	renewed, err := s.RenewClaim(context.Background(), fresh, schedulerNow.Add(32*time.Second))
	if err != nil || !renewed.ExpiresAt.After(fresh.ExpiresAt) {
		t.Fatal("renew", err)
	}
	if err = s.ReleaseClaim(context.Background(), renewed, schedulerNow.Add(33*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err = s.ReleaseClaim(context.Background(), renewed, schedulerNow.Add(33*time.Second)); !errors.Is(err, scheduler.ErrLeaseLost) {
		t.Fatal("released fence reusable", err)
	}
}
func TestSchedulerClaimAndEventFailureRollBackTogether(t *testing.T) {
	s, _ := schedulerStore(t)
	id := schedulerSeed(t, s, "a", "a", "p", "account")
	if _, err := s.db.Exec(`CREATE TRIGGER fail_scheduler_event BEFORE INSERT ON events WHEN NEW.type='scheduler.claimed' BEGIN SELECT RAISE(ABORT,'injected'); END`); err != nil {
		t.Fatal(err)
	}
	if r, err := s.ClaimNext(context.Background(), "w", schedulerNow); err == nil || r.Claim != nil {
		t.Fatal("failed transaction acknowledged", r, err)
	}
	a, _, err := loadAttempt(context.Background(), s.db, id.WorkspaceID, id.JobID, id.AttemptID)
	if err != nil || a.State.Orchestration != domain.OrchestrationQueued || a.Revision != 1 {
		t.Fatal("partial state persisted", a, err)
	}
	if schedulerCount(t, s, "SELECT count(*) FROM scheduler_leases") != 0 || schedulerCount(t, s, "SELECT count(*) FROM scheduler_control WHERE last_workspace!=''") != 0 {
		t.Fatal("partial lease/cursor persisted")
	}
	if _, err = s.db.Exec("DROP TRIGGER fail_scheduler_event"); err != nil {
		t.Fatal(err)
	}
	c := schedulerClaim(t, s, "w", schedulerNow)
	if _, err = s.db.Exec(`CREATE TRIGGER fail_defer BEFORE INSERT ON events WHEN NEW.type='scheduler.deferred' BEGIN SELECT RAISE(ABORT,'injected'); END`); err != nil {
		t.Fatal(err)
	}
	if err = s.DeferClaim(context.Background(), c, schedulerNow); err == nil {
		t.Fatal("defer fault ignored")
	}
	if schedulerCount(t, s, "SELECT count(*) FROM scheduler_leases WHERE held=1") != 1 {
		t.Fatal("failed defer released reservation")
	}
}

// Remote states below are controlled database fixtures for the future orchestrator.
// They do not call a provider and cannot be produced by the M3-03 local lease API.
func schedulerSetState(t *testing.T, s *Store, id scheduler.Identity, state domain.AttemptState) {
	t.Helper()
	if err := state.Validate(); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(state)
	if _, err := s.db.Exec("UPDATE attempts SET state=?,orchestration=? WHERE workspace_id=? AND job_id=? AND attempt_id=?", string(raw), string(state.Orchestration), string(id.WorkspaceID), string(id.JobID), string(id.AttemptID)); err != nil {
		t.Fatal(err)
	}
}
func TestSchedulerAmbiguousCapacitySurvivesExpiryAndRestart(t *testing.T) {
	s, path := schedulerStore(t)
	id := schedulerSeed(t, s, "old", "a", "p", "shared")
	schedulerSeed(t, s, "next", "b", "q", "shared")
	c := schedulerClaim(t, s, "w", schedulerNow)
	remote := domain.InitialAttemptState()
	remote.Orchestration = domain.OrchestrationReconciling
	remote.Execution = domain.ExecutionUnknown
	remote.RemoteActivity = domain.RemoteActivityPossible
	remote.DeadlineExceeded = true
	schedulerSetState(t, s, id, remote)
	if err := s.ReleaseClaim(context.Background(), c, schedulerNow); !errors.Is(err, scheduler.ErrPhaseGate) {
		t.Fatal("remote claim released", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	s, err = Open(context.Background(), path, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r, err := s.ClaimNext(context.Background(), "new", schedulerNow.Add(time.Hour))
	if err != nil || r.Claim != nil || r.View.AccountReservations["shared"] != 1 {
		t.Fatal("restart freed uncertain slot", r, err)
	}
	// Even an accidental reset cannot turn this old attempt into new work.
	schedulerSetState(t, s, id, domain.InitialAttemptState())
	r, err = s.ClaimNext(context.Background(), "reset", schedulerNow.Add(time.Hour))
	if err != nil || r.Claim != nil {
		t.Fatal("barrier forgotten", r, err)
	}
	remote = domain.InitialAttemptState()
	remote.Orchestration = domain.OrchestrationCollecting
	remote.Execution = domain.ExecutionSucceeded
	remote.RemoteActivity = domain.RemoteActivityInactive
	remote.Result = domain.ResultCollecting
	schedulerSetState(t, s, id, remote)
	next := schedulerClaim(t, s, "after-proof", schedulerNow.Add(time.Hour))
	if next.WorkspaceID != "b" {
		t.Fatal("old execution reclaimed", next)
	}
}
func TestSchedulerDeferPauseDisableAndBackwardClock(t *testing.T) {
	s, _ := schedulerStore(t)
	schedulerSeed(t, s, "a1", "a", "p", "shared")
	schedulerSeed(t, s, "a2", "a", "p", "shared")
	schedulerSeed(t, s, "b1", "b", "q", "other")
	c := schedulerClaim(t, s, "w", schedulerNow)
	if err := s.DeferClaim(context.Background(), c, schedulerNow); err != nil {
		t.Fatal(err)
	}
	next := schedulerClaim(t, s, "other", schedulerNow)
	if next.WorkspaceID != "b" {
		t.Fatal("deferred FIFO head overtaken")
	}
	settings := scheduler.DefaultSettings()
	settings.Paused = true
	if err := s.ConfigureScheduler(context.Background(), settings); err != nil {
		t.Fatal(err)
	}
	r, err := s.ClaimNext(context.Background(), "paused", schedulerNow)
	if err != nil || r.Claim != nil || r.View.LiveWorkers != 1 {
		t.Fatal("pause erased work", r, err)
	}
	if _, err = s.RenewClaim(context.Background(), next, schedulerNow.Add(time.Second)); err != nil {
		t.Fatal("pause prevented cooperative drain", err)
	}
	if _, err = s.ClaimNext(context.Background(), "backwards", schedulerNow); !errors.Is(err, scheduler.ErrClock) {
		t.Fatal("backward clock resurrected ownership", err)
	}
	settings.Paused = false
	if err = s.ConfigureScheduler(context.Background(), settings); err != nil {
		t.Fatal(err)
	}
	if err = s.ConfigureAccount(context.Background(), "shared", scheduler.AccountPolicy{Disabled: true, MaxActive: 1}); err != nil {
		t.Fatal(err)
	}
	r, err = s.ClaimNext(context.Background(), "disabled", schedulerNow.Add(2*time.Second))
	if err != nil || r.Claim != nil {
		t.Fatal(r, err)
	}
	view, err := s.InspectScheduler(context.Background(), schedulerNow.Add(2*time.Second))
	if err != nil || len(view.Heads) != 1 || view.Heads[0].Reason != scheduler.AccountDisabled {
		t.Fatal(view, err)
	}
}
func schedulerQuota(remaining float64, at time.Time) provider.QuotaObservation {
	limit := 10.0
	used := 0.0
	return provider.QuotaObservation{Status: provider.QuotaKnown, Resource: "gpu", Unit: "hours", Limit: &limit, Used: &used, Remaining: &remaining, ObservedAt: at, Source: "synthetic", Precision: "exact"}
}
func TestSchedulerQuotaLatchAndStrictPolicy(t *testing.T) {
	s, _ := schedulerStore(t)
	schedulerSeed(t, s, "a", "a", "p", "account")
	ctx := context.Background()
	if err := s.RecordQuota(ctx, "account", "gpu", schedulerQuota(0, schedulerNow), schedulerNow); err != nil {
		t.Fatal(err)
	}
	unavailable := provider.QuotaObservation{Status: provider.QuotaUnavailable, Resource: "gpu", ObservedAt: schedulerNow.Add(time.Second)}
	if err := s.RecordQuota(ctx, "account", "gpu", unavailable, unavailable.ObservedAt); err != nil {
		t.Fatal(err)
	}
	r, err := s.ClaimNext(ctx, "w", schedulerNow.Add(time.Hour))
	if err != nil || r.Claim != nil || r.View.Heads[0].Reason != scheduler.QuotaExhausted {
		t.Fatal("unknown/aging cleared exhaustion", r, err)
	}
	if err = s.RecordQuota(ctx, "account", "gpu", schedulerQuota(1, schedulerNow), schedulerNow.Add(time.Hour)); !errors.Is(err, ErrConflict) {
		t.Fatal("out-of-order observation accepted", err)
	}
	at := schedulerNow.Add(time.Hour)
	if err = s.ConfigureAccount(ctx, "account", scheduler.AccountPolicy{MaxActive: 1, StrictQuota: true}); err != nil {
		t.Fatal(err)
	}
	q := schedulerQuota(1, at)
	q.Precision = "rounded hours"
	if err = s.RecordQuota(ctx, "account", "gpu", q, at); err != nil {
		t.Fatal(err)
	}
	r, err = s.ClaimNext(ctx, "rounded", at)
	if err != nil || r.Claim != nil || r.View.Heads[0].Reason != scheduler.QuotaUncertain {
		t.Fatal("rounded quota proved strict sufficiency", r, err)
	}
	q = schedulerQuota(1, at.Add(time.Second))
	if err = s.RecordQuota(ctx, "account", "gpu", q, q.ObservedAt); err != nil {
		t.Fatal(err)
	}
	if schedulerClaim(t, s, "fresh", q.ObservedAt).QuotaWarning != "" {
		t.Fatal("fresh exact quota warning")
	}
}
func TestSchedulerStartsPausedAndEnqueueRollsBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	s, err := Open(context.Background(), path, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	schedulerSeed(t, s, "a", "a", "p", "account")
	r, err := s.ClaimNext(context.Background(), "w", schedulerNow)
	if err != nil || r.Claim != nil || r.View.Heads[0].Reason != scheduler.Paused {
		t.Fatal("migration started work", r, err)
	}
	before := schedulerCount(t, s, "SELECT count(*) FROM scheduler_queue")
	err = withTx(context.Background(), s.db, func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO attempts SELECT workspace_id,job_id,'rollback',2,nonce,state,orchestration,revision,created_at,updated_at FROM attempts LIMIT 1`)
		if err != nil {
			return dbError(err)
		}
		return errors.New("injected after enqueue")
	})
	if err == nil || schedulerCount(t, s, "SELECT count(*) FROM scheduler_queue") != before {
		t.Fatal("queue survived rolled-back attempt")
	}
}
