package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

type managedDispatchRepository struct{ *Store }

func (r managedDispatchRepository) ClaimNext(ctx context.Context, owner string, now time.Time) (scheduler.Result, error) {
	return r.Store.ClaimNextManaged(ctx, owner, now)
}
func (r managedDispatchRepository) ClaimRecovery(ctx context.Context, owner string, now time.Time) (*scheduler.Claim, error) {
	return r.Store.ClaimRecoveryManaged(ctx, owner, now)
}

func managedFixture(t testing.TB, scenario fake.Scenario) *dispatchFixture {
	t.Helper()
	f := newDispatchFixture(t, scenario)
	f.profile.Binding.ConfigurationRevision = "managed"
	f.profile.Binding.ProviderInstanceID = "con_fixture"
	f.profile.CredentialRef = "vault:con_fixture"
	seedAuthorizationConnection(t, f, "a", "con_fixture")
	if err := f.s.PutProfile(context.Background(), f.profile, true); err != nil {
		t.Fatal(err)
	}
	attachManaged(t, f)
	return f
}
func seedAuthorizationConnection(t testing.TB, f *dispatchFixture, workspace domain.WorkspaceID, id string) {
	t.Helper()
	_, err := f.s.db.Exec(`INSERT INTO managed_connections(connection_id,workspace_id,provider_type,label,revision,authentication,new_work,canonical_account,account_scope,active_credential_key,updated_at) VALUES(?,?,'fixture','fixture',1,'verified','enabled','fixture_account',?,'fixture_key',?)`, id, string(workspace), f.profile.AccountScope, f.clock.Now().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
}
func attachManaged(t testing.TB, f *dispatchFixture) {
	t.Helper()
	f.attach(t, f.s, nil)
	var err error
	f.engine, err = dispatch.New(managedDispatchRepository{f.s}, f.registry, f.blobs, nil, f.clock, dispatch.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
}
func authorizationService(t testing.TB, f *dispatchFixture, w domain.WorkspaceID, scopes ...auth.Scope) (*executionauth.Service, auth.Principal) {
	t.Helper()
	a, err := auth.New(f.s, f.s, nil)
	if err != nil {
		t.Fatal(err)
	}
	secret, _, err := a.Issue(context.Background(), w, scopes, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	p, err := a.Authenticate(context.Background(), secret.Reveal())
	if err != nil {
		t.Fatal(err)
	}
	service, err := executionauth.New(a, f.s, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	return service, p
}
func authorizationRequest(attempt domain.AttemptID, wall int64) []byte {
	raw, _ := json.Marshal(executionauth.Request{AttemptID: attempt, MaxRemoteWallSeconds: wall, AuthorizePrivateStaging: true, AuthorizeCompute: true})
	return raw
}
func grantAuthorization(t testing.TB, f *dispatchFixture, id scheduler.Identity) (executionauth.Receipt, *executionauth.Service, auth.Principal) {
	t.Helper()
	service, p := authorizationService(t, f, id.WorkspaceID, auth.Execute)
	r, err := service.Authorize(context.Background(), p, id.WorkspaceID, id.JobID, "grant-original-key", authorizationRequest(id.AttemptID, 10))
	if err != nil {
		t.Fatal(err)
	}
	return r, service, p
}
func managedClaim(t testing.TB, f *dispatchFixture) dispatch.Work {
	t.Helper()
	r, err := f.s.ClaimNextManaged(context.Background(), "managed_worker", f.clock.Now())
	if err != nil || r.Claim == nil {
		t.Fatalf("managed claim=%+v error=%v", r, err)
	}
	w, err := f.s.LoadDispatch(context.Background(), *r.Claim, f.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	return w
}
func TestAuthorizationConcurrentReplayAndOneGrantAcrossKeys(t *testing.T) {
	f := managedFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	service, p := authorizationService(t, f, "a", auth.Execute)
	const requests = 12
	results := make(chan executionauth.Receipt, requests)
	errs := make(chan error, requests)
	var wg sync.WaitGroup
	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := service.Authorize(context.Background(), p, "a", id.JobID, "same-grant-key", authorizationRequest(id.AttemptID, 10))
			results <- r
			errs <- e
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var grant domain.OperationID
	fresh := 0
	for r := range results {
		if grant == "" {
			grant = r.AuthorizationID
		}
		if r.AuthorizationID != grant {
			t.Fatal("duplicate receipt identity")
		}
		if !r.Replay {
			fresh++
		}
	}
	if fresh != 1 || controlCount(t, f.s, "SELECT count(*) FROM execution_authorizations") != 1 {
		t.Fatal("concurrent permit multiplication", fresh)
	}
	for _, request := range []struct {
		key  string
		wall int64
	}{{"same-grant-key", 11}, {"another-grant-key", 10}} {
		_, err := service.Authorize(context.Background(), p, "a", id.JobID, request.key, authorizationRequest(id.AttemptID, request.wall))
		if !errors.Is(err, executionauth.ErrConflict) {
			t.Fatal("changed key/payload replaced permit", err)
		}
	}
}
func TestAuthorizationCurrentAuthorityAndExactTarget(t *testing.T) {
	f := managedFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	for _, scope := range []auth.Scope{auth.Read, auth.Write, auth.Operate, auth.Manage} {
		service, p := authorizationService(t, f, "a", scope)
		if _, err := service.Authorize(context.Background(), p, "a", id.JobID, "scope-grant-key", authorizationRequest(id.AttemptID, 10)); !errors.Is(err, auth.ErrForbidden) {
			t.Fatalf("scope %s granted: %v", scope, err)
		}
	}
	service, p := authorizationService(t, f, "a", auth.Execute)
	for _, tc := range []struct {
		workspace domain.WorkspaceID
		job       domain.JobID
		attempt   domain.AttemptID
		wall      int64
		want      error
	}{
		{"b", id.JobID, id.AttemptID, 10, auth.ErrForbidden}, {"a", "missing", id.AttemptID, 10, executionauth.ErrNotFound}, {"a", id.JobID, "wrong", 10, executionauth.ErrConflict}, {"a", id.JobID, id.AttemptID, 11, executionauth.ErrRequest},
	} {
		_, err := service.Authorize(context.Background(), p, tc.workspace, tc.job, "wrong-target-key", authorizationRequest(tc.attempt, tc.wall))
		if !errors.Is(err, tc.want) {
			t.Fatalf("got %v want %v", err, tc.want)
		}
	}
	receipt, err := service.Authorize(context.Background(), p, "a", id.JobID, "valid-grant-key", authorizationRequest(id.AttemptID, 10))
	if err != nil {
		t.Fatal(err)
	}
	_, other := authorizationService(t, f, "b", auth.Execute)
	if _, err = f.s.ReadExecutionAuthorization(context.Background(), "b", other.TokenID(), id.JobID, receipt.AuthorizationID); !errors.Is(err, executionauth.ErrNotFound) {
		t.Fatal("cross-workspace receipt visible", err)
	}
	if _, err = f.s.db.Exec("UPDATE api_token_hashes SET revoked=1 WHERE token_id=?", p.TokenID()); err != nil {
		t.Fatal(err)
	}
	key, _ := admission.KeyDigest("valid-grant-key")
	req, _ := executionauth.Parse(authorizationRequest(id.AttemptID, 10))
	if _, err = f.s.AuthorizeExecution(context.Background(), executionauth.Command{WorkspaceID: "a", TokenID: p.TokenID(), JobID: id.JobID, KeyDigest: key, Request: req, Now: f.clock.Now()}); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("transaction accepted revoked replay", err)
	}
}
func TestAuthorizationReplayOriginalAndReadConsumedAfterRestart(t *testing.T) {
	f := managedFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	original, _, _ := grantAuthorization(t, f, id)
	f.restart(t)
	attachManaged(t, f)
	work := managedClaim(t, f)
	plan, op := storePlan(t, work)
	if _, err := f.s.CommitDispatch(context.Background(), work.Handle, dispatch.Action{Kind: dispatch.BeginPreparation, Plan: &plan, PreparationID: op}, f.clock.Now()); !errors.Is(err, dispatch.ErrPolicy) {
		t.Fatal("preparation bypassed consume", err)
	}
	bad := work.Handle
	bad.Claim.Fence = strings.Repeat("b", 64)
	if _, err := f.s.ConsumeAuthorization(context.Background(), bad, f.clock.Now()); !errors.Is(err, scheduler.ErrLeaseLost) {
		t.Fatal("unfenced consumption", err)
	}
	if _, err := f.s.ConsumeAuthorization(context.Background(), work.Handle, f.clock.Now()); err != nil {
		t.Fatal(err)
	}
	// Crash after consumption but before any remote intent. The next owner may only
	// continue this same attempt; receipt replay remains the original grant.
	f.restart(t)
	attachManaged(t, f)
	f.clock.advance(time.Minute)
	service, p := authorizationService(t, f, "a", auth.Execute)
	replay, err := service.Authorize(context.Background(), p, "a", id.JobID, "grant-original-key", authorizationRequest(id.AttemptID, 10))
	if err != nil {
		t.Fatal(err)
	}
	expected := original
	expected.Replay = true
	if !reflect.DeepEqual(replay, expected) {
		t.Fatal("POST replay returned current state", replay)
	}
	current, err := service.Get(context.Background(), p, "a", id.JobID, original.AuthorizationID)
	if err != nil || current.Status != executionauth.Consumed || current.ConsumedAt == nil {
		t.Fatal("consumption lost", current, err)
	}
	if _, err = f.s.ConsumeAuthorization(context.Background(), work.Handle, f.clock.Now()); !errors.Is(err, scheduler.ErrLeaseLost) {
		t.Fatal("old claim survived restart/expiry", err)
	}
	if err = f.step(t); err != nil {
		t.Fatal(err)
	}
	if f.backend.Stats().PrepareCalls != 1 || controlCount(t, f.s, "SELECT count(*) FROM execution_authorizations") != 1 {
		t.Fatal("restart minted grant or duplicated preparation")
	}
	f.restart(t)
	attachManaged(t, f)
	if err = f.step(t); err != nil {
		t.Fatal(err)
	}
	if f.backend.Stats().PrepareCalls != 1 || f.backend.Stats().SubmitCalls != 1 {
		t.Fatal("same permit repeated mutation")
	}
}
func TestAuthorizationManagedAndStandaloneClaimIsolation(t *testing.T) {
	f := managedFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	r, err := f.s.ClaimNextManaged(context.Background(), "managed", f.clock.Now())
	if err != nil || r.Claim != nil || len(r.View.Heads) != 1 || r.View.Heads[0].Reason != scheduler.AuthorizationRequired {
		t.Fatal("ungranted managed job claimed", r, err)
	}
	grantAuthorization(t, f, id)
	r, err = f.s.ClaimNext(context.Background(), "standalone", f.clock.Now())
	if err != nil || r.Claim != nil {
		t.Fatal("standalone claimed managed permit", r, err)
	}
	if err = f.step(t); err != nil {
		t.Fatal(err)
	}
	f.clock.advance(time.Minute)
	recovery, err := f.s.ClaimRecovery(context.Background(), "standalone", f.clock.Now())
	if err != nil || recovery != nil {
		t.Fatal("standalone claimed managed recovery", recovery, err)
	}
	standalone := newDispatchFixture(t, fake.DefaultScenario())
	sid := standalone.seed(t, "a", 1, false)
	service, p := authorizationService(t, standalone, "a", auth.Execute)
	if _, err = service.Authorize(context.Background(), p, "a", sid.JobID, "standalone-grant", authorizationRequest(sid.AttemptID, 10)); !errors.Is(err, executionauth.ErrState) {
		t.Fatal("standalone became managed", err)
	}
	if err = standalone.step(t); err != nil {
		t.Fatal(err)
	}
	if standalone.backend.Stats().PrepareCalls != 1 || controlCount(t, standalone.s, "SELECT count(*) FROM execution_authorizations") != 0 {
		t.Fatal("standalone requires grant")
	}
}
func TestAuthorizationAmbiguousSubmissionAndRetryNeedSeparateGrant(t *testing.T) {
	t.Run("ambiguous", func(t *testing.T) {
		scenario := fake.DefaultScenario()
		scenario.Mode = fake.AcceptLoseResponse
		f := managedFixture(t, scenario)
		id := f.seed(t, "a", 1, false)
		grantAuthorization(t, f, id)
		if err := f.step(t); err != nil {
			t.Fatal(err)
		}
		if err := f.step(t); err != nil {
			t.Fatal(err)
		}
		if f.journal(t, id).Phase != dispatch.Submitting {
			t.Fatal("ambiguity was hidden")
		}
		f.restart(t)
		attachManaged(t, f)
		if err := f.step(t); err != nil {
			t.Fatal(err)
		}
		if stats := f.backend.Stats(); stats.SubmitCalls != 1 || stats.Executions != 1 {
			t.Fatal("ambiguous submission duplicated", stats)
		}
	})
	t.Run("retry", func(t *testing.T) {
		f := managedFixture(t, fake.DefaultScenario())
		id := f.seed(t, "a", 1, false)
		grantAuthorization(t, f, id)
		control, p, _ := controlService(t, f, "a", auth.Operate)
		cancelControl(t, f, control, p, id)
		retry, err := control.Submit(context.Background(), p, "a", id.JobID, domain.OperationRetryCompute, "retry-grant-key", controlRequest(id.AttemptID))
		if err != nil {
			t.Fatal(err)
		}
		if retry.Effect != operations.NewAttemptCreated {
			t.Fatal("retry missing")
		}
		r, err := f.s.ClaimNextManaged(context.Background(), "after_retry", f.clock.Now())
		if err != nil || r.Claim != nil {
			t.Fatal("retry inherited authorization", r, err)
		}
		service, execute := authorizationService(t, f, "a", auth.Execute)
		if _, err = service.Authorize(context.Background(), execute, "a", id.JobID, "retry-new-permit", authorizationRequest(retry.NewAttemptID, 10)); err != nil {
			t.Fatal(err)
		}
		if err = f.step(t); err != nil {
			t.Fatal(err)
		}
		if f.backend.Stats().PrepareCalls != 1 || controlCount(t, f.s, "SELECT count(*) FROM execution_authorizations") != 2 {
			t.Fatal("retry did not keep separate permit")
		}
	})
}

// seedAuthorizationGPU creates canonical immutable fixtures through the same local
// tables as dispatchFixture.seed. No provider or credential store is contacted.
func seedAuthorizationGPU(t testing.TB, f *dispatchFixture, w domain.WorkspaceID, n int) scheduler.Identity {
	t.Helper()
	ctx := context.Background()
	profile := f.profile
	if w != "a" {
		connection := "con_fixture_" + string(w)
		seedAuthorizationConnection(t, f, w, connection)
		profile.Binding.Profile += "-" + string(w)
		profile.Binding.ConfigurationRevision = "managed_" + string(w)
		profile.Binding.ProviderInstanceID = domain.ProviderInstanceID(connection)
		profile.CredentialRef = "vault:" + connection
		if err := f.s.PutProfile(ctx, profile, true); err != nil {
			t.Fatal(err)
		}
	}
	raw := strings.Replace(dispatchJSON, `"accelerator":"cpu"`, `"accelerator":"gpu","minimum_gpu_count":1`, 1)
	raw = strings.Replace(raw, `"profile":"fixture"`, `"profile":"`+profile.Binding.Profile+`"`, 1)
	req, err := admission.Parse([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	id := scheduler.Identity{WorkspaceID: w, JobID: domain.JobID(fmt.Sprintf("gpu_job_%d", n)), AttemptID: domain.AttemptID(fmt.Sprintf("gpu_attempt_%d", n))}
	at := f.clock.Now().Add(-time.Millisecond).Format(time.RFC3339Nano)
	state, _ := json.Marshal(domain.InitialAttemptState())
	err = withTx(ctx, f.s.db, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, "INSERT INTO jobs VALUES(?,?,?,?,?,?,?,?,?)", string(w), string(id.JobID), string(req.Canonical()), admission.CanonicalVersion, string(req.Hash()), profile.Binding.Profile, profile.Binding.ConfigurationRevision, string(id.AttemptID), at); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO attempts VALUES(?,?,?,?,?,?,?,?,?,?)", string(w), string(id.JobID), string(id.AttemptID), 1, strings.Repeat("a", 64), string(state), "queued", 1, at, at); err != nil {
			return err
		}
		for role, object := range map[string]string{"bundle": "code", "input:0": "data"} {
			if _, err := tx.ExecContext(ctx, "INSERT INTO job_objects SELECT ?,?,?,object_id,bytes,sha256 FROM objects WHERE workspace_id=? AND object_id=?", string(w), string(id.JobID), role, string(w), object); err != nil {
				return err
			}
		}
		_, err := tx.ExecContext(ctx, "INSERT INTO events(workspace_id,job_id,event_id,sequence,attempt_id,type,occurred_at) VALUES(?,?,?,1,?,'job.accepted',?)", string(w), string(id.JobID), "accepted_"+string(id.AttemptID), string(id.AttemptID), at)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func TestAuthorizationQuotaReservationsAndUnresolvedInputs(t *testing.T) {
	f := managedFixture(t, fake.DefaultScenario())
	a := seedAuthorizationGPU(t, f, "a", 1)
	b := seedAuthorizationGPU(t, f, "b", 2)
	sa, pa := authorizationService(t, f, "a", auth.Execute)
	sb, pb := authorizationService(t, f, "b", auth.Execute)
	grant := func(service *executionauth.Service, p auth.Principal, id scheduler.Identity) (executionauth.Receipt, error) {
		return service.Authorize(context.Background(), p, id.WorkspaceID, id.JobID, "quota-permit-key", authorizationRequest(id.AttemptID, 10))
	}
	if _, err := grant(sa, pa, a); !errors.Is(err, executionauth.ErrQuota) {
		t.Fatal("unknown quota granted", err)
	}
	remaining, limit, used := 15.0, 15.0, 0.0
	q := provider.QuotaObservation{Status: provider.QuotaKnown, Resource: "gpu", Limit: &limit, Used: &used, Remaining: &remaining, Unit: "seconds", ObservedAt: f.clock.Now(), Source: "fixture", Precision: "lower_bound"}
	if err := f.s.RecordQuota(context.Background(), f.profile.AccountScope, "gpu", q, f.clock.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := grant(sa, pa, a); err != nil {
		t.Fatal(err)
	}
	if _, err := grant(sb, pb, b); !errors.Is(err, executionauth.ErrQuota) {
		t.Fatal("shared quota permit overbooking", err)
	}
	err := withTx(context.Background(), f.s.db, func(tx *sql.Tx) error {
		capacity, err := managedCapacity(context.Background(), tx, f.profile.AccountScope, f.clock.Now())
		if err == nil && (capacity.RemainingSeconds == nil || *capacity.RemainingSeconds != 5 || capacity.ReservedAttempts != 1) {
			t.Fatalf("wrong shared projection: %+v", capacity)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	// Its own reservation must not prevent the already-authorized attempt from starting.
	r, err := f.s.ClaimNextManaged(context.Background(), "quota_worker", f.clock.Now())
	if err != nil || r.Claim == nil || r.Claim.Identity != a {
		t.Fatal("own reservation double counted", r, err)
	}
	f.clock.advance(6 * time.Minute)
	if _, err = grant(sb, pb, b); !errors.Is(err, executionauth.ErrQuota) {
		t.Fatal("stale quota granted", err)
	}
	g := managedFixture(t, fake.DefaultScenario())
	url := g.seed(t, "a", 3, true)
	service, p := authorizationService(t, g, "a", auth.Execute)
	if _, err = service.Authorize(context.Background(), p, "a", url.JobID, "unfrozen-grant-key", authorizationRequest(url.AttemptID, 10)); !errors.Is(err, executionauth.ErrInputs) {
		t.Fatal("unfrozen URL authorized", err)
	}
}

func TestAuthorizationCollectionClaimModeIsolation(t *testing.T) {
	t.Run("managed", func(t *testing.T) {
		f := managedFixture(t, fake.DefaultScenario())
		id := f.seed(t, "a", 1, false)
		grantAuthorization(t, f, id)
		submittedControl(t, f, id)
		for i := 0; i < 2; i++ {
			if err := f.adapter.Advance(*f.journal(t, id).Remote); err != nil {
				t.Fatal(err)
			}
			if err := f.step(t); err != nil {
				t.Fatal(err)
			}
		}
		if work, err := f.s.ClaimCollection(context.Background(), f.clock.Now(), time.Minute, 1); err != nil || work != nil {
			t.Fatal("standalone claimed managed collection", work, err)
		}
		if count := controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='collect'"); count != 0 {
			t.Fatal("standalone created managed collection ticket", count)
		}
		work, err := f.s.ClaimCollectionManaged(context.Background(), f.clock.Now(), time.Minute, 1)
		if err != nil || work == nil || work.Lease.AttemptID != id.AttemptID {
			t.Fatal("missing managed collection", work, err)
		}
		f.clock.advance(2 * time.Minute)
		if work, err = f.s.ClaimCollection(context.Background(), f.clock.Now(), time.Minute, 1); err != nil || work != nil {
			t.Fatal("standalone reclaimed managed ticket", work, err)
		}
		if work, err = f.s.ClaimCollectionManaged(context.Background(), f.clock.Now(), time.Minute, 1); err != nil || work == nil {
			t.Fatal("managed ticket not recoverable", work, err)
		}
		if count := controlCount(t, f.s, "SELECT count(*) FROM execution_authorizations"); count != 1 {
			t.Fatal("collection created authorization", count)
		}
	})
	t.Run("standalone", func(t *testing.T) {
		f, _, _, _ := collectionFixture(t)
		if work, err := f.s.ClaimCollectionManaged(context.Background(), f.clock.Now(), time.Minute, 1); err != nil || work != nil {
			t.Fatal("managed claimed standalone collection", work, err)
		}
		if count := controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='collect'"); count != 0 {
			t.Fatal("managed created standalone ticket", count)
		}
		work, err := f.s.ClaimCollection(context.Background(), f.clock.Now(), time.Minute, 1)
		if err != nil || work == nil {
			t.Fatal("standalone collection missing", work, err)
		}
		f.clock.advance(2 * time.Minute)
		if work, err = f.s.ClaimCollectionManaged(context.Background(), f.clock.Now(), time.Minute, 1); err != nil || work != nil {
			t.Fatal("managed reclaimed standalone ticket", work, err)
		}
	})
}

func TestAuthorizationConcurrentDifferentKeysHaveOneWinner(t *testing.T) {
	f := managedFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	service, p := authorizationService(t, f, "a", auth.Execute)
	const count = 12
	results := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := service.Authorize(context.Background(), p, "a", id.JobID, fmt.Sprintf("distinct-permit-key-%d", i), authorizationRequest(id.AttemptID, 10))
			results <- err
		}(i)
	}
	wg.Wait()
	close(results)
	won := 0
	for err := range results {
		if err == nil {
			won++
		} else if !errors.Is(err, executionauth.ErrConflict) {
			t.Fatal(err)
		}
	}
	if won != 1 || controlCount(t, f.s, "SELECT count(*) FROM execution_authorizations") != 1 {
		t.Fatal("distinct keys multiplied grant", won)
	}
}

func TestAuthorizationConnectionOwnershipAndDisabledContinuation(t *testing.T) {
	for _, mode := range []string{"foreign_workspace", "missing_credential", "removed", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			f := managedFixture(t, fake.DefaultScenario())
			id := f.seed(t, "a", 1, false)
			service, p := authorizationService(t, f, "a", auth.Execute)
			statement := map[string]string{
				"foreign_workspace":  "UPDATE managed_connections SET workspace_id='b'",
				"missing_credential": "UPDATE managed_connections SET active_credential_key=''",
				"removed":            "UPDATE managed_connections SET new_work='removed'",
				"disabled":           "UPDATE managed_connections SET new_work='disabled'",
			}[mode]
			if _, err := f.s.db.Exec(statement); err != nil {
				t.Fatal(err)
			}
			_, err := service.Authorize(context.Background(), p, "a", id.JobID, "ownership-permit-key", authorizationRequest(id.AttemptID, 10))
			if mode == "disabled" {
				if err != nil {
					t.Fatal("disable retargeted an admitted job", err)
				}
			} else if !errors.Is(err, auth.ErrForbidden) {
				t.Fatal("unauthorized credential slot granted", err)
			}
		})
	}
}

func TestAuthorizationFrozenPermitMismatchPreventsMutation(t *testing.T) {
	f := managedFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	grantAuthorization(t, f, id)
	work := managedClaim(t, f)
	if _, err := f.s.db.Exec("UPDATE execution_authorizations SET inputs_sha256=?", strings.Repeat("b", 64)); err == nil {
		t.Fatal("frozen permit allowed routine update")
	}
	// Corrupt only a disposable fixture, then prove dispatch fails closed instead of
	// treating the row's existence as sufficient permission for different input bytes.
	if _, err := f.s.db.Exec("DROP TRIGGER immutable_execution_authorization"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.db.Exec("UPDATE execution_authorizations SET inputs_sha256=?", strings.Repeat("b", 64)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.ConsumeAuthorization(context.Background(), work.Handle, f.clock.Now()); !errors.Is(err, ErrCorrupt) {
		t.Fatal("mismatched input permit consumed", err)
	}
	if f.backend.Stats().PrepareCalls != 0 || controlCount(t, f.s, "SELECT count(*) FROM provider_resources") != 0 {
		t.Fatal("corrupt permit reached mutation")
	}
}
