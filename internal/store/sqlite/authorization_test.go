package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func managedFixture(t testing.TB, scenario fake.Scenario, maxActive ...int) *dispatchFixture {
	t.Helper()
	f := newDispatchFixture(t, scenario)
	f.authorizeSeed = false
	f.profile.MaxActiveAttempts = 1
	f.profile.Binding.ConfigurationRevision = "managed"
	f.profile.Binding.ProviderInstanceID = "con_fixture"
	f.profile.CredentialRef = "vault:con_fixture"
	if len(maxActive) == 1 {
		f.profile.MaxActiveAttempts = maxActive[0]
	}
	seedAuthorizationConnection(t, f, "a", "con_fixture")
	requireNoError(t, f.s.PutProfile(context.Background(), f.profile, true))
	attachManaged(t, f)
	return f
}
func seedAuthorizationConnection(t testing.TB, f *dispatchFixture, workspace domain.WorkspaceID, id string) {
	t.Helper()
	_, err := f.s.db.Exec(`INSERT INTO managed_connections(connection_id,workspace_id,provider_type,label,revision,authentication,new_work,canonical_account,account_scope,active_credential_key,updated_at) VALUES(?,?,'fixture','fixture',1,'verified','enabled','fixture_account',?,'fixture_key',?)`, id, string(workspace), f.profile.AccountScope, f.clock.Now().Format(time.RFC3339Nano))
	requireNoError(t, err)
}
func attachManaged(t testing.TB, f *dispatchFixture) {
	t.Helper()
	f.attach(t, f.s)
	var err error
	f.engine, err = dispatch.New(f.s, f.registry, f.blobs, f.clock, dispatch.DefaultConfig())
	requireNoError(t, err)
}
func authorizationService(t testing.TB, f *dispatchFixture, w domain.WorkspaceID, scopes ...auth.Scope) (*executionauth.Service, auth.Principal) {
	t.Helper()
	a, err := auth.New(f.s, f.s)
	requireNoError(t, err)
	secret, _, err := a.Issue(context.Background(), w, scopes, time.Time{})
	requireNoError(t, err)
	p, err := a.Authenticate(context.Background(), secret.Reveal())
	requireNoError(t, err)
	service, err := executionauth.New(a, f.s, f.clock)
	requireNoError(t, err)
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
	requireNoError(t, err)
	return r, service, p
}
func managedClaim(t testing.TB, f *dispatchFixture) dispatch.Work {
	t.Helper()
	r, err := f.s.ClaimNext(context.Background(), "managed_worker")
	if err != nil || r.Claim == nil {
		t.Fatalf("managed claim=%+v error=%v", r, err)
	}
	w, err := f.s.LoadDispatch(context.Background(), *r.Claim)
	requireNoError(t, err)
	return w
}
func TestAuthorizationConcurrentGrantsHaveOneWinner(t *testing.T) {
	for _, tc := range []struct {
		name      string
		key       func(int) string
		replays   int
		conflicts int
	}{
		{"same key", func(int) string { return "same-grant-key" }, 11, 0},
		{"distinct keys", func(i int) string { return fmt.Sprintf("distinct-permit-key-%d", i) }, 0, 11},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := managedFixture(t, fake.DefaultScenario())
			id := f.seed(t, "a", 1, false)
			service, actor := authorizationService(t, f, "a", auth.Execute)
			const requests = 12
			type result struct {
				receipt executionauth.Receipt
				err     error
			}
			results := make(chan result, requests)
			var wg sync.WaitGroup
			for i := range requests {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					r, err := service.Authorize(context.Background(), actor, "a", id.JobID, tc.key(i), authorizationRequest(id.AttemptID, 10))
					results <- result{r, err}
				}(i)
			}
			wg.Wait()
			close(results)
			var grant domain.OperationID
			fresh, replays, conflicts := 0, 0, 0
			for result := range results {
				if errors.Is(result.err, executionauth.ErrConflict) {
					conflicts++
					continue
				}
				if result.err != nil {
					t.Fatal(result.err)
				}
				if grant == "" {
					grant = result.receipt.AuthorizationID
				}
				if result.receipt.AuthorizationID != grant {
					t.Fatal("concurrent grant changed receipt identity")
				}
				if result.receipt.Replay {
					replays++
				} else {
					fresh++
				}
			}
			if fresh != 1 || replays != tc.replays || conflicts != tc.conflicts ||
				controlCount(t, f.s, "SELECT count(*) FROM attempt_grants WHERE kind='execution'") != 1 {
				t.Fatal("concurrent permit multiplication", fresh, replays, conflicts)
			}
		})
	}
}

func TestAuthorizationTransactionRejectsRevokedReplay(t *testing.T) {
	f := managedFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	service, p := authorizationService(t, f, "a", auth.Execute)
	if _, err := service.Authorize(context.Background(), p, "a", id.JobID, "valid-grant-key", authorizationRequest(id.AttemptID, 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.db.Exec("UPDATE api_token_hashes SET revoked=1 WHERE token_id=?", p.TokenID()); err != nil {
		t.Fatal(err)
	}
	key, _ := admission.KeyDigest("valid-grant-key")
	req, _ := executionauth.Parse(authorizationRequest(id.AttemptID, 10))
	if _, err := f.s.AuthorizeExecution(context.Background(), executionauth.Command{WorkspaceID: "a", TokenID: p.TokenID(), JobID: id.JobID, KeyDigest: key, Request: req}); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("transaction accepted revoked replay", err)
	}
}
func TestAuthorizationConsumptionIsFencedAndRequiredBeforeIntent(t *testing.T) {
	f := managedFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	grantAuthorization(t, f, id)
	f.restart(t)
	attachManaged(t, f)
	work := managedClaim(t, f)
	plan, op := storePlan(t, work)
	if _, err := f.s.CommitDispatch(context.Background(), work.Handle, dispatch.Action{Kind: dispatch.BeginPreparation, Plan: &plan, PreparationID: op}); !errors.Is(err, dispatch.ErrPolicy) {
		t.Fatal("preparation bypassed consume", err)
	}
	bad := work.Handle
	bad.Claim.Fence = strings.Repeat("b", 64)
	if _, err := f.s.ConsumeAuthorization(context.Background(), bad); !errors.Is(err, scheduler.ErrLeaseLost) {
		t.Fatal("unfenced consumption", err)
	}
	if _, err := f.s.ConsumeAuthorization(context.Background(), work.Handle); err != nil {
		t.Fatal(err)
	}
	// Crash after consumption but before any remote intent. The next owner may only
	// continue this same attempt; receipt replay remains the original grant.
	f.restart(t)
	attachManaged(t, f)
	f.clock.advance(time.Minute)
	if _, err := f.s.ConsumeAuthorization(context.Background(), work.Handle); !errors.Is(err, scheduler.ErrLeaseLost) {
		t.Fatal("old claim survived restart/expiry", err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM attempt_grants WHERE kind='execution' AND consumed_at IS NOT NULL") != 1 || controlCount(t, f.s, "SELECT count(*) FROM submission_intents") != 0 {
		t.Fatal("consume boundary changed grants or created remote intent")
	}
}
func TestEveryClaimRequiresDurableAuthorization(t *testing.T) {
	f := managedFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	r, err := f.s.ClaimNext(context.Background(), "worker")
	if err != nil || r.Claim != nil || len(r.View.Heads) != 1 || r.View.Heads[0].Reason != scheduler.AuthorizationRequired {
		t.Fatal("ungranted attempt claimed", r, err)
	}
	grantAuthorization(t, f, id)
	err = f.step(t)
	requireNoError(t, err)
	if f.backend.Stats().PrepareCalls != 1 {
		t.Fatal("authorized attempt did not prepare exactly once")
	}
	f.clock.advance(time.Minute)
	recovery, err := f.s.ClaimRecovery(context.Background(), "recovery")
	if err != nil || recovery == nil || recovery.Identity != id {
		t.Fatal("single recovery path lost original attempt", recovery, err)
	}
}
func TestAuthorizationRetryCreatesDistinctGrantRows(t *testing.T) {
	f := managedFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	grantAuthorization(t, f, id)
	control, p, _ := controlService(t, f, "a", auth.Operate)
	cancelControl(t, f, control, p, id)
	retry, err := control.Submit(context.Background(), p, "a", id.JobID, domain.OperationRetryCompute, "retry-grant-key", controlRequest(id.AttemptID))
	requireNoError(t, err)
	service, execute := authorizationService(t, f, "a", auth.Execute)
	if _, err = service.Authorize(context.Background(), execute, "a", id.JobID, "retry-new-permit", authorizationRequest(retry.NewAttemptID, 10)); err != nil {
		t.Fatal(err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM attempt_grants WHERE kind='execution'") != 2 || controlCount(t, f.s, "SELECT count(DISTINCT attempt_id) FROM attempt_grants WHERE kind='execution'") != 2 {
		t.Fatal("retry did not retain distinct attempt grants")
	}
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
		requireNoError(t, f.s.PutProfile(ctx, profile, true))
	}
	raw := strings.Replace(dispatchJSON, `"accelerator":"cpu"`, `"accelerator":"gpu","minimum_gpu_count":1`, 1)
	raw = strings.Replace(raw, `"profile":"fixture"`, `"profile":"`+profile.Binding.Profile+`"`, 1)
	req, err := admission.Parse([]byte(raw))
	requireNoError(t, err)
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
	requireNoError(t, err)
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
	requireNoError(t, f.s.RecordQuota(context.Background(), f.profile.AccountScope, "gpu", q))
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
	requireNoError(t, err)
	// Its own reservation must not prevent the already-authorized attempt from starting.
	r, err := f.s.ClaimNext(context.Background(), "quota_worker")
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

func TestCollectionRecoveryReusesOriginalPermitAndOperation(t *testing.T) {
	f, id, _, _ := collectionFixture(t)
	ctx := context.Background()
	work, err := f.s.ClaimCollection(ctx, time.Minute, 1)
	if err != nil || work == nil || work.Lease.AttemptID != id.AttemptID {
		t.Fatal("collection claim missing", work, err)
	}
	original := work.Lease
	binding := work.Binding
	if count := controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='collect'"); count != 1 {
		t.Fatal("collection created duplicate tickets", count)
	}
	if again, err := f.s.ClaimCollection(ctx, time.Minute, 1); err != nil || again != nil {
		t.Fatal("live collection claimed twice", again, err)
	}
	f.clock.advance(2 * time.Minute)
	f.restart(t)
	work, err = f.s.ClaimCollection(ctx, time.Minute, 1)
	if err != nil || work == nil || work.Lease.OperationID != original.OperationID || work.Lease.AttemptID != original.AttemptID || work.Lease.Generation != original.Generation+1 || work.Lease.Fence == original.Fence || work.Binding != binding {
		t.Fatal("collection recovery changed identity or lost fencing", work, err)
	}
	if count := controlCount(t, f.s, "SELECT count(*) FROM attempt_grants WHERE kind='execution'"); count != 1 {
		t.Fatal("collection created authorization", count)
	}
	assertNoNewCompute(t, f)
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
	if _, err := f.s.db.Exec("UPDATE attempt_grants SET inputs_sha256=?", strings.Repeat("b", 64)); err == nil {
		t.Fatal("frozen permit allowed routine update")
	}
	// Corrupt only a disposable fixture, then prove dispatch fails closed instead of
	// treating the row's existence as sufficient permission for different input bytes.
	if _, err := f.s.db.Exec("DROP TRIGGER immutable_attempt_grant"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.db.Exec("UPDATE attempt_grants SET inputs_sha256=?", strings.Repeat("b", 64)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.ConsumeAuthorization(context.Background(), work.Handle); !errors.Is(err, ErrCorrupt) {
		t.Fatal("mismatched input permit consumed", err)
	}
	if f.backend.Stats().PrepareCalls != 0 || controlCount(t, f.s, "SELECT count(*) FROM provider_resources") != 0 {
		t.Fatal("corrupt permit reached mutation")
	}
}
