package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

const multiDayWall = domain.MaxRemoteWallSeconds

// longWallAdapter advertises the SSH host maximum; the embedded fixture keeps Kaggle's.
type longWallAdapter struct{ *stateAdapter }

func (a longWallAdapter) Descriptor() connections.Descriptor {
	d := a.stateAdapter.Descriptor()
	d.Type = "long_fixture"
	d.Capabilities.MaxRemoteWallSeconds = multiDayWall
	return d
}

func wallJob(profile string, wall int64) []byte {
	raw := strings.Replace(dispatchJSON, `"profile":"fixture"`, `"profile":"`+profile+`"`, 1)
	return []byte(strings.Replace(raw, `"remote_wall_seconds":10`, fmt.Sprintf(`"remote_wall_seconds":%d`, wall), 1))
}

func TestMultiDayWallFollowsEachProviderAdvertisedMaximum(t *testing.T) {
	ctx := context.Background()
	f := newManagedStateFixture(t)
	var err error
	f.service, err = connections.New(f.access, f.s, f.vault, []connections.Adapter{f.discovery, longWallAdapter{f.discovery}})
	requireNoError(t, err)
	short := f.create(t, "a", "short-create", "synthetic-short")
	body, _ := json.Marshal(map[string]any{"provider_type": "long_fixture", "label": "Long host", "credentials": map[string]string{"api_token": "synthetic-long"}})
	op, err := f.service.Submit(ctx, f.principals["a"], "a", "", "long-create", body)
	requireNoError(t, err)
	f.run(t)
	long := f.get(t, "a", op.ConnectionID)

	// A provider capped at one day refuses a longer connection limit before any change.
	if _, err = f.service.Submit(ctx, f.principals["a"], "a", short.ID, "short-over", configureBody(short.Revision, 86401)); !errors.Is(err, connections.ErrRequest) {
		t.Fatal("one-day provider accepted a longer connection limit", err)
	}
	if _, err = f.service.Submit(ctx, f.principals["a"], "a", long.ID, "long-over", configureBody(long.Revision, multiDayWall+1)); !errors.Is(err, connections.ErrRequest) {
		t.Fatal("contract maximum exceeded", err)
	}

	admit, err := admission.New(f.access, f.s, admission.DefaultLimits())
	requireNoError(t, err)
	configureConnection(t, f, long, "long-two-days", 172800)
	long = f.get(t, "a", long.ID)
	if _, err = admit.Submit(ctx, f.principals["a"], "a", "above-connection", wallJob(long.Selection.Profile, 172801)); !errors.Is(err, admission.ErrRequirements) {
		t.Fatal("job above the connection limit was admitted", err)
	}
	configureConnection(t, f, long, "long-seven-days", multiDayWall)
	long = f.get(t, "a", long.ID)
	if long.Configuration.MaxRemoteWallSeconds != multiDayWall || long.Selection.MaxRemoteWallSeconds != multiDayWall {
		t.Fatal("seven-day connection limit was not stored", long.Configuration, long.Selection)
	}
	receipt, err := admit.Submit(ctx, f.principals["a"], "a", "seven-day-job", wallJob(long.Selection.Profile, multiDayWall))
	requireNoError(t, err)
	permits, err := executionauth.New(f.access, f.s, f.clock)
	requireNoError(t, err)
	if _, err = permits.Authorize(ctx, f.principals["a"], "a", receipt.JobID, "seven-day-grant", authorizationRequest(receipt.AttemptID, multiDayWall)); err != nil {
		t.Fatal("seven-day execution grant refused", err)
	}
	var stored int64
	if err = f.s.db.QueryRow(`SELECT max_remote_wall_seconds FROM attempt_grants WHERE kind='execution' AND job_id=?`, string(receipt.JobID)).Scan(&stored); err != nil || stored != multiDayWall {
		t.Fatal("grant budget changed", stored, err)
	}
	configureConnection(t, f, short, "short-one-day", 86400)
	short = f.get(t, "a", short.ID)
	if _, err = admit.Submit(ctx, f.principals["a"], "a", "short-job", wallJob(short.Selection.Profile, 86400)); err != nil {
		t.Fatal("one-day job refused", err)
	}
	if _, err = admit.Submit(ctx, f.principals["a"], "a", "short-over-job", wallJob(short.Selection.Profile, 86401)); !errors.Is(err, admission.ErrRequirements) {
		t.Fatal("one-day provider admitted a longer job", err)
	}
}

func multiDayGateFixture(t *testing.T) *dispatchFixture {
	t.Helper()
	scenario := fake.DefaultScenario()
	scenario.SeparateStart, scenario.EphemeralSecrets, scenario.Mode = true, true, fake.Accept
	f := newDispatchFixture(t, scenario)
	f.authorizeSeed = false
	f.profile.Binding.ConfigurationRevision, f.profile.Binding.ProviderInstanceID = "managed", "con_fixture"
	f.profile.CostClass, f.profile.MaxRemoteWallSeconds, f.profile.MaxActiveAttempts = admission.CostOperatorManaged, multiDayWall, 1
	seedAuthorizationConnection(t, f, "a", "con_fixture")
	requireNoError(t, f.s.PutProfile(context.Background(), f.profile, true))
	attachGate(t, f)
	return f
}

func TestMultiDayWallStartsAfterOneShotSecretDelivery(t *testing.T) {
	f := multiDayGateFixture(t)
	raw := strings.Replace(string(wallJob("fixture", multiDayWall)), `"name":"dispatch-fixture"`, `"name":"dispatch-fixture","execution_control":"prepare_then_start","workload_secrets":[{"name":"ingest","exposure":"file"}]`, 1)
	id := f.seedRaw(t, "a", 1, false, raw)
	gate, p, ready := readyGate(t, f, id)
	secrets, _ := attachSecrets(t, f, gate)
	supplied, err := secrets.Supply(t.Context(), p, "a", id.JobID, "multi-day-secret", secretRequest(f, id, ready))
	requireNoError(t, err)
	request := executionauth.StartRequest{SecretAuthorizationID: supplied.AuthorizationID, AttemptID: id.AttemptID, ReadinessRevision: ready.Revision,
		MaxRemoteWallSeconds: multiDayWall, ExecutionDeadline: f.clock.Now().Add(time.Duration(multiDayWall) * time.Second).UTC().Format(time.RFC3339), AuthorizeCompute: true}
	over := request
	over.MaxRemoteWallSeconds = multiDayWall + 1
	overRaw, _ := json.Marshal(over)
	if _, err = gate.Start(t.Context(), p, "a", id.JobID, "multi-day-over", overRaw); err == nil {
		t.Fatal("start above the contract maximum accepted")
	}
	startRaw, _ := json.Marshal(request)
	// The secret expires within a minute; it bounds delivery at start, not the seven-day run.
	if _, err = gate.Start(t.Context(), p, "a", id.JobID, "multi-day-start", startRaw); err != nil {
		t.Fatal("seven-day start with a short-lived secret refused", err)
	}
	var g *executionauth.Gate
	err = withTx(t.Context(), f.s.db, func(tx *sql.Tx) error {
		r, err := readJobRecord(t.Context(), tx, "a", id.JobID)
		if err == nil {
			g, err = loadExecutionGate(t.Context(), tx, r, f.clock.Now())
		}
		return err
	})
	if err != nil || g.Start == nil || g.Start.Secrets == nil || g.Start.StartBefore != g.Start.Secrets.ExpiresAt || g.Start.ExecutionDeadline != request.ExecutionDeadline {
		t.Fatal("start window was not bounded by secret expiry", g, err)
	}
	if worked, err := gateStep(t, f); err != nil || !worked {
		t.Fatal(worked, err)
	}
	var intents int
	if err = f.s.db.QueryRow("SELECT count(*) FROM submission_intents WHERE status='accepted'").Scan(&intents); err != nil || intents != 1 || f.backend.Stats().SubmitCalls != 1 {
		t.Fatal("seven-day attempt did not start", intents, err)
	}
}

func TestMultiDayWallSecretExpiryBeforeDispatchExpiresStart(t *testing.T) {
	f := multiDayGateFixture(t)
	raw := strings.Replace(string(wallJob("fixture", multiDayWall)), `"name":"dispatch-fixture"`, `"name":"dispatch-fixture","execution_control":"prepare_then_start","workload_secrets":[{"name":"ingest","exposure":"file"}]`, 1)
	id := f.seedRaw(t, "a", 1, false, raw)
	gate, p, ready := readyGate(t, f, id)
	secrets, _ := attachSecrets(t, f, gate)
	supplied, err := secrets.Supply(t.Context(), p, "a", id.JobID, "expiring-secret", secretRequest(f, id, ready))
	requireNoError(t, err)
	startRaw, _ := json.Marshal(executionauth.StartRequest{SecretAuthorizationID: supplied.AuthorizationID, AttemptID: id.AttemptID, ReadinessRevision: ready.Revision,
		MaxRemoteWallSeconds: multiDayWall, ExecutionDeadline: f.clock.Now().Add(time.Duration(multiDayWall) * time.Second).UTC().Format(time.RFC3339), AuthorizeCompute: true})
	_, err = gate.Start(t.Context(), p, "a", id.JobID, "expiring-start", startRaw)
	requireNoError(t, err)
	f.clock.advance(2 * time.Minute)
	if worked, err := gateStep(t, f); err != nil || !worked {
		t.Fatal(worked, err)
	}
	if state := f.state(t, id); f.backend.Stats().SubmitCalls != 0 || state.Orchestration != domain.OrchestrationTimedOut || state.Execution != domain.ExecutionNotSubmitted {
		t.Fatal("expired secret did not end the unstarted attempt", state, f.backend.Stats().SubmitCalls)
	}
}

func TestMultiDayWallMigrationKeepsRowsAndRelaxesConstraints(t *testing.T) {
	f, id, gate, p, ready, secrets, pool := secretFixture(t, fake.Accept)
	supplied, err := secrets.Supply(t.Context(), p, "a", id.JobID, "migration-secret", secretRequest(f, id, ready))
	requireNoError(t, err)
	_, err = gate.Start(t.Context(), p, "a", id.JobID, "migration-start", secretStartRequest(f, id, ready, supplied.AuthorizationID))
	requireNoError(t, err)
	combined := f.seed(t, "a", 2, false)
	grantAuthorization(t, f, combined)
	_, err = f.s.db.Exec(`UPDATE managed_connections SET max_remote_wall_seconds=7200 WHERE connection_id='con_fixture'`)
	requireNoError(t, err)
	const grants = `SELECT workspace_id,kind,authorization_id,job_id,attempt_id,key_sha256,status,consumed_at,request,binding,account_scope,inputs_sha256,attempt_nonce,max_remote_wall_seconds,granting_token_id,created_at,reserved_bytes,ready_until_ms,ready_revision,ready_at,storage_released,metadata,expires_at_ms,revoked_at,revocation_key_sha256 FROM attempt_grants ORDER BY kind,authorization_id`
	const receipts = `SELECT * FROM receipts ORDER BY namespace,scope,key_sha256`
	const walls = `SELECT connection_id,max_remote_wall_seconds FROM managed_connections ORDER BY connection_id`
	before := [][][]any{upgradeRows(t, f.s.db, grants), upgradeRows(t, f.s.db, receipts), upgradeRows(t, f.s.db, walls)}
	if len(before[0]) != 4 {
		t.Fatal("fixture lacks every grant kind", len(before[0]))
	}
	f.root = copyFixtureAtSchema(t, f.s, 35)
	pool.Close()
	f.restart(t)
	db := f.s.db
	after := [][][]any{upgradeRows(t, db, grants), upgradeRows(t, db, receipts), upgradeRows(t, db, walls)}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("migration changed existing grants, receipts or connection limits")
	}
	var version, triggers int
	if err = db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != len(migrations) {
		t.Fatal("schema not upgraded", version, err)
	}
	if err = db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE type IN ('trigger','index') AND tbl_name='attempt_grants' AND sql IS NOT NULL`).Scan(&triggers); err != nil || triggers != 6 {
		t.Fatal("grant triggers or index lost", triggers, err)
	}
	if _, err = db.Exec(`UPDATE attempt_grants SET max_remote_wall_seconds=10 WHERE kind='execution'`); err == nil {
		t.Fatal("grant budget became mutable")
	}
	for wall, valid := range map[int64]bool{multiDayWall: true, multiDayWall + 1: false, 0: false} {
		_, err = db.Exec(`UPDATE managed_connections SET max_remote_wall_seconds=? WHERE connection_id='con_fixture'`, wall)
		if valid != (err == nil) {
			t.Fatalf("connection wall %d: %v", wall, err)
		}
		tx, err := db.Begin()
		requireNoError(t, err)
		// Deferred receipt references are rolled back; the CHECK is evaluated at insert.
		_, err = tx.Exec(`INSERT INTO attempt_grants(workspace_id,kind,authorization_id,job_id,attempt_id,key_sha256,status,binding,account_scope,inputs_sha256,attempt_nonce,max_remote_wall_seconds,granting_token_id,created_at) VALUES('a','execution','grant_check',?,?,?,'granted','{}','scope',?,?,?,'token','2026-01-01T00:00:00Z')`,
			string(id.JobID), string(id.AttemptID), strings.Repeat("d", 64), strings.Repeat("e", 64), strings.Repeat("f", 64), wall)
		requireNoError(t, tx.Rollback())
		if valid != (err == nil) {
			t.Fatalf("grant wall %d: %v", wall, err)
		}
	}
	requireNoError(t, integrity(t.Context(), db))
}
