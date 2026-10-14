package sqlite

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

const workloadCanary = "synthetic-workload-canary/with+url?characters=only"

func (p *probeProvider) StartWithSecrets(ctx context.Context, prepared provider.Prepared, start provider.StartAuthorization, material workloadsecret.Material) provider.SubmissionOutcome {
	var count int
	err := p.store.db.QueryRowContext(ctx, `SELECT count(*) FROM workload_secret_authorizations m JOIN start_authorizations a ON a.workspace_id=m.workspace_id AND a.job_id=m.job_id AND a.attempt_id=m.attempt_id JOIN scheduler_queue q ON q.workspace_id=m.workspace_id AND q.job_id=m.job_id AND q.attempt_id=m.attempt_id JOIN submission_intents i ON i.queue_seq=q.queue_seq WHERE m.authorization_id=? AND m.consumed_at=a.consumed_at AND i.status='started'`, string(material.AuthorizationID)).Scan(&count)
	if err != nil || count != 1 {
		panic("secret delivery preceded durable consumption")
	}
	if string(material.Values["ingest"]) != workloadCanary {
		panic("wrong synthetic material")
	}
	outcome := p.Bound.StartWithSecrets(ctx, prepared, start, material)
	if p.panicSubmit {
		panic("uncertain secret start")
	}
	return outcome
}
func secretFixture(t testing.TB, mode fake.SubmitMode) (*dispatchFixture, scheduler.Identity, *executionauth.Service, auth.Principal, executionauth.Readiness, *workloadsecret.Service, *workloadsecret.Pool) {
	t.Helper()
	scenario := fake.DefaultScenario()
	scenario.SeparateStart = true
	scenario.EphemeralSecrets = true
	scenario.Mode = mode
	f := managedFixture(t, scenario)
	attachGate(t, f)
	raw := strings.Replace(dispatchJSON, `"name":"dispatch-fixture"`, `"name":"dispatch-fixture","execution_control":"prepare_then_start","workload_secrets":[{"name":"ingest","exposure":"file"}]`, 1)
	id := f.seedRaw(t, "a", 1, false, raw)
	gate, p, ready := readyGate(t, f, id)
	service, pool := attachSecrets(t, f, gate)
	return f, id, gate, p, ready, service, pool
}
func attachSecrets(t testing.TB, f *dispatchFixture, gate *executionauth.Service) (*workloadsecret.Service, *workloadsecret.Pool) {
	t.Helper()
	pool, err := workloadsecret.NewPool(bytes.Repeat([]byte{17}, 32))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	a, err := auth.New(f.s, f.s)
	if err != nil {
		t.Fatal(err)
	}
	service, err := workloadsecret.New(a, f.s, pool, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	gate.WithSecrets(pool)
	f.engine.WithSecrets(pool)
	return service, pool
}
func secretRequest(f *dispatchFixture, id scheduler.Identity, ready executionauth.Readiness) []byte {
	raw, _ := json.Marshal(workloadsecret.Supply{AttemptID: id.AttemptID, ReadinessRevision: ready.Revision, ExpiresAt: f.clock.Now().Add(time.Minute).UTC().Format(time.RFC3339), Values: map[string][]byte{"ingest": []byte(workloadCanary)}})
	return raw
}
func secretStartRequest(f *dispatchFixture, id scheduler.Identity, ready executionauth.Readiness, secret domain.OperationID) []byte {
	var request executionauth.StartRequest
	_ = json.Unmarshal(startGrantRaw(f, id, ready), &request)
	request.SecretAuthorizationID = secret
	raw, _ := json.Marshal(request)
	return raw
}
func assertNoWorkloadValues(t testing.TB, s *Store) {
	t.Helper()
	rows, err := s.db.Query(`SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'`)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var table string
		if err = rows.Scan(&table); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, table)
	}
	rows.Close()
	for _, table := range tables {
		rows, err := s.db.Query(`SELECT * FROM "` + strings.ReplaceAll(table, `"`, `""`) + `"`)
		if err != nil {
			t.Fatal(err)
		}
		cols, _ := rows.Columns()
		for rows.Next() {
			values := make([]any, len(cols))
			targets := make([]any, len(cols))
			for i := range values {
				targets[i] = &values[i]
			}
			if err = rows.Scan(targets...); err != nil {
				t.Fatal(err)
			}
			for _, value := range values {
				raw := fmt.Sprint(value)
				if strings.Contains(raw, workloadCanary) || strings.Contains(raw, base64.StdEncoding.EncodeToString([]byte(workloadCanary))) {
					t.Fatal("workload material persisted in", table)
				}
			}
		}
		if err = rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
	}
}
func TestWorkloadSecretRestartRequiresExplicitResupplyAndConsumesOnce(t *testing.T) {
	ctx := context.Background()
	f, id, gate, p, ready, secrets, pool := secretFixture(t, fake.AcceptLoseResponse)
	if ready.Secrets == nil || ready.Secrets.Status != "missing" {
		t.Fatal("missing slots not exposed")
	}
	if _, err := gate.Start(ctx, p, "a", id.JobID, "no-material-start", startGrantRaw(f, id, ready)); !errors.Is(err, workloadsecret.ErrUnavailable) {
		t.Fatal("start without slots", err)
	}
	raw := secretRequest(f, id, ready)
	receipt, err := secrets.Supply(ctx, p, "a", id.JobID, "secret-original", raw)
	if err != nil {
		t.Fatal(err)
	}
	startRaw := secretStartRequest(f, id, ready, receipt.AuthorizationID)
	if _, err = gate.Start(ctx, p, "a", id.JobID, "start-original", startRaw); err != nil {
		t.Fatal(err)
	}
	assertNoWorkloadValues(t, f.s)
	pool.Close()
	f.restart(t)
	attachGate(t, f)
	gate, p = authorizationService(t, f, "a", auth.Read, auth.Execute)
	secrets, pool = attachSecrets(t, f, gate)
	if _, err = gateStep(t, f); err != nil || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("memory loss started payload", err)
	}
	state, err := gate.Readiness(ctx, p, "a", id.JobID)
	if err != nil || state.Secrets.Status != "material_lost" {
		t.Fatal("material loss invisible", err)
	}
	replay, err := secrets.Supply(ctx, p, "a", id.JobID, "secret-original", raw)
	if err != nil || !replay.Replay || replay.AuthorizationID != receipt.AuthorizationID {
		t.Fatal("explicit resupply failed", err)
	}
	if _, err = gateStep(t, f); err != nil {
		t.Fatal(err)
	}
	if f.journal(t, id).Phase != dispatch.Submitting || f.backend.Stats().Executions != 1 {
		t.Fatal("lost ack was not retained")
	}
	if pool.Available(workloadsecret.Key{WorkspaceID: "a", JobID: id.JobID, AttemptID: id.AttemptID}, receipt.AuthorizationID, f.clock.Now()) {
		t.Fatal("used material retained")
	}
	current, err := secrets.Get(ctx, p, "a", id.JobID, receipt.AuthorizationID)
	if err != nil || current.Status != "consumed" {
		t.Fatal("missing consumption", err)
	}
	if _, err = secrets.Supply(ctx, p, "a", id.JobID, "secret-original", raw); err != nil {
		t.Fatal(err)
	}
	if pool.Available(current.Key(), receipt.AuthorizationID, f.clock.Now()) {
		t.Fatal("used replay restored material")
	}
	pool.Close()
	f.restart(t)
	attachGate(t, f)
	if _, err = gateStep(t, f); err != nil || f.backend.Stats().SubmitCalls != 1 || f.journal(t, id).Phase != dispatch.Submitted {
		t.Fatal("recovery repeated delivery/start", err)
	}
	assertNoWorkloadValues(t, f.s)
}
func TestWorkloadSecretRevokeDuringRemoteVerificationPreventsStart(t *testing.T) {
	ctx := context.Background()
	f, id, gate, p, ready, secrets, pool := secretFixture(t, fake.Accept)
	raw := secretRequest(f, id, ready)
	receipt, err := secrets.Supply(ctx, p, "a", id.JobID, "secret-original", raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = gate.Start(ctx, p, "a", id.JobID, "start-original", secretStartRequest(f, id, ready, receipt.AuthorizationID)); err != nil {
		t.Fatal(err)
	}
	revokeRaw, _ := json.Marshal(map[string]any{"attempt_id": id.AttemptID})
	f.adapter.afterPreparationRead = func() {
		f.adapter.afterPreparationRead = nil
		if _, err := secrets.Revoke(ctx, p, "a", id.JobID, receipt.AuthorizationID, "revoke-original", revokeRaw); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = gateStep(t, f); err != nil || f.backend.Stats().SubmitCalls != 0 || f.journal(t, id).SubmitStarted {
		t.Fatal("revocation lost start race", err)
	}
	if pool.Available(receipt.Key(), receipt.AuthorizationID, f.clock.Now()) {
		t.Fatal("revoked bytes retained")
	}
	replay, err := secrets.Revoke(ctx, p, "a", id.JobID, receipt.AuthorizationID, "revoke-original", revokeRaw)
	if err != nil || !replay.Replay || replay.Status != "revoked" {
		t.Fatal("revocation replay", err)
	}
	if _, err = secrets.Supply(ctx, p, "a", id.JobID, "secret-original", raw); err != nil || pool.Available(receipt.Key(), receipt.AuthorizationID, f.clock.Now()) {
		t.Fatal("revoked replay rearmed", err)
	}
	assertNoWorkloadValues(t, f.s)
}
func TestWorkloadSecretSupplyConflictScopesAndExpiry(t *testing.T) {
	ctx := context.Background()
	f, id, gate, p, ready, secrets, pool := secretFixture(t, fake.Accept)
	raw := secretRequest(f, id, ready)
	receipt, err := secrets.Supply(ctx, p, "a", id.JobID, "secret-original", raw)
	if err != nil {
		t.Fatal(err)
	}
	changed := bytes.Replace(raw, []byte(base64.StdEncoding.EncodeToString([]byte(workloadCanary))), []byte(base64.StdEncoding.EncodeToString([]byte("changed-value"))), 1)
	if _, err = secrets.Supply(ctx, p, "a", id.JobID, "secret-original", changed); !errors.Is(err, workloadsecret.ErrConflict) {
		t.Fatal("changed replay", err)
	}
	if _, err = secrets.Supply(ctx, p, "a", id.JobID, "secret-new-key", raw); !errors.Is(err, workloadsecret.ErrConflict) {
		t.Fatal("new supply rearmed", err)
	}
	_, read := authorizationService(t, f, "a", auth.Read)
	if _, err = secrets.Supply(ctx, read, "a", id.JobID, "secret-original", raw); !errors.Is(err, auth.ErrForbidden) {
		t.Fatal("read scope supplied values", err)
	}
	if _, err = secrets.Get(ctx, read, "a", id.JobID, receipt.AuthorizationID); !errors.Is(err, auth.ErrForbidden) {
		t.Fatal("read scope read write-only receipt", err)
	}
	if _, err = secrets.Supply(ctx, p, "b", id.JobID, "secret-original", raw); !errors.Is(err, auth.ErrForbidden) {
		t.Fatal("foreign workspace", err)
	}
	f.clock.advance(time.Minute)
	if _, err = gate.Start(ctx, p, "a", id.JobID, "expired-material", secretStartRequest(f, id, ready, receipt.AuthorizationID)); !errors.Is(err, workloadsecret.ErrUnavailable) {
		t.Fatal("expired supply usable", err)
	}
	if _, err = secrets.Supply(ctx, p, "a", id.JobID, "secret-original", raw); err != nil || pool.Available(receipt.Key(), receipt.AuthorizationID, f.clock.Now()) {
		t.Fatal("expired replay refreshed material", err)
	}
	assertNoWorkloadValues(t, f.s)
}

func TestWorkloadSecretCrashAfterConsumptionNeverRestoresOrSends(t *testing.T) {
	ctx := context.Background()
	f, id, gate, p, ready, secrets, pool := secretFixture(t, fake.Accept)
	raw := secretRequest(f, id, ready)
	receipt, err := secrets.Supply(ctx, p, "a", id.JobID, "crash-secret-original", raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = gate.Start(ctx, p, "a", id.JobID, "crash-start-original", secretStartRequest(f, id, ready, receipt.AuthorizationID)); err != nil {
		t.Fatal(err)
	}
	f.clock.advance(10 * time.Millisecond)
	claim, err := f.s.ClaimRecovery(ctx, "secret_crash_worker")
	if err != nil || claim == nil {
		t.Fatal("missing claim", err)
	}
	work, err := f.s.LoadDispatch(ctx, *claim)
	if err != nil {
		t.Fatal(err)
	}
	material, err := pool.Claim(receipt.Key(), receipt.AuthorizationID, f.clock.Now(), func() error {
		_, err := f.s.CommitDispatch(ctx, work.Handle, dispatch.Action{Kind: dispatch.BeginSubmission, SecretsAvailable: true})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	material.Clear()
	// Crash before the unique provider invocation leaves intent uncertain, not reusable.
	pool.Close()
	f.restart(t)
	attachGate(t, f)
	gate, p = authorizationService(t, f, "a", auth.Read, auth.Execute)
	secrets, pool = attachSecrets(t, f, gate)
	f.clock.advance(time.Minute)
	if _, err = gateStep(t, f); err == nil || !strings.HasPrefix(err.Error(), "PROVIDER_SUBMISSION_UNKNOWN:") {
		t.Fatal("crash lost uncertainty", err)
	}
	current, err := secrets.Get(ctx, p, "a", id.JobID, receipt.AuthorizationID)
	if err != nil || current.Status != "consumed" {
		t.Fatal("consumption lost", err)
	}
	if replay, err := secrets.Supply(ctx, p, "a", id.JobID, "crash-secret-original", raw); err != nil || !replay.Replay {
		t.Fatal("original replay lost", err)
	}
	if pool.Available(receipt.Key(), receipt.AuthorizationID, f.clock.Now()) || f.backend.Stats().SubmitCalls != 0 || !f.journal(t, id).SubmitStarted || f.state(t, id).RemoteActivity != domain.RemoteActivityPossible {
		t.Fatal("crash rearmed material or execution")
	}
	assertNoWorkloadValues(t, f.s)
}

func TestWorkloadSecretCancellationClearsMaterialAndBlocksExactResupply(t *testing.T) {
	ctx := context.Background()
	f, id, _, p, ready, secrets, pool := secretFixture(t, fake.Accept)
	raw := secretRequest(f, id, ready)
	receipt, err := secrets.Supply(ctx, p, "a", id.JobID, "cancel-secret-original", raw)
	if err != nil {
		t.Fatal(err)
	}
	control, op, _ := controlService(t, f, "a", auth.Operate)
	cancelControl(t, f, control, op, id)
	pool.Sweep(f.clock.Now(), func(key workloadsecret.Key, authID domain.OperationID) bool {
		usable, err := f.s.WorkloadSecretsUsable(ctx, key, authID)
		return err == nil && usable
	})
	if pool.Available(receipt.Key(), receipt.AuthorizationID, f.clock.Now()) {
		t.Fatal("cancelled bytes retained")
	}
	if _, err = secrets.Supply(ctx, p, "a", id.JobID, "cancel-secret-original", raw); err != nil {
		t.Fatal("cancelled original receipt lost", err)
	}
	if pool.Available(receipt.Key(), receipt.AuthorizationID, f.clock.Now()) || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("cancelled replay restored bytes")
	}
	assertNoWorkloadValues(t, f.s)
}

func TestWorkloadSecretConcurrentSupplyCoalescesBeforeOneStart(t *testing.T) {
	ctx := context.Background()
	f, id, gate, p, ready, secrets, pool := secretFixture(t, fake.Accept)
	raw := secretRequest(f, id, ready)
	var wg sync.WaitGroup
	receipts := make(chan workloadsecret.Receipt, 8)
	failures := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := secrets.Supply(ctx, p, "a", id.JobID, "concurrent-secret-original", raw)
			receipts <- r
			failures <- err
		}()
	}
	wg.Wait()
	close(receipts)
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	var first workloadsecret.Receipt
	originals := 0
	for receipt := range receipts {
		if first.AuthorizationID == "" {
			first = receipt
		}
		if receipt.AuthorizationID != first.AuthorizationID {
			t.Fatal("multiple secret authorities")
		}
		if !receipt.Replay {
			originals++
		}
	}
	if originals != 1 {
		t.Fatal("multiple original receipts")
	}
	if _, err := gate.Start(ctx, p, "a", id.JobID, "concurrent-secret-start", secretStartRequest(f, id, ready, first.AuthorizationID)); err != nil {
		t.Fatal(err)
	}
	if _, err := gateStep(t, f); err != nil {
		t.Fatal(err)
	}
	if f.backend.Stats().SubmitCalls != 1 || pool.Available(first.Key(), first.AuthorizationID, f.clock.Now()) {
		t.Fatal("concurrent supply repeated start or retained bytes")
	}
	assertNoWorkloadValues(t, f.s)
}
