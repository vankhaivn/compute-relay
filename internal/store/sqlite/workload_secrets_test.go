package sqlite

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

const workloadCanary = "synthetic-workload-canary/with+url?characters=only"

func (p *probeProvider) startWithSecrets(ctx context.Context, prepared provider.Prepared, start provider.StartAuthorization, material workloadsecret.Material) provider.SubmissionOutcome {
	var count int
	err := p.store.db.QueryRowContext(ctx, `SELECT count(*) FROM attempt_grants m JOIN attempt_grants a ON a.kind='start' AND a.workspace_id=m.workspace_id AND a.job_id=m.job_id AND a.attempt_id=m.attempt_id JOIN scheduler_queue q ON q.workspace_id=m.workspace_id AND q.job_id=m.job_id AND q.attempt_id=m.attempt_id JOIN submission_intents i ON i.queue_seq=q.queue_seq WHERE m.kind='secret' AND m.authorization_id=? AND m.consumed_at=a.consumed_at AND i.status='started'`, string(material.AuthorizationID)).Scan(&count)
	if err != nil || count != 1 {
		panic("secret delivery preceded durable consumption")
	}
	if string(material.Values["ingest"]) != workloadCanary {
		panic("wrong synthetic material")
	}
	outcome := p.Bound.Start(ctx, prepared, &start, &material)
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
	requireNoError(t, err)
	t.Cleanup(pool.Close)
	a, err := auth.New(f.s, f.s)
	requireNoError(t, err)
	service, err := workloadsecret.New(a, f.s, pool, f.clock)
	requireNoError(t, err)
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
	requireNoError(t, err)
	var tables []string
	for rows.Next() {
		var table string
		err = rows.Scan(&table)
		requireNoError(t, err)
		tables = append(tables, table)
	}
	rows.Close()
	for _, table := range tables {
		rows, err := s.db.Query(`SELECT * FROM "` + strings.ReplaceAll(table, `"`, `""`) + `"`)
		requireNoError(t, err)
		cols, _ := rows.Columns()
		for rows.Next() {
			values := make([]any, len(cols))
			targets := make([]any, len(cols))
			for i := range values {
				targets[i] = &values[i]
			}
			err = rows.Scan(targets...)
			requireNoError(t, err)
			for _, value := range values {
				raw := fmt.Sprint(value)
				if strings.Contains(raw, workloadCanary) || strings.Contains(raw, base64.StdEncoding.EncodeToString([]byte(workloadCanary))) {
					t.Fatal("workload material persisted in", table)
				}
			}
		}
		err = rows.Err()
		requireNoError(t, err)
		rows.Close()
	}
}

func TestWorkloadSecretDurableAbsenceAndAtomicConsumption(t *testing.T) {
	f, id, gate, p, ready, secrets, pool := secretFixture(t, fake.Accept)
	raw := secretRequest(f, id, ready)
	receipt, err := secrets.Supply(t.Context(), p, "a", id.JobID, "secret-storage", raw)
	requireNoError(t, err)
	assertNoWorkloadValues(t, f.s)
	start, err := gate.Start(t.Context(), p, "a", id.JobID, "secret-start-storage", secretStartRequest(f, id, ready, receipt.AuthorizationID))
	requireNoError(t, err)
	assertNoWorkloadValues(t, f.s)
	// startWithSecrets asserts equal durable consumption timestamps and a started
	// intent inside the callback before delivering the synthetic material.
	if worked, err := gateStep(t, f); err != nil || !worked {
		t.Fatal(worked, err)
	}
	var n int
	if err = f.s.db.QueryRow(`SELECT count(*) FROM attempt_grants m JOIN attempt_grants a ON a.kind='start' AND m.workspace_id=a.workspace_id AND m.job_id=a.job_id AND m.attempt_id=a.attempt_id JOIN scheduler_queue q ON q.workspace_id=m.workspace_id AND q.job_id=m.job_id AND q.attempt_id=m.attempt_id JOIN submission_intents i ON i.queue_seq=q.queue_seq WHERE m.kind='secret' AND m.authorization_id=? AND a.authorization_id=? AND m.consumed_at=a.consumed_at AND i.status='accepted'`, receipt.AuthorizationID, start.AuthorizationID).Scan(&n); err != nil || n != 1 {
		t.Fatal("secret/start/intent atomicity", n, err)
	}
	assertNoWorkloadValues(t, f.s)
	pool.Close()
	f.restart(t)
	assertNoWorkloadValues(t, f.s)
}

func TestWorkloadSecretRejectedRevokedAndCancelledRequestsPersistNoValues(t *testing.T) {
	for _, mode := range []string{"conflict", "revoked", "cancelled", "expired"} {
		t.Run(mode, func(t *testing.T) {
			f, id, _, p, ready, secrets, _ := secretFixture(t, fake.Accept)
			raw := secretRequest(f, id, ready)
			r, err := secrets.Supply(t.Context(), p, "a", id.JobID, "storage-absence", raw)
			requireNoError(t, err)
			switch mode {
			case "conflict":
				changed := bytes.Replace(raw, []byte(base64.StdEncoding.EncodeToString([]byte(workloadCanary))), []byte(base64.StdEncoding.EncodeToString([]byte("changed-value"))), 1)
				if _, err = secrets.Supply(t.Context(), p, "a", id.JobID, "storage-absence", changed); !errors.Is(err, workloadsecret.ErrConflict) {
					t.Fatal(err)
				}
			case "revoked":
				if _, err = secrets.Revoke(t.Context(), p, "a", id.JobID, r.AuthorizationID, "storage-revoke", []byte(`{"attempt_id":"`+string(id.AttemptID)+`"}`)); err != nil {
					t.Fatal(err)
				}
			case "cancelled":
				service, actor, _ := controlService(t, f, "a", auth.Operate)
				cancelControl(t, f, service, actor, id)
			case "expired":
				f.clock.advance(time.Minute)
			}
			if _, err = secrets.Supply(t.Context(), p, "a", id.JobID, "storage-absence", raw); err != nil {
				t.Fatal(err)
			}
			assertNoWorkloadValues(t, f.s)
		})
	}
}
