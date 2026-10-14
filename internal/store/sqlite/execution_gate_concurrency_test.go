package sqlite

import (
	"context"
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
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func configureHoldWorkers(t *testing.T, f *dispatchFixture, workers int) {
	t.Helper()
	settings := scheduler.DefaultSettings()
	settings.MaxWorkers = workers
	if err := f.s.ConfigureScheduler(context.Background(), settings); err != nil {
		t.Fatal(err)
	}
}

// These jobs use distinct frozen account bindings in the same operator workspace.
func holdSeed(t *testing.T, f *dispatchFixture, n, account int) scheduler.Identity {
	t.Helper()
	original := f.profile
	defer func() { f.profile = original }()
	scope := fmt.Sprintf("hold_account_%d", account)
	connection := "con_" + scope
	f.profile.AccountScope = scope
	f.profile.Binding.Profile = scope
	f.profile.Binding.ProviderInstanceID = domain.ProviderInstanceID(connection)
	f.profile.CredentialRef = "vault:" + connection
	if err := f.s.PutProfile(context.Background(), f.profile, true); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.db.Exec(`INSERT INTO managed_connections(connection_id,workspace_id,provider_type,label,revision,authentication,new_work,canonical_account,account_scope,active_credential_key,updated_at) VALUES(?,'a','fixture','fixture',1,'verified','enabled',?,?,'fixture_key',?) ON CONFLICT(connection_id) DO NOTHING`, connection, scope, scope, f.clock.Now().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	raw := strings.Replace(dispatchJSON, `"profile":"fixture"`, `"profile":"`+scope+`"`, 1)
	raw = strings.Replace(raw, `"name":"dispatch-fixture"`, `"name":"dispatch-fixture","execution_control":"prepare_then_start"`, 1)
	return f.seedRaw(t, "a", n, false, raw)
}

func holdRequest(f *dispatchFixture, id scheduler.Identity) []byte {
	raw, _ := json.Marshal(executionauth.PrepareRequest{AttemptID: id.AttemptID, MaxPreparationSeconds: 60, ReadyUntil: f.clock.Now().Add(30 * time.Minute).UTC().Format(time.RFC3339), MaxStagedBytes: executionauth.MaxStagedBytes, AuthorizePrivateStaging: true})
	return raw
}

func TestPreparationHoldsUsePersistedConcurrencyAcrossAccounts(t *testing.T) {
	for _, workers := range []int{1, 2, 16} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			f := gateFixture(t, fake.Accept)
			configureHoldWorkers(t, f, workers)
			service, actor := authorizationService(t, f, "a", auth.Execute)
			for n := 1; n <= workers+1; n++ {
				id := holdSeed(t, f, n, n%2)
				_, err := service.Prepare(context.Background(), actor, "a", id.JobID, "hold-"+string(id.JobID), holdRequest(f, id))
				if n <= workers && err != nil || n > workers && !errors.Is(err, admission.ErrLimit) {
					t.Fatal("persisted hold bound not enforced", n, err)
				}
			}
			var count, accounts int
			var total int64
			if err := f.s.db.QueryRow(`SELECT count(*),count(DISTINCT account_scope),sum(reserved_bytes) FROM attempt_grants WHERE kind='prepare' AND storage_released=0`).Scan(&count, &accounts, &total); err != nil {
				t.Fatal(err)
			}
			if count != workers || accounts != min(workers, 2) || total < 1 || total > int64(workers)*executionauth.MaxStagedBytes {
				t.Fatal("wrong global/account reservation or byte budget", count, accounts, total)
			}
		})
	}
}

func TestLowerPreparationConcurrencyPreservesExistingHoldsAndReceipts(t *testing.T) {
	f := gateFixture(t, fake.Accept)
	configureHoldWorkers(t, f, 2)
	service, actor := authorizationService(t, f, "a", auth.Execute)
	ids := []scheduler.Identity{holdSeed(t, f, 1, 0), holdSeed(t, f, 2, 1), holdSeed(t, f, 3, 2)}
	receipts := make([]executionauth.GateReceipt, 2)
	for i, id := range ids[:2] {
		var err error
		receipts[i], err = service.Prepare(context.Background(), actor, "a", id.JobID, "hold-"+string(id.JobID), holdRequest(f, id))
		if err != nil {
			t.Fatal(err)
		}
	}
	holdsQuery := `SELECT authorization_id,status,reserved_bytes,storage_released FROM attempt_grants WHERE kind='prepare' AND job_id IN ('job_1','job_2') ORDER BY job_id`
	receiptsQuery := `SELECT key_sha256,request_sha256,receipt FROM receipts WHERE namespace='grant' AND scope='prepare' AND job_id IN ('job_1','job_2') ORDER BY job_id`
	holds, rawReceipts := upgradeRows(t, f.s.db, holdsQuery), upgradeRows(t, f.s.db, receiptsQuery)
	if len(holds) != 2 || len(rawReceipts) != 2 {
		t.Fatal("missing original reservations or receipts", len(holds), len(rawReceipts))
	}
	configureHoldWorkers(t, f, 1)
	for i, id := range ids[:2] {
		replay, err := service.Prepare(context.Background(), actor, "a", id.JobID, "hold-"+string(id.JobID), holdRequest(f, id))
		if err != nil || !replay.Replay || replay.AuthorizationID != receipts[i].AuthorizationID {
			t.Fatal("lowering concurrency changed existing receipt", replay, err)
		}
	}
	third := ids[2]
	if _, err := service.Prepare(context.Background(), actor, "a", third.JobID, "hold-"+string(third.JobID), holdRequest(f, third)); !errors.Is(err, admission.ErrLimit) {
		t.Fatal("lower bound admitted another account", err)
	}
	if !reflect.DeepEqual(holds, upgradeRows(t, f.s.db, holdsQuery)) || !reflect.DeepEqual(rawReceipts, upgradeRows(t, f.s.db, receiptsQuery)) {
		t.Fatal("lowering concurrency changed existing reservations or receipt bytes")
	}
	configureHoldWorkers(t, f, 3)
	if _, err := service.Prepare(context.Background(), actor, "a", third.JobID, "hold-"+string(third.JobID), holdRequest(f, third)); err != nil {
		t.Fatal("raising concurrency did not admit a new reservation", err)
	}
	if !reflect.DeepEqual(holds, upgradeRows(t, f.s.db, holdsQuery)) || !reflect.DeepEqual(rawReceipts, upgradeRows(t, f.s.db, receiptsQuery)) {
		t.Fatal("raising concurrency changed historical reservations or receipts")
	}
}

func TestPreparationHoldsPreserveHistoricalConcurrencyAboveRuntimeFlagRange(t *testing.T) {
	f := gateFixture(t, fake.Accept)
	configureHoldWorkers(t, f, 64)
	service, actor := authorizationService(t, f, "a", auth.Execute)
	for n := 1; n <= 17; n++ {
		id := gateSeed(t, f, n)
		if _, err := service.Prepare(context.Background(), actor, "a", id.JobID, "historical-"+string(id.JobID), holdRequest(f, id)); err != nil {
			t.Fatal("historical scheduler settings narrowed", n, err)
		}
	}
}

func TestConcurrentPreparationRequestsCannotExceedPersistedConcurrency(t *testing.T) {
	f := gateFixture(t, fake.Accept)
	const workers, requests = 2, 12
	configureHoldWorkers(t, f, workers)
	service, actor := authorizationService(t, f, "a", auth.Execute)
	ids := make([]scheduler.Identity, requests)
	for n := range ids {
		ids[n] = holdSeed(t, f, n+1, n%2)
	}
	var wg sync.WaitGroup
	results := make(chan error, requests)
	start := make(chan struct{})
	for _, id := range ids {
		wg.Add(1)
		go func(id scheduler.Identity) {
			defer wg.Done()
			<-start
			_, err := service.Prepare(context.Background(), actor, "a", id.JobID, "parallel-"+string(id.JobID), holdRequest(f, id))
			results <- err
		}(id)
	}
	close(start)
	wg.Wait()
	close(results)
	admitted := 0
	for err := range results {
		if err == nil {
			admitted++
		} else if !errors.Is(err, admission.ErrLimit) {
			t.Fatal(err)
		}
	}
	if admitted != workers || controlCount(t, f.s, `SELECT count(*) FROM attempt_grants WHERE kind='prepare' AND storage_released=0`) != workers || controlCount(t, f.s, `SELECT count(*) FROM receipts WHERE namespace='grant' AND scope='prepare'`) != workers {
		t.Fatal("parallel requests oversubscribed or left receipts for rejected work", admitted)
	}
}
