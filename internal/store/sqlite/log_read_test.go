package sqlite

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/joblogs"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

func TestLogReadAuthorizesExactAttemptWithoutDispatchEffects(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	_, p, _ := controlService(t, f, "a", auth.Read)
	_, wrong, _ := controlService(t, f, "b", auth.Read)
	before := f.journal(t, id)
	target, err := f.s.ReadLogAttempt(context.Background(), "a", p.TokenID(), id.JobID, id.AttemptID)
	if err != nil || target.Remote != nil || !target.Binding.Valid() {
		t.Fatal("pre-submit logs invented remote", target, err)
	}
	if !reflect.DeepEqual(before, f.journal(t, id)) || controlCount(t, f.s, "SELECT count(*) FROM submission_intents") != 0 {
		t.Fatal("log read changed dispatch")
	}
	for _, tc := range []struct {
		w       domain.WorkspaceID
		token   string
		job     domain.JobID
		attempt domain.AttemptID
		want    error
	}{
		{"a", wrong.TokenID(), id.JobID, id.AttemptID, auth.ErrUnauthenticated},
		{"b", p.TokenID(), id.JobID, id.AttemptID, auth.ErrUnauthenticated},
		{"a", p.TokenID(), "missing", id.AttemptID, joblogs.ErrNotFound},
		{"a", p.TokenID(), id.JobID, "missing", joblogs.ErrNotFound},
	} {
		if _, err := f.s.ReadLogAttempt(context.Background(), tc.w, tc.token, tc.job, tc.attempt); !errors.Is(err, tc.want) {
			t.Fatal(tc, err)
		}
	}
	if err := f.s.RevokeToken(context.Background(), p.TokenID()); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.ReadLogAttempt(context.Background(), "a", p.TokenID(), id.JobID, id.AttemptID); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("revoked read accepted", err)
	}
}
func TestHistoricalLogsAndProviderPlanRemainExactAfterRetry(t *testing.T) {
	scenario := fake.DefaultScenario()
	scenario.States = []domain.ExecutionState{domain.ExecutionQueued, domain.ExecutionRunning, domain.ExecutionFailed}
	f, id, adapter, blobs := collectionScenarioFixture(t, scenario)
	runCollection(t, collectionEngine(t, f, adapter, blobs, f.s))
	original := f.journal(t, id)
	if original.Remote == nil || original.Plan == nil || original.Prepared == nil {
		t.Fatal("original provider identity missing")
	}
	service, p, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	retry, err := service.Submit(context.Background(), p, "a", id.JobID, domain.OperationRetryCompute, "logs-history-retry", controlRequest(id.AttemptID))
	if err != nil {
		t.Fatal(err)
	}
	if retry.NewAttemptID == id.AttemptID || !retry.NewAttemptID.Valid() {
		t.Fatal("retry did not select a new attempt")
	}
	tables := []string{"jobs", "attempts", "operations", "events", "submission_intents", "provider_resources", "collection_publications", "execution_authorizations"}
	counts := map[string]int{}
	for _, table := range tables {
		counts[table] = controlCount(t, f.s, "SELECT count(*) FROM "+table)
	}
	for i := 0; i < 2; i++ {
		target, err := f.s.ReadLogAttempt(context.Background(), "a", p.TokenID(), id.JobID, id.AttemptID)
		if err != nil || target.Remote == nil || *target.Remote != *original.Remote {
			t.Fatal("historical read substituted remote", target, err)
		}
		plan, prepared, err := f.s.LoadProviderAttempt(context.Background(), original.Remote.Identity)
		if err != nil || plan.Digest() != original.Plan.Digest() || prepared != *original.Prepared {
			t.Fatal("historical reconstruction changed plan", err)
		}
	}
	latest, err := f.s.ReadLogAttempt(context.Background(), "a", p.TokenID(), id.JobID, retry.NewAttemptID)
	if err != nil || latest.Remote != nil {
		t.Fatal("new pre-submit attempt used old remote", latest, err)
	}
	record, err := f.s.ReadJob(context.Background(), "a", p.TokenID(), id.JobID)
	if err != nil || record.Attempt.ID != retry.NewAttemptID {
		t.Fatal("log read changed active attempt", err)
	}
	if !reflect.DeepEqual(original, f.journal(t, id)) {
		t.Fatal("historical log read changed journal")
	}
	for _, table := range tables {
		if n := controlCount(t, f.s, "SELECT count(*) FROM "+table); n != counts[table] {
			t.Fatal("log read mutated " + table)
		}
	}
}
