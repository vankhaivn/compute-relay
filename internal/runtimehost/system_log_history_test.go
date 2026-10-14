package runtimehost

import (
	"context"
	"reflect"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/joblogs"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

type systemHistoricalLogProvider struct {
	provider.Provider
	seen *[]provider.RemoteReference
}

func (p systemHistoricalLogProvider) Logs(ctx context.Context, remote provider.RemoteReference, page provider.PageRequest) (provider.LogPage, error) {
	*p.seen = append(*p.seen, remote)
	return p.Provider.Logs(ctx, remote, page)
}
func TestHistoricalLogsAndProviderPlanRemainExactAfterRetry(t *testing.T) {
	scenario := fake.DefaultScenario()
	scenario.States = []domain.ExecutionState{domain.ExecutionQueued, domain.ExecutionRunning, domain.ExecutionFailed}
	f := newSystemFixture(t, scenario)
	var seen []provider.RemoteReference
	f.wrapProvider = func(p provider.Provider) provider.Provider {
		return systemHistoricalLogProvider{Provider: p, seen: &seen}
	}
	connection := f.connection(t, "log-history-connection", "SYNTHETIC_FIRST")
	job := f.submitJob(t, "log-history-job", f.jobSpec(t, connection))
	f.authorizeJob(t, job)
	f.runToCollection(t, job)
	f.collectOnce(t)
	original, err := f.host.store.ReadLogAttempt(t.Context(), "app", f.actor.TokenID(), job.JobID, job.AttemptID)
	if err != nil || original.Remote == nil {
		t.Fatal("original provider identity missing", err)
	}
	plan, prepared, err := f.host.store.LoadProviderAttempt(t.Context(), original.Remote.Identity)
	if err != nil {
		t.Fatal(err)
	}
	stats := f.backend.Stats()
	retry, err := f.controls(t).Submit(t.Context(), f.actor, "app", job.JobID, domain.OperationRetryCompute, "logs-history-retry", systemJSON(t, map[string]any{"attempt_id": job.AttemptID, "reason": "explicit retry"}))
	if err != nil || !retry.NewAttemptID.Valid() || retry.NewAttemptID == job.AttemptID {
		t.Fatal("retry did not select a new attempt", err)
	}
	f.reopen(t)
	reader, err := joblogs.NewReader(f.host.access, f.host.store, f.services.resolver)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := reader.Read(t.Context(), f.actor, "app", job.JobID, job.AttemptID, provider.PageRequest{Limit: 10}); err != nil {
			t.Fatal("historical log service could not resolve original provider", err)
		}
		target, err := f.host.store.ReadLogAttempt(t.Context(), "app", f.actor.TokenID(), job.JobID, job.AttemptID)
		if err != nil || target.Remote == nil || *target.Remote != *original.Remote {
			t.Fatal("historical read substituted remote", err)
		}
		currentPlan, currentPrepared, err := f.host.store.LoadProviderAttempt(t.Context(), original.Remote.Identity)
		if err != nil || currentPlan.Digest() != plan.Digest() || currentPrepared != prepared {
			t.Fatal("historical reconstruction changed plan", err)
		}
	}
	if len(seen) != 2 || seen[0] != *original.Remote || seen[1] != *original.Remote {
		t.Fatal("log service used the active attempt instead of historical ownership", seen)
	}
	if _, err = reader.Read(t.Context(), f.actor, "app", job.JobID, retry.NewAttemptID, provider.PageRequest{Limit: 10}); err != nil || len(seen) != 2 {
		t.Fatal("new pre-submit attempt reused old remote", err)
	}
	current, err := f.host.store.ReadJob(t.Context(), "app", f.actor.TokenID(), job.JobID)
	if err != nil || current.Attempt.ID != retry.NewAttemptID || !reflect.DeepEqual(f.backend.Stats(), stats) {
		t.Fatal("log reads changed the active attempt or caused provider effects", err)
	}
}
