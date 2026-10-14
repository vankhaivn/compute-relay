package sqlite

import (
	"context"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/auth"
)

func TestCleanupPreviewReadsPreserveSQLHistory(t *testing.T) {
	f, id := collectedFixture(t)
	_, actor := cleanupService(t, f, "a", auth.Read, auth.Operate)
	counts := map[string]int{}
	for _, table := range []string{"events", "operations", "receipts", "work_leases", "provider_resources"} {
		counts[table] = controlCount(t, f.s, "SELECT count(*) FROM "+table)
	}
	plan, err := f.s.PlanCleanup(context.Background(), "a", actor.TokenID(), id.JobID, id.AttemptID, false)
	if err != nil || !plan.Eligible || len(plan.Resources) != 2 {
		t.Fatal("missing original cleanup inventory", plan, err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='cleanup'") != 0 {
		t.Fatal("preview admitted cleanup")
	}
	for table, before := range counts {
		if after := controlCount(t, f.s, "SELECT count(*) FROM "+table); after != before {
			t.Fatal("preview changed durable history", table, before, after)
		}
	}
}
