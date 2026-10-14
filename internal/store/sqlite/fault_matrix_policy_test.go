package sqlite

import (
	"encoding/json"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

func TestQuotaRejectionStoresUnknownAllowanceWithoutInventedValues(t *testing.T) {
	scenario := fake.DefaultScenario()
	scenario.Mode = fake.Reject
	f := newDispatchFixture(t, scenario)
	f.adapter.rejectQuota = true
	f.seed(t, "a", 1, false)
	requireNoError(t, f.step(t))
	requireNoError(t, f.step(t))
	var raw string
	var exhausted bool
	requireNoError(t, f.s.db.QueryRow("SELECT observation,exhausted FROM scheduler_quotas WHERE account_scope=? AND resource='cpu'", f.profile.AccountScope).Scan(&raw, &exhausted))
	var q provider.QuotaObservation
	if json.Unmarshal([]byte(raw), &q) != nil || !exhausted || q.Limit != nil || q.Remaining != nil || q.Used != nil || q.Status != provider.QuotaUnknown {
		t.Fatal("invented provider allowance", raw)
	}
}
