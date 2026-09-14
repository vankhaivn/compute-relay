package scheduler

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

var testNow = time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)

func candidate(seq int64, w, account string) Candidate {
	return Candidate{Identity: Identity{domain.WorkspaceID(w), domain.JobID(fmt.Sprintf("job_%d", seq)), domain.AttemptID(fmt.Sprintf("att_%d", seq))}, Sequence: seq, State: domain.InitialAttemptState(), ActiveAttempt: true, WorkspaceEnabled: true, AccountScope: account, ProviderInstanceID: "instance", Policy: AccountPolicy{MaxActive: 1}, Resource: "gpu", WallSeconds: 120}
}
func TestSelectFIFOAndRoundRobin(t *testing.T) {
	rows := []Candidate{candidate(3, "a", "x"), candidate(2, "b", "x"), candidate(1, "a", "x")}
	i, v, err := Select(rows, "", DefaultSettings(), testNow)
	if err != nil || i != 2 || len(v.Heads) != 2 || v.Heads[0].Warning != QuotaUnknown {
		t.Fatalf("first selection=%d %+v %v", i, v, err)
	}
	i, _, err = Select(rows, "a", DefaultSettings(), testNow)
	if err != nil || i != 1 {
		t.Fatal("round robin", i, err)
	}
	i, _, err = Select(rows, "b", DefaultSettings(), testNow)
	if err != nil || i != 2 {
		t.Fatal("wrap", i, err)
	}
	rows[2].NotBefore = testNow.Add(time.Minute)
	i, _, err = Select(rows, "b", DefaultSettings(), testNow)
	if err != nil || i != 1 {
		t.Fatal("deferred head was overtaken", i, err)
	}
	rows[2].LeaseUntil = testNow.Add(time.Minute)
	i, v, err = Select(rows, "", DefaultSettings(), testNow)
	if err != nil || i != -1 || v.LiveWorkers != 1 || v.AccountReservations["x"] != 1 {
		t.Fatal("shared account overcommitted", i, v, err)
	}
	rows[1].AccountScope = "other"
	i, _, err = Select(rows, "a", DefaultSettings(), testNow)
	if err != nil || i != 1 {
		t.Fatal("unrelated account stalled", i, err)
	}
}
func TestAccountCapacityUsesEvidenceNotLeaseExpiry(t *testing.T) {
	for _, phase := range []domain.OrchestrationState{domain.OrchestrationDispatching, domain.OrchestrationSubmitted, domain.OrchestrationRunning, domain.OrchestrationReconciling, domain.OrchestrationCancelling, domain.OrchestrationNeedsAttention} {
		t.Run(string(phase), func(t *testing.T) {
			held := candidate(1, "a", "shared")
			held.State.Orchestration = phase
			held.State.Execution = domain.ExecutionUnknown
			held.State.RemoteActivity = domain.RemoteActivityPossible
			if phase == domain.OrchestrationRunning {
				held.State.Execution = domain.ExecutionRunning
				held.State.RemoteActivity = domain.RemoteActivityActive
			}
			if phase == domain.OrchestrationCancelling {
				held.State.Cancellation = domain.CancellationRequested
			}
			held.State.DeadlineExceeded = true
			held.DispatchBarrier = true
			held.LeaseUntil = testNow.Add(-time.Hour)
			rows := []Candidate{held, candidate(2, "b", "shared")}
			i, v, err := Select(rows, "", DefaultSettings(), testNow)
			if err != nil || i != -1 || v.AccountReservations["shared"] != 1 {
				t.Fatalf("expired remote capacity released: %d %+v %v", i, v, err)
			}
			held.State = domain.InitialAttemptState()
			held.State.Orchestration = domain.OrchestrationCollecting
			held.State.Execution = domain.ExecutionSucceeded
			held.State.RemoteActivity = domain.RemoteActivityInactive
			held.State.Result = domain.ResultCollecting
			held.LeaseUntil = testNow.Add(time.Minute) // local collection ownership is not remote capacity.
			rows[0] = held
			i, v, err = Select(rows, "", DefaultSettings(), testNow)
			if err != nil || i != 1 || v.AccountReservations["shared"] != 0 {
				t.Fatal("terminal evidence did not release capacity", i, v, err)
			}
		})
	}
	local := domain.InitialAttemptState()
	local.Orchestration = domain.OrchestrationFailed
	if HoldsAccount(local, true) {
		t.Fatal("proven pre-execution failure holds compute capacity")
	}
	if LocalOnly(local) {
		t.Fatal("terminal failure reclaimable")
	}
	unknown := domain.InitialAttemptState()
	unknown.Execution = domain.ExecutionUnknown
	unknown.RemoteActivity = domain.RemoteActivityUnknown
	if LocalOnly(unknown) || !HoldsAccount(unknown, false) {
		t.Fatal("unknown state treated as unsubmitted")
	}
}
func TestSelectionFailsClosedAndRetainsBackpressure(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Candidate, *Settings)
		reason Reason
	}{
		{"paused", func(_ *Candidate, s *Settings) { s.Paused = true }, Paused},
		{"workspace", func(c *Candidate, _ *Settings) { c.WorkspaceEnabled = false }, WorkspaceDisabled},
		{"account", func(c *Candidate, _ *Settings) { c.Policy.Disabled = true }, AccountDisabled},
		{"strict quota", func(c *Candidate, _ *Settings) { c.Policy.StrictQuota = true }, QuotaUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := candidate(1, "a", "x")
			s := DefaultSettings()
			tc.change(&c, &s)
			i, v, err := Select([]Candidate{c}, "", s, testNow)
			if err != nil || i != -1 || v.Heads[0].Reason != tc.reason {
				t.Fatal(i, v, err)
			}
		})
	}
	rows := []Candidate{candidate(1, "a", "a"), candidate(2, "b", "b")}
	rows[0].LeaseUntil = testNow.Add(time.Second)
	s := DefaultSettings()
	s.MaxWorkers = 1
	i, v, err := Select(rows, "", s, testNow)
	if err != nil || i != -1 || v.Heads[0].Reason != WorkerLimit {
		t.Fatal(i, v, err)
	}
	rows[0].State.Execution = "unrecognized"
	if _, _, err = Select(rows, "", s, testNow); !errors.Is(err, ErrInvalid) {
		t.Fatal("corrupt state ignored", err)
	}
	if _, _, err = Select(make([]Candidate, ScanLimit+1), "", s, testNow); !errors.Is(err, ErrScanLimit) {
		t.Fatal("partial window accepted", err)
	}
	rows = []Candidate{candidate(1, "a", "a"), candidate(1, "b", "b")}
	if _, _, err = Select(rows, "", s, testNow); !errors.Is(err, ErrInvalid) {
		t.Fatal("duplicate sequence accepted", err)
	}
}
func quotaValue(remaining float64, status provider.QuotaStatus, age time.Duration) Quota {
	limit := 10.0
	used := 0.0
	return Quota{Observation: &provider.QuotaObservation{Status: status, Resource: "gpu", Unit: "hours", Limit: &limit, Used: &used, Remaining: &remaining, ObservedAt: testNow.Add(-age), Source: "synthetic", Precision: "exact"}}
}
func TestQuotaDecisionsPreserveUnknownAndPrecision(t *testing.T) {
	tests := []struct {
		name          string
		q             Quota
		strict        bool
		want, warning Reason
	}{
		{"missing", Quota{}, false, Eligible, QuotaUnknown},
		{"strict missing", Quota{}, true, QuotaUnknown, ""},
		{"exhausted", quotaValue(0, provider.QuotaKnown, 0), false, QuotaExhausted, ""},
		{"stale zero", quotaValue(0, provider.QuotaStale, time.Hour), false, QuotaExhausted, ""},
		{"latched", Quota{Exhausted: true}, false, QuotaExhausted, ""},
		{"insufficient", quotaValue(.01, provider.QuotaKnown, 0), false, QuotaInsufficient, ""},
		{"enough", quotaValue(1, provider.QuotaKnown, 0), true, Eligible, ""},
		{"stale warn", quotaValue(1, provider.QuotaKnown, 5*time.Minute), false, Eligible, QuotaStale},
		{"stale strict", quotaValue(1, provider.QuotaKnown, 5*time.Minute), true, QuotaStale, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, w := tc.q.Decide("gpu", 120, tc.strict, 5*time.Minute, testNow)
			if r != tc.want || w != tc.warning {
				t.Fatalf("%s %s", r, w)
			}
		})
	}
	for _, field := range []string{"units", "precision", "future", "resource"} {
		q := quotaValue(1, provider.QuotaKnown, 0)
		switch field {
		case "units":
			q.Observation.Unit = "credits"
		case "precision":
			q.Observation.Precision = "rounded hours"
		case "future":
			q.Observation.ObservedAt = testNow.Add(time.Second)
		case "resource":
			q.Observation.Resource = "cpu"
		}
		if r, w := q.Decide("gpu", 120, false, 5*time.Minute, testNow); r != Eligible || w != QuotaUncertain {
			t.Fatal(field, r, w)
		}
		if r, _ := q.Decide("gpu", 120, true, 5*time.Minute, testNow); r != QuotaUncertain {
			t.Fatal(field, r)
		}
	}
}
func FuzzSelectionNeverOvercommits(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 128 {
			data = data[:128]
		}
		rows := make([]Candidate, len(data))
		for i, b := range data {
			rows[i] = candidate(int64(i+1), fmt.Sprintf("w_%d", b%7), fmt.Sprintf("account_%d", b%3))
			if b&8 != 0 {
				rows[i].LeaseUntil = testNow.Add(time.Minute)
			}
			if b&16 != 0 {
				rows[i].Policy.Disabled = true
			}
		}
		index, view, err := Select(rows, "w_3", DefaultSettings(), testNow)
		if err != nil {
			t.Fatal(err)
		}
		if index >= 0 {
			c := rows[index]
			if view.LiveWorkers >= 4 || view.AccountReservations[c.AccountScope] >= 1 || c.Policy.Disabled || c.LeaseUntil.After(testNow) {
				t.Fatal("unsafe selection")
			}
		}
	})
}
