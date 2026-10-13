package scheduler

import (
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

func machineCandidate(seq int64, workspace, account, resource string, limit int) Candidate {
	c := candidate(seq, workspace, account)
	c.ResourceKind, c.ResourceScope, c.ResourceMaxActive, c.Policy.MaxActive = "machine", resource, limit, limit
	return c
}

func TestResourceBudgetCombinesDifferentAccountsAndPreservesFrozenLimits(t *testing.T) {
	s := DefaultSettings()
	s.MaxActivePerAccount, s.MaxWorkers = 64, 4
	rows := []Candidate{
		machineCandidate(1, "a", "lan", "machine_a", 2),
		machineCandidate(2, "b", "tailscale", "machine_a", 2),
		machineCandidate(3, "c", "other_login", "machine_a", 2),
		machineCandidate(4, "d", "other_host", "machine_b", 2),
	}
	rows[0].LeaseUntil, rows[1].LeaseUntil = testNow.Add(time.Minute), testNow.Add(time.Minute)
	i, view, err := Select(rows, "", s, testNow)
	if err != nil || i != 3 || view.ResourceReservations["machine_a"] != 2 || view.Heads[0].Reason != ResourceLimit {
		t.Fatal("connection aliases multiplied machine capacity", i, view, err)
	}
	rows[1].State = domain.InitialAttemptState()
	rows[1].ActiveAttempt, rows[1].LeaseUntil = false, time.Time{}
	rows[0].ResourceMaxActive = 1
	i, view, err = Select(rows, "", s, testNow)
	if err != nil || i != 3 || view.Heads[0].Reason != ResourceLimit {
		t.Fatal("older active profile lost its lower resource bound", i, view, err)
	}
	s.MaxWorkers = 1
	if i, view, err = Select(rows, "", s, testNow); err != nil || i != -1 || view.Heads[1].Reason != WorkerLimit {
		t.Fatal("independent resource bypassed worker ceiling", i, view, err)
	}
}

func TestUnresolvedResourceCannotCreateOrHideCapacity(t *testing.T) {
	s := DefaultSettings()
	s.MaxActivePerAccount = 64
	unknown := machineCandidate(1, "a", "legacy", "", 1)
	known := machineCandidate(2, "b", "verified", "machine_a", 2)
	i, view, err := Select([]Candidate{unknown, known}, "", s, testNow)
	if err != nil || i != 1 || view.Heads[0].Reason != ResourceUnverified {
		t.Fatal("unverified queued job obtained a pool", i, view, err)
	}
	unknown.State.Orchestration = domain.OrchestrationReconciling
	unknown.State.Execution, unknown.State.RemoteActivity = domain.ExecutionUnknown, domain.RemoteActivityPossible
	unknown.LeaseUntil = testNow.Add(-time.Hour)
	i, view, err = Select([]Candidate{unknown, known}, "", s, testNow)
	if err != nil || i != -1 || len(view.Heads) != 1 || view.Heads[0].Reason != ResourceUnresolved {
		t.Fatal("unknown old work disappeared from resource capacity", i, view, err)
	}
	unknown.ResourceScope = known.ResourceScope
	i, view, err = Select([]Candidate{unknown, known}, "", s, testNow)
	if err != nil || i != -1 || view.ResourceReservations[known.ResourceScope] != 1 || view.Heads[0].Reason != ResourceLimit {
		t.Fatal("verification reset reservation or old lower limit", i, view, err)
	}
}

func TestResourceHoldsPreparedAndUncertainWorkUntilConfirmedInactive(t *testing.T) {
	s := DefaultSettings()
	s.MaxActivePerAccount = 64
	for _, phase := range []domain.OrchestrationState{domain.OrchestrationPreparing, domain.OrchestrationSubmitted, domain.OrchestrationRunning, domain.OrchestrationCancelling, domain.OrchestrationReconciling, domain.OrchestrationNeedsAttention} {
		t.Run(string(phase), func(t *testing.T) {
			held := machineCandidate(1, "a", "lan", "machine", 1)
			held.DispatchBarrier = true
			held.LeaseUntil = testNow.Add(-time.Hour)
			held.State.Orchestration = phase
			if phase != domain.OrchestrationPreparing {
				held.State.Execution, held.State.RemoteActivity = domain.ExecutionUnknown, domain.RemoteActivityPossible
			}
			if phase == domain.OrchestrationRunning {
				held.State.Execution, held.State.RemoteActivity = domain.ExecutionRunning, domain.RemoteActivityActive
			}
			if phase == domain.OrchestrationCancelling {
				held.State.Cancellation = domain.CancellationRequested
			}
			next := machineCandidate(2, "b", "vpn", "machine", 2)
			i, view, err := Select([]Candidate{held, next}, "", s, testNow)
			if err != nil || i != -1 || view.ResourceReservations["machine"] != 1 || view.Heads[0].Reason != ResourceLimit {
				t.Fatal("held or uncertain attempt freed capacity", i, view, err)
			}
			held.State = domain.InitialAttemptState()
			held.State.Orchestration = domain.OrchestrationCollecting
			held.State.Execution, held.State.RemoteActivity, held.State.Result = domain.ExecutionSucceeded, domain.RemoteActivityInactive, domain.ResultCollecting
			i, view, err = Select([]Candidate{held, next}, "", s, testNow)
			if err != nil || i != 1 || view.ResourceReservations["machine"] != 0 {
				t.Fatal("confirmed completion still holds execution capacity", i, view, err)
			}
		})
	}
}
