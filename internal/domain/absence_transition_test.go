package domain

import "testing"

func TestExactAbsenceCanEndActivityWithoutInventingExit(t *testing.T) {
	current := InitialAttemptState()
	current.Orchestration = OrchestrationRunning
	current.Execution = ExecutionRunning
	current.RemoteActivity = RemoteActivityActive
	next := current
	next.Orchestration = OrchestrationNeedsAttention
	next.Execution = ExecutionUnknown
	next.RemoteActivity = RemoteActivityInactive
	next.ReleaseEvidence = ReleaseEvidenceProviderReported
	if err := ValidateAttemptStateTransition(current, next); err != nil {
		t.Fatal("truthful inactivity rejected", err)
	}
	for _, mutate := range []func(*AttemptState){func(s *AttemptState) { s.ReleaseEvidence = ReleaseEvidenceUnknown }, func(s *AttemptState) { s.RemoteActivity = RemoteActivityPossible }, func(s *AttemptState) { s.Orchestration = OrchestrationReconciling }} {
		bad := next
		mutate(&bad)
		if ValidateAttemptStateTransition(current, bad) == nil {
			t.Fatal("unknown execution accepted without explicit inactive attention evidence", bad)
		}
	}
}
