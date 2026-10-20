package retention

import (
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

func TestPolicyConservativePinsAndWindows(t *testing.T) {
	now := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	old := now.Add(-30 * 24 * time.Hour)
	complete := domain.InitialAttemptState()
	complete.Orchestration = domain.OrchestrationSucceeded
	complete.Execution = domain.ExecutionSucceeded
	complete.Result = domain.ResultAvailable
	complete.RemoteActivity = domain.RemoteActivityInactive
	ref := Reference{State: complete, UpdatedAt: old}
	for _, tc := range []struct {
		name string
		edit func(*Reference)
		want string
	}{
		{"complete", func(*Reference) {}, "retention_elapsed"},
		{"busy", func(r *Reference) { r.Busy = true }, "local_work"},
		{"recovery", func(r *Reference) { r.RecoveryRequired = true }, "recovery_required"},
		{"queued", func(r *Reference) { r.State = domain.InitialAttemptState() }, "active_or_unresolved"},
		{"unknown", func(r *Reference) {
			r.State.Orchestration = domain.OrchestrationNeedsAttention
			r.State.Execution = domain.ExecutionUnknown
			r.State.Result = domain.ResultNotAvailable
			r.State.RemoteActivity = domain.RemoteActivityPossible
		}, "active_or_unresolved"},
		{"missing-result", func(r *Reference) {
			r.State.Orchestration = domain.OrchestrationFailed
			r.State.Result = domain.ResultIncomplete
		}, "recovery_required"},
		{"recent", func(r *Reference) { r.UpdatedAt = now }, "retention_window"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := ref
			tc.edit(&r)
			d, err := DefaultPolicy().Assess(now, old, []Reference{ref, r})
			if err != nil || d.Reason != tc.want || d.Eligible != (tc.want == "retention_elapsed") {
				t.Fatalf("decision = %+v, %v", d, err)
			}
			wantBlocker := tc.want
			if wantBlocker == "retention_elapsed" || wantBlocker == "retention_window" {
				wantBlocker = ""
			}
			if r.ReleaseBlocker() != wantBlocker {
				t.Fatal("explicit release changed safety guards", r.ReleaseBlocker(), wantBlocker)
			}
		})
	}
	p := DefaultPolicy()
	for _, delta := range []time.Duration{-time.Nanosecond, 0, time.Nanosecond} {
		d, err := p.Assess(now, now.Add(-p.UnreferencedFor).Add(-delta), nil)
		if err != nil || d.Eligible != (delta >= 0) {
			t.Fatal("unreferenced boundary", delta, d, err)
		}
	}
	if _, err := p.Assess(now, now.Add(time.Second), nil); err == nil {
		t.Fatal("future evidence accepted")
	}
	if _, err := p.Assess(now, old, make([]Reference, MaxReferences+1)); err == nil {
		t.Fatal("unbounded reference inventory accepted")
	}
}
