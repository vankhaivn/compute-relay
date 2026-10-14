package sqlite

import (
	"errors"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func TestPreparationProgressRejectsWrongPhaseAndForgedGeneration(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	f.seed(t, "a", 1, false)
	requireNoError(t, f.step(t))
	f.clock.advance(time.Minute)
	claim, err := f.s.ClaimRecovery(t.Context(), "progress-storage")
	if err != nil || claim == nil {
		t.Fatal(err)
	}
	work, err := f.s.LoadDispatch(t.Context(), *claim)
	requireNoError(t, err)
	sample := domain.PreparationProgress{Scope: "staged_input_bytes", Generation: work.Handle.Claim.Generation, BytesTotal: 100, ObservedAt: f.clock.Now()}
	if err = f.s.RecordPreparationProgress(t.Context(), work.Handle, sample); !errors.Is(err, dispatch.ErrConflict) {
		t.Fatal("progress outside staging", err)
	}
	stale := work.Handle
	stale.Claim.Generation++
	sample.Generation++
	if err = f.s.RecordPreparationProgress(t.Context(), stale, sample); !errors.Is(err, scheduler.ErrLeaseLost) {
		t.Fatal("forged owner recorded progress", err)
	}
}
