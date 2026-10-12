package runtimehost

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

type managedCancellationProbe struct {
	*sqlite.Store
	commit func(context.Context, dispatch.Handle, domain.OperationID, dispatch.CancellationAction, time.Time) (dispatch.Work, error)
}

func (p *managedCancellationProbe) CommitCancellation(ctx context.Context, handle dispatch.Handle, operation domain.OperationID, action dispatch.CancellationAction, now time.Time) (dispatch.Work, error) {
	return p.commit(ctx, handle, operation, action, now)
}

func TestManagedDispatchRepositoryPreservesCancellation(t *testing.T) {
	uncertain := errors.New("uncertain cancellation acknowledgement")
	for name, action := range map[string]dispatch.CancellationAction{
		"intent":      {Begin: true},
		"confirmed":   {Outcome: &provider.CancellationOutcome{Status: domain.CancellationConfirmed, TerminationConfirmed: true}},
		"unsupported": {Code: domain.CodeRemoteCancelUnsupported},
	} {
		for mode, firstError := range map[string]error{"committed": nil, "stale-clock": scheduler.ErrClock, "uncertain": uncertain} {
			t.Run(name+"/"+mode, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				handle := dispatch.Handle{Version: 7}
				operation := domain.OperationID("op_cancel_fixture")
				stale := time.Now().UTC().Add(-time.Second)
				want := dispatch.Work{Handle: dispatch.Handle{Version: 8}}
				calls := 0
				probe := &managedCancellationProbe{commit: func(gotCtx context.Context, gotHandle dispatch.Handle, gotOperation domain.OperationID, gotAction dispatch.CancellationAction, now time.Time) (dispatch.Work, error) {
					calls++
					if gotCtx != ctx || gotHandle != handle || gotOperation != operation || !reflect.DeepEqual(gotAction, action) {
						t.Fatal("cancellation commit changed its context, fence, operation or evidence")
					}
					if calls == 1 {
						if !now.Equal(stale) {
							t.Fatal("initial cancellation time changed")
						}
						return want, firstError
					}
					if calls > 2 || !now.After(stale) {
						t.Fatal("clock retry did not use fresh time or repeated a committed write")
					}
					return want, nil
				}}
				// Dispatch discovers this optional port before persisting the one-shot
				// intent. Hiding it leaves accepted cancellation stuck indefinitely.
				port, ok := any(managedDispatchRepository{probe}).(dispatch.CancellationRepository)
				if !ok {
					t.Fatal("managed dispatch hides the cancellation repository")
				}
				got, err := port.CommitCancellation(ctx, handle, operation, action, stale)
				wantCalls, wantErr := 1, firstError
				if errors.Is(firstError, scheduler.ErrClock) {
					wantCalls, wantErr = 2, nil
				}
				if calls != wantCalls || err != wantErr || !reflect.DeepEqual(got, want) {
					t.Fatalf("cancellation result lost or acknowledgement retried: calls=%d err=%v", calls, err)
				}
			})
		}
	}
}
