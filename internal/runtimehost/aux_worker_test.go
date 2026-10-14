package runtimehost

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/retention"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

type auxRetentionFunc func(context.Context) (retention.SweepReport, int64, error)

func (f auxRetentionFunc) SweepOnce(ctx context.Context, _ retention.Policy, _ int64, _ int) (retention.SweepReport, int64, error) {
	return f(ctx)
}

func TestAuxiliaryWorkersRetryTransientErrorsAndStopOnCorruption(t *testing.T) {
	for _, kind := range []string{"connection", "retention"} {
		for _, failure := range []error{sqlite.ErrBusy, context.DeadlineExceeded, sqlite.ErrCorrupt, errors.Join(sqlite.ErrBusy, sqlite.ErrCorrupt), errors.Join(connections.ErrVault, sqlite.ErrCorrupt)} {
			t.Run(kind+"/"+failure.Error(), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				calls := 0
				step := func(context.Context) error {
					calls++
					if calls == 1 {
						return failure
					}
					cancel()
					return nil
				}
				var err error
				started := time.Now()
				if kind == "connection" {
					err = runConnectionOperations(ctx, connectionRunnerFunc(func(ctx context.Context) (bool, error) { return false, step(ctx) }))
				} else {
					err = runRetention(ctx, auxRetentionFunc(func(ctx context.Context) (retention.SweepReport, int64, error) {
						return retention.SweepReport{}, 0, step(ctx)
					}), time.Hour)
				}
				if failure == sqlite.ErrBusy || failure == context.DeadlineExceeded {
					if calls != 2 || time.Since(started) < 5*time.Millisecond || time.Since(started) >= time.Second {
						t.Fatal("transient error did not back off and resume", calls, err)
					}
					if kind == "connection" && !errors.Is(err, context.Canceled) || kind == "retention" && err != nil {
						t.Fatal("shutdown failure", err)
					}
				} else if calls != 1 || err != ErrState {
					t.Fatal("invariant failure was retried or leaked", calls, err)
				}
			})
		}
	}
}

func TestAuxiliaryWorkersJoinCallbacksThatIgnoreCancellation(t *testing.T) {
	for _, kind := range []string{"connection", "retention"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered, release := make(chan struct{}), make(chan struct{})
			done := make(chan error, 1)
			step := func(ctx context.Context) error { close(entered); <-ctx.Done(); <-release; return ctx.Err() }
			go func() {
				if kind == "connection" {
					done <- runConnectionOperations(ctx, connectionRunnerFunc(func(ctx context.Context) (bool, error) { return false, step(ctx) }))
				} else {
					done <- runRetention(ctx, auxRetentionFunc(func(ctx context.Context) (retention.SweepReport, int64, error) {
						return retention.SweepReport{}, 0, step(ctx)
					}), time.Hour)
				}
			}()
			<-entered
			cancel()
			select {
			case err := <-done:
				t.Fatal("returned before callback joined", err)
			case <-time.After(20 * time.Millisecond):
			}
			close(release)
			select {
			case err := <-done:
				if kind == "connection" && !errors.Is(err, context.Canceled) || kind == "retention" && err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("callback was not joined")
			}
		})
	}
}

func TestRetentionPassAccumulatesTransientFailuresButStopsOnInvariant(t *testing.T) {
	pass := &scriptedPass{steps: []sweepStep{{1, 1, sqlite.ErrBusy}, {1, 2, sqlite.ErrCorrupt}, {1, 3, nil}}}
	err := sweepRetention(context.Background(), pass)
	if !errors.Is(err, sqlite.ErrBusy) || !errors.Is(err, sqlite.ErrCorrupt) || len(pass.calls) != 2 || len(pass.steps) != 1 {
		t.Fatal("invariant error did not stop the pass with both failures", pass.calls, err)
	}
	calls := 0
	err = sweepRetention(context.Background(), auxRetentionFunc(func(context.Context) (retention.SweepReport, int64, error) {
		calls++
		return retention.SweepReport{Examined: 1}, int64(calls), sqlite.ErrBusy
	}))
	if calls != retentionMaxPages || !errors.Is(err, sqlite.ErrBusy) {
		t.Fatal("pass lost its finite bound or failure", calls, err)
	}
}
