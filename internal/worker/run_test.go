package worker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

type sqliteError int

func (e sqliteError) Error() string { return "sqlite failure" }
func (e sqliteError) Code() int     { return int(e) }

type busyError struct{}

func (busyError) Error() string   { return "busy" }
func (busyError) Temporary() bool { return true }

func TestClassifyOnlyKnownTransientErrors(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want Decision
	}{
		{nil, Continue}, {context.DeadlineExceeded, Retry}, {fmt.Errorf("wrapped: %w", busyError{}), Retry},
		{sqliteError(5), Retry}, {sqliteError(6), Retry}, {sqliteError(5 + 256), Retry},
		{errors.Join(busyError{}, sqliteError(11)), Stop}, {errors.Join(context.DeadlineExceeded, sqliteError(26)), Stop}, {sqliteError(11), Stop}, {sqliteError(26), Stop}, {context.Canceled, Stop}, {errors.New("SQLITE_BUSY corruption"), Stop},
	} {
		if got := Classify(tc.err); got != tc.want {
			t.Fatalf("%v: %v, want %v", tc.err, got, tc.want)
		}
	}
}

func TestRunBacksOffAndResumesFreshClaims(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	started := time.Now()
	err := Run(ctx, Config{Workers: 1, PollDelay: time.Millisecond}, func(context.Context, int) (bool, error) {
		calls++
		if calls <= 3 {
			return false, context.DeadlineExceeded
		}
		cancel()
		return true, nil
	}, Classify)
	if !errors.Is(err, context.Canceled) || calls != 4 || time.Since(started) < 35*time.Millisecond {
		t.Fatal("worker did not back off and recover", calls, err)
	}
}

func TestRunWaitsForSideEffectsAfterFatalFailure(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	fatal := errors.New("invariant failed")
	go func() {
		done <- Run(context.Background(), Config{Workers: 2, PollDelay: time.Millisecond}, func(ctx context.Context, index int) (bool, error) {
			if index == 0 {
				close(started)
				<-ctx.Done()
				<-release
				return true, nil
			}
			<-started
			return false, fatal
		}, Classify)
	}()
	select {
	case err := <-done:
		t.Fatal("returned while callback still active", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, fatal) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("workers not joined")
	}
}
