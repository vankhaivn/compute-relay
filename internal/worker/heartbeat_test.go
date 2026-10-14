package worker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestHeartbeatRenewsAndJoinsOnStop(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	ctx, stop := Heartbeat(context.Background(), time.Now(), nil, func(ctx context.Context) (time.Time, error) {
		close(entered)
		<-ctx.Done()
		<-release // Acknowledging cancellation does not prove that a callback returned.
		return time.Time{}, ctx.Err()
	})
	<-entered
	done := make(chan error, 1)
	go func() { done <- stop() }()
	<-ctx.Done()
	select {
	case err := <-done:
		t.Fatal("returned before joining renewal", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestHeartbeatCancelsAndPreservesRenewalError(t *testing.T) {
	for _, failure := range []error{busyError{}, errors.New("invariant"), errors.New("lease lost")} {
		t.Run(failure.Error(), func(t *testing.T) {
			calls := 0
			ctx, stop := Heartbeat(context.Background(), time.Now(), nil, func(context.Context) (time.Time, error) {
				calls++
				return time.Time{}, failure
			})
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
				t.Fatal("renewal failure left callback context live")
			}
			if err := stop(); !errors.Is(err, failure) || calls != 1 || Classify(err) != Classify(failure) {
				t.Fatal("renewal was replayed or its policy changed", calls, err)
			}
		})
	}
}

func TestHeartbeatContinuesAfterSuccessfulRenewal(t *testing.T) {
	renewed := make(chan struct{})
	ctx, stop := Heartbeat(context.Background(), time.Now(), nil, func(context.Context) (time.Time, error) {
		close(renewed)
		return time.Now().Add(time.Minute), nil
	})
	<-renewed
	if ctx.Err() != nil {
		t.Fatal("successful renewal canceled callback")
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}

func TestHeartbeatErrorKeepsSubstantiveFailuresAndDropsCancellation(t *testing.T) {
	busy := busyError{}
	fatal := sqliteError(11)
	for _, tc := range []struct {
		name       string
		processing error
		want       Decision
	}{
		{"nil", nil, Retry},
		{"cancellation", context.Canceled, Retry},
		{"wrapped cancellation", fmt.Errorf("callback stopped: %w", context.Canceled), Retry},
		{"transient and cancellation", errors.Join(busy, context.Canceled), Retry},
		{"fatal", fatal, Stop},
		{"fatal and cancellation", errors.Join(fatal, context.Canceled), Stop},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, stop := Heartbeat(context.Background(), time.Now(), nil, func(context.Context) (time.Time, error) {
				return time.Time{}, busy
			})
			<-ctx.Done()
			combined := HeartbeatError(tc.processing, stop())
			if !errors.Is(combined, busy) || Classify(combined) != tc.want {
				t.Fatal("heartbeat changed error policy", combined, Classify(combined))
			}
			if errors.Is(tc.processing, fatal) && !errors.Is(combined, fatal) {
				t.Fatal("concurrent invariant failure was lost", combined)
			}
		})
	}
	if got := HeartbeatError(context.Canceled, nil); got != context.Canceled {
		t.Fatal("independent cancellation was discarded", got)
	}
}

func TestHeartbeatStopKeepsConcurrentRenewalFailure(t *testing.T) {
	entered := make(chan struct{})
	fatal := sqliteError(11)
	_, stop := Heartbeat(context.Background(), time.Now(), nil, func(ctx context.Context) (time.Time, error) {
		close(entered)
		<-ctx.Done()
		return time.Time{}, errors.Join(ctx.Err(), fatal)
	})
	<-entered
	err := stop()
	if !errors.Is(err, fatal) || errors.Is(err, context.Canceled) || Classify(err) != Stop {
		t.Fatal("stopping suppressed a substantive renewal error", err)
	}
}
