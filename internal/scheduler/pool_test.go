package scheduler

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Explicitly nondurable pool fixture. SQL ownership is exercised separately by
// store/sqlite tests; this fixture proves only goroutine/context and pacing behavior.
type poolRepo struct {
	mu     sync.Mutex
	n      int64
	defers int
	err    error
}

func (r *poolRepo) ClaimNext(ctx context.Context, owner string, now time.Time) (Result, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return Result{}, r.err
	}
	r.n++
	c := candidate(r.n, "a", "x")
	return Result{Claim: &Claim{Identity: c.Identity, Sequence: r.n, Owner: owner, Generation: 1, Fence: strings.Repeat("a", 64), ExpiresAt: now.Add(35 * time.Millisecond), AttemptRevision: 1, AccountScope: "x", ProviderInstanceID: "p"}}, nil
}
func (r *poolRepo) RenewClaim(context.Context, Claim, time.Time) (Claim, error) {
	return Claim{}, ErrLeaseLost
}
func (r *poolRepo) DeferClaim(context.Context, Claim, time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.defers++
	return nil
}
func (r *poolRepo) ReleaseClaim(context.Context, Claim, time.Time) error { return nil }
func (r *poolRepo) InspectScheduler(context.Context, time.Time) (View, error) {
	return View{MaxWorkers: 64}, nil
}

type processor func(context.Context, Claim) error

func (p processor) PrepareLocal(ctx context.Context, c Claim) error { return p(ctx, c) }
func TestFixedPoolNeverReplacesUncooperativeWorkers(t *testing.T) {
	repo := &poolRepo{}
	s, _ := New(repo, nil)
	ctx, cancel := context.WithCancel(context.Background())
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	done := make(chan error, 1)
	var active, maxActive atomic.Int32
	p := processor(func(ctx context.Context, _ Claim) error {
		n := active.Add(1)
		for old := maxActive.Load(); n > old && !maxActive.CompareAndSwap(old, n); old = maxActive.Load() {
		}
		entered <- struct{}{}
		<-ctx.Done()
		<-release
		active.Add(-1)
		return nil
	})
	go func() { done <- s.RunLocal(ctx, "local", 2, 10*time.Millisecond, p) }()
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("workers not started")
		}
	}
	// Deadline passes while both callbacks deliberately ignore cancellation. There must
	// be no replacement goroutine and shutdown must not report false completion.
	time.Sleep(60 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		t.Fatal("returned before local workers stopped", err)
	default:
	}
	if maxActive.Load() != 2 {
		t.Fatal("worker cap", maxActive.Load())
	}
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pool did not stop")
	}
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.n != 2 || repo.defers != 0 {
		t.Fatal("shutdown performed extra claims or false release", repo.n, repo.defers)
	}
}
func TestPoolStopsOnStoreFailureAndMasksProcessorPanic(t *testing.T) {
	repo := &poolRepo{err: ErrClock}
	s, _ := New(repo, nil)
	if err := s.RunLocal(context.Background(), "local", 1, 10*time.Millisecond, processor(func(context.Context, Claim) error { return nil })); !errors.Is(err, ErrClock) {
		t.Fatal(err)
	}
	if err := prepareSafely(context.Background(), processor(func(context.Context, Claim) error { panic("private canary") }), Claim{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := New(nil, nil); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := s.RunLocal(context.Background(), "local", 0, time.Second, nil); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

func TestServiceWaitHonorsCancellation(t *testing.T) {
	s, _ := New(&poolRepo{}, nil)
	s.gate <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := s.Next(ctx, "waiting"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("service gate ignored deadline")
	}
	<-s.gate
}
