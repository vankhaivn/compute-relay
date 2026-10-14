package workloadsecret

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

type sweepRepository struct {
	Repository
	usable func(context.Context, Key, domain.OperationID) (bool, error)
}

func (r sweepRepository) WorkloadSecretsUsable(ctx context.Context, key Key, id domain.OperationID) (bool, error) {
	return r.usable(ctx, key, id)
}

type sweepClock func() time.Time

func (c sweepClock) Now() time.Time { return c() }

type sweepBusy struct{}

func (sweepBusy) Error() string   { return "busy" }
func (sweepBusy) Temporary() bool { return true }

func suppliedSweepPool(t *testing.T) (*Pool, Key, []byte, time.Time) {
	t.Helper()
	key, req, now := sampleSupply()
	defer req.Clear()
	pool, err := NewPool(bytes.Repeat([]byte{1}, 32))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Supply(key, req, now, func(_ string, m Metadata) (Receipt, bool, error) {
		return Receipt{AuthorizationID: "secret_one", WorkspaceID: key.WorkspaceID, JobID: key.JobID, Metadata: m, Status: "granted", CreatedAt: now}, true, nil
	}); err != nil {
		t.Fatal(err)
	}
	return pool, key, pool.entries[key].Values["ingest"], now
}

func TestSecretWorkerClearsUnknownMaterialAndClassifiesRepositoryFailures(t *testing.T) {
	corrupt := errors.New("corrupt state")
	for _, failure := range []error{sweepBusy{}, context.DeadlineExceeded, corrupt, errors.Join(sweepBusy{}, corrupt)} {
		t.Run(failure.Error(), func(t *testing.T) {
			pool, key, original, now := suppliedSweepPool(t)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			polls, calls := 0, 0
			service := &Service{pool: pool, clock: sweepClock(func() time.Time {
				polls++
				if polls == 2 {
					cancel()
				}
				return now
			}), repo: sweepRepository{usable: func(context.Context, Key, domain.OperationID) (bool, error) {
				calls++
				return true, failure // Unknown usability must clear bytes even alongside true.
			}}}
			started := time.Now()
			err := service.Run(ctx)
			if errors.Is(failure, corrupt) {
				if polls != 1 || !errors.Is(err, corrupt) {
					t.Fatal("corruption was hidden or retried", polls, err)
				}
			} else if polls != 2 || !errors.Is(err, context.Canceled) || time.Since(started) < 5*time.Millisecond || time.Since(started) >= time.Second {
				t.Fatal("transient failure did not back off and resume", polls, err)
			}
			if calls != 1 || !pool.closed || pool.Available(key, "secret_one", now) || !bytes.Equal(original, make([]byte, len(original))) {
				t.Fatal("failed usability retained plaintext or skipped shutdown", calls, pool.closed)
			}
		})
	}
}

func TestSecretWorkerJoinsUsabilityCallbackBeforeClosingPool(t *testing.T) {
	pool, _, original, now := suppliedSweepPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	service := &Service{pool: pool, clock: sweepClock(func() time.Time { return now }), repo: sweepRepository{usable: func(ctx context.Context, _ Key, _ domain.OperationID) (bool, error) {
		close(entered)
		<-ctx.Done()
		<-release
		return false, ctx.Err()
	}}}
	go func() { done <- service.Run(ctx) }()
	<-entered
	cancel()
	select {
	case err := <-done:
		t.Fatal("returned while usability callback still active", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || !pool.closed || !bytes.Equal(original, make([]byte, len(original))) {
			t.Fatal("shutdown did not clear and join", err)
		}
	case <-time.After(time.Second):
		t.Fatal("usability callback was not joined")
	}
}

func TestSecretWorkerReportsEveryUsabilityFailureAndClearsAllMaterial(t *testing.T) {
	pool, _, first, now := suppliedSweepPool(t)
	key, req, _ := sampleSupply()
	defer req.Clear()
	key.JobID = "job_two"
	if _, err := pool.Supply(key, req, now, func(_ string, m Metadata) (Receipt, bool, error) {
		return Receipt{AuthorizationID: "secret_two", WorkspaceID: key.WorkspaceID, JobID: key.JobID, Metadata: m, Status: "granted", CreatedAt: now}, true, nil
	}); err != nil {
		t.Fatal(err)
	}
	second := pool.entries[key].Values["ingest"]
	corrupt := errors.New("corrupt state")
	calls := 0
	service := &Service{pool: pool, clock: sweepClock(func() time.Time { return now }), repo: sweepRepository{usable: func(_ context.Context, key Key, _ domain.OperationID) (bool, error) {
		calls++
		if key.JobID == "job_two" {
			return false, corrupt
		}
		return false, context.DeadlineExceeded
	}}}
	err := service.Run(context.Background())
	if !errors.Is(err, corrupt) || !errors.Is(err, context.DeadlineExceeded) || calls != 2 || !pool.closed || len(pool.entries) != 0 {
		t.Fatal("sweep suppressed a failure or stopped before clearing every entry", calls, err)
	}
	for _, value := range [][]byte{first, second} {
		if !bytes.Equal(value, make([]byte, len(value))) {
			t.Fatal("sweep retained plaintext")
		}
	}
}
