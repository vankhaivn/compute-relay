package runtimehost

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

type resilienceFault struct {
	failures []error
	retried  chan struct{}
	resumed  chan struct{}
}

func (f *resilienceFault) claim(ctx context.Context) error {
	if len(f.failures) > 0 {
		err := f.failures[0]
		f.failures = f.failures[1:]
		if len(f.failures) == 0 && f.retried != nil {
			close(f.retried)
		}
		return err
	}
	close(f.resumed)
	<-ctx.Done()
	return ctx.Err()
}

type resilienceDispatch struct {
	dispatch.Repository
	*resilienceFault
}

func (r resilienceDispatch) ClaimRecovery(ctx context.Context, _ string, _ time.Time) (*scheduler.Claim, error) {
	return nil, r.claim(ctx)
}

type resilienceCollection struct {
	collection.Repository
	*resilienceFault
}

func (r resilienceCollection) ClaimCollection(ctx context.Context, _ time.Time, _ time.Duration, _ int) (*collection.Work, error) {
	return nil, r.claim(ctx)
}

type resilienceCleanup struct {
	cleanup.WorkRepository
	*resilienceFault
}

func (r resilienceCleanup) ClaimCleanup(ctx context.Context, _ time.Time, _ time.Duration) (*cleanup.Work, error) {
	return nil, r.claim(ctx)
}

type resilienceResolver struct{ dispatch.Resolver }
type resilienceBlobs struct{ collection.BlobStore }

func resilienceEngine(t *testing.T, kind string, fault *resilienceFault) func(context.Context) error {
	t.Helper()
	switch kind {
	case "dispatch":
		cfg := dispatch.DefaultConfig()
		cfg.Workers = 1
		cfg.PollDelay = time.Millisecond
		engine, err := dispatch.New(resilienceDispatch{resilienceFault: fault}, resilienceResolver{}, resilienceBlobs{}, nil, cfg)
		if err != nil {
			t.Fatal(err)
		}
		return engine.Run
	case "collection":
		cfg := collection.DefaultConfig()
		cfg.Workers = 1
		cfg.PollDelay = time.Millisecond
		engine, err := collection.New(resilienceCollection{resilienceFault: fault}, resilienceResolver{}, resilienceBlobs{}, nil, cfg)
		if err != nil {
			t.Fatal(err)
		}
		return engine.Run
	default:
		cfg := cleanup.DefaultConfig()
		cfg.Workers = 1
		cfg.PollDelay = time.Millisecond
		engine, err := cleanup.New(resilienceCleanup{resilienceFault: fault}, resilienceResolver{}, nil, cfg)
		if err != nil {
			t.Fatal(err)
		}
		return engine.Run
	}
}

func TestHTTPKeepsServingWhileWorkersRetryStoreFailures(t *testing.T) {
	for _, kind := range []string{"dispatch", "collection", "cleanup"} {
		t.Run(kind, func(t *testing.T) {
			listener := testListener(t)
			server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			fault := &resilienceFault{failures: []error{sqlite.ErrBusy, context.DeadlineExceeded}, retried: make(chan struct{}), resumed: make(chan struct{})}
			run := resilienceEngine(t, kind, fault)
			ready, done := make(chan struct{}), make(chan error, 1)
			go func() {
				done <- runRuntimeServices(ctx, server, listener, "managed-workers", Listening{}, func(Listening) error { close(ready); return nil }, []func(context.Context) error{run})
			}()
			select {
			case <-ready:
			case <-ctx.Done():
				t.Fatal("HTTP did not start")
			}
			select {
			case <-fault.retried:
			case err := <-done:
				t.Fatal("transient store failure stopped HTTP", err)
			case <-ctx.Done():
				t.Fatal("worker did not retry")
			}
			client := &http.Client{Timeout: time.Second}
			response, err := client.Get("http://" + listener.Addr().String() + "/readyz")
			if err != nil {
				t.Fatal("HTTP disappeared during retries", err)
			}
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Fatal(response.StatusCode)
			}
			select {
			case <-fault.resumed:
			case err := <-done:
				t.Fatal("worker did not resume", err)
			case <-ctx.Done():
				t.Fatal("worker did not resume")
			}
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal("runtime shutdown failed", err)
				}
			case <-time.After(time.Second):
				t.Fatal("runtime did not join workers")
			}
		})
	}
}

func TestInvariantWorkerErrorsStopRuntime(t *testing.T) {
	for _, kind := range []string{"dispatch", "collection", "cleanup"} {
		t.Run(kind, func(t *testing.T) {
			listener := testListener(t)
			server := &http.Server{Handler: http.NotFoundHandler()}
			fault := &resilienceFault{failures: []error{sqlite.ErrCorrupt}, resumed: make(chan struct{})}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := runRuntimeServices(ctx, server, listener, "managed-workers", Listening{}, func(Listening) error { return nil }, []func(context.Context) error{resilienceEngine(t, kind, fault)})
			if !errors.Is(err, ErrState) {
				t.Fatal("corrupt state did not stop runtime safely", err)
			}
		})
	}
}
