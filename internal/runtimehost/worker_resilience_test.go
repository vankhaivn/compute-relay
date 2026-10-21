package runtimehost

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
	"github.com/vankhaivn/compute-relay/internal/provider"
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

func (r resilienceDispatch) ClaimRecovery(ctx context.Context, _ string) (*scheduler.Claim, error) {
	return nil, r.claim(ctx)
}

type resilienceCollection struct {
	collection.Repository
	*resilienceFault
}

func (r resilienceCollection) ClaimCollection(ctx context.Context, _ time.Duration, _ int) (*collection.Work, error) {
	return nil, r.claim(ctx)
}

type resilienceCleanup struct {
	cleanup.WorkRepository
	*resilienceFault
}

func (r resilienceCleanup) ClaimCleanup(ctx context.Context, _ time.Duration) (*cleanup.Work, error) {
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
		requireNoError(t, err)
		return engine.Run
	case "collection":
		cfg := collection.DefaultConfig()
		cfg.Workers = 1
		cfg.PollDelay = time.Millisecond
		engine, err := collection.New(resilienceCollection{resilienceFault: fault}, resilienceResolver{}, resilienceBlobs{}, nil, cfg)
		requireNoError(t, err)
		return engine.Run
	default:
		cfg := cleanup.DefaultConfig()
		cfg.Workers = 1
		cfg.PollDelay = time.Millisecond
		engine, err := cleanup.New(resilienceCleanup{resilienceFault: fault}, resilienceResolver{}, cfg)
		requireNoError(t, err)
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
			if kind == "dispatch" {
				fault.failures = append(fault.failures, provider.Problem(domain.CodeProviderSubmissionUnknown, domain.FailureStageSubmission, "submission unknown"))
			}
			run := resilienceEngine(t, kind, fault)
			ready, done := make(chan struct{}), make(chan error, 1)
			go func() {
				done <- runRuntimeServices(ctx, server, listener, "managed-workers", Listening{}, func(Listening) error { close(ready); return nil }, []runtimeService{{kind, run}})
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
			err := runRuntimeServices(ctx, server, listener, "managed-workers", Listening{}, func(Listening) error { return nil }, []runtimeService{{kind, resilienceEngine(t, kind, fault)}})
			var cause ServiceStopped
			if !errors.Is(err, ErrState) || !errors.As(err, &cause) || cause != (ServiceStopped{kind, "state_corrupt"}) {
				t.Fatal("corrupt state did not stop runtime with a named cause", err)
			}
			if got := cause.DiagnosticCause(); got != "Runtime stopped: service="+kind+" class=state_corrupt." {
				t.Fatal(got)
			}
		})
	}
}

type idleCleanup struct{ cleanup.WorkRepository }

func (idleCleanup) ClaimCleanup(context.Context, time.Duration) (*cleanup.Work, error) {
	return nil, nil
}

// interruptedStorageCleanup offers one running deletion whose lease the engine must not settle.
type interruptedStorageCleanup struct {
	claims    chan struct{}
	completed chan struct{}
}

func (r interruptedStorageCleanup) ClaimStorageCleanup(context.Context, time.Duration) (*cleanup.StorageWork, error) {
	select {
	case r.claims <- struct{}{}:
	default:
	}
	return &cleanup.StorageWork{
		Lease:   cleanup.Lease{WorkspaceID: "a", OperationID: "op_1", Generation: 1, Fence: strings.Repeat("f", 64), Until: time.Now().Add(time.Minute)},
		Binding: provider.BindingSnapshot{Binding: domain.ProviderBinding{Profile: "fixture", ProviderInstanceID: "fake_1", ConfigurationRevision: "one"}, AccountScope: "account", CredentialRef: "env:FIXTURE"},
		Target: hoststorage.Target{Scope: hoststorage.Scope{InstallationID: "rt_1", MachineID: domain.SHA256Digest(strings.Repeat("a", 64)), EngineID: domain.SHA256Digest(strings.Repeat("b", 64))},
			Class: hoststorage.Volume, ID: "relay-cache-example", Labels: map[string]string{"io.compute-relay.installation": "rt_1"}, Action: hoststorage.OrphanVolume},
		State: "running",
	}, nil
}
func (interruptedStorageCleanup) RenewStorageCleanup(context.Context, cleanup.StorageWork, time.Duration) (time.Time, error) {
	return time.Now().Add(time.Minute), nil
}
func (interruptedStorageCleanup) BeginStorageCleanup(context.Context, cleanup.StorageWork) error {
	return nil
}
func (r interruptedStorageCleanup) CompleteStorageCleanup(context.Context, cleanup.StorageWork, hoststorage.DeleteOutcome) error {
	close(r.completed)
	return nil
}

type unreachableProvider struct{ provider.Provider }

func (unreachableProvider) Verify(context.Context, provider.BindingSnapshot) error {
	return errors.New("SYNTHETIC_SSH_CONNECTION_RESET")
}

type unreachableResolver struct{}

func (unreachableResolver) Resolve(provider.BindingSnapshot) (provider.Provider, error) {
	return unreachableProvider{}, nil
}

func TestProviderOutageDuringCleanupRecoveryKeepsRuntimeServing(t *testing.T) {
	listener := testListener(t)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })}
	storage := interruptedStorageCleanup{claims: make(chan struct{}), completed: make(chan struct{})}
	cfg := cleanup.DefaultConfig()
	cfg.PollDelay = time.Millisecond
	engine, err := cleanup.New(idleCleanup{}, unreachableResolver{}, cfg)
	requireNoError(t, err)
	engine.WithStorage(storage)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runRuntimeServices(ctx, server, listener, "managed-workers", Listening{}, func(Listening) error { return nil },
			[]runtimeService{{"cleanup", engine.Run}})
	}()
	// A second claim proves the provider failure deferred recovery instead of stopping the service.
	for range 2 {
		select {
		case <-storage.claims:
		case err := <-done:
			t.Fatal("provider outage stopped the runtime", err)
		case <-ctx.Done():
			t.Fatal("cleanup did not retry recovery")
		}
	}
	response, err := (&http.Client{Timeout: time.Second}).Get("http://" + listener.Addr().String() + "/readyz")
	if err != nil {
		t.Fatal("HTTP disappeared during provider outage", err)
	}
	_ = response.Body.Close()
	select {
	case <-storage.completed:
		t.Fatal("an unverified provider settled an interrupted deletion")
	default:
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal("runtime shutdown failed", err)
	}
}
