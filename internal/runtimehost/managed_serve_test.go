package runtimehost

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/connections"
)

func TestManagedHTTPAdvertisesComposedServicesWithoutStartupAccountReads(t *testing.T) {
	f := newManagedTestFixture(t, true)
	saved := f.connection(t, "saved-account-startup-key", "SYNTHETIC_FIRST")
	config := f.config
	t.Setenv("PATH", t.TempDir())
	secret, _, err := f.host.access.Issue(context.Background(), "app", []auth.Scope{auth.Read}, time.Time{})
	requireNoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	announced, done := make(chan Listening, 1), make(chan error, 1)
	go func() {
		done <- f.host.ServeConfigured(ctx, ServeConfig{Address: "127.0.0.1:0", Managed: &config}, func(v Listening) error { announced <- v; return nil })
	}()
	var listening Listening
	select {
	case listening = <-announced:
	case err := <-done:
		t.Fatal("managed HTTP startup failed", err)
	case <-time.After(5 * time.Second):
		t.Fatal("managed startup deadline")
	}
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error("managed shutdown failed", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("managed shutdown failed to join")
		}
	}
	t.Cleanup(stop)
	wantMode, wantDispatch, wantStorage := "managed-connections", false, "unsupported"
	if runtime.GOOS == "darwin" {
		wantMode, wantDispatch, wantStorage = "managed-workers", true, "available"
	}
	if listening.Mode != wantMode || listening.DispatchEnabled != wantDispatch {
		t.Fatal("misleading managed runtime mode")
	}
	get := func(path string) []byte {
		t.Helper()
		request, err := http.NewRequest(http.MethodGet, "http://"+listening.Address+path, nil)
		requireNoError(t, err)
		request.Header.Set("Authorization", "Bearer "+secret.Reveal())
		response, err := boundedClient().Do(request)
		requireNoError(t, err)
		defer response.Body.Close()
		raw, err := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		if err != nil || response.StatusCode != 200 || response.Header.Get("X-Compute-Relay-Mode") != wantMode {
			t.Fatal("managed read failed", response.StatusCode, err)
		}
		if strings.Contains(string(raw), "fixture_user") || strings.Contains(string(raw), "SYNTHETIC_FIRST") {
			t.Fatal("private runtime details escaped in public reads")
		}
		return raw
	}
	var info struct {
		Features []string `json:"features"`
	}
	if json.Unmarshal(get("/v1/info"), &info) != nil || !strings.Contains(strings.Join(info.Features, ","), "managed_connections") ||
		!strings.Contains(strings.Join(info.Features, ","), "attempt_authorization") ||
		!strings.Contains(strings.Join(info.Features, ","), "job_logs") ||
		!strings.Contains(strings.Join(info.Features, ","), "job_cleanup") {
		t.Fatal("managed feature discovery missing")
	}
	var providers struct {
		Providers []connections.Descriptor `json:"providers"`
	}
	if json.Unmarshal(get("/v1/workspaces/app/providers"), &providers) != nil || len(providers.Providers) != 2 ||
		providers.Providers[0].Type != "kaggle" || providers.Providers[1].Type != "ssh" {
		t.Fatal("managed provider types wrong")
	}
	for _, descriptor := range providers.Providers {
		if descriptor.CredentialStorage != wantStorage {
			t.Fatal("storage availability projection wrong")
		}
	}
	get("/v1/workspaces/app/connections")
	get("/v1/workspaces/app/connections/" + saved.ID)
	get("/readyz")
	stop()
	if f.adapter.checks != 1 {
		t.Fatal("saved account was reverified on startup")
	}
}

func TestManagedServeRejectsInvalidFiniteConfiguration(t *testing.T) {
	f := newManagedTestFixture(t, true)
	for _, mutate := range []func(*ManagedServeConfig){
		func(c *ManagedServeConfig) { c.MachineShape = "" },
		func(c *ManagedServeConfig) { c.MachineShape = "paid-gpu" },
		func(c *ManagedServeConfig) { c.MaxRemoteWallSeconds = 0 },
		func(c *ManagedServeConfig) { c.MaxRemoteWallSeconds = 86401 },
		func(c *ManagedServeConfig) { c.MaxWorkers = 0 },
		func(c *ManagedServeConfig) { c.MaxWorkers = 17 },
	} {
		config := f.config
		mutate(&config)
		if config.validate() != ErrRequest {
			t.Fatal("invalid managed configuration accepted")
		}
	}
}

func TestManagedRuntimeShutdownWaitsForEveryWorker(t *testing.T) {
	listener := testListener(t)
	server := &http.Server{Handler: http.NotFoundHandler()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, exited := make(chan struct{}, 3), make(chan struct{}, 3)
	release := make(chan struct{})
	services := make([]runtimeService, 3)
	for i := range services {
		index := i
		services[i].name = "worker"
		services[i].run = func(ctx context.Context) error {
			started <- struct{}{}
			<-ctx.Done()
			if index == 2 {
				<-release
			}
			exited <- struct{}{}
			return ctx.Err()
		}
	}
	ready, done := make(chan struct{}), make(chan error, 1)
	go func() {
		done <- runRuntimeServices(ctx, server, listener, "managed-workers", Listening{}, func(Listening) error { close(ready); return nil }, services)
	}()
	<-ready
	for range services {
		<-started
	}
	cancel()
	select {
	case err := <-done:
		close(release)
		t.Fatal("managed runtime released store before final worker", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil || len(exited) != 3 {
			t.Fatal("managed worker shutdown was incomplete", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("managed worker join exceeded deadline")
	}
}

func TestManagedWorkerFailureClosesHTTPAndOtherWorkersWithoutRawError(t *testing.T) {
	listener := testListener(t)
	server := &http.Server{Handler: http.NotFoundHandler()}
	stopped := make(chan struct{})
	services := []runtimeService{
		{"collection", func(context.Context) error { return errors.New("SYNTHETIC_SECRET") }},
		{"dispatch", func(ctx context.Context) error { <-ctx.Done(); close(stopped); return ctx.Err() }},
	}
	err := runRuntimeServices(context.Background(), server, listener, "managed-workers", Listening{}, func(Listening) error { return nil }, services)
	var cause ServiceStopped
	if !errors.Is(err, ErrState) || !errors.As(err, &cause) || cause != (ServiceStopped{"collection", "unclassified"}) ||
		strings.Contains(err.Error(), "SYNTHETIC") || strings.Contains(cause.DiagnosticCause(), "SYNTHETIC") {
		t.Fatal("raw worker error escaped or cause was not bounded", err)
	}
	select {
	case <-stopped:
	default:
		t.Fatal("failed runtime did not join other workers")
	}
}
