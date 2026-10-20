package runtimehost

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/api"
	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
	"github.com/vankhaivn/compute-relay/internal/joblogs"
	"github.com/vankhaivn/compute-relay/internal/objects"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/retention"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

type clock struct{}

func (clock) Now() time.Time { return time.Now().UTC() }

type Listening struct {
	Status          string `json:"status"`
	Address         string `json:"address"`
	Mode            string `json:"mode"`
	DispatchEnabled bool   `json:"dispatch_enabled"`
}

type ServeConfig struct {
	Address string
	Managed *ManagedServeConfig
}

// Serve composes managed connections with finite CPU policy by default.
// Empty installations perform no provider work until a connection is configured.
func (h *Host) Serve(ctx context.Context, address string, announce func(Listening) error) error {
	return h.ServeConfigured(ctx, ServeConfig{Address: address}, announce)
}

// ServeConfigured joins HTTP handlers and managed provider workers
// before returning. Local shutdown never calls provider cancellation or cleanup.
func (h *Host) ServeConfigured(ctx context.Context, serve ServeConfig, announce func(Listening) error) error {
	if h == nil || announce == nil || !ValidListen(serve.Address) {
		return ErrRequest
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	objectService, err := objects.New(h.access, h.inputs, h.store)
	if err != nil {
		return ErrState
	}
	jobs, err := admission.New(h.access, h.store, admission.DefaultLimits())
	if err != nil {
		return ErrState
	}
	controls, err := operations.New(h.access, h.store, h.inputs, admission.DefaultLimits())
	if err != nil {
		return ErrState
	}
	results, err := collection.NewReader(h.access, h.store, h.results)
	if err != nil {
		return ErrState
	}
	cleanups, err := cleanup.NewService(h.access, h.store, nil)
	if err != nil {
		return ErrState
	}

	config := DefaultManagedServeConfig()
	if serve.Managed != nil {
		config = *serve.Managed
	}
	managed, err := h.managedWorkers(ctx, config)
	if err != nil {
		return err
	}
	jobs.WithPreflight(managed.storageGuard)
	dispatcher, collector, schedulerSettings := managed.dispatcher, managed.collector, managed.settings
	mode := "managed-connections"
	dispatchEnabled := dispatcher != nil
	var logResolver joblogs.Resolver
	var cleanupRepo cleanup.WorkRepository
	var cleanupResolver cleanup.Resolver
	if managed.resolver != nil {
		logResolver, cleanupResolver = managed.resolver, managed.resolver
	}
	if dispatchEnabled {
		mode = "managed-workers"
		cleanupRepo = h.store
	}
	if dispatchEnabled {
		defer func() {
			pauseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			schedulerSettings.Paused = true
			_ = h.store.ConfigureScheduler(pauseCtx, schedulerSettings)
		}()
	}
	var cleaner *cleanup.Engine
	cleanups = cleanups.WithPreviewResolver(cleanupResolver)
	if cleanupRepo != nil {
		if cleaner, err = cleanupEngine(cleanupRepo, cleanupResolver); err != nil {
			return err
		}
		cleaner.WithStorage(h.store)
	}

	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", serve.Address)
	if err != nil {
		return ErrState
	}
	defer listener.Close()
	cfg := api.DefaultConfig()
	cfg.Listen = listener.Addr().String()
	cfg.Jobs, cfg.Operations, cfg.Results, cfg.Cleanup = jobs, controls, results, cleanups
	sweeper, err := h.retentionSweeper()
	if err != nil {
		return err
	}
	cfg.ResultReleases, err = retention.NewReleaser(h.access, h.store, sweeper)
	if err != nil {
		return ErrState
	}
	cfg.Logs, err = joblogs.NewReader(h.access, h.store, logResolver)
	if err != nil {
		return ErrState
	}
	if managed != nil {
		cfg.Connections, cfg.Authorizations = managed.connections, managed.authorizations
		cfg.Secrets = managed.secrets
		cfg.Health = managed.health
		cfg.HostWatchdog = managed.watchdog
		cfg.Storage = h.store
		if managed.resolver != nil {
			cfg.StorageCleanup, err = hoststorage.NewService(h.store, managed.resolver)
			if err != nil {
				return ErrState
			}
		}
	}
	server, err := api.NewServer(cfg, h.access, objectService, h.store.Ready)
	if err != nil {
		return ErrState
	}
	server.ErrorLog = log.New(io.Discard, "", 0)
	listening := Listening{"listening", cfg.Listen, mode, dispatchEnabled}
	if dispatcher == nil {
		return serveHTTPMode(ctx, server, listener, 10*time.Second, mode, func() error {
			return announce(listening)
		})
	}
	extra := []runtimeService{{"retention", func(ctx context.Context) error { return runRetention(ctx, sweeper, retentionInterval) }}}
	if cleaner != nil {
		extra = append(extra, runtimeService{"cleanup", cleaner.Run})
	}
	if managed != nil {
		extra = append(extra, runtimeService{"connection_operations", func(ctx context.Context) error { return runConnectionOperations(ctx, managed.connections) }},
			runtimeService{"host_health", managed.guard.Run}, runtimeService{"host_storage", managed.storageGuard.Run},
			runtimeService{"host_watchdog", managed.watchdog.Run})
		if managed.secrets != nil {
			extra = append(extra, runtimeService{"workload_secrets", managed.secrets.Run})
		}
	}
	return runProviderServices(ctx, server, listener, mode, listening, announce, dispatcher, collector, extra...)
}

// runtimeService pairs a background worker with the closed name reported if it stops serve.
type runtimeService struct {
	name string
	run  func(context.Context) error
}

// ServiceStopped is ErrState with a bounded cause for the operator diagnostic. It holds
// only a closed service name and error class, never provider, URL, path or OS text.
type ServiceStopped struct{ Service, Class string }

func (e ServiceStopped) Error() string        { return ErrState.Error() }
func (e ServiceStopped) Is(target error) bool { return target == ErrState }
func (e ServiceStopped) DiagnosticCause() string {
	return "Runtime stopped: service=" + e.Service + " class=" + e.Class + "."
}

// stopClass maps a fatal service error to a closed class. Order prefers the most specific cause.
func stopClass(err error) string {
	for _, known := range []struct {
		err   error
		class string
	}{
		{sqlite.ErrCorrupt, "state_corrupt"}, {sqlite.ErrSchema, "state_schema"}, {sqlite.ErrDiskFull, "state_disk_full"},
		{sqlite.ErrClosed, "state_closed"}, {sqlite.ErrUnavailable, "state_unavailable"}, {sqlite.ErrBusy, "state_busy"},
		{sqlite.ErrInvalid, "state_rejected"}, {sqlite.ErrConflict, "state_rejected"},
		{dispatch.ErrInvalid, "transition_rejected"}, {dispatch.ErrConflict, "transition_rejected"},
		{ErrState, "runtime_state"},
	} {
		if errors.Is(err, known.err) {
			return known.class
		}
	}
	if provider.IsLocalStateError(err) {
		return "local_state"
	}
	var problem *domain.Problem
	if errors.As(err, &problem) {
		return "provider_problem"
	}
	return "unclassified"
}

func runProviderServices(
	ctx context.Context,
	server *http.Server,
	listener net.Listener,
	mode string,
	listening Listening,
	announce func(Listening) error,
	dispatcher *dispatch.Engine,
	collector *collection.Engine,
	extra ...runtimeService,
) error {
	services := append([]runtimeService{{"dispatch", dispatcher.Run}, {"collection", collector.Run}}, extra...)
	return runRuntimeServices(ctx, server, listener, mode, listening, announce, services)
}

func runRuntimeServices(
	ctx context.Context,
	server *http.Server,
	listener net.Listener,
	mode string,
	listening Listening,
	announce func(Listening) error,
	services []runtimeService,
) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	workers := make(chan struct{}, len(services))
	var first sync.Once
	var stopped *ServiceStopped
	for _, service := range services {
		go func(service runtimeService) {
			defer func() { workers <- struct{}{} }()
			err := service.run(runCtx)
			// The first fatal error is recorded before cancelling, so it names the initiator.
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				first.Do(func() { stopped = &ServiceStopped{service.name, stopClass(err)} })
			}
			if err != nil && runCtx.Err() == nil {
				cancel()
			}
		}(service)
	}
	httpErr := serveHTTPMode(runCtx, server, listener, 10*time.Second, mode, func() error {
		return announce(listening)
	})
	cancel()
	for range services {
		<-workers
	}
	if stopped != nil {
		return *stopped
	}
	if httpErr != nil {
		return ServiceStopped{"http", "listener"}
	}
	return nil
}
