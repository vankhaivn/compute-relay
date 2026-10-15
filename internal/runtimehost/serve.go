package runtimehost

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/api"
	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/joblogs"
	"github.com/vankhaivn/compute-relay/internal/objects"
	"github.com/vankhaivn/compute-relay/internal/operations"
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
	}

	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", serve.Address)
	if err != nil {
		return ErrState
	}
	defer listener.Close()
	cfg := api.DefaultConfig()
	cfg.Listen = listener.Addr().String()
	cfg.Jobs, cfg.Operations, cfg.Results, cfg.Cleanup = jobs, controls, results, cleanups
	cfg.Logs, err = joblogs.NewReader(h.access, h.store, logResolver)
	if err != nil {
		return ErrState
	}
	if managed != nil {
		cfg.Connections, cfg.Authorizations = managed.connections, managed.authorizations
		cfg.Secrets = managed.secrets
		cfg.Health = managed.health
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
	sweeper, err := h.retentionSweeper()
	if err != nil {
		return err
	}
	extra := []func(context.Context) error{func(ctx context.Context) error { return runRetention(ctx, sweeper, retentionInterval) }}
	if cleaner != nil {
		extra = append(extra, cleaner.Run)
	}
	if managed != nil {
		extra = append(extra, func(ctx context.Context) error { return runConnectionOperations(ctx, managed.connections) })
		extra = append(extra, managed.health.Run)
		if managed.secrets != nil {
			extra = append(extra, managed.secrets.Run)
		}
	}
	return runProviderServices(ctx, server, listener, mode, listening, announce, dispatcher, collector, extra...)
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
	extra ...func(context.Context) error,
) error {
	services := append([]func(context.Context) error{dispatcher.Run, collector.Run}, extra...)
	return runRuntimeServices(ctx, server, listener, mode, listening, announce, services)
}

func runRuntimeServices(
	ctx context.Context,
	server *http.Server,
	listener net.Listener,
	mode string,
	listening Listening,
	announce func(Listening) error,
	services []func(context.Context) error,
) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	workers := make(chan error, len(services))
	for _, run := range services {
		go func(run func(context.Context) error) {
			err := run(runCtx)
			workers <- err
			if err != nil && runCtx.Err() == nil {
				cancel()
			}
		}(run)
	}
	httpErr := serveHTTPMode(runCtx, server, listener, 10*time.Second, mode, func() error {
		return announce(listening)
	})
	cancel()
	var workerErr error
	for i := 0; i < len(services); i++ {
		err := <-workers
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			workerErr = errors.Join(workerErr, err)
		}
	}
	if httpErr != nil || workerErr != nil {
		return ErrState
	}
	return nil
}
