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
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/objects"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
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
	Kaggle  *KaggleServeConfig
	Managed *ManagedServeConfig
}

// Serve preserves the safe default: HTTP admission and published-result delivery
// only. Provider workers require ServeConfigured with explicit provider settings.
func (h *Host) Serve(ctx context.Context, address string, announce func(Listening) error) error {
	return h.ServeConfigured(ctx, ServeConfig{Address: address}, announce)
}

// ServeConfigured joins HTTP handlers and any explicitly enabled provider workers
// before returning. Local shutdown never calls provider cancellation or cleanup.
func (h *Host) ServeConfigured(ctx context.Context, serve ServeConfig, announce func(Listening) error) error {
	if h == nil || announce == nil || !ValidListen(serve.Address) || serve.Kaggle != nil && serve.Managed != nil {
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
	controls, err := operations.New(h.access, h.store, h.inputs, clock{}, admission.DefaultLimits())
	if err != nil {
		return ErrState
	}
	results, err := collection.NewReader(h.access, h.store, h.results)
	if err != nil {
		return ErrState
	}

	mode := "local-admission-only"
	dispatchEnabled := false
	var dispatcher *dispatch.Engine
	var collector *collection.Engine
	var schedulerSettings scheduler.Settings
	var managed *managedServices
	if serve.Kaggle != nil {
		dispatcher, collector, schedulerSettings, err = h.kaggleWorkers(ctx, *serve.Kaggle)
		if err != nil {
			return err
		}
		mode = "kaggle-workers"
		dispatchEnabled = true
	}
	if serve.Managed != nil {
		managed, err = h.managedWorkers(ctx, *serve.Managed)
		if err != nil {
			return err
		}
		dispatcher, collector, schedulerSettings = managed.dispatcher, managed.collector, managed.settings
		mode = "managed-connections"
		dispatchEnabled = dispatcher != nil
		if dispatchEnabled {
			mode = "managed-workers"
		}
	}
	if dispatchEnabled {
		defer func() {
			pauseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			schedulerSettings.Paused = true
			_ = h.store.ConfigureScheduler(pauseCtx, schedulerSettings)
		}()
	}

	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", serve.Address)
	if err != nil {
		return ErrState
	}
	defer listener.Close()
	cfg := api.DefaultConfig()
	cfg.Listen = listener.Addr().String()
	cfg.Jobs, cfg.Operations, cfg.Results = jobs, controls, results
	if managed != nil {
		cfg.Connections, cfg.Authorizations = managed.connections, managed.authorizations
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
	var extra []func(context.Context) error
	if managed != nil {
		extra = append(extra, func(ctx context.Context) error { return runConnectionOperations(ctx, managed.connections) })
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
