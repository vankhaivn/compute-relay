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
}

// Serve preserves the safe default: HTTP admission and published-result delivery
// only. Provider workers require ServeConfigured with explicit provider settings.
func (h *Host) Serve(ctx context.Context, address string, announce func(Listening) error) error {
	return h.ServeConfigured(ctx, ServeConfig{Address: address}, announce)
}

// ServeConfigured joins HTTP handlers and any explicitly enabled provider workers
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
	if serve.Kaggle != nil {
		dispatcher, collector, schedulerSettings, err = h.kaggleWorkers(ctx, *serve.Kaggle)
		if err != nil {
			return err
		}
		mode = "kaggle-workers"
		dispatchEnabled = true
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
	return runProviderServices(ctx, server, listener, mode, listening, announce, dispatcher, collector)
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
) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	workers := make(chan error, 2)
	go func() {
		err := dispatcher.Run(runCtx)
		workers <- err
		if err != nil && runCtx.Err() == nil {
			cancel()
		}
	}()
	go func() {
		err := collector.Run(runCtx)
		workers <- err
		if err != nil && runCtx.Err() == nil {
			cancel()
		}
	}()
	httpErr := serveHTTPMode(runCtx, server, listener, 10*time.Second, mode, func() error {
		return announce(listening)
	})
	cancel()
	var workerErr error
	for i := 0; i < 2; i++ {
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
