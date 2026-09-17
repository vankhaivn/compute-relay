package runtimehost

import (
	"context"
	"io"
	"log"
	"net"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/api"
	"github.com/vankhaivn/compute-relay/internal/collection"
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

// Serve blocks until cancellation or failure and joins HTTP handlers before
// returning. Its caller may then close the stores. It creates no provider,
// scheduler, dispatcher, collector, expiry sweeper or remote worker.
func (h *Host) Serve(ctx context.Context, address string, announce func(Listening) error) error {
	if h == nil || announce == nil || !ValidListen(address) {
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
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", address)
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
	return serveHTTP(ctx, server, listener, 10*time.Second, func() error {
		return announce(Listening{"listening", cfg.Listen, "local-admission-only", false})
	})
}
