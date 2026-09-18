package runtimehost

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"sync"
	"time"
)

// ValidListen permits literal loopback only. No DNS, wildcard, proxy or remote
// exposure flag exists. Port zero asks the OS for a port; Host checks bind the
// actual assigned address, not the original zero wildcard.
func ValidListen(address string) bool {
	host, port, err := net.SplitHostPort(address)
	ip, ipErr := netip.ParseAddr(host)
	n, portErr := strconv.Atoi(port)
	return err == nil && ipErr == nil && ip.IsLoopback() && ip.Zone() == "" && ip == ip.Unmap() && portErr == nil && n >= 0 && n <= 65535 && strconv.Itoa(n) == port
}

type requestDrain struct {
	next     http.Handler
	mode     string
	mu       sync.Mutex
	stopping bool
	requests sync.WaitGroup
}

func (d *requestDrain) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	if d.stopping {
		d.mu.Unlock()
		w.Header().Set("Connection", "close")
		http.Error(w, "local runtime is stopping", http.StatusServiceUnavailable)
		return
	}
	d.requests.Add(1)
	d.mu.Unlock()
	defer d.requests.Done()
	w.Header().Set("X-Compute-Relay-Mode", d.mode)
	d.next.ServeHTTP(w, r)
}
func (d *requestDrain) stop() { d.mu.Lock(); d.stopping = true; d.mu.Unlock() }

// Shutdown returning (or Serve returning ErrServerClosed) is not enough to prove
// handlers have exited after forced close. Keep ownership until the drain joins.
// Existing handlers do not hijack connections and must honor context/body closure.
func serveHTTP(ctx context.Context, server *http.Server, listener net.Listener, grace time.Duration, announce func() error) error {
	return serveHTTPMode(ctx, server, listener, grace, "local-admission-only", announce)
}

func serveHTTPMode(ctx context.Context, server *http.Server, listener net.Listener, grace time.Duration, mode string, announce func() error) error {
	if mode == "" {
		return ErrRequest
	}
	drain := &requestDrain{next: server.Handler, mode: mode}
	server.Handler = drain
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	var runErr error
	finished := false
	if err := announce(); err != nil {
		runErr = ErrState
	} else {
		select {
		case <-ctx.Done():
		case err := <-done:
			finished = true
			if !errors.Is(err, http.ErrServerClosed) {
				runErr = ErrState
			}
		}
	}
	drain.stop()
	deadline, cancel := context.WithTimeout(context.Background(), grace)
	shutdownErr := server.Shutdown(deadline)
	cancel()
	if shutdownErr != nil {
		_ = server.Close()
		runErr = ErrState
	}
	if !finished {
		if err := <-done; !errors.Is(err, http.ErrServerClosed) {
			runErr = ErrState
		}
	}
	drain.requests.Wait()
	return runErr
}
