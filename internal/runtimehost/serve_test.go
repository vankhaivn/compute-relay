package runtimehost

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/connections"
)

func startHost(t *testing.T, h *Host) (string, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	announced, done := make(chan Listening, 1), make(chan error, 1)
	go func() { done <- h.Serve(ctx, "127.0.0.1:0", func(v Listening) error { announced <- v; return nil }) }()
	var event Listening
	select {
	case event = <-announced:
	case err := <-done:
		cancel()
		t.Fatal("serve failed", err)
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("serve startup deadline")
	}
	if !event.DispatchEnabled || event.Mode != "managed-workers" {
		t.Fatal("misleading serve mode")
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
				t.Error("shutdown failed", err)
			}
		case <-time.After(15 * time.Second):
			t.Error("shutdown did not join")
		}
	}
	t.Cleanup(stop)
	return "http://" + event.Address, stop
}
func requestHost(t *testing.T, url, method, token, body string, headers map[string]string, want int) []byte {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	requireNoError(t, err)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		if k == "Host" {
			req.Host = v
		} else {
			req.Header.Set(k, v)
		}
	}
	resp, err := boundedClient().Do(req)
	requireNoError(t, err)
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	closeErr := resp.Body.Close()
	if readErr != nil || closeErr != nil || resp.StatusCode != want || resp.Header.Get("X-Compute-Relay-Mode") != "managed-workers" {
		t.Fatalf("HTTP %d want %d body=%s read=%v close=%v", resp.StatusCode, want, raw, readErr, closeErr)
	}
	return raw
}

// These HTTP tests admit without execution authority. The profile still comes
// from a real verified connection rather than the removed offline profile path.
type localManagedAdapter struct{ *managedTestAdapter }

func (a *localManagedAdapter) Descriptor() connections.Descriptor {
	d := a.managedTestAdapter.Descriptor()
	d.Type = "fixture"
	return d
}
func createLocalManagedProfile(t *testing.T, h *Host) string {
	t.Helper()
	ctx := context.Background()
	token, receipt, err := h.access.Issue(ctx, "app", []auth.Scope{auth.Read, auth.Manage}, time.Time{})
	requireNoError(t, err)
	defer h.access.Revoke(ctx, receipt.ID)
	actor, err := h.access.Authenticate(ctx, token.Reveal())
	requireNoError(t, err)
	adapter := &localManagedAdapter{&managedTestAdapter{managedKaggleAdapter: &managedKaggleAdapter{config: DefaultManagedServeConfig()}}}
	service, err := connections.New(h.access, h.store, &managedTestVault{entries: map[string][]byte{}}, []connections.Adapter{adapter})
	requireNoError(t, err)
	op, err := service.Submit(ctx, actor, "app", "", "create-fixture-profile", []byte(`{"provider_type":"fixture","label":"Local fixture","credentials":{"api_token":"SYNTHETIC_FIRST"}}`))
	requireNoError(t, err)
	if worked, err := service.RunOnce(ctx); err != nil || !worked {
		t.Fatal("verify fixture connection", worked, err)
	}
	connection, err := service.Get(ctx, actor, "app", op.ConnectionID)
	if err != nil || connection.Selection == nil {
		t.Fatal("fixture profile unavailable", err)
	}
	return connection.Selection.Profile
}
