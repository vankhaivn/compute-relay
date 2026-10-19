package kaggleapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/statefs"
)

func pacedClient(t *testing.T, pacing *RequestPacer, account, token string, api fixtureTransport) *Client {
	t.Helper()
	client, err := New([]byte(token), Options{Pacing: pacing, AccountScope: account, APITransport: api})
	requireOK(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestAccountAndProviderSpacingCoversRPCAndLogsAcrossClients(t *testing.T) {
	gate, _ := fakeCooldowns()
	gate.accountGap, gate.providerGap = 500*time.Millisecond, 100*time.Millisecond
	var at []time.Time
	api := func(r *http.Request) (*http.Response, error) {
		at = append(at, gate.clock())
		if r.Method == http.MethodGet {
			return response(200, "application/json", `[{"data":"first\n"}]`), nil
		}
		return response(200, "application/json", `{"id":123,"ref":"fixture_user/fixture-stage"}`), nil
	}
	a := pacedClient(t, gate, "fixture_user", "FIRST_TOKEN", api)
	b := pacedClient(t, gate, "fixture_user", "REPLACEMENT_TOKEN", api)
	other := pacedClient(t, gate, "other_user", "OTHER_TOKEN", api)
	_, err := a.GetDataset(context.Background(), fixtureDataset)
	requireOK(t, err)
	_, err = b.ReadLogs(context.Background(), fixtureKernel, KernelComplete, LogRequest{})
	requireOK(t, err)
	_, err = other.GetDataset(context.Background(), fixtureDataset)
	requireOK(t, err)
	if len(at) != 3 || at[1].Sub(at[0]) != 500*time.Millisecond || at[2].Sub(at[1]) != 100*time.Millisecond {
		t.Fatal("clients bypassed shared account/provider spacing", at)
	}
}

type advancingClock struct {
	mu      sync.Mutex
	at      time.Time
	changed chan struct{}
}

func (c *advancingClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *advancingClock) advance(by time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = c.at.Add(by)
	close(c.changed)
	c.changed = make(chan struct{})
}

func (c *advancingClock) waitUntil(ctx context.Context, until time.Time) error {
	c.mu.Lock()
	if !c.at.Before(until) {
		c.mu.Unlock()
		return nil
	}
	changed := c.changed
	c.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-changed:
		return nil
	}
}

func TestConcurrentReadsStaySpacedAfterCooldown(t *testing.T) {
	clock := &advancingClock{at: time.Now(), changed: make(chan struct{})}
	gate := newRequestPacer()
	gate.now, gate.jitter = clock.now, nil
	gate.sleep = func(ctx context.Context, _ time.Duration) error {
		gate.mu.Lock()
		account := gate.scopes["fixture_user"]
		until := latest(gate.providerNext, gate.providerUntil, account.next, account.until)
		gate.mu.Unlock()
		return clock.waitUntil(ctx, until)
	}
	gate.scopes = map[string]*requestScope{"fixture_user": {until: clock.now().Add(2 * time.Second)}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const count = 8
	calls := make(chan time.Time, count)
	results := make(chan error, count)
	for range count {
		c := pacedClient(t, gate, "fixture_user", "SYNTHETIC_TOKEN", func(*http.Request) (*http.Response, error) {
			calls <- clock.now()
			return response(200, "application/json", `{"id":123,"ref":"fixture_user/fixture-stage"}`), nil
		})
		go func() {
			_, err := c.GetDataset(ctx, fixtureDataset)
			results <- err
		}()
	}
	clock.advance(2 * time.Second)
	var previous time.Time
	for i := range count {
		select {
		case at := <-calls:
			if i != 0 && at.Sub(previous) < 500*time.Millisecond {
				t.Fatal("concurrent wakeup bypassed account spacing", previous, at)
			}
			previous = at
			clock.advance(500 * time.Millisecond)
		case <-time.After(5 * time.Second):
			t.Fatal("paced readers did not progress")
		}
	}
	for range count {
		requireOK(t, <-results)
	}
}

func TestInFlightReadInstallsCooldownBeforeReleasingWaiters(t *testing.T) {
	gate := &RequestPacer{}
	entered, finish := make(chan struct{}), make(chan struct{})
	calls := 0
	client := pacedClient(t, gate, "fixture_user", "SYNTHETIC_TOKEN", func(*http.Request) (*http.Response, error) {
		calls++
		close(entered)
		<-finish
		return rateLimited("7200"), nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := client.GetDataset(ctx, fixtureDataset); result <- err }()
	<-entered
	cancelled, stop := context.WithCancel(context.Background())
	stop()
	if _, err := gate.acquire(cancelled, "fixture_user", authRPC); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled waiter acquired an in-flight account", err)
	}
	waiter := pacedClient(t, gate, "fixture_user", "OTHER_TOKEN", func(*http.Request) (*http.Response, error) {
		t.Error("waiter bypassed in-flight cooldown")
		return rateLimited("7200"), nil
	})
	waiting := make(chan error, 1)
	go func() { _, err := waiter.ReadLogs(ctx, fixtureKernel, KernelRunning, LogRequest{}); waiting <- err }()
	close(finish)
	for _, err := range []error{<-result, <-waiting} {
		var failure *Error
		if !errors.As(err, &failure) || failure.StatusCode != 429 {
			t.Fatal("read lost account cooldown", err)
		}
	}
	if calls != 1 {
		t.Fatal("read amplification during rate limit", calls)
	}
}

func TestReadCooldownPersistsAcrossRestartAndCredentialReplacement(t *testing.T) {
	for _, account := range []string{"fixture_user", ""} {
		t.Run(account, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state")
			root, err := statefs.Open(path)
			requireOK(t, err)
			gate, err := OpenRequestPacer(root)
			requireOK(t, err)
			calls := 0
			c := pacedClient(t, gate, account, "SYNTHETIC_TOKEN", func(*http.Request) (*http.Response, error) {
				calls++
				return rateLimited(time.Now().Add(2 * time.Hour).UTC().Format(http.TimeFormat)), nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			_, err = c.GetDataset(ctx, fixtureDataset)
			if !errors.Is(err, ErrHTTP) || calls != 1 {
				t.Fatal("long cooldown failed before restart", err, calls)
			}
			requireOK(t, gate.Close())
			requireOK(t, root.Close())
			raw, err := os.ReadFile(filepath.Join(path, cooldownFile))
			requireOK(t, err)
			digest := sha256.Sum256([]byte("SYNTHETIC_TOKEN"))
			if strings.Contains(string(raw), "SYNTHETIC_TOKEN") || strings.Contains(string(raw), hex.EncodeToString(digest[:])) {
				t.Fatal("credential-derived data persisted")
			}
			root, err = statefs.Open(path)
			requireOK(t, err)
			defer root.Close()
			gate, err = OpenRequestPacer(root)
			requireOK(t, err)
			defer gate.Close()
			restarted := pacedClient(t, gate, account, "REPLACEMENT_TOKEN", func(*http.Request) (*http.Response, error) {
				t.Fatal("restart/credential replacement bypassed known cooldown")
				return nil, nil
			})
			_, err = restarted.ReadLogs(ctx, fixtureKernel, KernelRunning, LogRequest{})
			var failure *Error
			if !errors.As(err, &failure) || failure.StatusCode != 429 || failure.Sent {
				t.Fatal("restored cooldown did not refuse unsendable read", err)
			}
		})
	}
}

func TestAccountCooldownLeavesOtherAccountsAvailable(t *testing.T) {
	gate, _ := fakeCooldowns()
	gate.scopes = map[string]*requestScope{"fixture_user": {until: gate.clock().Add(time.Hour)}}
	calls := 0
	c := pacedClient(t, gate, "other_user", "OTHER_TOKEN", func(*http.Request) (*http.Response, error) {
		calls++
		return response(200, "application/json", `{"active":true,"username":"other_user"}`), nil
	})
	_, err := c.IntrospectToken(context.Background())
	if err != nil || calls != 1 {
		t.Fatal("known account cooldown blocked unrelated account", err, calls)
	}
}

func TestInvalidCooldownStateRefusesOpen(t *testing.T) {
	root, err := statefs.Open(filepath.Join(t.TempDir(), "state"))
	requireOK(t, err)
	defer root.Close()
	requireOK(t, os.WriteFile(filepath.Join(root.Path, cooldownFile), []byte(`{"version":1,"version":1}`), 0o600))
	if _, err := OpenRequestPacer(root); !errors.Is(err, ErrPacingState) {
		t.Fatal("corrupt pacing state was silently discarded", err)
	}
}

func TestLog429HonorsCooldownBeforeReplay(t *testing.T) {
	gate, waits := fakeCooldowns()
	calls := 0
	c := pacedClient(t, gate, "fixture_user", "SYNTHETIC_TOKEN", func(*http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			r := rateLimited("90")
			r.Body = &unreadBody{t}
			return r, nil
		}
		return response(200, "application/json", `[{"data":"verified line\n"}]`), nil
	})
	page, err := c.ReadLogs(context.Background(), fixtureKernel, KernelComplete, LogRequest{})
	if err != nil || string(page.Text) != "verified line\n" || calls != 2 || len(*waits) != 1 || (*waits)[0] != 90*time.Second {
		t.Fatal("logs bypassed Retry-After or replayed failed response", err, calls, *waits)
	}
}

func TestLogCooldownBlocksDownloadAndMutationWithoutSending(t *testing.T) {
	gate, _ := fakeCooldowns()
	calls := 0
	c := pacedClient(t, gate, "fixture_user", "SYNTHETIC_TOKEN", func(*http.Request) (*http.Response, error) {
		calls++
		return rateLimited("7200"), nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	_, err := c.ReadLogs(ctx, fixtureKernel, KernelRunning, LogRequest{})
	if !errors.Is(err, ErrHTTP) || calls != 1 {
		t.Fatal("log stream did not establish cooldown", err, calls)
	}
	_, err = c.DownloadKernelOutput(ctx, fixtureKernel, "answer.txt", 10, io.Discard)
	if !errors.Is(err, ErrHTTP) || calls != 1 {
		t.Fatal("download ticket bypassed log cooldown", err, calls)
	}
	_, err = c.rpc(ctx, datasetService+"CreateDataset", struct{}{}, maxAccountBytes)
	var apiErr *Error
	if !errors.As(err, &apiErr) || apiErr.Sent || calls != 1 {
		t.Fatal("mutation ignored active cooldown", err, calls)
	}
}

func TestMutation429ThrottlesFollowingReadsWithoutReplay(t *testing.T) {
	gate, _ := fakeCooldowns()
	calls := 0
	c := pacedClient(t, gate, "fixture_user", "SYNTHETIC_TOKEN", func(*http.Request) (*http.Response, error) {
		calls++
		return rateLimited("7200"), nil
	})
	_, err := c.rpc(context.Background(), datasetService+"CreateDataset", struct{}{}, maxAccountBytes)
	var apiErr *Error
	if !errors.As(err, &apiErr) || !apiErr.Sent || calls != 1 {
		t.Fatal("mutation retried or lost its sent state", err, calls)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	_, err = c.GetDataset(ctx, fixtureDataset)
	if !errors.As(err, &apiErr) || apiErr.Sent || calls != 1 {
		t.Fatal("read bypassed mutation cooldown", err, calls)
	}
}

func TestFailedCooldownPersistenceStopsFollowingRequests(t *testing.T) {
	root, err := statefs.Open(filepath.Join(t.TempDir(), "state"))
	requireOK(t, err)
	defer root.Close()
	gate, err := OpenRequestPacer(root)
	requireOK(t, err)
	defer gate.Close()
	// A non-regular target must not be replaced or ignored.
	requireOK(t, os.Mkdir(filepath.Join(root.Path, cooldownFile), 0o700))
	calls := 0
	c := pacedClient(t, gate, "fixture_user", "SYNTHETIC_TOKEN", func(*http.Request) (*http.Response, error) {
		calls++
		return rateLimited("7200"), nil
	})
	_, err = c.GetDataset(context.Background(), fixtureDataset)
	if !errors.Is(err, ErrPacingState) || calls != 1 {
		t.Fatal("failed cooldown persistence was hidden", err, calls)
	}
	_, err = c.IntrospectToken(context.Background())
	if !errors.Is(err, ErrPacingState) || calls != 1 {
		t.Fatal("state failure allowed further API calls", err, calls)
	}
}
