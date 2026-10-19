package kaggleapi

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func fakeCooldowns() (*readCooldowns, *[]time.Duration) {
	now := time.Now()
	var waits []time.Duration
	return &readCooldowns{now: func() time.Time { return now }, sleep: func(_ context.Context, delay time.Duration) error {
		waits = append(waits, delay)
		now = now.Add(delay)
		return nil
	}}, &waits
}

func rateLimited(after string) *http.Response {
	r := response(429, "text/plain", "provider text must not be retained")
	r.Header.Set("Retry-After", after)
	return r
}

func TestReadHonorsLongServerCooldownBeforeRecovering(t *testing.T) {
	calls := 0
	c := newFixtureClient(t, func(r *http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return rateLimited("1910"), nil
		}
		if r.Context().Err() != nil {
			t.Fatal("retry reused expired request context")
		}
		return response(200, "application/json", `{"id":123,"ref":"fixture_user/fixture-stage"}`), nil
	}, nil)
	gate, waits := fakeCooldowns()
	c.state.cooldowns = gate
	dataset, err := c.GetDataset(context.Background(), fixtureDataset)
	if err != nil || dataset.ID != "123" || calls != 2 || len(*waits) != 1 || (*waits)[0] != 1910*time.Second {
		t.Fatalf("read did not recover after server cooldown: %v, calls=%d, waits=%v", err, calls, *waits)
	}
}

func TestCooldownSurvivesFreshClientAndShortControlBudget(t *testing.T) {
	gate, _ := fakeCooldowns()
	calls := 0
	api := fixtureTransport(func(*http.Request) (*http.Response, error) {
		calls++
		return rateLimited("1910"), nil
	})
	for range 2 {
		c := newFixtureClient(t, api, nil)
		c.state.cooldowns = gate
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		_, err := c.GetDataset(ctx, fixtureDataset)
		cancel()
		var apiErr *Error
		if !errors.As(err, &apiErr) || apiErr.StatusCode != 429 {
			t.Fatal("short control call lost throttling reason", err)
		}
		_ = c.Close()
	}
	if calls != 1 {
		t.Fatal("fresh client ignored active cooldown", calls)
	}
	// A dataset limit must not block unrelated account verification.
	c := newFixtureClient(t, func(*http.Request) (*http.Response, error) {
		return response(200, "application/json", `{"active":true,"username":"fixture_user"}`), nil
	}, nil)
	c.state.cooldowns = gate
	if _, err := c.IntrospectToken(context.Background()); err != nil {
		t.Fatal("dataset cooldown blocked independent account read", err)
	}
}

func TestReadRateLimitRetriesAreFiniteAndSanitized(t *testing.T) {
	for _, header := range []string{"", "invalid", "0"} {
		t.Run(header, func(t *testing.T) {
			calls := 0
			c := newFixtureClient(t, func(*http.Request) (*http.Response, error) {
				calls++
				r := rateLimited(header)
				r.Body = &unreadBody{t: t}
				return r, nil
			}, nil)
			gate, waits := fakeCooldowns()
			c.state.cooldowns = gate
			_, err := c.GetDataset(context.Background(), fixtureDataset)
			var apiErr *Error
			if !errors.As(err, &apiErr) || apiErr.StatusCode != 429 || calls != 3 {
				t.Fatal("persistent rate limit escaped finite retry bound", err, calls)
			}
			if header != "0" && (len(*waits) != 2 || (*waits)[0] != 30*time.Second || (*waits)[1] != time.Minute) {
				t.Fatal("missing/invalid delay did not back off", *waits)
			}
		})
	}
}

func TestMutationsAndOtherFailuresNeverRetry(t *testing.T) {
	for _, op := range []string{datasetService + "CreateDataset", datasetService + "DeleteDataset", kernelService + "SaveKernel", kernelService + "DeleteKernel", blobRPC, "unknown.Read"} {
		t.Run(op, func(t *testing.T) {
			calls := 0
			c := newFixtureClient(t, func(*http.Request) (*http.Response, error) {
				calls++
				return rateLimited("0"), nil
			}, nil)
			_, err := c.rpc(context.Background(), op, struct{}{}, maxAccountBytes)
			if !errors.Is(err, ErrHTTP) || calls != 1 {
				t.Fatal("non-allowlisted call was repeated", err, calls)
			}
		})
	}
	for _, status := range []int{401, 403, 404, 500, 503} {
		calls := 0
		c := newFixtureClient(t, func(*http.Request) (*http.Response, error) {
			calls++
			return response(status, "text/plain", "ignored"), nil
		}, nil)
		_, _ = c.GetDataset(context.Background(), fixtureDataset)
		if calls != 1 {
			t.Fatal("unrelated failure retried", status, calls)
		}
	}
}

func TestReadCooldownCancellationAndHTTPDate(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	if got := retryDelay(now.Add(32*time.Minute).Format(http.TimeFormat), now, 0); got != 32*time.Minute {
		t.Fatal("HTTP-date not honored", got)
	}
	if got := retryDelay(now.Add(-time.Minute).Format(http.TimeFormat), now, 0); got != 0 {
		t.Fatal("past HTTP-date became negative", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	c := newFixtureClient(t, func(*http.Request) (*http.Response, error) {
		calls++
		return rateLimited("30"), nil
	}, nil)
	gate, _ := fakeCooldowns()
	gate.sleep = func(ctx context.Context, _ time.Duration) error { cancel(); return ctx.Err() }
	c.state.cooldowns = gate
	_, err := c.GetDataset(ctx, fixtureDataset)
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatal("cancelled wait sent another read", err, calls)
	}
}
