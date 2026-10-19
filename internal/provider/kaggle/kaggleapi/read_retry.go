package kaggleapi

import (
	"context"
	"crypto/sha256"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const readTries = 3

// Fresh clients resolving the same credential share server cooldowns. Neither
// credentials nor provider headers are retained. Restart clears this advisory state.
var sharedReadCooldowns = &readCooldowns{}

type readKey struct {
	credential [sha256.Size]byte
	operation  string
}

type readCooldowns struct {
	mu    sync.Mutex
	until map[readKey]time.Time
	now   func() time.Time
	sleep func(context.Context, time.Duration) error
}

func (r *readCooldowns) clock() time.Time {
	if r.now != nil {
		return r.now()
	}
	return time.Now()
}

func (r *readCooldowns) remaining(key readKey) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.until[key].Sub(r.clock())
}

func (r *readCooldowns) remember(key readKey, delay time.Duration) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.clock()
	if r.until == nil {
		r.until = make(map[readKey]time.Time)
	}
	for key, until := range r.until {
		if !until.After(now) {
			delete(r.until, key)
		}
	}
	// Bound memory even if an operator cycles through many credentials.
	if len(r.until) >= 1024 && r.until[key].IsZero() {
		return false
	}
	if until := now.Add(delay); until.After(r.until[key]) {
		r.until[key] = until
	}
	return true
}

func (r *readCooldowns) wait(ctx context.Context, key readKey) error {
	for {
		if err := ctx.Err(); err != nil {
			return failure(key.operation, err, 0, false)
		}
		delay := r.remaining(key)
		if delay <= 0 {
			return nil
		}
		if deadline, ok := ctx.Deadline(); ok && !r.clock().Add(delay).Before(deadline) {
			return failure(key.operation, ErrHTTP, http.StatusTooManyRequests, false)
		}
		sleep := r.sleep
		if sleep == nil {
			sleep = waitRead
		}
		if err := sleep(ctx, delay); err != nil {
			return failure(key.operation, err, 0, false)
		}
		// Another concurrent read may have extended the cooldown while we waited.
	}
}

func waitRead(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func retryDelay(header string, now time.Time, attempt int) time.Duration {
	if seconds, err := strconv.ParseUint(header, 10, 32); err == nil {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(header); err == nil {
		return max(0, at.Sub(now))
	}
	return (30 * time.Second) << attempt
}

// RPCs use POST even for reads, so retry eligibility is an explicit allowlist.
func retryableRead(op string) bool {
	switch op {
	case authRPC, quotaRPC,
		datasetService + "GetDataset", datasetService + "GetDatasetStatus", datasetService + "ListDatasetFiles",
		kernelService + "GetKernel", kernelService + "GetKernelSessionStatus":
		return true
	}
	return false
}

type rpcResponse struct {
	response *http.Response
	ctx      context.Context
	cancel   context.CancelFunc
}

func (c *Client) rpcResponse(parent context.Context, op string, payload any) (*rpcResponse, error) {
	if !retryableRead(op) {
		return c.sendRPC(parent, op, payload)
	}
	// Waiting does not consume an individual HTTP request's timeout. Preparation
	// can use its existing budget; short control calls return 429 without sending.
	ctx, cancel := context.WithTimeout(parent, time.Hour)
	key := readKey{c.state.credentialKey, op}
	for attempt := 0; attempt < readTries; attempt++ {
		if err := c.state.cooldowns.wait(ctx, key); err != nil {
			cancel()
			return nil, err
		}
		response, err := c.sendRPC(ctx, op, payload)
		if err != nil {
			cancel()
			return nil, err
		}
		if response.response.StatusCode != http.StatusTooManyRequests {
			requestCancel := response.cancel
			response.cancel = func() { requestCancel(); cancel() }
			return response, nil
		}
		delay := retryDelay(response.response.Header.Get("Retry-After"), c.state.cooldowns.clock(), attempt)
		remembered := c.state.cooldowns.remember(key, delay)
		_ = response.response.Body.Close()
		response.cancel()
		if !remembered {
			break
		}
	}
	cancel()
	return nil, failure(op, ErrHTTP, http.StatusTooManyRequests, true)
}
