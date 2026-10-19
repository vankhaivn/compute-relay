package kaggleapi

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const readTries = 3

func retryDelay(header string, now time.Time, attempt int) time.Duration {
	header = strings.TrimSpace(header)
	if header != "" && strings.IndexFunc(header, func(r rune) bool { return r < '0' || r > '9' }) == -1 {
		seconds, err := strconv.ParseUint(header, 10, 64)
		// Saturating avoids overflow without shortening a valid server cooldown.
		const maximum = time.Duration(1<<63 - 1)
		if err != nil || seconds > uint64(maximum/time.Second) {
			return maximum
		}
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
	tries := 1
	if retryableRead(op) {
		tries = readTries
	}
	return c.requestResponse(parent, op, tries, func(ctx context.Context) (*rpcResponse, error) {
		return c.sendRPC(ctx, op, payload)
	})
}

// Waiting uses the caller budget, capped at one hour; each actual HTTP request
// gets a fresh timeout. Mutations share admission but always receive one send.
func (c *Client) requestResponse(parent context.Context, op string, tries int, send func(context.Context) (*rpcResponse, error)) (*rpcResponse, error) {
	ctx, cancel := context.WithTimeout(parent, time.Hour)
	scope := c.requestScope()
	for attempt := 0; attempt < tries; attempt++ {
		release, err := c.state.pacing.acquire(ctx, scope, op)
		if err != nil {
			cancel()
			return nil, err
		}
		response, err := send(ctx)
		if err != nil {
			release()
			cancel()
			return nil, err
		}
		if response.response.StatusCode != http.StatusTooManyRequests {
			release()
			requestCancel := response.cancel
			response.cancel = func() { requestCancel(); cancel() }
			return response, nil
		}
		delay := retryDelay(response.response.Header.Get("Retry-After"), c.state.pacing.clock(), attempt)
		err = c.state.pacing.remember(scope, delay)
		_ = response.response.Body.Close()
		response.cancel()
		release()
		if err != nil {
			cancel()
			return nil, failure(op, ErrPacingState, 0, true)
		}
	}
	cancel()
	return nil, failure(op, ErrHTTP, http.StatusTooManyRequests, true)
}
