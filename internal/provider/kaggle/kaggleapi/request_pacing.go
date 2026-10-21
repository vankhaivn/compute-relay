package kaggleapi

import (
	"context"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"github.com/vankhaivn/compute-relay/internal/statefs"
)

const maxRequestScopes = 1024

var sharedRequestPacer = newRequestPacer()

// RequestPacer owns account/provider request admission and server cooldowns. It never
// caches identity or quota evidence. A managed host supplies one durable instance;
// standalone clients share the process-local default.
type RequestPacer struct {
	mu            sync.Mutex
	scopes        map[string]*requestScope
	providerNext  time.Time
	providerUntil time.Time
	accountGap    time.Duration
	providerGap   time.Duration
	jitter        func() time.Duration
	now           func() time.Time
	sleep         func(context.Context, time.Duration) error
	root          *statefs.Root
	stateErr      error
	closed        bool
}

type requestScope struct {
	next   time.Time
	until  time.Time
	active chan struct{}
}

func newRequestPacer() *RequestPacer {
	return &RequestPacer{
		accountGap: 500 * time.Millisecond, providerGap: 100 * time.Millisecond,
		jitter: func() time.Duration { return time.Duration(rand.Int64N(int64(100 * time.Millisecond))) },
	}
}

func (p *RequestPacer) clock() time.Time {
	if p.now != nil {
		return p.now()
	}
	return time.Now()
}

// acquire reserves only the next actual send, never a queue of future slots.
// One account request owns admission through its response headers, so a 429 is
// remembered before another reader can start. The body remains caller-owned.
func (p *RequestPacer) acquire(ctx context.Context, scope, op string) (func(), error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, failure(op, err, 0, false)
		}
		p.mu.Lock()
		if p.closed || p.stateErr != nil {
			p.mu.Unlock()
			return nil, failure(op, ErrPacingState, 0, false)
		}
		now := p.clock()
		entry := p.scopes[scope]
		if entry == nil {
			if p.scopes == nil {
				p.scopes = make(map[string]*requestScope)
			}
			for key, old := range p.scopes {
				if old.active == nil && !old.next.After(now) && !old.until.After(now) {
					delete(p.scopes, key)
				}
			}
			if len(p.scopes) >= maxRequestScopes {
				p.mu.Unlock()
				return nil, failure(op, ErrLimit, 0, false)
			}
			entry = &requestScope{}
			p.scopes[scope] = entry
		}
		if active := entry.active; active != nil {
			p.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, failure(op, ctx.Err(), 0, false)
			case <-active:
				continue
			}
		}
		until := latest(p.providerUntil, entry.until, p.providerNext, entry.next)
		delay := until.Sub(now)
		if delay <= 0 {
			jitter := time.Duration(0)
			if p.jitter != nil {
				jitter = p.jitter()
			}
			entry.next = now.Add(p.accountGap + jitter)
			p.providerNext = now.Add(p.providerGap)
			entry.active = make(chan struct{})
			p.mu.Unlock()
			return func() {
				p.mu.Lock()
				close(entry.active)
				entry.active = nil
				p.mu.Unlock()
			}, nil
		}
		p.mu.Unlock()
		if deadline, ok := ctx.Deadline(); ok && !until.Before(deadline) {
			return nil, failure(op, ErrHTTP, http.StatusTooManyRequests, false)
		}
		sleep := p.sleep
		if sleep == nil {
			sleep = waitRead
		}
		if err := sleep(ctx, delay); err != nil {
			return nil, failure(op, err, 0, false)
		}
		// Recheck after waking: another request may have extended the cooldown.
	}
}

func latest(values ...time.Time) time.Time {
	var result time.Time
	for _, value := range values {
		if value.After(result) {
			result = value
		}
	}
	return result
}

func (p *RequestPacer) remember(scope string, delay time.Duration) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	until := p.clock().Add(delay)
	if accountName.MatchString(scope) {
		// The scope exists and is held by the sender until remember returns.
		p.scopes[scope].until = latest(p.scopes[scope].until, until)
	} else {
		// Before discovery there is no verified account. Persist only a provider
		// cooldown, never the process-local credential fingerprint.
		p.providerUntil = latest(p.providerUntil, until)
	}
	if p.root != nil {
		p.stateErr = p.saveLocked()
	}
	return p.stateErr
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

func (p *RequestPacer) wait(ctx context.Context, delay time.Duration) error {
	if p.sleep != nil {
		return p.sleep(ctx, delay)
	}
	return waitRead(ctx, delay)
}
