package api

import (
	"math"
	"sync"
	"time"
)

type Rate struct {
	PerSecond float64
	Burst     int
}

func (r Rate) valid() bool {
	return r.PerSecond >= 1e-6 && r.PerSecond <= 1e6 && !math.IsNaN(r.PerSecond) && !math.IsInf(r.PerSecond, 0) && r.Burst > 0 && r.Burst <= 100000
}

type bucket struct {
	tokens float64
	last   time.Time
}

type limiter struct {
	mu      sync.Mutex
	rate    Rate
	maxKeys int
	buckets map[string]bucket
}

func newLimiter(rate Rate, maxKeys int) *limiter {
	return &limiter{rate: rate, maxKeys: maxKeys, buckets: make(map[string]bucket)}
}

// allow never evicts a live bucket to admit a new key: that would reset its allowance.
// Only authenticated workspace IDs reach the per-workspace limiter. Pre-auth uses one
// global bucket, so arbitrary tokens/IPs cannot grow this map.
func (l *limiter) allow(key string, now time.Time) (bool, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	b, exists := l.buckets[key]
	if !exists {
		if len(l.buckets) >= l.maxKeys {
			for k, old := range l.buckets {
				if now.Sub(old.last).Seconds() >= math.Max(60, float64(l.rate.Burst)/l.rate.PerSecond) {
					delete(l.buckets, k)
				}
			}
		}
		if len(l.buckets) >= l.maxKeys {
			return false, 60
		}
		b = bucket{tokens: float64(l.rate.Burst), last: now}
	}
	if now.After(b.last) {
		b.tokens = math.Min(float64(l.rate.Burst), b.tokens+now.Sub(b.last).Seconds()*l.rate.PerSecond)
		b.last = now
	}
	if b.tokens < 1 {
		l.buckets[key] = b
		return false, max(1, int(math.Ceil((1-b.tokens)/l.rate.PerSecond)))
	}
	b.tokens--
	l.buckets[key] = b
	return true, 0
}
