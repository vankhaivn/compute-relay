package kaggle

import (
	"context"
	"time"

	"github.com/vankhaivn/compute-relay/internal/provider"
)

const QuotaFreshFor = 5 * time.Minute
const quotaSource = "kaggle-sdk/0.1.35:gpu_quota:reservations_included"

var _ provider.QuotaReader = (*Monitor)(nil)

// ReadQuota uses the original server-verified account, not a job/workspace quota.
// The observation timestamp precedes the read so response latency cannot make old
// evidence appear fresh. There is no secret/value cache and no inferred reset day.
func (m *Monitor) ReadQuota(ctx context.Context) (provider.QuotaObservation, error) {
	if m == nil {
		return provider.QuotaObservation{}, ErrConfig
	}
	at := m.clock.Now().UTC()
	unknown := provider.QuotaObservation{Status: provider.QuotaUnavailable, Resource: "gpu", Unit: "seconds", Source: quotaSource, Precision: "unknown", ObservedAt: at}
	if at.IsZero() {
		return unknown, ErrConfig
	}
	r, err := m.call(ctx, "quota", nil)
	if err != nil {
		return unknown, err
	}
	if m.clock.Now().Before(at) {
		return unknown, ErrProtocol
	}
	return quotaFromResponse(r, at)
}

func quotaFromResponse(r monitorResponse, at time.Time) (provider.QuotaObservation, error) {
	q := provider.QuotaObservation{Status: provider.QuotaUnavailable, Resource: "gpu", Unit: "seconds", Source: quotaSource, Precision: "unknown", ObservedAt: at}
	if at.IsZero() || !r.valid("quota") {
		return q, ErrProtocol
	}
	if r.Status == "unknown" {
		q.Status = provider.QuotaUnknown
		return q, nil
	}
	if r.Status == "unavailable" {
		return q, nil
	}
	limit, _ := quotaNanos(r.LimitNS)
	used, _ := quotaNanos(r.UsedNS)
	reserved, _ := quotaNanos(r.ReservedNS)
	const second int64 = 1_000_000_000
	// Whole-second lower bound: floor total, ceil used AND reserved. Each number
	// is exactly representable as float64. Missing fields never become zero.
	l := limit / second
	u := (used + second - 1) / second
	s := (reserved + second - 1) / second
	remaining := l - u - s
	if remaining < 0 {
		remaining = 0
	}
	lf, uf, rf := float64(l), float64(u), float64(remaining)
	q.Status = provider.QuotaKnown
	q.Limit = &lf
	q.Used = &uf
	q.Remaining = &rf
	q.Precision = "lower_bound"
	return q, q.Validate()
}

// AgeQuota is pure and preserves provenance and exhaustion. It never refreshes
// ObservedAt or invents an allowance from clock passage/reset expectations.
func AgeQuota(q provider.QuotaObservation, now time.Time) (provider.QuotaObservation, error) {
	if q.Validate() != nil || q.ObservedAt.IsZero() || now.IsZero() || now.Before(q.ObservedAt) {
		return provider.QuotaObservation{}, ErrProtocol
	}
	clone := func(value *float64) *float64 {
		if value == nil {
			return nil
		}
		copy := *value
		return &copy
	}
	q.Limit, q.Used, q.Remaining = clone(q.Limit), clone(q.Used), clone(q.Remaining)
	if q.ResetAt != nil {
		copy := *q.ResetAt
		q.ResetAt = &copy
	}
	if q.Status == provider.QuotaKnown && now.Sub(q.ObservedAt) >= QuotaFreshFor {
		q.Status = provider.QuotaStale
	}
	return q, nil
}
