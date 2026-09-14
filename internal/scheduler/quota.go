package scheduler

import (
	"math"
	"time"

	"github.com/vankhaivn/compute-relay/internal/provider"
)

// Quota preserves the provider observation, including original units and precision.
// Exhausted is a durable latch cleared only by NEWER fresh positive evidence in
// understood units. Failure/aging/reset timestamps cannot manufacture new allowance.
type Quota struct {
	Observation *provider.QuotaObservation
	Exhausted   bool
}

func Seconds(q provider.QuotaObservation) (float64, bool) {
	if q.Remaining == nil || q.Validate() != nil {
		return 0, false
	}
	value := *q.Remaining
	switch q.Unit {
	case "seconds":
	case "hours":
		value *= 3600
	default:
		return 0, false
	}
	return value, !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0
}

func (q Quota) Decide(resource string, wall int64, strict bool, freshFor time.Duration, now time.Time) (Reason, Reason) {
	if q.Exhausted {
		return QuotaExhausted, ""
	}
	warning := QuotaUnknown
	if o := q.Observation; o != nil {
		if o.Validate() != nil || o.Resource != resource || o.ObservedAt.IsZero() || o.ObservedAt.After(now) {
			warning = QuotaUncertain
		} else {
			switch o.Status {
			case provider.QuotaUnavailable:
				warning = QuotaUnavailable
			case provider.QuotaUnknown:
				warning = QuotaUnknown
			case provider.QuotaKnown, provider.QuotaStale:
				remaining, ok := Seconds(*o)
				if ok && remaining == 0 {
					return QuotaExhausted, ""
				}
				switch {
				case !ok:
					warning = QuotaUncertain
				case o.Status == provider.QuotaStale || now.Sub(o.ObservedAt) >= freshFor:
					warning = QuotaStale
				case remaining < float64(wall):
					return QuotaInsufficient, ""
				// Formatted/rounded upstream quota is not an exact available-time guarantee.
				// Strict mode accepts only explicitly exact or conservative lower-bound values.
				case o.Precision != "exact" && o.Precision != "lower_bound":
					warning = QuotaUncertain
				default:
					return Eligible, ""
				}
			default:
				warning = QuotaUncertain
			}
		}
	}
	if strict {
		return warning, ""
	}
	return Eligible, warning
}
