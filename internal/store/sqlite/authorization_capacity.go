package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

type managedCapacityView struct {
	LimitSeconds         *int64
	UsedSeconds          *int64
	LocalReservedSeconds *int64
	ResetAt              *time.Time
	Status               string
	RemainingSeconds     *int64
	ObservedAt           *time.Time
	ReservedAttempts     int
}

// managedReserved includes finite consent waiting for dispatch and work which may
// still hold remote capacity. Terminal/inactive evidence releases the reservation;
// a consumed permit by itself is never evidence of active remote execution.
func managedReserved(row scheduler.Candidate) bool {
	return row.AuthorizationGranted && scheduler.LocalOnly(row.State) || scheduler.HoldsAccount(row.State, row.DispatchBarrier)
}

// managedCapacity is shared by authorization and sanitized connection projections.
// It never refreshes the provider or treats stale/rounded observations as capacity.
func managedCapacity(ctx context.Context, tx *sql.Tx, account string, now time.Time) (managedCapacityView, error) {
	result := managedCapacityView{Status: "unknown"}
	settings, _, err := schedulerControl(ctx, tx, now)
	if err != nil {
		return result, err
	}
	rows, err := schedulerSnapshot(ctx, tx, settings, now)
	if err != nil {
		return result, err
	}
	var reserved int64
	for _, row := range rows {
		if row.AccountScope == account && managedReserved(row) {
			result.ReservedAttempts++
			if row.Resource == "gpu" {
				reserved += row.WallSeconds
			}
		}
	}
	var raw string
	var exhausted bool
	var observed int64
	err = tx.QueryRowContext(ctx, "SELECT observation,exhausted,observed_ms FROM scheduler_quotas WHERE account_scope=? AND resource='gpu'", account).Scan(&raw, &exhausted, &observed)
	if errors.Is(err, sql.ErrNoRows) {
		return result, nil
	}
	if err != nil {
		return result, dbError(err)
	}
	var q provider.QuotaObservation
	if len(raw) > 4096 || json.Unmarshal([]byte(raw), &q) != nil || q.Validate() != nil || q.Resource != "gpu" || !scheduler.ValidTime(q.ObservedAt) || q.ObservedAt.UnixMilli() != observed {
		return result, ErrCorrupt
	}
	result.ObservedAt = &q.ObservedAt
	if q.ObservedAt.After(now) {
		return result, nil
	}
	if q.Status != provider.QuotaKnown && q.Status != provider.QuotaStale {
		return result, nil
	}
	// Presentation metadata comes from the same validated observation. It never
	// changes the conservative admission calculation or invents a reset schedule.
	secondsValue := func(value *float64, round func(float64) float64) *int64 {
		copy := q
		copy.Remaining = value
		seconds, ok := scheduler.Seconds(copy)
		if !ok || seconds > float64(math.MaxInt64/2) {
			return nil
		}
		result := int64(round(seconds))
		return &result
	}
	result.LimitSeconds = secondsValue(q.Limit, math.Floor)
	result.UsedSeconds = secondsValue(q.Used, math.Ceil)
	result.LocalReservedSeconds = &reserved
	if q.ResetAt != nil && scheduler.ValidTime(*q.ResetAt) {
		result.ResetAt = q.ResetAt
	}
	freshFor := min(settings.QuotaFreshFor, 5*time.Minute)
	if q.Status == provider.QuotaStale || now.Sub(q.ObservedAt) >= freshFor {
		result.Status = "stale"
		return result, nil
	}
	remaining, ok := scheduler.Seconds(q)
	if q.Status != provider.QuotaKnown || !ok || q.Precision != "exact" && q.Precision != "lower_bound" {
		return result, nil
	}
	result.Status = "known"
	remaining = math.Max(0, remaining-float64(reserved))
	if exhausted {
		remaining = 0
	}
	seconds := int64(min(math.Floor(remaining), float64(math.MaxInt64/2)))
	result.RemainingSeconds = &seconds
	return result, nil
}

// reserveManagedQuota applies the same reservation accounting at the dispatch
// gate, excluding the candidate's own already-reserved budget. Other modes remain
// compatible while managed work requires a conservative positive observation.
func reserveManagedQuota(rows []scheduler.Candidate, now time.Time) {
	totals := map[string]int64{}
	for _, row := range rows {
		if row.Resource == "gpu" && managedReserved(row) {
			totals[row.AccountScope] += row.WallSeconds
		}
	}
	for i := range rows {
		row := &rows[i]
		if !row.Managed || row.Resource != "gpu" || row.Quota.Observation == nil {
			continue
		}
		q := *row.Quota.Observation
		if q.Status == provider.QuotaKnown && now.Sub(q.ObservedAt) >= 5*time.Minute {
			q.Status = provider.QuotaStale
		}
		remaining, ok := scheduler.Seconds(q)
		if !ok {
			continue
		}
		reserved := totals[row.AccountScope]
		if managedReserved(*row) {
			reserved -= row.WallSeconds
		}
		value := math.Max(0, remaining-float64(reserved))
		if q.Unit == "hours" {
			value /= 3600
		}
		q.Remaining = &value
		row.Quota.Observation = &q
	}
}
