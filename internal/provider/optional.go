package provider

import (
	"errors"
	"math"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

func (outcome CancellationOutcome) Validate() error {
	switch outcome.Status {
	case domain.CancellationConfirmed:
		if !outcome.TerminationConfirmed {
			return errors.New("confirmed cancellation lacks termination evidence")
		}
	case domain.CancellationAccepted, domain.CancellationTooLate, domain.CancellationManual:
		if outcome.TerminationConfirmed {
			return errors.New("request or manual guidance cannot confirm termination")
		}
	default:
		return errors.New("invalid cancellation outcome")
	}
	return nil
}
func (page LogPage) Validate(request PageRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if page.Source != "provider" && page.Source != "payload" {
		return errors.New("invalid provider log source")
	}
	switch page.Availability {
	case "live", "delayed", "after_completion", "unavailable", "unknown":
	default:
		return errors.New("invalid log availability")
	}
	if len(page.Lines) > request.Limit || len(page.NextCursor) > 512 {
		return errors.New("log page exceeds bounds")
	}
	total := 0
	for _, line := range page.Lines {
		if len(line) > 16<<10 {
			return errors.New("log line exceeds bound")
		}
		total += len(line)
	}
	if total > 256<<10 {
		return errors.New("log bytes exceed bound")
	}
	if (page.Availability == "unavailable" || page.Availability == "unknown") && (len(page.Lines) > 0 || page.NextCursor != "") {
		return errors.New("unavailable logs cannot contain invented output")
	}
	return nil
}
func (quota QuotaObservation) Validate() error {
	switch quota.Status {
	case QuotaKnown, QuotaUnknown, QuotaStale, QuotaUnavailable:
	default:
		return errors.New("invalid quota status")
	}
	for _, value := range []*float64{quota.Limit, quota.Used, quota.Remaining} {
		if value != nil && (math.IsInf(*value, 0) || math.IsNaN(*value) || *value < 0) {
			return errors.New("invalid quota value")
		}
	}
	if quota.Status == QuotaKnown || quota.Status == QuotaStale {
		if quota.Limit == nil || quota.Used == nil || quota.Remaining == nil || quota.ObservedAt.IsZero() ||
			!safeText(quota.Source, 256) || !safeText(quota.Resource, 64) || !safeText(quota.Unit, 64) || !safeText(quota.Precision, 128) {
			return errors.New("known or stale quota requires values, units, provenance and timestamp")
		}
	}
	if quota.ResetAt != nil && quota.ResetAt.IsZero() {
		return errors.New("invalid quota reset timestamp")
	}
	return nil
}
