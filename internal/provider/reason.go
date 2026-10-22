package provider

import (
	"errors"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

type stepFailure struct {
	reason string
	cause  error
}

func (e stepFailure) Error() string         { return "provider step failed: " + e.reason }
func (e stepFailure) Unwrap() error         { return e.cause }
func (e stepFailure) FailureReason() string { return e.reason }

// WithReason adds a closed step identifier without changing error classification,
// retry semantics or an already retained specific reason.
func WithReason(err error, fallback string) error {
	if err == nil || Reason(err, "") != "" || !domain.ValidReason(fallback) {
		return err
	}
	return stepFailure{reason: fallback, cause: err}
}

// Reason returns only a bounded diagnostic identifier carried by a typed error.
// Callers supply a closed fallback; provider or OS error text is never inspected.
func Reason(err error, fallback string) string {
	var reasoned interface{ FailureReason() string }
	if errors.As(err, &reasoned) && domain.ValidReason(reasoned.FailureReason()) {
		return reasoned.FailureReason()
	}
	var value domain.Problem
	if errors.As(err, &value) {
		if reason := value.PublicDetails()[domain.ReasonDetail]; reason != "" {
			return reason
		}
	}
	var pointer *domain.Problem
	if errors.As(err, &pointer) && pointer != nil {
		if reason := pointer.PublicDetails()[domain.ReasonDetail]; reason != "" {
			return reason
		}
	}
	var read ReadError
	if errors.As(err, &read) && domain.ValidReason(read.Reason) {
		return read.Reason
	}
	var logRead LogReadFailure
	if errors.As(err, &logRead) && logRead.Valid() {
		return logRead.Reason
	}
	if domain.ValidReason(fallback) {
		return fallback
	}
	return ""
}
