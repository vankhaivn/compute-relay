package provider

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

func TestReasonPreservesIdentityAndSpecificDiagnostic(t *testing.T) {
	cause := errors.New("private provider response /secret/path")
	err := WithReason(cause, "storage_inventory_read_failed")
	if !errors.Is(err, cause) || strings.Contains(err.Error(), "private") || strings.Contains(err.Error(), "/secret") {
		t.Fatalf("unsafe wrapper or lost cause: %v", err)
	}
	if got := Reason(WithReason(fmt.Errorf("wrapped: %w", err), "generic_failed"), "fallback"); got != "storage_inventory_read_failed" {
		t.Fatalf("lost specific reason: %s", got)
	}
	problem := Problem(domain.CodeProviderStateUnknown, domain.FailureStageObservation, "State unknown").
		WithDetail(domain.ReasonDetail, "execution_status_read_failed").WithRetrySemantics(false, true).WithCause(cause)
	for _, source := range []error{problem, &problem} {
		wrapped := WithReason(source, "generic_failed")
		if Reason(wrapped, "") != "execution_status_read_failed" || !errors.Is(wrapped, cause) {
			t.Fatal("changed structured problem or identity")
		}
	}
	if got := Reason(ReadError{Reason: "staging_metadata_invalid", Err: cause}, "fallback"); got != "staging_metadata_invalid" {
		t.Fatalf("lost read reason: %s", got)
	}
}

func TestReasonNeverPromotesUntrustedText(t *testing.T) {
	for _, unsafe := range []string{"", "Secret", "a/b", "a b", strings.Repeat("x", 65)} {
		err := errors.New(unsafe)
		if WithReason(err, unsafe) != err || Reason(ReadError{Reason: unsafe, Err: err}, unsafe) != "" {
			t.Fatalf("accepted unsafe diagnostic %q", unsafe)
		}
	}
	if WithReason(nil, "read_failed") != nil {
		t.Fatal("created a failure from nil")
	}
}

func TestReadReasonIsSafeForEmptyAndPrivateCauses(t *testing.T) {
	for _, err := range []error{ReadError{}, ReadError{Reason: "read_failed", Err: errors.New("private-canary")}, ReadError{Reason: "private /path"}} {
		if strings.Contains(err.Error(), "private") {
			t.Fatal("read error exposed text", err)
		}
	}
	if Reason(LogReadFailure{Reason: "private_provider_canary"}, "") != "" {
		t.Fatal("unrecognized log reason escaped")
	}
}
