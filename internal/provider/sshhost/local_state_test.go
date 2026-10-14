package sshhost

import (
	"context"
	"errors"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

type localCredentialFailure struct{ err error }

func (c localCredentialFailure) WithCredential(context.Context, ports.CredentialRef, func([]byte) error) error {
	return c.err
}

func TestLocalCredentialReadPreservesSSHOriginAndUnknownStart(t *testing.T) {
	for _, cause := range []error{context.DeadlineExceeded, errors.New("private invariant diagnostic")} {
		t.Run(cause.Error(), func(t *testing.T) {
			h := newHarness(t)
			plan, prepared := h.prepare(t)
			request := provider.CleanupResourceRequest{CleanupRequest: provider.CleanupRequest{
				Remote:   provider.RemoteReference{Identity: prepared.Identity, Resource: prepared.Resource, Version: "prepared"},
				LedgerID: "res_fixture", CreationOperationID: prepared.PreparationID, Mode: provider.CleanupApply},
				Purpose: "staging", OwnershipOperationID: prepared.PreparationID, PlanSHA256: plan.Digest()}
			h.adapter.creds = localCredentialFailure{provider.LocalStateError{Err: cause}}
			before := h.dials
			calls := []func() error{
				func() error { return h.adapter.Verify(t.Context(), h.adapter.binding) },
				func() error { _, err := h.adapter.Reconcile(t.Context(), prepared.Identity); return err },
				func() error { _, err := h.adapter.CleanupResource(t.Context(), request); return err },
			}
			for _, call := range calls {
				if err := call(); !provider.IsLocalStateError(err) || !errors.Is(err, cause) {
					t.Fatal("SSH credential slot failure was replaced", err)
				}
			}
			out := h.adapter.Start(t.Context(), prepared, nil, nil)
			if out.Status != provider.SubmissionUnknown || out.Validate(prepared.Identity) != nil || h.dials != before {
				t.Fatal("unavailable local state acquired definitive rejection or remote effects", out)
			}
			// An actual vault denial retains the existing closed refusal semantics.
			h.adapter.creds = localCredentialFailure{credentials.ErrUnavailable}
			if out := h.adapter.Start(t.Context(), prepared, nil, nil); out.Status != provider.SubmissionRejected {
				t.Fatal("vault denial changed semantics", out)
			}
		})
	}
}
