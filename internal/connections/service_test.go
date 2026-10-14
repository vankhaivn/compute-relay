package connections

import (
	"context"
	"errors"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

type credentialReadRepository struct {
	Repository
	err error
}

func (r credentialReadRepository) ResolveConnectionCredential(context.Context, string) (string, error) {
	return "credential_fixture", r.err
}

type credentialReadVault struct {
	credentials.Vault
	calls int
	err   error
}

func (v *credentialReadVault) WithSecret(context.Context, string, func([]byte) error) error {
	v.calls++
	return v.err
}

func TestCredentialSlotReadPreservesLocalFailureAndRefusals(t *testing.T) {
	local := errors.New("private store diagnostic")
	for _, cause := range []error{local, context.DeadlineExceeded, credentials.ErrInvalid, credentials.ErrUnconfigured,
		errors.Join(credentials.ErrInvalid, local), errors.Join(credentials.ErrUnconfigured, local)} {
		t.Run(cause.Error(), func(t *testing.T) {
			vault := &credentialReadVault{}
			s := Service{repo: credentialReadRepository{err: cause}, vault: vault}
			err := s.WithCredential(t.Context(), "vault:con_fixture", func([]byte) error {
				t.Fatal("failed credential lookup invoked the consumer")
				return nil
			})
			refused := cause == credentials.ErrInvalid || cause == credentials.ErrUnconfigured
			if vault.calls != 0 || provider.IsLocalStateError(err) == refused {
				t.Fatal("credential slot refusal changed origin", err)
			}
			if refused && err != credentials.ErrUnavailable || !refused && !errors.Is(err, cause) {
				t.Fatal("credential slot failure was replaced", err)
			}
		})
	}
	vault := &credentialReadVault{err: credentials.ErrUnavailable}
	s := Service{repo: credentialReadRepository{}, vault: vault}
	err := s.WithCredential(t.Context(), "vault:con_fixture", func([]byte) error { t.Fatal("denied vault invoked consumer"); return nil })
	if err != credentials.ErrUnavailable || provider.IsLocalStateError(err) || vault.calls != 1 {
		t.Fatal("vault denial acquired a local repository origin", err)
	}
}
