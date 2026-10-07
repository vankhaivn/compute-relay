package connections

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
)

const fingerprintKey = "request_fingerprint_v1"

type Service struct {
	access        *auth.Service
	repo          Repository
	vault         credentials.Vault
	adapters      map[string]Adapter
	now           func() time.Time
	fingerprintMu sync.Mutex
	secretMu      sync.Mutex
	workerMu      sync.Mutex
}

func New(access *auth.Service, repo Repository, vault credentials.Vault, adapters []Adapter, now func() time.Time) (*Service, error) {
	if access == nil || repo == nil || len(adapters) == 0 || len(adapters) > 32 {
		return nil, ErrUnavailable
	}
	if now == nil {
		now = time.Now
	}
	service := &Service{access: access, repo: repo, vault: vault, adapters: map[string]Adapter{}, now: now}
	for _, adapter := range adapters {
		if adapter == nil {
			return nil, ErrUnavailable
		}
		d := adapter.Descriptor()
		if !providerTypePattern.MatchString(d.Type) || service.adapters[d.Type] != nil || len(d.Fields) == 0 || len(d.Fields) > 16 {
			return nil, ErrUnavailable
		}
		seen := map[string]bool{}
		for _, field := range d.Fields {
			if field.Name == "" || seen[field.Name] || !field.WriteOnly || field.MaxBytes < 1 || field.MaxBytes > 16384 {
				return nil, ErrUnavailable
			}
			seen[field.Name] = true
		}
		service.adapters[d.Type] = adapter
	}
	return service, nil
}
func (s *Service) require(ctx context.Context, p auth.Principal, w domain.WorkspaceID, scope auth.Scope) (auth.Principal, error) {
	fresh, err := s.access.Revalidate(ctx, p)
	if err != nil {
		return auth.Principal{}, err
	}
	if err = auth.Require(fresh, w, scope); err != nil {
		return auth.Principal{}, err
	}
	return fresh, nil
}
func (s *Service) Descriptors(ctx context.Context, p auth.Principal, w domain.WorkspaceID) ([]Descriptor, error) {
	if _, err := s.require(ctx, p, w, auth.Read); err != nil {
		return nil, err
	}
	return s.descriptors(), nil
}
func (s *Service) Get(ctx context.Context, p auth.Principal, w domain.WorkspaceID, id string) (Connection, error) {
	p, err := s.require(ctx, p, w, auth.Read)
	if err != nil {
		return Connection{}, err
	}
	return s.repo.ReadConnection(ctx, w, p.TokenID(), id, s.now().UTC())
}
func (s *Service) List(ctx context.Context, p auth.Principal, w domain.WorkspaceID) ([]Connection, error) {
	p, err := s.require(ctx, p, w, auth.Read)
	if err != nil {
		return nil, err
	}
	return s.repo.ListConnections(ctx, w, p.TokenID(), s.now().UTC())
}
func (s *Service) Operation(ctx context.Context, p auth.Principal, w domain.WorkspaceID, id domain.OperationID) (Operation, error) {
	p, err := s.require(ctx, p, w, auth.Manage)
	if err != nil {
		return Operation{}, err
	}
	return s.repo.ReadConnectionOperation(ctx, w, p.TokenID(), id)
}
func (s *Service) Submit(ctx context.Context, p auth.Principal, w domain.WorkspaceID, connection, key string, raw []byte) (Operation, error) {
	p, err := s.require(ctx, p, w, auth.Manage)
	if err != nil {
		return Operation{}, err
	}
	keyHash, err := admission.KeyDigest(key)
	if err != nil {
		return Operation{}, err
	}
	kind := "action"
	if connection == "" {
		kind = "create"
	}
	req, canonical, err := parse(raw, kind)
	if err != nil {
		return Operation{}, err
	}
	defer func() { clear(canonical) }()
	if s.vault == nil {
		return Operation{}, ErrVault
	}
	// Target identity is part of replay, even when two connections share one request body.
	// Only the keyed fingerprint is persisted. Hashing the bounded canonical input
	// first avoids copying secret bytes while binding the target into a fixed-size MAC.
	digest := sha256.Sum256(canonical)
	fingerprintInput := []byte(connection + "\x00" + hex.EncodeToString(digest[:]))
	defer clear(fingerprintInput)
	fingerprint, err := s.fingerprint(ctx, fingerprintInput)
	if err != nil {
		return Operation{}, ErrVault
	}
	// Serialize the durable intent read with vault staging and exact deletion. A
	// delayed replay must not recreate a generation that removal already erased.
	s.secretMu.Lock()
	defer s.secretMu.Unlock()
	input := Accept{Workspace: w, TokenID: p.TokenID(), KeyHash: string(keyHash), Fingerprint: fingerprint, ConnectionID: connection, ExpectedRevision: req.Revision, ProviderType: req.ProviderType, Label: req.Label, Action: req.Action, Secret: req.Credentials != nil, Now: s.now().UTC()}
	record, replay, err := s.repo.ReplayConnection(ctx, input)
	if err != nil {
		return Operation{}, err
	}
	// The original receipt survives changes to installed adapters or their field
	// rules. Only an unfinished vault write still needs credential encoding.
	if replay && record.Stage != "waiting_secret" {
		return record.Operation, nil
	}
	var secret []byte
	if req.Credentials != nil {
		providerType := req.ProviderType
		if connection != "" {
			// Management authority may inspect private target type without requiring read scope.
			current, readErr := s.repo.ReadConnection(ctx, w, p.TokenID(), connection, s.now().UTC())
			if readErr != nil {
				return Operation{}, readErr
			}
			providerType = current.ProviderType
		}
		adapter := s.adapters[providerType]
		if adapter == nil {
			return Operation{}, ErrUnsupported
		}
		if err = validateFields(adapter.Descriptor(), req.Credentials); err != nil {
			return Operation{}, err
		}
		secret, err = adapter.Encode(req.Credentials)
		if err != nil {
			return Operation{}, ErrRequest
		}
		defer clear(secret)
		if len(secret) == 0 || len(secret) > credentials.MaxVaultBytes {
			return Operation{}, ErrRequest
		}
	}
	if kind == "create" && s.adapters[req.ProviderType] == nil {
		return Operation{}, ErrUnsupported
	}
	if !replay {
		record, err = s.repo.AcceptConnection(ctx, input)
		if err != nil {
			return Operation{}, err
		}
	}
	if record.Stage == "waiting_secret" {
		if err = s.vault.Create(ctx, record.CredentialKey, secret); err != nil {
			// A cancellation/unknown commit preserves the reserved intent for an exact replay.
			if ctx.Err() != nil {
				return Operation{}, ctx.Err()
			}
			if failErr := s.repo.FailConnection(ctx, record.Operation.ID, "credential_store_unavailable", s.now().UTC()); failErr != nil {
				return Operation{}, failErr
			}
		} else if err = s.repo.ReadyConnection(ctx, w, p.TokenID(), record.Operation.ID); err != nil {
			return Operation{}, err
		}
	}
	return record.Operation, nil
}
func (s *Service) fingerprint(ctx context.Context, canonical []byte) (string, error) {
	s.fingerprintMu.Lock()
	defer s.fingerprintMu.Unlock()
	initialized, err := s.repo.FingerprintInitialized(ctx)
	if err != nil {
		return "", err
	}
	if !initialized {
		err = s.vault.WithSecret(ctx, fingerprintKey, func(secret []byte) error {
			if len(secret) != 32 {
				return credentials.ErrUnavailable
			}
			return nil
		})
		if errors.Is(err, credentials.ErrUnconfigured) {
			var key [32]byte
			if _, err = rand.Read(key[:]); err != nil {
				return "", err
			}
			err = s.vault.Create(ctx, fingerprintKey, key[:])
			clear(key[:])
			if errors.Is(err, credentials.ErrConflict) {
				err = nil
			} // Concurrent initialization keeps the first immutable key.
		}
		if err != nil {
			return "", err
		}
		if err = s.repo.MarkFingerprintInitialized(ctx); err != nil {
			return "", err
		}
	}
	return credentials.Fingerprint(ctx, s.vault, fingerprintKey, canonical)
}

// WithCredential resolves only an explicitly configured stable slot, never ambient keys.
func (s *Service) WithCredential(ctx context.Context, ref ports.CredentialRef, use func([]byte) error) error {
	id, ok := credentials.VaultReference(ref)
	if !ok || use == nil {
		return credentials.ErrInvalid
	}
	if s.vault == nil {
		return credentials.ErrUnavailable
	}
	secret, err := s.readCredential(ctx, id)
	defer clear(secret)
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	return use(secret)
}

func (s *Service) readCredential(ctx context.Context, id string) ([]byte, error) {
	s.secretMu.Lock()
	defer s.secretMu.Unlock()
	key, err := s.repo.ResolveConnectionCredential(ctx, id)
	if err != nil {
		return nil, credentials.ErrUnavailable
	}
	var owned []byte
	err = s.vault.WithSecret(ctx, key, func(secret []byte) error {
		if len(secret) == 0 || len(secret) > credentials.MaxVaultBytes {
			return credentials.ErrUnavailable
		}
		// Keep the selected generation alive through the read, then release the
		// guard before caller code can perform network work or another lookup.
		owned = append([]byte(nil), secret...)
		return nil
	})
	return owned, err
}
