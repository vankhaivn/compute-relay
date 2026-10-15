package connections

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

const fingerprintKey = "request_fingerprint_v1"

type Service struct {
	access        *auth.Service
	repo          Repository
	vault         credentials.Vault
	adapters      map[string]Adapter
	fingerprintMu sync.Mutex
	secretMu      sync.Mutex
	workerMu      sync.Mutex
}

func New(access *auth.Service, repo Repository, vault credentials.Vault, adapters []Adapter) (*Service, error) {
	if access == nil || repo == nil || len(adapters) == 0 || len(adapters) > 32 {
		return nil, ErrUnavailable
	}
	service := &Service{access: access, repo: repo, vault: vault, adapters: map[string]Adapter{}}
	for _, adapter := range adapters {
		if adapter == nil {
			return nil, ErrUnavailable
		}
		d := adapter.Descriptor()
		if !providerTypePattern.MatchString(d.Type) || service.adapters[d.Type] != nil || len(d.Fields) == 0 || len(d.Fields) > 16 {
			return nil, ErrUnavailable
		}
		if d.Capabilities.Concurrency != nil && d.Capabilities.Concurrency.Validate() != nil {
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
	c, err := s.repo.ReadConnection(ctx, w, p.TokenID(), id)
	return s.project(c), err
}
func (s *Service) List(ctx context.Context, p auth.Principal, w domain.WorkspaceID) ([]Connection, error) {
	p, err := s.require(ctx, p, w, auth.Read)
	if err != nil {
		return nil, err
	}
	items, err := s.repo.ListConnections(ctx, w, p.TokenID())
	for i := range items {
		items[i] = s.project(items[i])
	}
	return items, err
}

func (s *Service) project(c Connection) Connection {
	a := s.adapters[c.ProviderType]
	if a == nil {
		return c
	}
	capabilities := a.Descriptor().Capabilities
	if capabilities.Quota == "unsupported" {
		c.Quotas = []Quota{}
	}
	if capability := capabilities.Concurrency; capability != nil {
		if c.Concurrency == nil {
			limit := capability.Default
			if c.Configuration != nil && c.Configuration.MaxActiveAttempts != 0 {
				limit = c.Configuration.MaxActiveAttempts
			}
			reason := "connection_unverified"
			c.Concurrency = &ConcurrencyStatus{ConfiguredLimit: limit, WaitReason: &reason}
		}
		c.Concurrency.Scope = capability.Scope
	}
	return c
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
	input := Accept{Workspace: w, TokenID: p.TokenID(), KeyHash: string(keyHash), Fingerprint: fingerprint, ConnectionID: connection, ExpectedRevision: req.Revision, ProviderType: req.ProviderType, Label: req.Label, Action: req.Action, Secret: req.Credentials != nil, Configuration: req.Configuration}
	record, replay, err := s.repo.ReplayConnection(ctx, input)
	if err != nil {
		return Operation{}, err
	}
	// The original receipt survives changes to installed adapters or their field
	// rules. Only an unfinished vault write still needs credential encoding.
	if replay && record.Stage != "waiting_secret" {
		return record.Operation, nil
	}
	if req.Configuration != nil && (req.Configuration.MaxActiveAttempts != 0 || req.Configuration.MaxProcesses != 0 || req.Configuration.HostHealth != nil) {
		providerType := req.ProviderType
		if connection != "" {
			current, err := s.repo.ReadConnection(ctx, w, p.TokenID(), connection)
			if err != nil {
				return Operation{}, err
			}
			providerType = current.ProviderType
		}
		if req.Configuration.HostHealth != nil && providerType != "ssh" {
			return Operation{}, ErrRequest
		}
		validator, ok := s.adapters[providerType].(ConfigurationValidator)
		if !ok || validator.ValidateConfiguration(*req.Configuration) != nil {
			return Operation{}, ErrRequest
		}
	}
	var secret []byte
	if req.Credentials != nil {
		providerType := req.ProviderType
		if connection != "" {
			// Management authority may inspect private target type without requiring read scope.
			current, readErr := s.repo.ReadConnection(ctx, w, p.TokenID(), connection)
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
	if !replay && req.Action == "install_image" {
		current, err := s.repo.ReadConnection(ctx, w, p.TokenID(), connection)
		if err != nil {
			return Operation{}, err
		}
		installer, ok := s.adapters[current.ProviderType].(ImageInstaller)
		if !ok {
			return Operation{}, ErrUnsupported
		}
		plan, err := installer.PlanImageInstallation(current)
		if err != nil || plan.Validate() != nil {
			return Operation{}, ErrRequest
		}
		input.Installation = &plan
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
			if failErr := s.repo.FailConnection(ctx, record.Operation.ID, "credential_store_unavailable"); failErr != nil {
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
		if err == credentials.ErrInvalid || err == credentials.ErrUnconfigured {
			return nil, credentials.ErrUnavailable
		}
		return nil, provider.LocalStateError{Err: err}
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
