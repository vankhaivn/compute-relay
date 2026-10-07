package connections

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"strconv"
	"time"

	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/jsonwire"
)

func (s *Service) descriptors() []Descriptor {
	result := make([]Descriptor, 0, len(s.adapters))
	for _, adapter := range s.adapters {
		d := adapter.Descriptor()
		d.Fields = append([]Field(nil), d.Fields...)
		d.Capabilities.Accelerators = append([]string(nil), d.Capabilities.Accelerators...)
		d.CredentialStorage = "available"
		if s.vault == nil {
			d.CredentialStorage = "unsupported"
		}
		result = append(result, d)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Type < result[j].Type })
	return result
}
func validateFields(d Descriptor, values map[string]string) error {
	allowed := map[string]bool{}
	for _, field := range d.Fields {
		allowed[field.Name] = true
		value, present := values[field.Name]
		if field.Required && !present || present && (len(value) == 0 || len(value) > field.MaxBytes) {
			return ErrRequest
		}
	}
	for key := range values {
		if !allowed[key] {
			return ErrRequest
		}
	}
	return nil
}

// AccountScope groups credentials for one canonical provider account, including across
// workspaces. It is an opaque capacity identity, not a secrecy boundary for account names.
func AccountScope(provider, account string) string {
	digest := sha256.Sum256([]byte(provider + "\x00" + account))
	return "account_" + hex.EncodeToString(digest[:])
}
func ProfileBinding(id string, revision int64) domain.ProviderBinding {
	revisionText := "r" + strconv.FormatInt(revision, 10)
	return domain.ProviderBinding{Profile: id + "_" + revisionText, ProviderInstanceID: domain.ProviderInstanceID(id), ConfigurationRevision: revisionText}
}

// RunOnce performs one durable operation or exact pending secret deletion. Composition
// owns the bounded scheduling loop. Read-only provider verification is restartable;
// it never grants compute, stages provider data or submits a remote job.
func (s *Service) RunOnce(ctx context.Context) (bool, error) {
	s.workerMu.Lock()
	defer s.workerMu.Unlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if s.vault == nil {
		return false, ErrVault
	}
	record, found, err := s.repo.NextConnectionOperation(ctx, s.now().UTC())
	if err != nil {
		return false, err
	}
	if !found {
		s.secretMu.Lock()
		defer s.secretMu.Unlock()
		key, pending, err := s.repo.NextSecretDeletion(ctx)
		if err != nil || !pending {
			return pending, err
		}
		if err = s.vault.Delete(ctx, key); err != nil {
			return true, ErrVault
		}
		return true, s.repo.CompleteSecretDeletion(ctx, key)
	}
	op := record.Operation
	if record.Stage == "waiting_secret" {
		s.secretMu.Lock()
		defer s.secretMu.Unlock()
		err = s.vault.WithSecret(ctx, record.CredentialKey, func([]byte) error { return nil })
		if err == nil {
			err = s.repo.ReadyConnection(ctx, op.Workspace, record.ActorTokenID, op.ID)
		}
		if err != nil {
			if ctx.Err() != nil {
				return true, ctx.Err()
			}
			// Absence after a crash is not a failed user request. Keep the original
			// reservation writable by exact replay, and defer its next inspection so it
			// cannot starve ready operations or pending exact secret cleanup.
			return true, s.repo.DeferConnectionSecret(ctx, op.ID, s.now().UTC())
		}
		return true, nil
	}
	if op.Action == "remove" {
		s.secretMu.Lock()
		defer s.secretMu.Unlock()
		keys, err := s.repo.ConnectionSecrets(ctx, op.ID)
		if err != nil {
			return true, err
		}
		for _, key := range keys {
			if err = s.vault.Delete(ctx, key); err != nil {
				if ctx.Err() != nil {
					return true, ctx.Err()
				}
				return true, s.repo.FailConnection(ctx, op.ID, "credential_store_unavailable", s.now().UTC())
			}
		}
		return true, s.repo.FinishConnection(ctx, record, Completion{Now: s.now().UTC()})
	}
	if op.Action == "disable" {
		return true, s.repo.FinishConnection(ctx, record, Completion{Now: s.now().UTC()})
	}
	adapter := s.adapters[record.ProviderType]
	if adapter == nil {
		return true, s.repo.FailConnection(ctx, op.ID, "unsupported_provider", s.now().UTC())
	}
	var verified Verification
	verifyCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	err = s.vault.WithSecret(verifyCtx, record.CredentialKey, func(secret []byte) error {
		var err error
		verified, err = adapter.Verify(verifyCtx, secret)
		return err
	})
	if ctx.Err() != nil {
		return true, ctx.Err()
	}
	if err != nil {
		problem := "provider_unavailable"
		if errors.Is(err, ErrCredentialRejected) {
			problem = "credential_rejected"
		}
		if errors.Is(err, credentials.ErrUnavailable) || errors.Is(err, credentials.ErrUnconfigured) {
			problem = "credential_store_unavailable"
		}
		return true, s.repo.FailConnection(ctx, op.ID, problem, s.now().UTC())
	}
	if !domain.ObjectID(verified.CanonicalAccount).Valid() || verified.Quota.Validate() != nil {
		return true, s.repo.FailConnection(ctx, op.ID, "provider_unavailable", s.now().UTC())
	}
	if record.CanonicalAccount != "" && record.CanonicalAccount != verified.CanonicalAccount {
		return true, s.repo.FailConnection(ctx, op.ID, "account_changed", s.now().UTC())
	}
	profile := adapter.Profile(ProfileBinding(op.ConnectionID, op.ConnectionRevision), AccountScope(record.ProviderType, verified.CanonicalAccount))
	profile.CredentialRef = "vault:" + op.ConnectionID
	if profile.Validate() != nil {
		return true, s.repo.FailConnection(ctx, op.ID, "unsupported_provider", s.now().UTC())
	}
	var runtimeConfig []byte
	if configured, ok := adapter.(RuntimeConfigurator); ok {
		candidate := configured.RuntimeConfig()
		if _, err := jsonwire.Object(candidate, 8192); err != nil {
			return true, s.repo.FailConnection(ctx, op.ID, "unsupported_provider", s.now().UTC())
		}
		runtimeConfig = append([]byte(nil), candidate...)
	}
	return true, s.repo.FinishConnection(ctx, record, Completion{Verification: verified, Profile: profile, RuntimeConfig: runtimeConfig, Now: s.now().UTC()})
}
