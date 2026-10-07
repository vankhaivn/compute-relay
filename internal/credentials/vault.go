package credentials

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"regexp"
	"strings"

	"github.com/vankhaivn/compute-relay/internal/ports"
)

const MaxVaultBytes = 64 << 10

var (
	ErrUnsupported  = errors.New("protected credential storage is unsupported")
	ErrConflict     = errors.New("protected credential entry already exists with different contents")
	vaultKey        = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	vaultConnection = regexp.MustCompile(`^con_[A-Za-z0-9][A-Za-z0-9._-]{0,123}$`)
)

// VaultReference validates a stable connection slot, not a secret generation key.
// The runtime resolves the slot to its current verified generation separately.
func VaultReference(ref ports.CredentialRef) (string, bool) {
	id, ok := strings.CutPrefix(string(ref), "vault:")
	if !ok || !vaultConnection.MatchString(id) {
		return "", false
	}
	return id, true
}

// Vault stores immutable, installation-scoped entries. Create is idempotent only for
// identical bytes. Callers durably reserve a key before creating it and retain exact
// deletion intents; no list, arbitrary path or ambient-credential lookup is exposed.
// WithSecret clears its owned buffer after the callback, including error/panic exits.
// Same-user host processes and the protected store implementation remain trusted.
type Vault interface {
	Create(context.Context, string, []byte) error
	WithSecret(context.Context, string, func([]byte) error) error
	Delete(context.Context, string) error
}

// Fingerprint uses an explicitly provisioned protected key. It never creates or
// replaces a missing key: that would lose the identity of earlier secret requests.
func Fingerprint(ctx context.Context, vault Vault, key string, canonical []byte) (string, error) {
	if vault == nil || !vaultKey.MatchString(key) || len(canonical) == 0 || len(canonical) > MaxVaultBytes {
		return "", ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	var result string
	err := vault.WithSecret(ctx, key, func(secret []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(secret) != 32 {
			return ErrUnavailable
		}
		mac := hmac.New(sha256.New, secret)
		_, _ = mac.Write([]byte("compute-relay/credential-request/v1\x00"))
		_, _ = mac.Write(canonical)
		result = hex.EncodeToString(mac.Sum(nil))
		return nil
	})
	if err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if result == "" {
		return "", ErrUnavailable
	}
	return result, nil
}
