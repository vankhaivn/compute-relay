package credentials

import (
	"context"
	"crypto/subtle"
)

// nativeKeychainStore owns only installation-scoped Security framework operations.
// Its read result is an owned Go buffer; no secret is converted to a string.
type nativeKeychainStore interface {
	add(string, []byte) error
	read(string) ([]byte, error)
	remove(string) error
}

type nativeKeychain struct{ store nativeKeychainStore }

// NewNativeKeychain opens no credential and needs neither Python nor CGo.
// Unsupported platforms have no plaintext or ambient-credential fallback.
func NewNativeKeychain(installation string) (Vault, error) {
	if !vaultKey.MatchString(installation) {
		return nil, ErrInvalid
	}
	store, err := newNativeKeychainStore(installation)
	if err != nil {
		return nil, err
	}
	return &nativeKeychain{store: store}, nil
}

func (k *nativeKeychain) Create(ctx context.Context, key string, secret []byte) error {
	if !vaultKey.MatchString(key) || len(secret) == 0 || len(secret) > MaxVaultBytes {
		return ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	err := k.store.add(key, secret)
	if err == ErrConflict {
		stored, readErr := k.store.read(key)
		defer clear(stored)
		if readErr != nil {
			return readErr
		}
		if subtle.ConstantTimeCompare(stored, secret) == 1 {
			err = nil
		}
	}
	if err != nil {
		return err
	}
	return ctx.Err()
}

func (k *nativeKeychain) WithSecret(ctx context.Context, key string, use func([]byte) error) error {
	if !vaultKey.MatchString(key) || use == nil {
		return ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	secret, err := k.store.read(key)
	defer clear(secret)
	if err != nil {
		return err
	}
	if len(secret) == 0 || len(secret) > MaxVaultBytes {
		return ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := use(secret); err != nil {
		return err
	}
	return ctx.Err()
}

func (k *nativeKeychain) Delete(ctx context.Context, key string) error {
	if !vaultKey.MatchString(key) {
		return ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := k.store.remove(key); err != nil && err != ErrUnconfigured {
		return err
	}
	return ctx.Err()
}
