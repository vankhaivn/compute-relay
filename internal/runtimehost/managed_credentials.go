package runtimehost

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/statefs"
)

// The host lock serializes first-use selection. Persist it before provisioning any
// secret so a missing or denied backend cannot redirect later work to another store.
func (h *Host) managedCredentialStore(ctx context.Context, requested string) (string, error) {
	if requested != "" && requested != "file" && requested != "keychain" {
		return "", ErrRequest
	}
	path := filepath.Join(h.root.Path, "state", "credential-store")
	if _, err := os.Lstat(path); err == nil {
		raw, err := readPrivate(path, 16)
		if err != nil || string(raw) != "file\n" && string(raw) != "keychain\n" {
			return "", ErrState
		}
		stored := string(raw[:len(raw)-1])
		if requested != "" && requested != stored {
			return "", ErrRequest
		}
		return stored, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", ErrState
	}
	// Older installations have no selection marker and only supported Keychain.
	// A file directory without its marker is incomplete state, not a legacy install.
	if _, err := os.Lstat(filepath.Join(h.root.Path, "credentials")); !errors.Is(err, os.ErrNotExist) {
		return "", ErrState
	}
	initialized, err := h.store.FingerprintInitialized(ctx)
	if err != nil {
		return "", ErrState
	}
	selected := requested
	if initialized {
		if selected == "file" {
			return "", ErrRequest
		}
		selected = "keychain"
	} else if selected == "" {
		selected = "file"
	}
	if statefs.WriteNew(path, []byte(selected+"\n")) != nil || statefs.SyncDir(filepath.Dir(path)) != nil {
		return "", ErrState
	}
	return selected, nil
}

func (h *Host) managedVault(ctx context.Context, config ManagedServeConfig) (credentials.Vault, error) {
	selected, err := h.managedCredentialStore(ctx, config.CredentialStore)
	if err != nil {
		return nil, err
	}
	if selected == "file" {
		vault, err := credentials.NewFile(h.identity.InstallationID, filepath.Join(h.root.Path, "credentials"))
		if err != nil {
			return nil, ErrState
		}
		return vault, nil
	}
	var vault credentials.Vault
	if config.PythonExecutable == "" {
		vault, err = credentials.NewNativeKeychain(h.identity.InstallationID)
	} else {
		vault, err = credentials.NewKeychain(h.identity.InstallationID, config.PythonExecutable)
	}
	if errors.Is(err, credentials.ErrUnsupported) {
		return nil, nil
	}
	if err != nil {
		return nil, ErrState
	}
	return vault, nil
}
