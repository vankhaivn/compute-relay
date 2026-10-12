package runtimehost

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/credentials"
)

func TestManagedCredentialBackendSelectionSurvivesRestart(t *testing.T) {
	for _, tc := range []struct {
		name, requested, want string
		legacy                bool
	}{
		{"new-default", "", "file", false},
		{"explicit-file", "file", "file", false},
		{"optional-keychain", "keychain", "keychain", false},
		{"legacy-keychain", "", "keychain", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			path := initialized(t)
			h, err := Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			if tc.legacy {
				if err := h.store.MarkFingerprintInitialized(ctx); err != nil {
					t.Fatal(err)
				}
				if _, err := h.managedCredentialStore(ctx, "file"); err != ErrRequest {
					t.Fatal("legacy secrets silently switched to file", err)
				}
			}
			selected, err := h.managedCredentialStore(ctx, tc.requested)
			if err != nil || selected != tc.want {
				t.Fatal("wrong initial backend", selected, err)
			}
			if err := h.Close(); err != nil {
				t.Fatal(err)
			}
			h, err = Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			defer h.Close()
			selected, err = h.managedCredentialStore(ctx, "")
			if err != nil || selected != tc.want {
				t.Fatal("restart changed backend", selected, err)
			}
			other := "file"
			if tc.want == "file" {
				other = "keychain"
			}
			if _, err := h.managedCredentialStore(ctx, other); err != ErrRequest {
				t.Fatal("existing backend was replaced", err)
			}
		})
	}
}

func TestManagedCredentialBackendRejectsMissingOrCorruptSelection(t *testing.T) {
	for _, marker := range []string{"missing", "file\nkeychain\n", "unknown\n"} {
		t.Run(marker, func(t *testing.T) {
			f := newManagedTestFixture(t, true)
			if _, err := f.host.managedVault(context.Background(), f.config); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(f.host.root.Path, "state", "credential-store")
			var err error
			if marker == "missing" {
				err = os.Remove(path)
			} else {
				err = os.WriteFile(path, []byte(marker), 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.host.managedVault(context.Background(), f.config); err != ErrState {
				t.Fatal("incomplete state silently selected a backend", err)
			}
		})
	}
}

func TestManagedFileCredentialsReplayAfterReopenWithoutSecretInSQLite(t *testing.T) {
	f := newManagedTestFixture(t, true)
	ctx := context.Background()
	vault, err := f.host.managedVault(ctx, f.config)
	if err != nil {
		t.Fatal(err)
	}
	f.services, err = f.host.composeManaged(ctx, f.config, vault, f.adapter)
	if err != nil {
		t.Fatal(err)
	}
	canary := "SYNTHETIC_FIRST"
	connection := f.connection(t, "file-connection", canary)
	path := f.host.root.Path
	if err := f.host.Close(); err != nil {
		t.Fatal(err)
	}
	f.host, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.host.Close() })
	vault, err = f.host.managedVault(ctx, f.config)
	if err != nil {
		t.Fatal(err)
	}
	f.services, err = f.host.composeManaged(ctx, f.config, vault, f.adapter)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"provider_type":"kaggle","label":"Fixture","credentials":{"api_token":"` + canary + `"}}`)
	replay, err := f.services.connections.Submit(ctx, f.actor, "app", "", "file-connection", body)
	if err != nil || !replay.Replay || replay.ConnectionID != connection.ID || f.adapter.checks != 1 {
		t.Fatal("restart lost secret-bearing request identity", err)
	}
	check, err := f.services.connections.Submit(ctx, f.actor, "app", connection.ID, "check-after-restart",
		[]byte(fmt.Sprintf(`{"action":"check","expected_revision":%d}`, connection.Revision)))
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := f.services.connections.RunOnce(ctx); err != nil || !worked || f.adapter.checks != 2 {
		t.Fatal("restarted worker could not read the stored provider credential", err)
	}
	if result, err := f.services.connections.Operation(ctx, f.actor, "app", check.ID); err != nil || result.Status != "succeeded" {
		t.Fatal("restarted account verification did not complete", err)
	}
	for _, name := range []string{"runtime.db", "runtime.db-wal"} {
		raw, err := os.ReadFile(filepath.Join(path, "state", name))
		if err != nil && !errors.Is(err, os.ErrNotExist) || bytes.Contains(raw, []byte(canary)) {
			t.Fatal("provider secret escaped into SQLite")
		}
	}
	// A missing file must not mint a new fingerprint key or recreate credentials.
	if err := os.Remove(filepath.Join(path, "credentials", "secrets.env")); err != nil {
		t.Fatal(err)
	}
	if _, err := f.services.connections.Submit(ctx, f.actor, "app", "", "file-connection", body); err == nil {
		t.Fatal("missing vault was silently initialized")
	}
	if _, err := os.Stat(filepath.Join(path, "credentials", "secrets.env")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("missing vault was recreated")
	}
}

func TestManagedFileVaultFailureDoesNotFallBackToKeychain(t *testing.T) {
	f := newManagedTestFixture(t, true)
	vault, err := f.host.managedVault(context.Background(), f.config)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.host.root.Path, "credentials", "secrets.env")
	if err := os.WriteFile(path, []byte("SYNTHETIC_CORRUPTION"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := vault.WithSecret(context.Background(), "request_fingerprint_v1", func([]byte) error { t.Fatal("corruption invoked consumer"); return nil }); err != credentials.ErrUnavailable {
		t.Fatal("file failure was not surfaced", err)
	}
	selected, err := f.host.managedCredentialStore(context.Background(), "")
	if err != nil || selected != "file" {
		t.Fatal("failed file backend changed selection", err)
	}
}
