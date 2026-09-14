// Command storesmoke is a finite local metadata check with a private temporary SQLite
// database. It never runs workloads or providers and is not a production serve command.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/objects"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "state smoke failed:", err)
		os.Exit(1)
	}
}
func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	root, err := os.MkdirTemp("", "compute-relay-state-smoke-")
	if err != nil {
		return errors.New("create smoke directory")
	}
	defer os.RemoveAll(root)
	path := filepath.Join(root, "state")
	options := sqlite.DefaultOptions()
	s, err := sqlite.Open(ctx, path, options)
	if err != nil {
		return err
	}
	defer s.Close()
	if err := s.PutWorkspace(ctx, auth.Workspace{ID: "smoke", Enabled: true}); err != nil {
		return err
	}
	access, _ := auth.New(s, s, nil)
	secret, record, err := access.Issue(ctx, "smoke", []auth.Scope{auth.Read, auth.Write}, time.Time{})
	if err != nil {
		return err
	}
	sum := sha256.Sum256([]byte("synthetic metadata fixture"))
	meta := domain.ObjectMetadata{ID: "obj_smoke", WorkspaceID: "smoke", Bytes: 26, SHA256: domain.SHA256Digest(hex.EncodeToString(sum[:]))}
	// This is a metadata-only fixture, not a verified blob upload or an admitted job.
	if err := s.CommitObject(ctx, meta); err != nil {
		return err
	}
	before, err := s.Info(ctx)
	if err != nil {
		return err
	}
	if err := s.Close(); err != nil {
		return err
	}
	s, err = sqlite.Open(ctx, path, options)
	if err != nil {
		return err
	}
	defer s.Close()
	access, _ = auth.New(s, s, nil)
	if _, err := access.Authenticate(ctx, secret.Reveal()); err != nil {
		return err
	}
	if got, err := s.GetObject(ctx, "smoke", meta.ID); err != nil || got != meta {
		return errors.New("metadata lost on restart")
	}
	if _, err := s.GetObject(ctx, "other", meta.ID); !errors.Is(err, objects.ErrNotFound) {
		return errors.New("workspace isolation failed")
	}
	if err := access.Revoke(ctx, record.ID); err != nil {
		return err
	}
	backup := filepath.Join(root, "backup")
	receipt, err := s.Backup(ctx, backup)
	if err != nil {
		return err
	}
	restoredPath := filepath.Join(root, "restored")
	if err := sqlite.Restore(ctx, backup, restoredPath, options); err != nil {
		return err
	}
	restored, err := sqlite.Open(ctx, restoredPath, options)
	if err != nil {
		return err
	}
	defer restored.Close()
	after, err := restored.Info(ctx)
	if err != nil {
		return err
	}
	if before.InstallationID != after.InstallationID {
		return errors.New("restored installation identity changed")
	}
	access, _ = auth.New(restored, restored, nil)
	if _, err := access.Authenticate(ctx, secret.Reveal()); !errors.Is(err, auth.ErrUnauthenticated) {
		return errors.New("restored revocation failed")
	}
	if err := restored.Ready(ctx); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"status": "passed-offline", "sqlite_version": after.SQLiteVersion, "schema_version": after.SchemaVersion, "restart": true, "workspace_isolation": true, "revocation": true, "consistent_backup_restore": true, "backup_scope": receipt.Scope, "blob_bytes_checked": false, "provider_calls": 0})
}
