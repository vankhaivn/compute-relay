package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/statefs"
)

func TestConsistentLiveWALBackupAndOfflineRestore(t *testing.T) {
	s, _ := newStore(t)
	workspace(t, s, "a")
	access, _ := auth.New(s, s, nil)
	secret, record, err := access.Issue(testctx, "a", []auth.Scope{auth.Read}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CommitObject(testctx, object("input")); err != nil {
		t.Fatal(err)
	}
	if err := access.Revoke(testctx, record.ID); err != nil {
		t.Fatal(err)
	}
	info, _ := s.Info(testctx)
	backup := filepath.Join(t.TempDir(), "backup # 'quoted'")
	receipt, err := s.Backup(testctx, backup)
	if err != nil {
		t.Fatal(err)
	}
	if !receipt.valid(s.options.MaxBackupBytes) || receipt.InstallationID != info.InstallationID {
		t.Fatal("bad receipt", receipt)
	}
	workspace(t, s, "after-backup")
	if _, err := s.Backup(testctx, backup); err == nil {
		t.Fatal("existing backup overwritten")
	}
	target := filepath.Join(t.TempDir(), "restored")
	if err := Restore(testctx, backup, target, DefaultOptions()); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(testctx, target, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	actual, _ := restored.Info(testctx)
	if actual.InstallationID != info.InstallationID {
		t.Fatal("restore regenerated identity")
	}
	if m, err := restored.GetObject(testctx, "a", "input"); err != nil || m != object("input") {
		t.Fatal("snapshot lost committed WAL data", err)
	}
	if _, err := restored.LookupWorkspace(testctx, "after-backup"); !errors.Is(err, auth.ErrNotFound) {
		t.Fatal("snapshot changed with source", err)
	}
	access, _ = auth.New(restored, restored, nil)
	if _, err := access.Authenticate(testctx, secret.Reveal()); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("revocation not backed up", err)
	}
	if err := Restore(testctx, backup, target, DefaultOptions()); err == nil {
		t.Fatal("active state overwritten")
	}
	if err := restored.Ready(testctx); err != nil {
		t.Fatal("restore refusal damaged live state", err)
	}
}

func TestRestoreRejectsIncompleteCorruptOrOversizedBackups(t *testing.T) {
	for _, kind := range []string{"missing-receipt", "bad-digest", "tampered-file", "trailing-json", "future-version", "missing-file", "oversized", "wal-sidecar"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := newStore(t)
			backup := filepath.Join(t.TempDir(), "backup")
			receipt, err := s.Backup(testctx, backup)
			if err != nil {
				t.Fatal(err)
			}
			options := DefaultOptions()
			switch kind {
			case "missing-receipt":
				os.Remove(filepath.Join(backup, receiptName))
			case "bad-digest":
				receipt.SHA256 = strings.Repeat("0", 64)
				rewriteReceipt(t, backup, receipt)
			case "future-version":
				receipt.SchemaVersion = 999
				rewriteReceipt(t, backup, receipt)
			case "tampered-file":
				f, err := os.OpenFile(filepath.Join(backup, snapshotName), os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				_, err = f.WriteAt([]byte("corruption"), 128)
				f.Close()
				if err != nil {
					t.Fatal(err)
				}
			case "trailing-json":
				f, err := os.OpenFile(filepath.Join(backup, receiptName), os.O_APPEND|os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				f.WriteString("{}")
				f.Close()
			case "missing-file":
				os.Remove(filepath.Join(backup, snapshotName))
			case "oversized":
				options.MaxBackupBytes = 1
			case "wal-sidecar":
				if err := statefs.WriteNew(filepath.Join(backup, snapshotName+"-wal"), nil); err != nil {
					t.Fatal(err)
				}
			}
			target := filepath.Join(t.TempDir(), "restore")
			if err := Restore(testctx, backup, target, options); err == nil {
				t.Fatal("invalid backup restored")
			}
			if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("failed restore left usable-looking state", err)
			}
		})
	}
}
func rewriteReceipt(t *testing.T, path string, r BackupReceipt) {
	t.Helper()
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, receiptName), b, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestBackupLimitsCancellationAndIndependentWriters(t *testing.T) {
	s, _ := newStore(t)
	workspace(t, s, "a")
	s.options.MaxBackupBytes = 1
	dest := filepath.Join(t.TempDir(), "too-small")
	if _, err := s.Backup(testctx, dest); !errors.Is(err, ErrBackup) {
		t.Fatal(err)
	}
	if _, err := os.Stat(dest); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("failed backup not removed")
	}
	s.options = DefaultOptions()
	ctx, cancel := context.WithCancel(testctx)
	cancel()
	if _, err := s.Backup(ctx, filepath.Join(t.TempDir(), "cancelled")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.PutWorkspace(testctx, auth.Workspace{ID: "concurrent", Enabled: true}); err != nil {
				t.Error(err)
			}
		}()
	}
	receipt, err := s.Backup(testctx, filepath.Join(t.TempDir(), "concurrent"))
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if !receipt.valid(s.options.MaxBackupBytes) {
		t.Fatal("invalid concurrent backup")
	}
	if err := s.Ready(testctx); err != nil {
		t.Fatal(err)
	}
}

func TestRestoreNeverOverwritesAnExistingEmptyDirectory(t *testing.T) {
	s, _ := newStore(t)
	backup := filepath.Join(t.TempDir(), "backup")
	if _, err := s.Backup(testctx, backup); err != nil {
		t.Fatal(err)
	}
	target := t.TempDir()
	if err := Restore(testctx, backup, target, DefaultOptions()); err == nil {
		t.Fatal("existing target claimed")
	}
	entries, err := os.ReadDir(target)
	if err != nil || len(entries) != 0 {
		t.Fatal("existing target changed", err)
	}
}
