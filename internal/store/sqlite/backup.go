package sqlite

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/vankhaivn/compute-relay/internal/statefs"
)

const snapshotName = "metadata.sqlite"
const receiptName = "backup.json"

// BackupReceipt describes a DATABASE-ONLY snapshot. Matching blob bytes must be
// preserved separately. This digest detects corruption, not malicious tampering.
type BackupReceipt struct {
	Format         int    `json:"format"`
	Scope          string `json:"scope"`
	InstallationID string `json:"installation_id"`
	SchemaVersion  int    `json:"schema_version"`
	CreatedAt      string `json:"created_at"`
	Bytes          int64  `json:"bytes"`
	SHA256         string `json:"sha256"`
}

func (r BackupReceipt) valid(maxBytes int64) bool {
	if r.Format != 1 || r.Scope != "database-only" || !validID(r.InstallationID) || r.SchemaVersion < 1 || r.SchemaVersion > len(migrations) || !validTime(r.CreatedAt) || r.Bytes <= 0 || r.Bytes > maxBytes || len(r.SHA256) != 64 {
		return false
	}
	b, err := hex.DecodeString(r.SHA256)
	return err == nil && len(b) == 32 && strings.ToLower(r.SHA256) == r.SHA256
}

// Backup takes a SQLite-consistent snapshot of a live WAL database. It uses SQLite's
// VACUUM INTO rather than copying runtime.db and losing committed WAL content.
// destination must not exist. A receipt is published last; interrupted bundles without
// one are rejected by Restore. Never send backups to ordinary diagnostics/artifact APIs.
func (s *Store) Backup(ctx context.Context, destination string) (receipt BackupReceipt, err error) {
	ctx, done, err := s.operation(ctx, s.options.BackupTimeout)
	if err != nil {
		return receipt, err
	}
	defer done()
	if err := ctx.Err(); err != nil {
		return receipt, err
	}
	destination, err = statefs.PrivateDir(destination, true)
	if err != nil {
		return receipt, err
	}
	complete := false
	defer func() {
		if !complete {
			_ = os.RemoveAll(destination)
		}
	}()
	path := filepath.Join(destination, snapshotName)
	// SQLite accepts an existing empty output. Precreate privately instead of allowing
	// the engine to choose broader default permissions during backup creation.
	if err = statefs.WriteNew(path, nil); err != nil {
		return receipt, err
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return receipt, dbError(err)
	}
	defer conn.Close()
	var pages, pageSize int64
	if err = conn.QueryRowContext(ctx, "PRAGMA page_count").Scan(&pages); err != nil {
		return receipt, dbError(err)
	}
	if err = conn.QueryRowContext(ctx, "PRAGMA page_size").Scan(&pageSize); err != nil {
		return receipt, dbError(err)
	}
	if pageSize <= 0 || pages > s.options.MaxBackupBytes/pageSize {
		return receipt, ErrBackup
	}
	if _, err = conn.ExecContext(ctx, "VACUUM INTO ?", path); err != nil {
		return receipt, dbError(err)
	}
	// Release the source connection before filesystem hashing/validation.
	if err := conn.Close(); err != nil {
		return receipt, dbError(err)
	}
	// Convert a private snapshot to standalone DELETE mode before hashing it, so the
	// receipt never depends on a WAL/SHM sidecar. This does not alter the live source.
	snapshot, err := connect(path, false)
	if err != nil {
		return receipt, err
	}
	var mode string
	err = snapshot.QueryRowContext(ctx, "PRAGMA journal_mode=DELETE").Scan(&mode)
	if closeErr := snapshot.Close(); err == nil {
		err = closeErr
	}
	if err != nil || mode != "delete" {
		return receipt, ErrBackup
	}
	if err = statefs.SyncFile(path); err != nil {
		return receipt, err
	}
	receipt, err = inspectSnapshot(ctx, path, s.options.MaxBackupBytes)
	if err != nil {
		return receipt, err
	}
	receipt.CreatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	encoded, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		return BackupReceipt{}, ErrBackup
	}
	if err = statefs.WriteNew(filepath.Join(destination, receiptName+".pending"), append(encoded, '\n')); err != nil {
		return BackupReceipt{}, err
	}
	if err = statefs.SyncDir(destination); err != nil {
		return BackupReceipt{}, ErrUnavailable
	}
	if err = os.Rename(filepath.Join(destination, receiptName+".pending"), filepath.Join(destination, receiptName)); err != nil {
		return BackupReceipt{}, ErrUnavailable
	}
	complete = true // A late flush error is ambiguous; preserve the complete snapshot.
	if err = statefs.SyncDir(destination); err != nil {
		return BackupReceipt{}, ErrUnavailable
	}
	if err = statefs.SyncDir(filepath.Dir(destination)); err != nil {
		return BackupReceipt{}, ErrUnavailable
	}
	return receipt, nil
}

// Restore is OFFLINE and refuses every existing destination, including a running state
// directory. It preserves the installation identity. Never start the original and its
// restored copy concurrently against the same provider resources. Restoring old token
// records may undo revocations; operator review/rotation is required before exposure.
func Restore(ctx context.Context, source, destination string, options Options) error {
	if !options.valid() {
		return ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, options.BackupTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := statefs.CheckDir(source); err != nil {
		return ErrBackup
	}
	if err := statefs.CheckFile(filepath.Join(source, receiptName)); err != nil {
		return ErrBackup
	}
	info, err := os.Stat(filepath.Join(source, receiptName))
	if err != nil || info.Size() > 8192 {
		return ErrBackup
	}
	raw, err := os.ReadFile(filepath.Join(source, receiptName))
	if err != nil {
		return ErrBackup
	}
	var expected BackupReceipt
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&expected) != nil || decoder.Decode(new(any)) != io.EOF || !expected.valid(options.MaxBackupBytes) {
		return ErrBackup
	}
	input := filepath.Join(source, snapshotName)
	if err := standaloneFile(input, options.MaxBackupBytes); err != nil {
		return err
	}
	destination, err = statefs.PrivateDir(destination, true)
	if err != nil {
		return err
	}
	keep := false
	defer func() {
		if !keep {
			_ = os.RemoveAll(destination)
		}
	}()
	root, err := statefs.Open(destination)
	if err != nil {
		return err
	}
	defer root.Close()
	target := filepath.Join(root.Path, databaseName+".restore")
	if err := copySnapshot(ctx, input, target, expected); err != nil {
		return err
	}
	actual, err := inspectSnapshot(ctx, target, options.MaxBackupBytes)
	if err != nil {
		return err
	}
	if actual.InstallationID != expected.InstallationID || actual.SchemaVersion != expected.SchemaVersion || actual.Bytes != expected.Bytes || actual.SHA256 != expected.SHA256 {
		return ErrBackup
	}
	if err := writeInstallation(root.Path, actual.InstallationID); err != nil {
		return err
	}
	if err := initializeRootForRestore(root.Path); err != nil {
		return err
	}
	if err := os.Rename(target, filepath.Join(root.Path, databaseName)); err != nil {
		return ErrUnavailable
	}
	keep = true // Never remove an already published restore after a late sync error.
	if err := statefs.SyncDir(root.Path); err != nil {
		return ErrUnavailable
	}
	return statefs.SyncDir(filepath.Dir(root.Path))
}

func initializeRootForRestore(path string) error {
	// Destination was exclusively created by Restore and is locked, never an existing DB.
	return statefs.WriteNew(filepath.Join(path, formatName), []byte(formatContent))
}

func standaloneFile(path string, maxBytes int64) error {
	if err := statefs.CheckFile(path); err != nil {
		return ErrBackup
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() <= 0 || info.Size() > maxBytes {
		return ErrBackup
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(path + suffix); !errors.Is(err, os.ErrNotExist) {
			return ErrBackup
		}
	}
	return nil
}
func inspectSnapshot(ctx context.Context, path string, maxBytes int64) (BackupReceipt, error) {
	var r BackupReceipt
	if err := standaloneFile(path, maxBytes); err != nil {
		return r, err
	}
	db, err := connect(path, true)
	if err != nil {
		return r, err
	}
	defer db.Close()
	version, err := inspectSchema(ctx, db, migrations, false)
	if err != nil {
		return r, err
	}
	if err := integrity(ctx, db); err != nil {
		return r, err
	}
	if err := db.QueryRowContext(ctx, "SELECT installation_id FROM runtime_installation WHERE singleton=1").Scan(&r.InstallationID); err != nil {
		return r, ErrBackup
	}
	f, err := os.Open(path)
	if err != nil {
		return r, ErrBackup
	}
	defer f.Close()
	hash := sha256.New()
	n, err := io.CopyBuffer(hash, &limitedContextReader{ctx: ctx, reader: io.LimitReader(f, maxBytes+1)}, make([]byte, 64*1024))
	if err != nil {
		return r, err
	}
	if n > maxBytes {
		return r, ErrBackup
	}
	r.Format = 1
	r.Scope = "database-only"
	r.SchemaVersion = version
	r.Bytes = n
	r.SHA256 = hex.EncodeToString(hash.Sum(nil))
	return r, nil
}
func copySnapshot(ctx context.Context, source, destination string, expected BackupReceipt) error {
	in, err := os.Open(source)
	if err != nil {
		return ErrBackup
	}
	defer in.Close()
	out, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return ErrUnavailable
	}
	defer out.Close()
	hash := sha256.New()
	n, err := io.CopyBuffer(io.MultiWriter(out, hash), &limitedContextReader{ctx: ctx, reader: io.LimitReader(in, expected.Bytes+1)}, make([]byte, 64*1024))
	if err != nil {
		return ErrBackup
	}
	if n != expected.Bytes || hex.EncodeToString(hash.Sum(nil)) != expected.SHA256 {
		return ErrBackup
	}
	if err := out.Sync(); err != nil {
		return ErrUnavailable
	}
	return dbError(out.Close())
}

type limitedContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *limitedContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}
