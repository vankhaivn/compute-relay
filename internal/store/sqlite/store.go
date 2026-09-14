// Package sqlite is the durable local metadata foundation. It contains no provider
// dispatch, job admission, background scheduler, or public administration endpoint.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vankhaivn/compute-relay/internal/statefs"
)

var (
	ErrUnavailable = errors.New("state store unavailable")
	ErrClosed      = errors.New("state store is closed")
	ErrSchema      = errors.New("unsupported or inconsistent state schema")
	ErrCorrupt     = errors.New("state integrity check failed")
	ErrConflict    = errors.New("state record conflicts with an existing record")
	ErrInvalid     = errors.New("invalid state record or options")
	ErrBusy        = errors.New("state store is busy")
	ErrDiskFull    = errors.New("state storage is full")
	ErrBackup      = errors.New("backup is incomplete or invalid")
)

const (
	databaseName  = "runtime.db"
	formatName    = ".compute-relay-state"
	formatContent = "compute-relay/state-v1\n"
	applicationID = 1129466969 // ASCII CRLY, application identity rather than a schema version.
)

type Options struct {
	OperationTimeout time.Duration
	BackupTimeout    time.Duration
	MaxBackupBytes   int64
}

func DefaultOptions() Options {
	return Options{OperationTimeout: 5 * time.Second, BackupTimeout: time.Minute, MaxBackupBytes: 256 << 20}
}
func (o Options) valid() bool {
	return o.OperationTimeout > 0 && o.OperationTimeout <= time.Minute && o.BackupTimeout >= o.OperationTimeout && o.BackupTimeout <= time.Hour && o.MaxBackupBytes > 0 && o.MaxBackupBytes <= 1<<40
}

// Store has one connection. All operations are bounded; a busy connection queues at
// database/sql, not a spin/retry loop. Close drains operations before releasing the OS lock.
type Store struct {
	db      *sql.DB
	root    *statefs.Root
	options Options
	life    sync.RWMutex
	closed  bool
}

func Open(ctx context.Context, path string, options Options) (*Store, error) {
	if !options.valid() {
		return nil, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root, err := statefs.Open(path)
	if err != nil {
		return nil, err
	}
	s := &Store{root: root, options: options}
	success := false
	defer func() {
		if !success {
			if s.db != nil {
				_ = s.db.Close()
			}
			_ = root.Close()
		}
	}()
	if _, err := os.Lstat(filepath.Join(root.Path, databaseName+".restore")); !errors.Is(err, os.ErrNotExist) {
		return nil, ErrBackup
	}
	if err := initializeRoot(root.Path); err != nil {
		return nil, err
	}
	path = filepath.Join(root.Path, databaseName)
	if err := preflightFiles(root.Path); err != nil {
		return nil, err
	}
	if err := checkInitializedFile(root.Path, path); err != nil {
		return nil, err
	}
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		if err := statefs.WriteNew(path, nil); err != nil {
			return nil, err
		}
	}
	s.db, err = connect(path, false)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, options.OperationTimeout)
	defer cancel()
	// Inspect identity/version BEFORE any persistent journal or migration change.
	if _, err := inspectSchema(ctx, s.db, migrations, true); err != nil {
		return nil, err
	}
	var journal string
	if err := s.db.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&journal); err != nil {
		return nil, dbError(err)
	}
	if strings.ToLower(journal) != "wal" {
		return nil, ErrUnavailable
	}
	if err := migrate(ctx, s.db, migrations); err != nil {
		return nil, err
	}
	if err := recordInstallation(ctx, s.db, root.Path); err != nil {
		return nil, err
	}
	if err := integrity(ctx, s.db); err != nil {
		return nil, err
	}
	if err := preflightFiles(root.Path); err != nil {
		return nil, err
	}
	if err := statefs.SyncDir(root.Path); err != nil {
		return nil, ErrUnavailable
	}
	success = true
	return s, nil
}

// A dedicated marked root prevents accidental use/migration of an unrelated database.
// Marker creation uses atomic rename; interrupted marker staging is the only file removed.
func initializeRoot(path string) error {
	marker := filepath.Join(path, formatName)
	if err := statefs.CheckFile(marker); err == nil {
		info, err := os.Stat(marker)
		if err != nil || info.Size() != int64(len(formatContent)) {
			return ErrSchema
		}
		data, err := os.ReadFile(marker)
		if err != nil || string(data) != formatContent {
			return ErrSchema
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return ErrUnavailable
	}
	for _, e := range entries {
		if e.Name() == "runtime.lock" {
			continue
		}
		if e.Name() == formatName+".pending" {
			if err := statefs.CheckFile(filepath.Join(path, e.Name())); err != nil {
				return err
			}
			if err := os.Remove(filepath.Join(path, e.Name())); err != nil {
				return ErrUnavailable
			}
			continue
		}
		return ErrSchema
	}
	temporary := marker + ".pending"
	if err := statefs.WriteNew(temporary, []byte(formatContent)); err != nil {
		return err
	}
	if err := os.Rename(temporary, marker); err != nil {
		return ErrUnavailable
	}
	return statefs.SyncDir(path)
}

func preflightFiles(path string) error {
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		if err := statefs.CheckFile(filepath.Join(path, databaseName+suffix)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func connect(path string, readonly bool) (*sql.DB, error) {
	// Never treat operator paths as raw DSNs. Escaping preserves #, ?, spaces and Windows
	// drive letters; arbitrary URI parameters cannot enable shared cache or change mode.
	p := filepath.ToSlash(path)
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	u := url.URL{Scheme: "file", Path: p}
	q := url.Values{}
	if readonly {
		q.Set("mode", "ro")
		// Only validated, copied standalone backup snapshots use immutable, never a live DB.
		q.Set("immutable", "1")
	} else {
		q.Set("mode", "rw")
	}
	q.Set("cache", "private")
	q.Set("_txlock", "immediate")
	for _, pragma := range []string{"busy_timeout(1000)", "foreign_keys(ON)", "synchronous(FULL)", "trusted_schema(OFF)", "wal_autocheckpoint(1000)"} {
		q.Add("_pragma", pragma)
	}
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, dbError(err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db, nil
}

func (s *Store) Close() error {
	s.life.Lock()
	defer s.life.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	err := s.db.Close()
	lockErr := s.root.Close()
	return errors.Join(dbError(err), lockErr)
}

// operation pins the store lifetime and always applies a finite timeout, including
// connection-pool wait. No exported transaction callback can hold a lock over a transfer.
func (s *Store) operation(ctx context.Context, timeout time.Duration) (context.Context, func(), error) {
	s.life.RLock()
	if s.closed {
		s.life.RUnlock()
		return nil, nil, ErrClosed
	}
	bounded, cancel := context.WithTimeout(ctx, timeout)
	return bounded, func() { cancel(); s.life.RUnlock() }, nil
}

type Info struct {
	InstallationID string `json:"installation_id"`
	SchemaVersion  int    `json:"schema_version"`
	SQLiteVersion  string `json:"sqlite_version"`
	JournalMode    string `json:"journal_mode"`
	Synchronous    int    `json:"synchronous"`
	ForeignKeys    bool   `json:"foreign_keys"`
}

func (s *Store) Info(ctx context.Context) (Info, error) {
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return Info{}, err
	}
	defer done()
	var info Info
	var fk int
	for _, query := range []struct {
		sql  string
		dest any
	}{
		{"SELECT installation_id FROM runtime_installation WHERE singleton=1", &info.InstallationID},
		{"PRAGMA user_version", &info.SchemaVersion}, {"SELECT sqlite_version()", &info.SQLiteVersion},
		{"PRAGMA journal_mode", &info.JournalMode}, {"PRAGMA synchronous", &info.Synchronous}, {"PRAGMA foreign_keys", &fk},
	} {
		if err := s.db.QueryRowContext(ctx, query.sql).Scan(query.dest); err != nil {
			return Info{}, dbError(err)
		}
	}
	info.ForeignKeys = fk == 1
	return info, nil
}

func (s *Store) Ready(ctx context.Context) error {
	info, err := s.Info(ctx)
	if err != nil {
		return err
	}
	if info.InstallationID == "" || info.SchemaVersion != len(migrations) || info.JournalMode != "wal" || info.Synchronous != 2 || !info.ForeignKeys {
		return ErrUnavailable
	}
	return nil
}

func withTx(ctx context.Context, db *sql.DB, fn func(*sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return dbError(err)
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	// Never replay after Commit returns an error; its outcome may be uncertain.
	return dbError(tx.Commit())
}

func dbError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	var coded interface{ Code() int }
	if errors.As(err, &coded) {
		switch coded.Code() & 255 {
		case 5, 6:
			return ErrBusy
		case 13:
			return ErrDiskFull
		case 19:
			return ErrConflict
		case 11, 26:
			return ErrCorrupt
		}
	}
	return ErrUnavailable // Never export driver SQL, token digests, paths or row contents.
}

func pragmaVersion(version int) string { return "PRAGMA user_version=" + strconv.Itoa(version) }
