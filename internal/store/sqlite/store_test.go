package sqlite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/objects"
	"github.com/vankhaivn/compute-relay/internal/statefs"
)

var testctx = context.Background()

func newStore(t *testing.T) (*Store, string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "state # 'quoted'")
	s, err := Open(testctx, root, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, root
}
func workspace(t *testing.T, s *Store, id string) {
	t.Helper()
	if err := s.PutWorkspace(testctx, auth.Workspace{ID: domain.WorkspaceID(id), Enabled: true, AllowedProfiles: []string{"default-gpu"}}); err != nil {
		t.Fatal(err)
	}
}
func object(id string) domain.ObjectMetadata {
	return domain.ObjectMetadata{ID: domain.ObjectID(id), WorkspaceID: "a", Bytes: 7, SHA256: domain.SHA256Digest(strings.Repeat("a", 64))}
}

func TestIdentityPragmasAndDurableRepositories(t *testing.T) {
	s, root := newStore(t)
	workspace(t, s, "a")
	workspace(t, s, "b")
	info, err := s.Info(testctx)
	if err != nil || !validID(info.InstallationID) || info.SchemaVersion != len(migrations) || info.JournalMode != "wal" || info.Synchronous != 2 || !info.ForeignKeys {
		t.Fatalf("bad info: %+v %v", info, err)
	}
	if err := s.Ready(testctx); err != nil {
		t.Fatal(err)
	}
	access, _ := auth.New(s, s, nil)
	secret, record, err := access.Issue(testctx, "a", []auth.Scope{auth.Read, auth.Write}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CommitObject(testctx, object("input")); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitObject(testctx, object("input")); !errors.Is(err, ErrConflict) {
		t.Fatal("immutable object overwritten", err)
	}
	if _, err := s.GetObject(testctx, "b", "input"); !errors.Is(err, objects.ErrNotFound) {
		t.Fatal("wrong workspace visible", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(testctx, root, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	after, _ := s.Info(testctx)
	if after.InstallationID != info.InstallationID {
		t.Fatal("restart changed installation")
	}
	access, _ = auth.New(s, s, nil)
	if _, err := access.Authenticate(testctx, secret.Reveal()); err != nil {
		t.Fatal("persisted token rejected", err)
	}
	if m, err := s.GetObject(testctx, "a", "input"); err != nil || m != object("input") {
		t.Fatal("metadata lost", err)
	}
	if err := access.Revoke(testctx, record.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(testctx, root, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	access, _ = auth.New(s, s, nil)
	if _, err := access.Authenticate(testctx, secret.Reveal()); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("revocation lost", err)
	}
	for _, suffix := range []string{"", "-wal"} {
		data, _ := os.ReadFile(filepath.Join(root, databaseName+suffix))
		if bytes.Contains(data, []byte(secret.Reveal())) {
			t.Fatal("plaintext token persisted")
		}
	}
}

func TestConstraintsAndIndependentSnapshots(t *testing.T) {
	s, _ := newStore(t)
	workspace(t, s, "a")
	w, _ := s.LookupWorkspace(testctx, "a")
	w.AllowedProfiles[0] = "mutated"
	again, _ := s.LookupWorkspace(testctx, "a")
	if again.AllowedProfiles[0] != "default-gpu" {
		t.Fatal("aliased profiles")
	}
	r := auth.TokenRecord{ID: "tok_test", WorkspaceID: "a", Digest: sha256.Sum256([]byte("synthetic digest")), Scopes: []auth.Scope{auth.Read}, CreatedAt: time.Now().UTC()}
	if err := s.CreateToken(testctx, r); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateToken(testctx, r); !errors.Is(err, ErrConflict) {
		t.Fatal("duplicate token", err)
	}
	r.ID = "tok_other"
	if err := s.CreateToken(testctx, r); !errors.Is(err, ErrConflict) {
		t.Fatal("duplicate digest", err)
	}
	r.WorkspaceID = "missing"
	r.Digest = sha256.Sum256([]byte("new"))
	if err := s.CreateToken(testctx, r); !errors.Is(err, ErrConflict) {
		t.Fatal("foreign key disabled", err)
	}
	m := object("unknown-owner")
	m.WorkspaceID = "missing"
	if err := s.CommitObject(testctx, m); !errors.Is(err, ErrConflict) {
		t.Fatal("orphan ownership accepted", err)
	}
	m.Bytes = -1
	if err := s.CommitObject(testctx, m); !errors.Is(err, objects.ErrInvalid) {
		t.Fatal("invalid metadata accepted")
	}
	for _, w := range []auth.Workspace{{ID: "bad/id"}, {ID: "a", AllowedProfiles: []string{"../escape"}}, {ID: "a", AllowedProfiles: []string{"x", "x"}}} {
		if err := s.PutWorkspace(testctx, w); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid workspace accepted", err)
		}
	}
	r.WorkspaceID = "a"
	r.Scopes = []auth.Scope{"admin"}
	if err := s.CreateToken(testctx, r); !errors.Is(err, ErrInvalid) {
		t.Fatal("unknown scope accepted")
	}
	if _, err := s.LookupToken(testctx, [32]byte{}); !errors.Is(err, auth.ErrNotFound) {
		t.Fatal("token miss sentinel", err)
	}
	if err := s.RevokeToken(testctx, "absent"); !errors.Is(err, auth.ErrNotFound) {
		t.Fatal("revoke miss sentinel", err)
	}
}

func TestTransactionsRollbackAndPoolWaitIsBounded(t *testing.T) {
	s, _ := newStore(t)
	injected := errors.New("fault after first write")
	err := withTx(testctx, s.db, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(testctx, "INSERT INTO workspaces VALUES('rolled-back',1,'[]')"); err != nil {
			return err
		}
		return injected
	})
	if !errors.Is(err, injected) {
		t.Fatal(err)
	}
	if _, err := s.LookupWorkspace(testctx, "rolled-back"); !errors.Is(err, auth.ErrNotFound) {
		t.Fatal("partial transaction survived", err)
	}
	conn, err := s.db.Conn(testctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(testctx, 20*time.Millisecond)
	defer cancel()
	if _, err := s.LookupWorkspace(ctx, "a"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("pool wait not cancelled", err)
	}
}

func TestConcurrentWritesAndClosedStore(t *testing.T) {
	s, root := newStore(t)
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := s.PutWorkspace(testctx, auth.Workspace{ID: domain.WorkspaceID(fmt.Sprintf("w%d", i)), Enabled: true}); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Ready(testctx); !errors.Is(err, ErrClosed) {
		t.Fatal("closed store answered", err)
	}
	next, err := Open(testctx, root, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	for i := 0; i < 24; i++ {
		if _, err := next.LookupWorkspace(testctx, domain.WorkspaceID(fmt.Sprintf("w%d", i))); err != nil {
			t.Fatal("concurrent write lost", err)
		}
	}
}

func TestSQLiteDiskFullRollsBackWithoutReplay(t *testing.T) {
	s, _ := newStore(t)
	var count int64
	if err := s.db.QueryRow("PRAGMA page_count").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(fmt.Sprintf("PRAGMA max_page_count=%d", count+1)); err != nil {
		t.Fatal(err)
	}
	calls := 0
	err := withTx(testctx, s.db, func(tx *sql.Tx) error {
		calls++
		if _, err := tx.Exec("INSERT INTO workspaces VALUES('must-rollback',1,'[]')"); err != nil {
			return dbError(err)
		}
		_, err := tx.Exec("CREATE TABLE disk_fault AS SELECT zeroblob(1048576) AS data")
		return dbError(err)
	})
	if !errors.Is(err, ErrDiskFull) || calls != 1 {
		t.Fatalf("disk-full result=%v calls=%d", err, calls)
	}
	if _, err := s.LookupWorkspace(testctx, "must-rollback"); !errors.Is(err, auth.ErrNotFound) {
		t.Fatal("disk-full partial commit", err)
	}
}

func TestProcessKillPreservesCommittedWALAndRollsBackIncompleteTransaction(t *testing.T) {
	root := filepath.Join(t.TempDir(), "state")
	marker := filepath.Join(t.TempDir(), "ready")
	cmd := exec.Command(os.Args[0], "-test.run=^TestStateCrashHelper$")
	cmd.Env = append(os.Environ(), "CR_STATE_CRASH_ROOT="+root, "CR_STATE_CRASH_MARKER="+marker)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("child did not reach write boundary")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if other, err := Open(testctx, root, DefaultOptions()); !errors.Is(err, statefs.ErrLocked) {
		if other != nil {
			other.Close()
		}
		t.Fatal("second process not locked", err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	s, err := Open(testctx, root, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.LookupWorkspace(testctx, "committed"); err != nil {
		t.Fatal("committed WAL lost", err)
	}
	if _, err := s.LookupWorkspace(testctx, "uncommitted"); !errors.Is(err, auth.ErrNotFound) {
		t.Fatal("uncommitted write survived", err)
	}
}
func TestStateCrashHelper(t *testing.T) {
	root := os.Getenv("CR_STATE_CRASH_ROOT")
	if root == "" {
		return
	}
	s, err := Open(testctx, root, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	workspace(t, s, "committed")
	tx, err := s.db.BeginTx(testctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO workspaces VALUES('uncommitted',1,'[]')"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv("CR_STATE_CRASH_MARKER"), []byte("ready"), 0o600); err != nil {
		t.Fatal(err)
	}
	for {
		time.Sleep(time.Second)
	}
}
