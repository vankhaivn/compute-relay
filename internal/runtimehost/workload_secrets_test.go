package runtimehost

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/statefs"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

func secretPoolFingerprint(t *testing.T, pool *workloadsecret.Pool) string {
	t.Helper()
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	key := workloadsecret.Key{WorkspaceID: "app", JobID: "job_1", AttemptID: "attempt_1"}
	request := workloadsecret.Supply{AttemptID: key.AttemptID, ReadinessRevision: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ExpiresAt: now.Add(time.Hour).Format(time.RFC3339), Values: map[string][]byte{"ingest": []byte("synthetic-key-restart-canary")}}
	var result string
	_, err := pool.Supply(key, request, now, func(hash string, metadata workloadsecret.Metadata) (workloadsecret.Receipt, bool, error) {
		result = hash
		return workloadsecret.Receipt{AuthorizationID: "secret_1", WorkspaceID: key.WorkspaceID, JobID: key.JobID, Metadata: metadata, Status: "granted", CreatedAt: now}, true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func TestWorkloadSecretReplayKeySurvivesRestartAndPlaintextDoesNot(t *testing.T) {
	ctx := context.Background()
	root := initialized(t)
	host, err := Open(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if host != nil {
			_ = host.Close()
		}
	}()
	pool, err := host.workloadSecretPool(ctx)
	if err != nil {
		t.Fatal(err)
	}
	first := secretPoolFingerprint(t, pool)
	path := filepath.Join(root, "state", "workload-secret-key")
	if err := statefs.CheckFile(path); err != nil {
		t.Fatal("unprotected replay key", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil || len(raw) != 32 || bytes.Contains(raw, []byte("synthetic-key-restart-canary")) {
		t.Fatal("invalid replay key")
	}
	clear(raw)
	if err = host.Close(); err != nil {
		t.Fatal(err)
	}
	host = nil
	if pool.Available(workloadsecret.Key{WorkspaceID: "app", JobID: "job_1", AttemptID: "attempt_1"}, "secret_1", time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)) {
		t.Fatal("close retained material")
	}
	host, err = Open(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	pool, err = host.workloadSecretPool(ctx)
	if err != nil || first != secretPoolFingerprint(t, pool) {
		t.Fatal("restart changed keyed replay identity", err)
	}
}
func TestWorkloadSecretKeyRejectsUnsafeOrCorruptState(t *testing.T) {
	for _, kind := range []string{"short", "permissions", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			root := initialized(t)
			host, err := Open(ctx, root)
			if err != nil {
				t.Fatal(err)
			}
			defer host.Close()
			path := filepath.Join(root, "state", "workload-secret-key")
			switch kind {
			case "short":
				err = os.WriteFile(path, []byte("short"), 0600)
			case "permissions":
				err = os.WriteFile(path, bytes.Repeat([]byte{3}, 32), 0644)
			case "symlink":
				target := filepath.Join(t.TempDir(), "key")
				if err = os.WriteFile(target, bytes.Repeat([]byte{4}, 32), 0600); err == nil {
					err = os.Symlink(target, path)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err = host.workloadSecretPool(ctx); err != ErrState {
				t.Fatal("unsafe key accepted", err)
			}
		})
	}
}

func TestWorkloadSecretMissingKeyWithReceiptNeverReinitializes(t *testing.T) {
	ctx := context.Background()
	root := initialized(t)
	host, err := Open(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	// Even an incomplete restored receipt must prevent key regeneration. This
	// artificial damaged database intentionally has no related attempt or key.
	db, err := sql.Open("sqlite", filepath.Join(root, "state", "runtime.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.ExecContext(ctx, "PRAGMA foreign_keys=OFF"); err != nil {
		t.Fatal(err)
	}
	_, err = conn.ExecContext(ctx, `INSERT INTO workload_secret_authorizations(workspace_id,authorization_id,job_id,attempt_id,key_sha256,request_hmac,metadata,original_receipt,expires_at_ms) VALUES('app','secret_1','job_1','attempt_1',?,?,'{}','{}',1)`, strings.Repeat("a", 64), strings.Repeat("b", 64))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = host.workloadSecretPool(ctx); err != ErrState {
		t.Fatal("missing replay key was regenerated", err)
	}
	if _, err = os.Lstat(filepath.Join(root, "state", "workload-secret-key")); !os.IsNotExist(err) {
		t.Fatal("failure wrote replacement evidence", err)
	}
}
