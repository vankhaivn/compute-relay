package sqlite

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
)

func TestAdmissionProcessCrashBeforeAndAfterCommit(t *testing.T) {
	for _, phase := range []string{"before", "after"} {
		t.Run(phase, func(t *testing.T) {
			f := newAdmission(t)
			if err := f.s.Close(); err != nil {
				t.Fatal(err)
			}
			marker := filepath.Join(t.TempDir(), "ready.json")
			cmd := exec.Command(os.Args[0], "-test.run=^TestAdmissionCrashHelper$")
			cmd.Env = append(os.Environ(), "CR_TEST_ADMISSION_ROOT="+f.root, "CR_TEST_ADMISSION_MARKER="+marker, "CR_TEST_ADMISSION_PHASE="+phase, "CR_TEST_ADMISSION_TOKEN_ID="+f.p.TokenID())
			cmd.Stdout = os.Stderr
			cmd.Stderr = os.Stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
			deadline := time.Now().Add(15 * time.Second)
			var data []byte
			for {
				var err error
				data, err = os.ReadFile(marker)
				if err == nil && json.Valid(data) {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("admission crash helper failed to reach failpoint")
				}
				time.Sleep(10 * time.Millisecond)
			}
			if err := cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = cmd.Wait()
			s, err := Open(context.Background(), f.root, DefaultOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			req, err := admission.Parse([]byte(admissionJSON))
			if err != nil {
				t.Fatal(err)
			}
			key, _ := admission.KeyDigest("crash-request-key")
			receipt, err := s.AdmitJob(context.Background(), "a", f.p.TokenID(), key, req, admission.DefaultLimits())
			if err != nil {
				t.Fatal(err)
			}
			if receipt.Replay != (phase == "after") {
				t.Fatal("wrong commit recovery result")
			}
			if phase == "after" {
				var expected admission.Receipt
				if err = json.Unmarshal(data, &expected); err != nil {
					t.Fatal(err)
				}
				expected.Replay = true
				if receipt != expected {
					t.Fatal("post-commit crash changed IDs/receipt")
				}
			}
			for _, table := range []string{"jobs", "attempts", "events", "idempotency"} {
				if tableCount(t, s, table) != 1 {
					t.Fatal("crash lost or duplicated admission", table)
				}
			}
		})
	}
}
func TestAdmissionCrashHelper(t *testing.T) {
	root := os.Getenv("CR_TEST_ADMISSION_ROOT")
	if root == "" {
		return
	}
	ctx := context.Background()
	s, err := Open(ctx, root, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	req, err := admission.Parse([]byte(admissionJSON))
	if err != nil {
		t.Fatal(err)
	}
	var report any = map[string]bool{"uncommitted": true}
	if os.Getenv("CR_TEST_ADMISSION_PHASE") == "after" {
		key, _ := admission.KeyDigest("crash-request-key")
		report, err = s.AdmitJob(ctx, "a", os.Getenv("CR_TEST_ADMISSION_TOKEN_ID"), key, req, admission.DefaultLimits())
		if err != nil {
			t.Fatal(err)
		}
	} else {
		// Model process death after the first insert but before transaction acceptance.
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO jobs VALUES(?,?,?,?,?,?,?,?,?)`, "a", "job_interrupted", string(req.Canonical()), admission.CanonicalVersion, string(req.Hash()), "default-gpu", "rev_1", "att_interrupted", time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			t.Fatal(err)
		}
	}
	b, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(os.Getenv("CR_TEST_ADMISSION_MARKER"), b, 0600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Minute)
	t.Fatal("helper was not killed")
}

// Compile-time domain association check on admitted identities.
var _ domain.JobID = admission.Receipt{}.JobID
