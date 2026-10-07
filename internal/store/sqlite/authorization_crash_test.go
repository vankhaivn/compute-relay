package sqlite

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

func TestAuthorizationProcessKillAtConsumptionBoundary(t *testing.T) {
	for _, mode := range []string{"consume_before", "consume_after", "prepare_after"} {
		t.Run(mode, func(t *testing.T) {
			f := managedFixture(t, fake.DefaultScenario())
			id := f.seed(t, "a", 1, false)
			grantAuthorization(t, f, id)
			if err := f.s.Close(); err != nil {
				t.Fatal(err)
			}
			marker := filepath.Join(t.TempDir(), "authorization-boundary")
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAuthorizationCrashHelper$", "-test.timeout=18s")
			child.Env = append(os.Environ(), "CR_AUTH_CRASH_ROOT="+f.root, "CR_AUTH_CRASH_MODE="+mode, "CR_AUTH_CRASH_NOW="+f.clock.Now().Format(time.RFC3339Nano), "CR_AUTH_CRASH_MARKER="+marker)
			child.Stdout, child.Stderr = os.Stderr, os.Stderr
			if err := child.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
			until := time.Now().Add(12 * time.Second)
			for {
				if _, err := os.Stat(marker); err == nil {
					break
				}
				if time.Now().After(until) {
					t.Fatal("child did not reach authorization boundary")
				}
				time.Sleep(10 * time.Millisecond)
			}
			if err := child.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = child.Wait()
			s, err := Open(context.Background(), f.root, DefaultOptions())
			if err != nil {
				t.Fatal(err)
			}
			f.s = s
			attachManaged(t, f)
			var status string
			if err = s.db.QueryRow("SELECT status FROM execution_authorizations").Scan(&status); err != nil {
				t.Fatal(err)
			}
			want := executionauth.Consumed
			if mode == "consume_before" {
				want = executionauth.Granted
			}
			if status != string(want) {
				t.Fatalf("consumption=%s want=%s", status, want)
			}
			err = f.step(t)
			if mode == "prepare_after" {
				var p *domain.Problem
				if !errors.As(err, &p) {
					t.Fatal("missing unresolved preparation evidence", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			stats := f.backend.Stats()
			expectedPrepare := 1
			if mode == "prepare_after" {
				expectedPrepare = 0
			}
			if stats.PrepareCalls != expectedPrepare || stats.SubmitCalls != 0 || stats.Executions != 0 {
				t.Fatal("crash repeated remote mutation", stats)
			}
			if count := controlCount(t, f.s, "SELECT count(*) FROM execution_authorizations"); count != 1 {
				t.Fatal("restart refilled permit", count)
			}
		})
	}
}

func TestAuthorizationCrashHelper(t *testing.T) {
	root := os.Getenv("CR_AUTH_CRASH_ROOT")
	if root == "" {
		return
	}
	mode := os.Getenv("CR_AUTH_CRASH_MODE")
	now, err := time.Parse(time.RFC3339Nano, os.Getenv("CR_AUTH_CRASH_NOW"))
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), root, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	claim, err := s.ClaimNextManaged(context.Background(), "authorization_crash", now)
	if err != nil || claim.Claim == nil {
		t.Fatal("missing child claim", err)
	}
	work, err := s.LoadDispatch(context.Background(), *claim.Claim, now)
	if err != nil {
		t.Fatal(err)
	}
	if mode == "consume_before" {
		tx, err := s.db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		// This intentional uncommitted fault prefix is confined to a disposable test DB.
		if _, err = tx.Exec("UPDATE execution_authorizations SET status='consumed',consumed_at=?", now.Format(time.RFC3339Nano)); err != nil {
			t.Fatal(err)
		}
	} else {
		if _, err = s.ConsumeAuthorization(context.Background(), work.Handle, now); err != nil {
			t.Fatal(err)
		}
		if mode == "prepare_after" {
			plan, op := storePlan(t, work)
			if _, err = s.CommitDispatch(context.Background(), work.Handle, dispatch.Action{Kind: dispatch.BeginPreparation, Plan: &plan, PreparationID: op}, now); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err = os.WriteFile(os.Getenv("CR_AUTH_CRASH_MARKER"), []byte("boundary"), 0600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Minute)
	t.Fatal("parent did not terminate authorization fixture")
}
