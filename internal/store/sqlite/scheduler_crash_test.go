package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func TestSchedulerProcessKillBeforeAndAfterClaimCommit(t *testing.T) {
	for _, phase := range []string{"before", "after"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			state := filepath.Join(dir, "state")
			marker := filepath.Join(dir, "ready")
			cmd := exec.Command(os.Args[0], "-test.run=^TestSchedulerCrashHelper$")
			cmd.Env = append(os.Environ(), "CR_SCHEDULER_ROOT="+state, "CR_SCHEDULER_MARKER="+marker, "CR_SCHEDULER_PHASE="+phase)
			cmd.Stdout = os.Stderr
			cmd.Stderr = os.Stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			waited := false
			t.Cleanup(func() {
				if !waited {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
				}
			})
			deadline := time.Now().Add(10 * time.Second)
			for {
				if _, err := os.Stat(marker); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("child did not reach claim boundary")
				}
				time.Sleep(10 * time.Millisecond)
			}
			if err := cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = cmd.Wait()
			waited = true
			s, err := Open(context.Background(), state, DefaultOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			c := schedulerClaim(t, s, "recovered", schedulerNow.Add(time.Minute))
			wantGeneration := int64(1)
			if phase == "after" {
				wantGeneration = 2
			}
			if c.Generation != wantGeneration || c.JobID != "job_crash" || c.AttemptID != "att_crash" {
				t.Fatal("new execution or lost fencing", c)
			}
			a, nonce, err := loadAttempt(context.Background(), s.db, c.WorkspaceID, c.JobID, c.AttemptID)
			if err != nil || a.Number != 1 || nonce != strings.Repeat("a", 64) || a.State.Orchestration != domain.OrchestrationPreparing {
				t.Fatal("attempt changed across process kill", a, err)
			}
			if schedulerCount(t, s, "SELECT count(*) FROM attempts") != 1 || schedulerCount(t, s, "SELECT count(*) FROM scheduler_leases WHERE held=1") != 1 {
				t.Fatal("duplicate attempt or claim")
			}
		})
	}
}
func TestSchedulerCrashHelper(t *testing.T) {
	path := os.Getenv("CR_SCHEDULER_ROOT")
	if path == "" {
		return
	}
	ctx := context.Background()
	s, err := Open(ctx, path, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err = s.ConfigureScheduler(ctx, scheduler.DefaultSettings()); err != nil {
		t.Fatal(err)
	}
	id := schedulerSeed(t, s, "crash", "a", "p", "account")
	ready := func() {
		if err := os.WriteFile(os.Getenv("CR_SCHEDULER_MARKER"), []byte("at-claim-boundary"), 0600); err != nil {
			t.Fatal(err)
		}
		for {
			time.Sleep(time.Second)
		}
	}
	if os.Getenv("CR_SCHEDULER_PHASE") == "after" {
		_ = schedulerClaim(t, s, "crashed", schedulerNow)
		ready()
		return
	}
	// Kill with the same queue/lease/state/event/cursor writes inside an uncommitted
	// transaction. The post-commit branch above uses the public ClaimNext operation.
	err = withTx(ctx, s.db, func(tx *sql.Tx) error {
		a, _, err := loadAttempt(ctx, tx, id.WorkspaceID, id.JobID, id.AttemptID)
		if err != nil {
			return err
		}
		next := a.State
		next.Orchestration = domain.OrchestrationPreparing
		if _, err = schedulerTransition(ctx, tx, id, a, next, domain.EventSchedulerClaimed, schedulerNow); err != nil {
			return err
		}
		if _, err = tx.Exec("INSERT INTO scheduler_leases VALUES(1,1,'crashed',?,?,1)", strings.Repeat("b", 64), schedulerNow.Add(30*time.Second).UnixMilli()); err != nil {
			return err
		}
		if _, err = tx.Exec("UPDATE scheduler_control SET last_workspace='a',clock_ms=?", schedulerNow.UnixMilli()); err != nil {
			return err
		}
		ready()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
func TestSchedulerDiskFullDoesNotAcknowledgeOrPartiallyClaim(t *testing.T) {
	s, _ := schedulerStore(t)
	id := schedulerSeed(t, s, "disk", "a", "p", "account")
	if _, err := s.db.Exec(`CREATE TABLE scheduler_pressure(v BLOB);
 CREATE TRIGGER scheduler_fill BEFORE INSERT ON events WHEN NEW.type='scheduler.claimed'
 BEGIN INSERT INTO scheduler_pressure VALUES(zeroblob(1048576)); END;`); err != nil {
		t.Fatal(err)
	}
	var pages int64
	if err := s.db.QueryRow("PRAGMA page_count").Scan(&pages); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(fmt.Sprintf("PRAGMA max_page_count=%d", pages+1)); err != nil {
		t.Fatal(err)
	}
	r, err := s.ClaimNext(context.Background(), "w", schedulerNow)
	if !errors.Is(err, ErrDiskFull) || r.Claim != nil {
		t.Fatal("disk-full claim acknowledged", r, err)
	}
	if schedulerCount(t, s, "SELECT count(*) FROM scheduler_leases") != 0 || schedulerCount(t, s, "SELECT count(*) FROM scheduler_control WHERE last_workspace!=''") != 0 {
		t.Fatal("disk-full left partial claim")
	}
	var raw string
	if err = s.db.QueryRow("SELECT state FROM attempts WHERE attempt_id=?", string(id.AttemptID)).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var state domain.AttemptState
	if err = json.Unmarshal([]byte(raw), &state); err != nil || state.Orchestration != domain.OrchestrationQueued {
		t.Fatal("disk-full advanced state", err)
	}
}
