package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

// The child is a real process writing the actual intent transaction. No provider is
// contacted by the child. Fake remote state remains in the parent; this establishes
// SQLite crash behavior, not persistence of a real provider service.
func TestDispatchProcessKillAtMutationIntentBoundaries(t *testing.T) {
	for _, mode := range []string{"prepare_before", "prepare_after", "submit_before", "submit_after"} {
		t.Run(mode, func(t *testing.T) {
			f := newDispatchFixture(t, fake.DefaultScenario())
			id := f.seed(t, "a", 1, false)
			submission := strings.HasPrefix(mode, "submit")
			committed := strings.HasSuffix(mode, "after")
			if submission {
				requireNoError(t, f.step(t))
			}
			f.clock.advance(10 * time.Minute)
			now := f.clock.Now()
			requireNoError(t, f.s.Close())
			marker := filepath.Join(t.TempDir(), "intent-boundary")
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDispatchCrashHelper$", "-test.timeout=18s")
			child.Env = append(os.Environ(), "CR_DISPATCH_CRASH_ROOT="+f.root, "CR_DISPATCH_CRASH_MODE="+mode, "CR_DISPATCH_CRASH_NOW="+now.Format(time.RFC3339Nano), "CR_DISPATCH_CRASH_MARKER="+marker)
			child.Stdout, child.Stderr = os.Stderr, os.Stderr
			requireNoError(t, child.Start())
			defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
			until := time.Now().Add(12 * time.Second)
			for {
				if _, err := os.Stat(marker); err == nil {
					break
				}
				if time.Now().After(until) {
					t.Fatal("child did not reach intent boundary")
				}
				time.Sleep(10 * time.Millisecond)
			}
			requireNoError(t, child.Process.Kill())
			_ = child.Wait()
			s, err := Open(context.Background(), f.root, DefaultOptions())
			requireNoError(t, err)
			f.attach(t, s)
			phase := dispatch.Local
			if submission {
				phase = dispatch.Ready
			}
			if committed {
				phase = dispatch.Staging
				if submission {
					phase = dispatch.Submitting
				}
			}
			if got := f.journal(t, id).Phase; got != phase {
				t.Fatalf("phase=%s want=%s", got, phase)
			}
			var intents int
			err = s.db.QueryRow("SELECT count(*) FROM submission_intents").Scan(&intents)
			requireNoError(t, err)
			if intents != 0 && !(submission && committed && intents == 1) {
				t.Fatal("partial intent survived rollback", intents)
			}
			err = f.step(t)
			if committed {
				var problem *domain.Problem
				if !errors.As(err, &problem) {
					t.Fatalf("missing unresolved outcome: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			stats := f.backend.Stats()
			wantPrepare, wantSubmit := 1, 0
			if !submission && committed {
				wantPrepare = 0
			}
			if submission && !committed {
				wantSubmit = 1
			}
			if stats.PrepareCalls != wantPrepare || stats.SubmitCalls != wantSubmit || stats.Executions != wantSubmit {
				t.Fatalf("intent recovery repeated a mutation: %+v", stats)
			}
		})
	}
}

func TestDispatchCrashHelper(t *testing.T) {
	root := os.Getenv("CR_DISPATCH_CRASH_ROOT")
	if root == "" {
		return
	}
	mode := os.Getenv("CR_DISPATCH_CRASH_MODE")
	now, err := time.Parse(time.RFC3339Nano, os.Getenv("CR_DISPATCH_CRASH_NOW"))
	requireNoError(t, err)
	opts := DefaultOptions()
	opts.Clock = func() time.Time { return now }
	s, err := Open(context.Background(), root, opts)
	requireNoError(t, err)
	defer s.Close()
	c, err := s.ClaimRecovery(context.Background(), "crash_child")
	requireNoError(t, err)
	if c == nil {
		r, err := s.ClaimNext(context.Background(), "crash_child")
		if err != nil || r.Claim == nil {
			t.Fatal("no child claim", err)
		}
		c = r.Claim
	}
	work, err := s.LoadDispatch(context.Background(), *c)
	requireNoError(t, err)
	action := dispatch.Action{Kind: dispatch.BeginSubmission}
	if strings.HasPrefix(mode, "prepare") {
		work, err = s.ConsumeAuthorization(context.Background(), work.Handle)
		requireNoError(t, err)
		plan, op := storePlan(t, work)
		action = dispatch.Action{Kind: dispatch.BeginPreparation, Plan: &plan, PreparationID: op}
	}
	if strings.HasSuffix(mode, "after") {
		if _, err = s.CommitDispatch(context.Background(), work.Handle, action); err != nil {
			t.Fatal(err)
		}
	} else {
		// Deliberately write the transaction's full prefix without its commit. This is a
		// controlled fault fixture, never a production transaction callback API.
		tx, err := s.db.BeginTx(context.Background(), nil)
		requireNoError(t, err)
		defer tx.Rollback()
		err = uncommittedDispatchPrefix(tx, work, action, now)
		requireNoError(t, err)
	}
	err = os.WriteFile(os.Getenv("CR_DISPATCH_CRASH_MARKER"), []byte("boundary"), 0600)
	requireNoError(t, err)
	time.Sleep(time.Minute)
	t.Fatal("parent did not terminate crash fixture")
}

func uncommittedDispatchPrefix(tx *sql.Tx, work dispatch.Work, action dispatch.Action, now time.Time) error {
	ctx := context.Background()
	j, state, event, err := dispatch.Apply(work.Journal, work.Job.Attempt.State, action, now)
	if err != nil {
		return err
	}
	if err = writeDispatchLedger(ctx, tx, work, j, action.Kind, now); err != nil {
		return err
	}
	if action.Kind == dispatch.BeginPreparation {
		if _, err = tx.ExecContext(ctx, "UPDATE scheduler_queue SET dispatch_barrier=1 WHERE queue_seq=?", work.Handle.Claim.Sequence); err != nil {
			return err
		}
		if _, err = schedulerTransition(ctx, tx, work.Handle.Claim.Identity, work.Job.Attempt, work.Job.Attempt.State, domain.EventInputsReady, now); err != nil {
			return err
		}
	}
	if _, err = schedulerTransition(ctx, tx, work.Handle.Claim.Identity, work.Job.Attempt, state, event, now); err != nil {
		return err
	}
	raw, err := json.Marshal(j)
	if err != nil {
		return err
	}
	if work.Journal.Version == 0 {
		_, err = tx.ExecContext(ctx, "INSERT INTO dispatch_journals VALUES(?,?,?,?)", work.Handle.Claim.Sequence, j.Version, string(j.Phase), string(raw))
	} else {
		_, err = tx.ExecContext(ctx, "UPDATE dispatch_journals SET version=?,phase=?,journal=? WHERE queue_seq=?", j.Version, string(j.Phase), string(raw), work.Handle.Claim.Sequence)
	}
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE scheduler_control SET clock_ms=? WHERE singleton=1", now.UnixMilli())
	return err
}
