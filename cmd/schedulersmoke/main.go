// Command schedulersmoke is a finite metadata-only developer check. No provider,
// payload, network transfer, GPU or production runtime is started. Time is advanced
// explicitly to exercise lease expiry without waiting for a real remote execution.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "scheduler smoke failed:", err)
		os.Exit(1)
	}
}
func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	root, err := os.MkdirTemp("", "compute-relay-scheduler-smoke-")
	if err != nil {
		return errors.New("create temporary state")
	}
	defer os.RemoveAll(root)
	path := filepath.Join(root, "state")
	store, err := sqlite.Open(ctx, path, sqlite.DefaultOptions())
	if err != nil {
		return err
	}
	defer store.Close()
	profile := admission.DefaultProfile(domain.ProviderBinding{Profile: "fixture", ProviderInstanceID: "fixture_instance", ConfigurationRevision: "rev_1"}, "shared_account")
	if err = store.PutProfile(ctx, profile, true); err != nil {
		return err
	}
	access, err := auth.New(store, store, nil)
	if err != nil {
		return err
	}
	jobs, err := admission.New(access, store, admission.DefaultLimits())
	if err != nil {
		return err
	}
	raw := []byte(`{"api_version":"compute-connector/v1alpha1","name":"scheduler-smoke","profile":"fixture","bundle":{"object_id":"code"},"execution":{"kind":"python","command":["python","not-executed.py"]},"inputs":[],"outputs":[{"path":"answer.json","required":true}],"resources":{"accelerator":"cpu"},"network":{"remote_internet":"disabled"},"timeouts":{"remote_wall_seconds":10,"setup_seconds":2,"finalization_grace_seconds":2}}`)
	ids := map[string]domain.JobID{}
	var firstToken auth.Secret
	for _, w := range []domain.WorkspaceID{"a", "b"} {
		if err = store.PutWorkspace(ctx, auth.Workspace{ID: w, Enabled: true, AllowedProfiles: []string{"fixture"}}); err != nil {
			return err
		}
		if err = store.CommitObject(ctx, domain.ObjectMetadata{ID: "code", WorkspaceID: w, Bytes: 0, SHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}); err != nil {
			return err
		}
		token, _, err := access.Issue(ctx, w, []auth.Scope{auth.Read, auth.Write}, time.Time{})
		if err != nil {
			return err
		}
		p, err := access.Authenticate(ctx, token.Reveal())
		if err != nil {
			return err
		}
		receipt, err := jobs.Submit(ctx, p, w, "scheduler-first-job", raw)
		if err != nil {
			return err
		}
		ids[string(w)] = receipt.JobID
		if w == "a" {
			firstToken = token
			if _, err = jobs.Submit(ctx, p, w, "scheduler-second-job", raw); err != nil {
				return err
			}
		}
	}
	if err = store.ConfigureScheduler(ctx, scheduler.DefaultSettings()); err != nil {
		return err
	}
	now := time.Now().UTC()
	r, err := store.ClaimNext(ctx, "worker_a", now)
	if err != nil {
		return err
	}
	if r.Claim == nil || r.Claim.JobID != ids["a"] {
		return errors.New("FIFO first claim mismatch")
	}
	first := *r.Claim
	r, err = store.ClaimNext(ctx, "contender", now)
	if err != nil {
		return err
	}
	if r.Claim != nil || r.View.AccountReservations["shared_account"] != 1 {
		return errors.New("account overcommitted")
	}
	if err = store.Close(); err != nil {
		return err
	}
	store, err = sqlite.Open(ctx, path, sqlite.DefaultOptions())
	if err != nil {
		return err
	}
	defer store.Close()
	now = first.ExpiresAt.Add(time.Millisecond)
	r, err = store.ClaimNext(ctx, "worker_b", now)
	if err != nil {
		return err
	}
	if r.Claim == nil || r.Claim.JobID != ids["b"] {
		return errors.New("restart lost round-robin cursor")
	}
	if err = store.ReleaseClaim(ctx, *r.Claim, now); err != nil {
		return err
	}
	r, err = store.ClaimNext(ctx, "worker_recovered", now)
	if err != nil {
		return err
	}
	if r.Claim == nil || r.Claim.AttemptID != first.AttemptID || r.Claim.Generation != first.Generation+1 || r.Claim.Fence == first.Fence {
		return errors.New("reclaim lost attempt/fence identity")
	}
	if _, err = store.RenewClaim(ctx, first, now); !errors.Is(err, scheduler.ErrLeaseLost) {
		return errors.New("old worker retained authority")
	}
	access, err = auth.New(store, store, nil)
	if err != nil {
		return err
	}
	p, err := access.Authenticate(ctx, firstToken.Reveal())
	if err != nil {
		return err
	}
	jobs, err = admission.New(access, store, admission.DefaultLimits())
	if err != nil {
		return err
	}
	record, err := jobs.Get(ctx, p, "a", ids["a"])
	if err != nil {
		return err
	}
	if record.Attempt.Number != 1 || record.Attempt.State.Execution != domain.ExecutionNotSubmitted || record.Attempt.State.RemoteActivity != domain.RemoteActivityNotStarted {
		return errors.New("scheduler invented compute")
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"status": "passed-offline", "claim_order": []string{"a", "b", "a"}, "account_capacity_verified": true, "restart_cursor": true, "fenced_generation": r.Claim.Generation, "attempt_number": record.Attempt.Number, "metadata_only": true, "blob_bytes_checked": false, "clock": "explicitly advanced for lease expiry", "provider_calls": 0, "payload_executions": 0})
}
