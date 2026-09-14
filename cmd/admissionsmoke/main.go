// Command admissionsmoke is a finite, metadata-only developer check. It does not
// launch a scheduler, download inputs, invoke a provider, or execute the job command.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "admission smoke failed:", err)
		os.Exit(1)
	}
}
func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	root, err := os.MkdirTemp("", "compute-relay-admission-smoke-")
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
	if err = store.PutWorkspace(ctx, auth.Workspace{ID: "smoke", Enabled: true, AllowedProfiles: []string{"fixture"}}); err != nil {
		return err
	}
	profile := admission.DefaultProfile(domain.ProviderBinding{Profile: "fixture", ProviderInstanceID: "fixture_instance", ConfigurationRevision: "rev_1"}, "fixture_account")
	if err = store.PutProfile(ctx, profile, true); err != nil {
		return err
	}
	// Synthetic metadata only; this check deliberately does not claim blob preparation.
	if err = store.CommitObject(ctx, domain.ObjectMetadata{ID: "code", WorkspaceID: "smoke", Bytes: 0, SHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}); err != nil {
		return err
	}
	access, err := auth.New(store, store, nil)
	if err != nil {
		return err
	}
	secret, _, err := access.Issue(ctx, "smoke", []auth.Scope{auth.Read, auth.Write}, time.Time{})
	if err != nil {
		return err
	}
	principal, err := access.Authenticate(ctx, secret.Reveal())
	if err != nil {
		return err
	}
	service, err := admission.New(access, store, admission.DefaultLimits())
	if err != nil {
		return err
	}
	raw := []byte(`{"api_version":"compute-connector/v1alpha1","name":"metadata-smoke","profile":"fixture","bundle":{"object_id":"code"},"execution":{"kind":"python","command":["python","not-executed.py"]},"inputs":[],"outputs":[{"path":"answer.json","required":true}],"resources":{"accelerator":"cpu"},"network":{"remote_internet":"disabled"},"timeouts":{"remote_wall_seconds":10,"setup_seconds":2,"finalization_grace_seconds":2}}`)
	receipt, err := service.Submit(ctx, principal, "smoke", "lost-response-smoke", raw)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	failures := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := service.Submit(ctx, principal, "smoke", "lost-response-smoke", raw)
			if e != nil {
				failures <- e
				return
			}
			if !r.Replay || r.JobID != receipt.JobID || r.AttemptID != receipt.AttemptID {
				failures <- errors.New("concurrent replay changed identity")
			}
		}()
	}
	wg.Wait()
	close(failures)
	for e := range failures {
		return e
	}
	if _, err = service.Submit(ctx, principal, "smoke", "lost-response-smoke", []byte(strings.Replace(string(raw), "metadata-smoke", "changed", 1))); !errors.Is(err, admission.ErrConflict) {
		return errors.New("changed request did not conflict")
	}
	if err = store.Close(); err != nil {
		return err
	}
	store, err = sqlite.Open(ctx, path, sqlite.DefaultOptions())
	if err != nil {
		return err
	}
	defer store.Close()
	access, err = auth.New(store, store, nil)
	if err != nil {
		return err
	}
	principal, err = access.Authenticate(ctx, secret.Reveal())
	if err != nil {
		return err
	}
	service, err = admission.New(access, store, admission.DefaultLimits())
	if err != nil {
		return err
	}
	replay, err := service.Submit(ctx, principal, "smoke", "lost-response-smoke", raw)
	if err != nil || !replay.Replay || replay.JobID != receipt.JobID || replay.AttemptID != receipt.AttemptID {
		return errors.New("restart lost idempotent admission")
	}
	record, err := service.Get(ctx, principal, "smoke", receipt.JobID)
	if err != nil {
		return err
	}
	if record.Attempt.Number != 1 || record.Attempt.State != domain.InitialAttemptState() || record.Profile != profile {
		return errors.New("stored resolution/state changed")
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"status": "passed-offline", "concurrent_replays": 8, "restart_replay": true, "conflict_detected": true, "attempt_number": record.Attempt.Number, "state": record.Attempt.State.Orchestration, "metadata_only": true, "blob_bytes_checked": false, "provider_calls": 0, "payload_executions": 0})
}
