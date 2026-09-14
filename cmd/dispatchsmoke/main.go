// Command dispatchsmoke is a finite, opt-in offline developer check. It uses actual
// admission, SQLite and blob storage with a fixture-only provider: no workload runs.
package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/packaging"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

type clock struct {
	mu sync.Mutex
	at time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.at }
func (c *clock) advance()       { c.mu.Lock(); c.at = c.at.Add(10 * time.Minute); c.mu.Unlock() }

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "dispatch smoke failed:", err)
		os.Exit(1)
	}
}
func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	root, err := os.MkdirTemp("", "compute-relay-dispatch-smoke-")
	if err != nil {
		return errors.New("create private smoke directory")
	}
	defer os.RemoveAll(root)
	store, err := sqlite.Open(ctx, filepath.Join(root, "state"), sqlite.DefaultOptions())
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	blobs, err := blobfs.New(filepath.Join(root, "blobs"), blobfs.Limits{MaxObjectBytes: 2 << 20, MaxTotalBytes: 4 << 20})
	if err != nil {
		return err
	}
	defer blobs.Close()
	if err = store.PutWorkspace(ctx, auth.Workspace{ID: "smoke", Enabled: true, AllowedProfiles: []string{"fixture"}}); err != nil {
		return err
	}
	profile := admission.DefaultProfile(domain.ProviderBinding{Profile: "fixture", ProviderInstanceID: "fixture_instance", ConfigurationRevision: "one"}, "fixture_account")
	if err = store.PutProfile(ctx, profile, true); err != nil {
		return err
	}
	if err = store.ConfigureScheduler(ctx, scheduler.DefaultSettings()); err != nil {
		return err
	}
	bundle, err := codeBundle()
	if err != nil {
		return err
	}
	input := bytes.Repeat([]byte("synthetic-input!"), 65536)
	for id, data := range map[domain.ObjectID][]byte{"code": bundle, "input": input} {
		m, err := blobs.Put(ctx, domain.ObjectMetadata{ID: id, WorkspaceID: "smoke", Bytes: -1}, bytes.NewReader(data))
		if err != nil {
			return err
		}
		if err = store.CommitObject(ctx, m); err != nil {
			return err
		}
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
	jobs, err := admission.New(access, store, admission.DefaultLimits())
	if err != nil {
		return err
	}
	raw := []byte(`{"api_version":"compute-connector/v1alpha1","name":"offline-dispatch-smoke","profile":"fixture","bundle":{"object_id":"code"},"execution":{"kind":"python","command":["python","never-executed.py"]},"inputs":[{"name":"input","source":{"kind":"object","object_id":"input"},"target":"input.txt"}],"outputs":[{"path":"result.json","required":true}],"resources":{"accelerator":"cpu"},"network":{"remote_internet":"disabled"},"timeouts":{"remote_wall_seconds":10,"setup_seconds":2,"finalization_grace_seconds":2}}`)
	receipt, err := jobs.Submit(ctx, principal, "smoke", "dispatch-smoke-only", raw)
	if err != nil {
		return err
	}
	c := &clock{at: time.Now().UTC().Add(time.Second)}
	scenario := fake.DefaultScenario()
	scenario.Mode = fake.AcceptLoseResponse
	scenario.States = []domain.ExecutionState{domain.ExecutionSucceeded}
	backend, err := fake.NewBackend(scenario)
	if err != nil {
		return err
	}
	binding := provider.BindingSnapshot{Binding: profile.Binding, AccountScope: profile.AccountScope}
	makeEngine := func() (*dispatch.Engine, error) {
		p, err := fake.NewBound(backend, c, binding)
		if err != nil {
			return nil, err
		}
		registry := provider.NewSnapshotRegistry()
		if err = registry.Register(binding, p); err != nil {
			return nil, err
		}
		return dispatch.New(store, registry, blobs, nil, c, dispatch.DefaultConfig())
	}
	engine, err := makeEngine()
	if err != nil {
		return err
	}
	step := func() error {
		c.advance()
		worked, err := engine.RunOnce(ctx, "smoke_worker")
		if err != nil {
			return err
		}
		if !worked {
			return errors.New("expected one phase")
		}
		return nil
	}
	if err = step(); err != nil {
		return err
	} // Verified bytes and private fixture staging.
	if err = step(); err != nil {
		return err
	} // Fixture accepts once, but loses the response.
	before, err := jobs.Get(ctx, principal, "smoke", receipt.JobID)
	if err != nil {
		return err
	}
	if before.Attempt.State.Orchestration != domain.OrchestrationReconciling || before.Attempt.State.RemoteActivity != domain.RemoteActivityPossible {
		return errors.New("lost response did not retain uncertainty")
	}
	if err = store.Close(); err != nil {
		return err
	}
	store, err = sqlite.Open(ctx, filepath.Join(root, "state"), sqlite.DefaultOptions())
	if err != nil {
		return err
	}
	access, err = auth.New(store, store, nil)
	if err != nil {
		return err
	}
	principal, err = access.Authenticate(ctx, secret.Reveal())
	if err != nil {
		return err
	}
	jobs, err = admission.New(access, store, admission.DefaultLimits())
	if err != nil {
		return err
	}
	engine, err = makeEngine()
	if err != nil {
		return err
	}
	if err = step(); err != nil {
		return err
	} // Read-only reconciliation of the same intent.
	if err = step(); err != nil {
		return err
	} // Terminal fixture observation, NOT artifact success.
	after, err := jobs.Get(ctx, principal, "smoke", receipt.JobID)
	if err != nil {
		return err
	}
	stats := backend.Stats()
	if stats.PrepareCalls != 1 || stats.SubmitCalls != 1 || stats.Executions != 1 || after.Attempt.ID != receipt.AttemptID || after.AttemptNonce != before.AttemptNonce || after.Attempt.Number != 1 {
		return errors.New("recovery changed identity or repeated compute")
	}
	if after.Attempt.State.Execution != domain.ExecutionSucceeded || after.Attempt.State.Orchestration != domain.OrchestrationCollecting || after.Attempt.State.Result != domain.ResultNotAvailable || after.Attempt.State.RemoteActivity != domain.RemoteActivityInactive {
		return errors.New("execution and result evidence were conflated")
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"status": "passed-offline", "input_bytes_verified": len(input), "bundle_bytes_verified": len(bundle), "blob_bytes_checked": true, "restart_reconciliation": true, "fake_prepare_calls": stats.PrepareCalls, "fake_submit_calls": stats.SubmitCalls, "simulated_executions": stats.Executions, "result_state": after.Attempt.State.Result, "orchestration": after.Attempt.State.Orchestration, "external_provider_calls": 0, "payload_executions": 0})
}
func codeBundle() ([]byte, error) {
	payload := []byte("raise SystemExit('must never execute on the control host')\n")
	m := packaging.Manifest{Version: packaging.Version, Files: []packaging.File{{Path: "never-executed.py", Bytes: int64(len(payload)), SHA256: string(provider.Digest(payload))}}}
	manifest, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	gz.Header.OS = 255
	tw := tar.NewWriter(gz)
	for _, entry := range []struct {
		name string
		data []byte
	}{{packaging.ManifestPath, manifest}, {"code/never-executed.py", payload}} {
		if err = tw.WriteHeader(&tar.Header{Name: entry.name, Size: int64(len(entry.data)), Mode: 0644, Typeflag: tar.TypeReg, ModTime: time.Unix(0, 0).UTC(), Format: tar.FormatUSTAR}); err != nil {
			return nil, err
		}
		if _, err = tw.Write(entry.data); err != nil {
			return nil, err
		}
	}
	if err = tw.Close(); err != nil {
		return nil, err
	}
	if err = gz.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}
