package collection

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func manifestFixture(t *testing.T) (Work, map[string]any, map[string][]byte) {
	t.Helper()
	at := time.Date(2026, 9, 15, 10, 0, 0, 0, time.UTC)
	identity := provider.Identity{InstallationID: "runtime", WorkspaceID: "workspace", JobID: "job", AttemptID: "attempt", InstanceID: "fake", IntentID: "intent", ResourceKey: "resource", Nonce: strings.Repeat("n", 32), BundleSHA256: provider.Digest([]byte("code")), InputManifestSHA256: provider.Digest([]byte("inputs"))}
	binding := domain.ProviderBinding{Profile: "fixture", ProviderInstanceID: "fake", ConfigurationRevision: "one"}
	spec := []byte(`{"outputs":[{"path":"answer.json","required":true}],"resources":{"accelerator":"cpu"}}`)
	remote := provider.RemoteReference{Identity: identity, Resource: "resource", Version: "one"}
	w := Work{Lease: Lease{WorkspaceID: "workspace", JobID: "job", AttemptID: "attempt", OperationID: "operation", Generation: 1, Fence: strings.Repeat("f", 64), Until: at.Add(time.Hour), AttemptRevision: 1}, Plan: provider.Plan{Job: provider.ResolvedJob{Identity: identity, Binding: binding, Specification: spec, SpecificationSHA256: provider.Digest(spec), WallSeconds: 10, Required: []domain.CapabilityName{domain.CapabilityBatchExecution}}}, Binding: provider.BindingSnapshot{Binding: binding, AccountScope: "account"}, Observation: provider.Observation{Remote: remote, Execution: domain.ExecutionSucceeded, RemoteActivity: domain.RemoteActivityInactive, ReleaseEvidence: domain.ReleaseEvidenceNotObservable, RawState: "completed", ObservedAt: at}}
	files := map[string][]byte{"outputs/answer.json": []byte("{\"answer\":42}\n"), "control/stdout.log": []byte("fixture log\n")}
	m := map[string]any{"manifest_version": "1", "job_id": identity.JobID, "attempt_id": identity.AttemptID, "attempt_nonce": identity.Nonce, "runner_version": "fixture", "bundle_sha256": identity.BundleSHA256, "input_manifest_sha256": identity.InputManifestSHA256, "started_at": at.Add(-time.Second).Format(time.RFC3339), "finished_at": at.Format(time.RFC3339), "phase": "completed", "exit_code": 0, "timed_out": false, "resource_check": map[string]any{"gpu_required": false, "gpu_verified": false}, "artifacts": []any{map[string]any{"path": "answer.json", "bytes": len(files["outputs/answer.json"]), "sha256": provider.Digest(files["outputs/answer.json"])}}, "error": nil}
	if !w.Valid() {
		t.Fatal("invalid collection work fixture")
	}
	return w, m, files
}
func snapshotFixture(t *testing.T, w Work, m map[string]any, files map[string][]byte) ([]byte, []provider.Artifact) {
	t.Helper()
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	catalog := []provider.Artifact{{Remote: w.Observation.Remote, Path: ManifestPath, Bytes: int64(len(raw)), SHA256: provider.Digest(raw)}}
	for path, data := range files {
		catalog = append(catalog, provider.Artifact{Remote: w.Observation.Remote, Path: path, Bytes: int64(len(data)), SHA256: provider.Digest(data)})
	}
	return raw, catalog
}
func TestManifestIdentityOutputsAndFrozenRequirements(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Work, map[string]any, map[string][]byte)
	}{
		{"nonce", func(_ *Work, m map[string]any, _ map[string][]byte) { m["attempt_nonce"] = strings.Repeat("x", 32) }},
		{"job", func(_ *Work, m map[string]any, _ map[string][]byte) { m["job_id"] = "other" }},
		{"attempt", func(_ *Work, m map[string]any, _ map[string][]byte) { m["attempt_id"] = "other" }},
		{"bundle", func(_ *Work, m map[string]any, _ map[string][]byte) {
			m["bundle_sha256"] = provider.Digest([]byte("changed"))
		}},
		{"inputs", func(_ *Work, m map[string]any, _ map[string][]byte) {
			m["input_manifest_sha256"] = provider.Digest([]byte("changed"))
		}},
		{"missing", func(_ *Work, _ map[string]any, f map[string][]byte) { delete(f, "outputs/answer.json") }},
		{"wrong digest", func(_ *Work, _ map[string]any, f map[string][]byte) { f["outputs/answer.json"] = []byte("changed") }},
		{"omitted required", func(_ *Work, m map[string]any, _ map[string][]byte) { m["artifacts"] = []any{} }},
		{"unknown field", func(_ *Work, m map[string]any, _ map[string][]byte) { m["unexpected"] = true }},
		{"timestamp order", func(_ *Work, m map[string]any, _ map[string][]byte) { m["started_at"] = "2026-09-16T00:00:00Z" }},
		{"timeout contradiction", func(_ *Work, m map[string]any, _ map[string][]byte) { m["timed_out"] = true }},
		{"gpu lie", func(w *Work, _ map[string]any, _ map[string][]byte) {
			w.Plan.Job.Specification = []byte(strings.Replace(string(w.Plan.Job.Specification), "cpu", "gpu", 1))
			w.Plan.Job.SpecificationSHA256 = provider.Digest(w.Plan.Job.Specification)
		}},
		{"unsafe provider path", func(_ *Work, _ map[string]any, f map[string][]byte) { f["../escape"] = []byte("x") }},
		{"case collision", func(_ *Work, _ map[string]any, f map[string][]byte) { f["OUTPUTS/answer.json"] = []byte("x") }},
		{"prefix collision", func(_ *Work, _ map[string]any, f map[string][]byte) { f["outputs"] = []byte("x") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, m, files := manifestFixture(t)
			tc.change(&w, m, files)
			raw, catalog := snapshotFixture(t, w, m, files)
			if _, err := BuildSnapshot(w, raw, catalog, DefaultConfig()); err == nil {
				t.Fatal("unsafe result accepted")
			}
		})
	}
	w, m, files := manifestFixture(t)
	files["scratch/unselected.txt"] = []byte("must not publish")
	raw, catalog := snapshotFixture(t, w, m, files)
	snapshot, err := BuildSnapshot(w, raw, catalog, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Files) != 3 || ValidateSnapshot(w, snapshot) != nil {
		t.Fatal("selected result invalid")
	}
	if (Verified{}).Matches(w) {
		t.Fatal("forged zero proof accepted")
	}
	changed := snapshot.Clone()
	changed.Files[0].ID = "foreign"
	if ValidateSnapshot(w, changed) == nil {
		t.Fatal("artifact identity tampering accepted")
	}
	cfg := DefaultConfig()
	cfg.MaxBytes = 1
	if _, err = BuildSnapshot(w, raw, catalog, cfg); err == nil {
		t.Fatal("byte ceiling ignored")
	}
	cfg = DefaultConfig()
	cfg.MaxFiles = 1
	if _, err = BuildSnapshot(w, raw, catalog, cfg); err == nil {
		t.Fatal("file ceiling ignored")
	}
	catalog[0].Remote.Version = "other"
	if _, err = BuildSnapshot(w, raw, catalog, DefaultConfig()); err == nil {
		t.Fatal("cross-version catalog accepted")
	}
}
func TestFailureArtifactsDoNotInventSuccess(t *testing.T) {
	w, m, files := manifestFixture(t)
	m["phase"] = "failed"
	m["exit_code"] = 2
	m["error"] = map[string]any{"code": "COMMAND_FAILED", "message": "payload failed", "stage": "execution"}
	m["artifacts"] = []any{}
	delete(files, "outputs/answer.json")
	raw, catalog := snapshotFixture(t, w, m, files)
	snapshot, err := BuildSnapshot(w, raw, catalog, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Files) != 2 {
		t.Fatal("failure manifest/logs not retained")
	}
	s := domain.InitialAttemptState()
	s.Execution = domain.ExecutionSucceeded
	s.RemoteActivity = domain.RemoteActivityInactive
	s.Orchestration = domain.OrchestrationCollecting
	s.Result = domain.ResultCollecting
	final, err := FinalState(s, "failed")
	if err != nil || final.Orchestration != domain.OrchestrationFailed || final.Execution != s.Execution || final.ReleaseEvidence != s.ReleaseEvidence || final.Result != domain.ResultAvailable {
		t.Fatal("wrapper success fabricated payload success", err)
	}
	s.Execution = domain.ExecutionCancelled
	final, err = FinalState(s, "cancelled")
	if err != nil || final.Orchestration != domain.OrchestrationNeedsAttention || final.Cancellation != domain.CancellationNotRequested {
		t.Fatal("spontaneous provider cancellation invented local intent", err)
	}
}
func TestRequiredDirectoriesAndAggregateBounds(t *testing.T) {
	w, m, files := manifestFixture(t)
	w.Plan.Job.Specification = []byte(`{"outputs":[{"path":"dir","kind":"directory","required":true,"max_bytes":2}],"resources":{"accelerator":"cpu"}}`)
	w.Plan.Job.SpecificationSHA256 = provider.Digest(w.Plan.Job.Specification)
	delete(files, "outputs/answer.json")
	files["outputs/dir/a"] = []byte("a")
	files["outputs/dir/b"] = []byte("b")
	m["artifacts"] = []any{map[string]any{"path": "dir/a", "bytes": 1, "sha256": provider.Digest([]byte("a"))}, map[string]any{"path": "dir/b", "bytes": 1, "sha256": provider.Digest([]byte("b"))}}
	raw, catalog := snapshotFixture(t, w, m, files)
	if _, err := BuildSnapshot(w, raw, catalog, DefaultConfig()); err != nil {
		t.Fatal(err)
	}
	w.Plan.Job.Specification = []byte(strings.Replace(string(w.Plan.Job.Specification), `"max_bytes":2`, `"max_bytes":1`, 1))
	w.Plan.Job.SpecificationSHA256 = provider.Digest(w.Plan.Job.Specification)
	if _, err := BuildSnapshot(w, raw, catalog, DefaultConfig()); err == nil {
		t.Fatal("directory aggregate ceiling ignored")
	}
	m["artifacts"] = []any{}
	raw, catalog = snapshotFixture(t, w, m, files)
	if _, err := BuildSnapshot(w, raw, catalog, DefaultConfig()); err == nil {
		t.Fatal("empty required directory invented")
	}
}
