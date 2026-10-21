package kaggle

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

// v1MarkerSHA256 is the staging marker the fixture produced before shared inputs existed.
const v1MarkerSHA256 = "71fe65503c7868eab615406ef7998654508d5f91ad618962acebeaaedcf86673"

func sharedPolicy() StagingPolicy {
	policy := DefaultStagingPolicy()
	policy.SharedMinBytes = 3 // The fixture input "abc" becomes shared; the bundle never does.
	return policy
}

func TestStagingWithoutSharedInputsKeepsV1Marker(t *testing.T) {
	c, plan := stagingFixture(t)
	plain, err := buildStagingPlan(c, DefaultStagingPolicy(), plan, "prep")
	if err != nil || plain.request.MarkerSHA256 != v1MarkerSHA256 || len(plain.shared) != 0 {
		t.Fatal("staging without shared inputs changed its marker", plain.request.MarkerSHA256, err)
	}
	plans, err := buildStagingPlans(c, sharedPolicy(), plan, "prep")
	if err != nil || len(plans) != 2 || plans[1].request.MarkerSHA256 != v1MarkerSHA256 || plans[0].request.Slug != plans[1].request.Slug {
		t.Fatal("legacy layout is not the second candidate", err)
	}
	current := plans[0]
	if !strings.Contains(string(current.marker), `"compute-relay/staging/v2"`) || len(current.shared) != 1 ||
		len(current.request.Files) != 2 || len(current.objects) != 1 || current.inputs[0] != (stagedInput{current.shared[0].request.Slug, sharedBlobFile}) {
		t.Fatal("large input was not moved to a shared dataset", string(current.marker))
	}
	other, err := buildSharedInput(c, sharedPolicy(), "other-installation", current.shared[0].object)
	if err != nil || other.request.Slug == current.shared[0].request.Slug || !strings.HasPrefix(other.request.Slug, "crx-") {
		t.Fatal("shared dataset name is not scoped to the installation", err)
	}
}

func sharedRuntime(t *testing.T) (*RuntimeAdapter, provider.Plan, *provider.Prepared, *stagingTestBlobs, *nativeFixture) {
	t.Helper()
	p, plan, saved, blobs, f := nativeRuntime(t, false)
	p.stager.policy = sharedPolicy()
	return p, plan, saved, blobs, f
}

func TestNativeSharedInputUploadsOnceAndMountsWithoutReadBack(t *testing.T) {
	p, plan, saved, _, f := sharedRuntime(t)
	ctx := context.Background()
	var progress [][2]int64
	prepared, err := p.Prepare(provider.WithPreparationProgress(ctx, func(done, total int64) {
		progress = append(progress, [2]int64{done, total})
	}), plan, "prep")
	if err != nil || !prepared.Ready || f.count("CreateDataset") != 2 || f.count("StartBlobUpload") != 4 || len(f.shared) != 1 {
		t.Fatal("miss did not create one shared and one attempt dataset", prepared, err)
	}
	if len(progress) == 0 || progress[len(progress)-1] != [2]int64{17, 17} {
		t.Fatal("upload progress lost the shared input", progress)
	}
	if _, ok := f.staged["input-000.bin"]; ok {
		t.Fatal("shared input was also staged with the attempt")
	}
	*saved = prepared
	downloads := f.count("DownloadDataset")
	out := p.Start(ctx, prepared, nil, nil)
	if out.Status != provider.SubmissionAccepted || f.count("SaveKernel") != 1 || f.count("DownloadDataset") != downloads {
		t.Fatal("submission re-read staged bytes or was not accepted", out)
	}
	var slug string
	for name := range f.shared {
		slug = name
	}
	if len(f.kernel.Datasets) != 2 || f.kernel.Datasets[1] != f.owner+"/"+slug || !strings.Contains(f.kernel.Source, "\nrun_remote_shared(") {
		t.Fatal("kernel does not mount the shared dataset", f.kernel.Datasets)
	}
	f.state = "COMPLETE"
	for _, purpose := range []string{"execution", "staging"} {
		remote, operation := *out.Remote, domain.OperationID(plan.Job.Identity.IntentID)
		if purpose == "staging" {
			remote = provider.RemoteReference{Identity: plan.Job.Identity, Resource: prepared.Resource, Version: "prepared"}
			operation = prepared.PreparationID
		}
		cleaned, err := p.CleanupResource(ctx, provider.CleanupResourceRequest{CleanupRequest: provider.CleanupRequest{Remote: remote,
			LedgerID: "res_fixture", CreationOperationID: prepared.PreparationID, Mode: provider.CleanupApply},
			Purpose: purpose, OwnershipOperationID: operation, PlanSHA256: plan.Digest()})
		if err != nil || !cleaned.Deleted {
			t.Fatal("attempt cleanup unconfirmed", purpose, cleaned, err)
		}
	}
	if f.dataset || f.kernel != nil || len(f.shared) != 1 || f.count("DeleteDataset") != 1 {
		t.Fatal("attempt cleanup touched the shared dataset")
	}

	// A later attempt with the same bytes finds the shared dataset and uploads only its own staging.
	q, plan2, _, _, g := sharedRuntime(t)
	g.shared = f.shared
	if _, err := q.Prepare(ctx, plan2, "prep"); err != nil || g.count("CreateDataset") != 1 || g.count("StartBlobUpload") != 2 {
		t.Fatal("hit uploaded the shared input again", err)
	}
}

func TestNativeSharedInputRejectsChangedOrUnreadyEntry(t *testing.T) {
	for _, mode := range []string{"marker", "extra-file", "failed", "pending"} {
		t.Run(mode, func(t *testing.T) {
			seed, plan, _, _, f := sharedRuntime(t)
			if _, err := seed.Prepare(context.Background(), plan, "prep"); err != nil {
				t.Fatal(err)
			}
			p, plan, _, _, g := sharedRuntime(t)
			g.shared = f.shared
			for _, entry := range g.shared {
				switch mode {
				case "marker":
					entry.stage.MarkerSHA256 = provider.Digest([]byte("changed"))
				case "extra-file":
					entry.staged["extra.bin"] = nil
				case "failed":
					entry.state = "FAILED"
				case "pending":
					entry.state = "BLOBS_RECEIVED"
				}
			}
			prepared, err := p.Prepare(context.Background(), plan, "prep")
			if mode == "pending" {
				if err != nil || prepared.Ready || g.count("CreateDataset") != 1 {
					t.Fatal("pending shared dataset was not awaited", prepared, err)
				}
				return
			}
			if err == nil || g.count("CreateDataset") != 0 || g.count("StartBlobUpload") != 0 {
				t.Fatal("changed shared dataset was used or replaced", err)
			}
		})
	}
}

func TestNativeSharedInputChangedAfterReadinessStopsBeforeSave(t *testing.T) {
	p, plan, saved, _, f := sharedRuntime(t)
	prepared, err := p.Prepare(context.Background(), plan, "prep")
	if err != nil || !prepared.Ready {
		t.Fatal(err)
	}
	*saved = prepared
	for _, entry := range f.shared {
		entry.staged[sharedBlobFile] = []byte("abcd")
	}
	if out := p.Start(context.Background(), prepared, nil, nil); out.Status == provider.SubmissionAccepted || f.count("SaveKernel") != 0 {
		t.Fatal("changed shared dataset reached submission", out)
	}
}

func TestNativeSharedInputLostCreateIsObservedNotRepeated(t *testing.T) {
	p, plan, _, _, f := sharedRuntime(t)
	f.fault = "lost-shared-create"
	prepared, err := p.Prepare(context.Background(), plan, "prep")
	if err != nil || !prepared.Ready || f.count("CreateDataset") != 2 || len(f.shared) != 1 || !f.dataset {
		t.Fatal("a shared dataset created without acknowledgement was not observed once", prepared, err)
	}
}

func TestNativeStagingUploadIsSentAgainOnceAndReportsItsStep(t *testing.T) {
	oldDelay := stagingUploadRetryDelay
	stagingUploadRetryDelay = time.Millisecond
	t.Cleanup(func() { stagingUploadRetryDelay = oldDelay })
	p, plan, _, _, f := sharedRuntime(t)
	f.putFailures = 1
	if prepared, err := p.Prepare(context.Background(), plan, "prep"); err != nil || !prepared.Ready ||
		f.count("StartBlobUpload") != 5 || f.count("CreateDataset") != 2 {
		t.Fatal("a failed upload was not sent again with a new ticket", err)
	}
	q, plan2, _, _, g := sharedRuntime(t)
	g.putFailures = stagingUploadTries
	_, err := q.Prepare(context.Background(), plan2, "prep")
	var problem domain.Problem
	if !errors.As(err, &problem) || !errors.Is(err, ErrStaging) || problem.Code != domain.CodeStagingFailed ||
		problem.PublicDetails()[domain.ReasonDetail] != "shared_upload_failed_http_500" || g.count("StartBlobUpload") != stagingUploadTries || g.count("CreateDataset") != 0 {
		t.Fatal("upload failure was repeated beyond its bound or lost its step", err)
	}
}

func TestNativeAttemptStagedBeforeUpgradeKeepsItsLayout(t *testing.T) {
	p, plan, saved, _, f := nativeRuntime(t, false)
	p.stager.policy.SharedMinBytes = 0
	prepared, err := p.Prepare(context.Background(), plan, "prep")
	if err != nil || !prepared.Ready {
		t.Fatal(err)
	}
	*saved = prepared
	p.stager.policy = sharedPolicy()
	if seen, err := p.ReconcilePrepare(context.Background(), plan, "prep"); err != nil || seen.Status != provider.ReconciliationFound {
		t.Fatal("legacy staging no longer reconciles", seen, err)
	}
	out := p.Start(context.Background(), prepared, nil, nil)
	if out.Status != provider.SubmissionAccepted || len(f.kernel.Datasets) != 1 || strings.Contains(f.kernel.Source, "run_remote_shared") || len(f.shared) != 0 {
		t.Fatal("legacy attempt changed layout after upgrade", out, f.kernel)
	}
}

func TestSharedBootstrapResolvesInputsAcrossMounts(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("Python unavailable for bootstrap check")
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"datasets/owner/crs-a/code.bin", "datasets/owner/crs-a/input-001.bin", "datasets/owner/crx-b/blob.bin"} {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(root, name)), 0o755); err != nil || os.WriteFile(filepath.Join(root, name), nil, 0o644) != nil {
			t.Fatal(err)
		}
	}
	source := filepath.Join(t.TempDir(), "bootstrap.py")
	if err := os.WriteFile(source, []byte(executionBootstrap+executionSharedBootstrap), 0o644); err != nil {
		t.Fatal(err)
	}
	payload := map[string]any{"dataset_owner": "owner", "dataset_slug": "crs-a", "shared_datasets": []string{"crx-b"},
		"input_files": [][2]string{{"crx-b", "blob.bin"}, {"crs-a", "input-001.bin"}},
		"manifest":    map[string]any{"bundle": map[string]any{"path": "code.bin"}, "inputs": []map[string]any{{"path": "input-000.bin"}, {"path": "input-001.bin"}}}}
	script := `import json, sys
from pathlib import Path
scope = {"__name__": "bootstrap"}
exec(open(sys.argv[1]).read(), scope)
scope["SHARED_INPUT_ROOT"] = Path(sys.argv[2])
payload = json.load(sys.stdin)
staging = Path(sys.argv[3])
try:
    root, manifest = scope["shared_staging"](payload, staging)
except RuntimeError as error:
    print(json.dumps({"error": str(error)}))
else:
    print(json.dumps({"root": str(root), "bundle": manifest["bundle"]["path"], "inputs": [x["path"] for x in manifest["inputs"]]}))
`
	staging := filepath.Join(root, "datasets", "owner", "crs-a")
	run := func(payload map[string]any, inputRoot string) map[string]any {
		raw, _ := json.Marshal(payload)
		cmd := exec.Command(python, "-I", "-c", script, source, inputRoot, staging)
		cmd.Stdin = strings.NewReader(string(raw))
		output, err := cmd.CombinedOutput()
		var result map[string]any
		if err != nil || json.Unmarshal(output, &result) != nil {
			t.Fatal("bootstrap check failed", err, string(output))
		}
		return result
	}
	got := run(payload, root)
	want := map[string]any{"root": filepath.Join(root, "datasets", "owner"), "bundle": "crs-a/code.bin", "inputs": []any{"crx-b/blob.bin", "crs-a/input-001.bin"}}
	if encoded, _ := json.Marshal(got); string(encoded) != mustJSON(t, want) {
		t.Fatal("shared inputs did not resolve under one staging root", string(encoded))
	}
	payload["shared_datasets"] = []string{"crx-missing"}
	if got := run(payload, root); got["error"] != "Compute Relay shared input mount is unavailable" {
		t.Fatal("missing shared mount was not rejected", got)
	}
	// The attempt mount lies outside this input root, so no common dataset root exists.
	other, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil || os.MkdirAll(filepath.Join(other, "datasets", "owner", "crx-b"), 0o755) != nil {
		t.Fatal(err)
	}
	payload["shared_datasets"] = []string{"crx-b"}
	if got := run(payload, other); got["error"] != "Compute Relay staging mounts do not share a dataset root" {
		t.Fatal("mounts outside the input root were accepted", got)
	}
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestCleanupDiscoveryDoesNotRequireSharedInputReadiness(t *testing.T) {
	p, plan, saved, blobs, f := sharedRuntime(t)
	if _, err := p.Prepare(t.Context(), plan, "prep"); err != nil {
		t.Fatal(err)
	}
	// Its private attempt dataset remains exact, but an independent shared input
	// has disappeared before late cleanup discovery.
	f.shared = map[string]*fixtureDataset{}
	beforeCreate, beforeUpload, beforeReads := f.count("CreateDataset"), f.count("StartBlobUpload"), blobs.reads
	blobs.mode = "missing"
	seen, err := p.ObserveStagingCleanup(t.Context(), plan, "prep")
	if err != nil || seen.Status != provider.ReconciliationFound || seen.Prepared == nil || seen.Prepared.Ready || !seen.Prepared.Private {
		t.Fatal("missing shared input blocked ownership discovery", seen, err)
	}
	*saved = *seen.Prepared
	out, err := p.CleanupResource(t.Context(), provider.CleanupResourceRequest{CleanupRequest: provider.CleanupRequest{Remote: provider.RemoteReference{Identity: plan.Job.Identity, Resource: saved.Resource, Version: "prepared"}, LedgerID: "res_fixture", CreationOperationID: "prep", Mode: provider.CleanupDryRun}, Purpose: "staging", OwnershipOperationID: "prep", PlanSHA256: plan.Digest()})
	if err != nil || !out.WouldDelete {
		t.Fatal("confirmed staging stayed uncleanable", out, err)
	}
	if f.count("CreateDataset") != beforeCreate || f.count("StartBlobUpload") != beforeUpload || f.count("DeleteDataset") != 0 || blobs.reads != beforeReads {
		t.Fatal("cleanup discovery created, uploaded or deleted", f.calls)
	}
}
