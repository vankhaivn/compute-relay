package kaggle

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle/kaggleapi"
)

func nativeRuntime(t *testing.T, gpu bool) (*RuntimeAdapter, provider.Plan, *provider.Prepared, *stagingTestBlobs, *nativeFixture) {
	t.Helper()
	oldRetry := stagingUploadRetryDelay
	stagingUploadRetryDelay = time.Millisecond
	t.Cleanup(func() { stagingUploadRetryDelay = oldRetry })
	c, plan, prepared := executionFixture(t)
	policy := DefaultExecutionPolicy()
	if gpu {
		policy.MachineShape = "NvidiaTeslaT4"
		plan.Job.Specification = []byte(strings.Replace(executionSpec, `"accelerator":"cpu"`, `"accelerator":"gpu","minimum_gpu_count":1`, 1))
		plan.Job.SpecificationSHA256 = provider.Digest(plan.Job.Specification)
		plan.Job.Required = append(plan.Job.Required, domain.CapabilityGPU)
		plan.VerifyAfterStart = []domain.CapabilityName{domain.CapabilityGPU}
	}
	blobs := &stagingTestBlobs{data: map[domain.ObjectID][]byte{"code": []byte("synthetic-code"), "input": []byte("abc")}}
	resolver, err := fixtureCredentials([]ports.CredentialRef{c.CredentialRef}, func(string) (string, bool) { return "SYNTHETIC_TOKEN", true })
	if err != nil {
		t.Fatal(err)
	}
	binding := provider.BindingSnapshot{Binding: plan.Job.Binding, AccountScope: c.AccountName, CredentialRef: string(c.CredentialRef)}
	load := func(context.Context, provider.Identity) (provider.Plan, provider.Prepared, error) {
		return plan, prepared, nil
	}
	p, err := NewRuntimeAdapter(c, binding, resolver, blobs, &stagingClock{at: time.Now().UTC()}, load, policy, true)
	if err != nil {
		t.Fatal(err)
	}
	f := newNativeFixture(t, c.AccountName)
	f.bind(p)
	return p, plan, &prepared, blobs, f
}

func (f *nativeFixture) result(id provider.Identity) {
	payload := []byte("yes")
	raw, err := json.Marshal(map[string]any{"manifest_version": "1", "runner_version": "fixture", "job_id": id.JobID,
		"attempt_id": id.AttemptID, "attempt_nonce": id.Nonce, "bundle_sha256": id.BundleSHA256, "input_manifest_sha256": id.InputManifestSHA256,
		"started_at": "2026-10-14T00:00:00Z", "finished_at": "2026-10-14T00:00:01Z", "phase": "completed", "exit_code": 0,
		"timed_out": false, "resource_check": map[string]any{"gpu_required": f.kernel.GPU, "gpu_verified": f.kernel.GPU},
		"artifacts": []map[string]any{{"path": "answer.txt", "bytes": len(payload), "sha256": provider.Digest(payload)}}, "error": nil})
	if err != nil {
		f.t.Fatal(err)
	}
	f.outputs = map[string][]byte{artifactManifest: raw, "outputs/answer.txt": payload, "control/stdout.log": []byte("finished\n")}
}

func TestNativeRuntimePrivateBatchRestartLogsArtifactsAndCleanup(t *testing.T) {
	p, plan, saved, blobs, f := nativeRuntime(t, false)
	ctx := context.Background()
	if err := p.Verify(ctx, p.binding); err != nil {
		t.Fatal(err)
	}
	validated, err := p.Validate(ctx, plan.Job)
	if err != nil || validated.Digest() != plan.Digest() {
		t.Fatal("native plan validation changed original identity", err)
	}
	var progress [][2]int64
	prepared, err := p.Prepare(provider.WithPreparationProgress(ctx, func(done, total int64) {
		progress = append(progress, [2]int64{done, total})
	}), plan, "prep")
	if err != nil || !prepared.Ready || !prepared.Private || f.count("CreateDataset") != 1 || f.count("StartBlobUpload") != 3 {
		t.Fatal("private staging not verified", prepared, err)
	}
	*saved = prepared
	if len(progress) == 0 || progress[len(progress)-1] != [2]int64{17, 17} {
		t.Fatal("input upload progress lost", progress)
	}
	out := p.Start(ctx, prepared, nil, nil)
	if out.Status != provider.SubmissionAccepted || out.Remote == nil || out.Validate(plan.Job.Identity) != nil || f.count("SaveKernel") != 1 {
		t.Fatal("native one-shot save was not accepted", out)
	}
	if retry := p.Start(ctx, prepared, nil, nil); retry.Status != provider.SubmissionUnknown || f.count("SaveKernel") != 1 {
		t.Fatal("submission was repeated")
	}
	// Reconstruct all provider components from the original durable plan and
	// prepared resource. Missing local payloads cannot cause staging to repeat.
	blobs.mode = "missing"
	restarted, err := NewRuntimeAdapter(p.config, p.binding, p.stager.credentials, blobs, p.clock, p.load, p.policy, true)
	if err != nil {
		t.Fatal(err)
	}
	f.bind(restarted)
	reconciled, err := restarted.Reconcile(ctx, plan.Job.Identity)
	if err != nil || reconciled.Status != provider.ReconciliationFound || *reconciled.Remote != *out.Remote || f.count("SaveKernel") != 1 || f.count("CreateDataset") != 1 {
		t.Fatal("restart changed or resubmitted original attempt", reconciled, err)
	}
	observation, err := restarted.Observe(ctx, *out.Remote)
	if err != nil || observation.Execution != domain.ExecutionRunning || observation.ReleaseEvidence != domain.ReleaseEvidenceNotObservable {
		t.Fatal("execution or release evidence invented", observation, err)
	}
	first, err := restarted.Logs(ctx, *out.Remote, provider.PageRequest{Limit: 1})
	if err != nil || strings.Join(first.Lines, "") != "first" || first.NextCursor == "" {
		t.Fatal("first native replay page missing", first, err)
	}
	second, err := restarted.Logs(ctx, *out.Remote, provider.PageRequest{Limit: 1, Cursor: first.NextCursor})
	if err != nil || strings.Join(second.Lines, "") != "second" {
		t.Fatal("original replay cursor skipped or repeated lines", second, err)
	}
	f.state = "COMPLETE"
	f.result(plan.Job.Identity)
	page, err := restarted.ListArtifacts(ctx, *out.Remote)
	if err != nil || len(page) != 3 {
		t.Fatal("full verified catalog missing", page, err)
	}
	for _, file := range page {
		var dst bytes.Buffer
		receipt, err := restarted.Fetch(ctx, *out.Remote, file, &dst, file.Bytes, nil)
		if err != nil || receipt.Bytes != file.Bytes || receipt.SHA256 != file.SHA256 || !bytes.Equal(dst.Bytes(), f.outputs[file.Path]) {
			t.Fatal("artifact download did not verify final bytes", file.Path, err)
		}
	}
	for _, purpose := range []string{"execution", "staging"} {
		remote, operation := *out.Remote, domain.OperationID(plan.Job.Identity.IntentID)
		if purpose == "staging" {
			remote = provider.RemoteReference{Identity: plan.Job.Identity, Resource: prepared.Resource, Version: "prepared"}
			operation = prepared.PreparationID
		}
		request := provider.CleanupResourceRequest{CleanupRequest: provider.CleanupRequest{Remote: remote, LedgerID: "res_fixture",
			CreationOperationID: prepared.PreparationID, Mode: provider.CleanupApply}, Purpose: purpose, OwnershipOperationID: operation,
			PlanSHA256: plan.Digest()}
		cleaned, err := restarted.CleanupResource(ctx, request)
		if err != nil || !cleaned.Deleted {
			t.Fatal("original private resource cleanup unconfirmed", purpose, cleaned, err)
		}
	}
	if f.kernel != nil || f.dataset || f.count("DeleteKernel") != 1 || f.count("DeleteDataset") != 1 {
		t.Fatal("original resources remain or deletion repeated")
	}
}

func TestNativeSubmissionLostAcknowledgementReconcilesWithoutSave(t *testing.T) {
	for _, tc := range []struct{ fault, reason string }{
		{"found", ""}, {"kernel-hidden", "kernel_not_visible"},
		{"kernel-denied", "lookup_access_denied"}, {"kernel-throttled", "lookup_rate_limited"},
		{"kernel-invalid", "lookup_response_invalid"}, {"absent", ""}, {"save-throttled", ""},
		{"list-pagination", ""}, {"list-unavailable", "lookup_rate_limited"},
		{"list-incomplete", "lookup_access_denied"}, {"list-loop", "lookup_response_invalid"}, {"list-wrong-owner", "lookup_response_invalid"},
	} {
		t.Run(tc.fault, func(t *testing.T) {
			p, plan, saved, _, f := nativeRuntime(t, false)
			prepared, err := p.Prepare(context.Background(), plan, "prep")
			if err != nil {
				t.Fatal(err)
			}
			*saved, f.fault = prepared, "lost-save-ack"
			if tc.fault == "save-throttled" {
				f.fault = tc.fault
			}
			out := p.Start(context.Background(), prepared, nil, nil)
			if out.Status != provider.SubmissionUnknown || !out.Problem.ComputeMayHaveStarted || out.Problem.SafeOperationRetry {
				t.Fatal("lost save acknowledgement became rejection or retry permission", out)
			}
			details := out.Problem.PublicDetails()
			if details["reason"] != "submission_save_failed" || details["resource_name"] == "" || details["submission_operation"] != "save_kernel" ||
				tc.fault == "save-throttled" && (details["submission_http_status"] != "429" || details["submission_error_type"] != "RESOURCE_EXHAUSTED") ||
				strings.Contains(out.Problem.Error(), "SYNTHETIC_TOKEN") {
				t.Fatal("submission diagnostics missing or unsafe", details)
			}
			if out := p.Start(context.Background(), prepared, nil, nil); out.Status != provider.SubmissionUnknown || f.count("SaveKernel") != 1 {
				t.Fatal("ambiguous save was repeated")
			}
			f.fault = tc.fault
			if tc.fault == "absent" {
				f.kernel = nil
			}
			seen, err := p.Reconcile(context.Background(), plan.Job.Identity)
			if tc.reason != "" {
				var problem domain.Problem
				if !errors.As(err, &problem) || problem.Code != domain.CodeProviderSubmissionUnknown ||
					problem.PublicDetails()[domain.ReasonDetail] != tc.reason || !problem.ComputeMayHaveStarted || problem.SafeOperationRetry {
					t.Fatalf("unresolved lookup lost its reason or allowed replacement: %+v, %v", seen, err)
				}
			} else if tc.fault == "absent" || tc.fault == "save-throttled" {
				if err != nil || seen.Status != provider.ReconciliationAbsent || seen.ResourceName != details["resource_name"] {
					t.Fatal("complete inventory did not resolve exact absence", seen, err)
				}
			} else if err != nil || seen.Status != provider.ReconciliationFound {
				t.Fatal("original save could not be recovered by reads", seen, err)
			}
			if f.count("SaveKernel") != 1 {
				t.Fatal("lookup repeated the save")
			}
		})
	}
}

func TestNativeGPUQuotaCheckedImmediatelyBeforeSave(t *testing.T) {
	for _, free := range []bool{true, false} {
		t.Run(map[bool]string{true: "free", false: "paid"}[free], func(t *testing.T) {
			p, plan, saved, _, f := nativeRuntime(t, true)
			prepared, err := p.Prepare(context.Background(), plan, "prep")
			if err != nil {
				t.Fatal(err)
			}
			*saved = prepared
			if !free {
				f.quota = `{"gpuQuota":{"isPayToScaleEnabled":true}}`
			}
			out := p.Start(context.Background(), prepared, nil, nil)
			if !free {
				if out.Status != provider.SubmissionRejected || out.Problem.ComputeMayHaveStarted || f.count("SaveKernel") != 0 {
					t.Fatal("paid quota reached save", out)
				}
				return
			}
			if out.Status != provider.SubmissionAccepted || f.kernel == nil || !f.kernel.GPU || f.kernel.MachineShape != "NvidiaTeslaT4" {
				t.Fatal("explicit GPU binding lost", out)
			}
			for i, name := range f.calls {
				if name == "SaveKernel" && (i == 0 || f.calls[i-1] != "GetAcceleratorQuotaStatistics") {
					t.Fatal("GPU save did not immediately recheck free quota", f.calls)
				}
			}
		})
	}
}

func TestNativeStagingLostCreateAckAndObservationNeverUpload(t *testing.T) {
	p, plan, _, blobs, f := nativeRuntime(t, false)
	f.fault = "lost-create-ack"
	if prepared, err := p.Prepare(context.Background(), plan, "prep"); err != nil || !prepared.Ready {
		t.Fatal("exact observation did not resolve lost creation acknowledgement", err)
	}
	blobs.mode = "missing"
	before := blobs.reads
	seen, err := p.ReconcilePrepare(context.Background(), plan, "prep")
	if err != nil || seen.Status != provider.ReconciliationFound || seen.Prepared == nil || !seen.Prepared.Ready || f.count("CreateDataset") != 1 || f.count("StartBlobUpload") != 3 || blobs.reads != before {
		t.Fatal("recovery repeated staging or lost original identity", seen, err)
	}
}

func TestNativeStagingRecoversRateLimitedReadsWithoutRepeatingUploads(t *testing.T) {
	for _, fault := range []string{"throttle-before-create", "throttle-after-create"} {
		t.Run(fault, func(t *testing.T) {
			p, plan, _, blobs, f := nativeRuntime(t, false)
			f.fault = fault
			prepared, err := p.Prepare(context.Background(), plan, "prep")
			if err != nil || !prepared.Ready || !prepared.Private || f.count("CreateDataset") != 1 || f.count("StartBlobUpload") != 3 {
				t.Fatal("rate-limited metadata read stranded staging or repeated mutations", prepared, err, f.calls)
			}
			blobs.mode = "missing"
			seen, err := p.ReconcilePrepare(context.Background(), plan, "prep")
			if err != nil || seen.Status != provider.ReconciliationFound || seen.Prepared == nil || seen.Prepared.Resource != prepared.Resource || f.count("CreateDataset") != 1 || f.count("StartBlobUpload") != 3 {
				t.Fatal("subsequent reconciliation changed the original staging", seen, err, f.calls)
			}
		})
	}
}

func TestNativeStagingWaitsReadOnlyUntilCreatedDatasetIsVisible(t *testing.T) {
	shortVisibility(t, time.Minute, time.Millisecond)
	p, plan, _, _, f := nativeRuntime(t, false)
	f.fault, f.hiddenReads = "missing-name-403", 4
	prepared, err := p.Prepare(context.Background(), plan, "prep")
	if err != nil || !prepared.Ready || f.hiddenReads != 0 || f.count("CreateDataset") != 1 || f.count("StartBlobUpload") != 3 {
		t.Fatal("a created dataset that became visible later failed or was staged again", prepared, err)
	}
}

func TestNativeStagingNeverVisibleStopsWithoutAnotherCreate(t *testing.T) {
	for _, fault := range []string{"missing-name-403", "lost-create-ack"} {
		t.Run(fault, func(t *testing.T) {
			shortVisibility(t, 20*time.Millisecond, time.Millisecond)
			p, plan, _, _, f := nativeRuntime(t, false)
			f.fault, f.hiddenReads = fault, 1<<30
			_, err := p.Prepare(context.Background(), plan, "prep")
			var problem domain.Problem
			if !errors.As(err, &problem) || problem.Code != domain.CodeStagingFailed || problem.PublicDetails()[domain.ReasonDetail] != "staging_not_visible" ||
				f.count("CreateDataset") != 1 || f.count("StartBlobUpload") != 3 || f.count("GetDataset") < 4 {
				t.Fatal("an invisible created dataset was not observed read-only until the grace ended", err, f.calls)
			}
		})
	}
}

func TestNativeStagingReadBackFailureNamesItsStep(t *testing.T) {
	p, plan, _, _, f := nativeRuntime(t, false)
	f.fault = "status-failed"
	_, err := p.Prepare(context.Background(), plan, "prep")
	var problem domain.Problem
	if !errors.As(err, &problem) || problem.PublicDetails()[domain.ReasonDetail] != "staging_status_failed_http_500" || f.count("CreateDataset") != 1 {
		t.Fatal("a failed read-back after creation lost its step", err)
	}
}

// Relay's own uploads are SHA-checked and the remote runner rehashes every input, so
// readiness never reads the attempt's bytes back.
func TestNativeStagingReadyByMetadataWithoutReadBack(t *testing.T) {
	p, plan, _, blobs, f := nativeRuntime(t, false)
	prepared, err := p.Prepare(context.Background(), plan, "prep")
	if err != nil || !prepared.Ready || !prepared.Private || f.count("DownloadDataset") != 0 || f.transferred != 0 {
		t.Fatal("fresh staging was not ready by metadata alone", prepared, err)
	}
	blobs.mode = "missing"
	seen, err := p.ReconcilePrepare(context.Background(), plan, "prep")
	if err != nil || seen.Status != provider.ReconciliationFound || seen.Prepared == nil || !seen.Prepared.Ready || seen.Prepared.Resource != prepared.Resource ||
		f.count("DownloadDataset") != 0 || f.count("CreateDataset") != 1 || f.count("StartBlobUpload") != 3 {
		t.Fatal("observation read bytes back or repeated staging", seen, err)
	}
}

func TestNativeStagingConfirmsPrivacyByReadingAgain(t *testing.T) {
	for reads, private := range map[int]bool{2: true, 10: false} {
		p, plan, _, _, f := nativeRuntime(t, false)
		f.publicReads = reads
		prepared, err := p.Prepare(context.Background(), plan, "prep")
		if err != nil || prepared.Private != private || f.count("CreateDataset") != 1 {
			t.Fatal("privacy was reported from a single read", reads, prepared, err)
		}
	}
}

func TestNativeStagingRejectsChangedCompleteCatalogAndIdentity(t *testing.T) {
	for _, fault := range []string{"extra-staging-file", "resized-staging-file", "changed-marker", "dataset-replaced", "public-dataset", "pending-dataset"} {
		t.Run(fault, func(t *testing.T) {
			p, plan, _, _, f := nativeRuntime(t, false)
			f.fault = fault
			prepared, err := p.Prepare(context.Background(), plan, "prep")
			if fault == "public-dataset" || fault == "pending-dataset" {
				if err != nil || prepared.Ready || prepared.Private != (fault != "public-dataset") {
					t.Fatal("pending/public staging state was not retained", prepared, err)
				}
			} else if err == nil {
				t.Fatal("changed staging was accepted")
			}
			if f.count("CreateDataset") != 1 || f.count("DownloadDataset") != 0 {
				t.Fatal("staging retry after uncertainty or byte read-back")
			}
		})
	}
}

func TestNativeStagingUploadRequiresEverySourceEOFHashAndClose(t *testing.T) {
	for _, fault := range []string{"short", "long", "digest", "late", "close", "missing"} {
		t.Run(fault, func(t *testing.T) {
			p, plan, _, blobs, f := nativeRuntime(t, false)
			stagePlan, err := buildStagingPlan(p.config, p.stager.policy, plan, "prep")
			if err != nil {
				t.Fatal(err)
			}
			blobs.mode = fault
			client, err := f.client([]byte("SYNTHETIC_TOKEN"))
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			response, err := stage(context.Background(), client, "create", stagePlan, blobs)
			if err != nil || response.Status != "unknown" || f.count("CreateDataset") != 0 {
				t.Fatal("uncertain upload authorized dataset creation", response, err)
			}
		})
	}
}

func TestNativeArtifactFinalIdentityAndPinnedManifestMustHold(t *testing.T) {
	for _, fault := range []string{"kernel-replaced", "manifest-changed", "payload-changed", "unresolved-state", "undeclared", "required-missing"} {
		t.Run(fault, func(t *testing.T) {
			p, plan, saved, _, f := nativeRuntime(t, false)
			prepared, err := p.Prepare(context.Background(), plan, "prep")
			if err != nil {
				t.Fatal(err)
			}
			*saved = prepared
			out := p.Start(context.Background(), prepared, nil, nil)
			if out.Status != provider.SubmissionAccepted {
				t.Fatal(out)
			}
			f.state = "COMPLETE"
			f.result(plan.Job.Identity)
			page, err := p.ListArtifacts(context.Background(), *out.Remote)
			if err != nil {
				t.Fatal(err)
			}
			file := page[len(page)-1]
			switch fault {
			case "kernel-replaced":
				f.getKernel = 0
				f.fault = fault
			case "manifest-changed":
				file = page[0]
				f.outputs[artifactManifest] = append(f.outputs[artifactManifest], ' ')
			case "payload-changed":
				f.outputs[file.Path] = []byte("bad")
			case "unresolved-state":
				f.state = "CANCEL_ACKNOWLEDGED"
			case "undeclared", "required-missing":
				var manifest map[string]any
				_ = json.Unmarshal(f.outputs[artifactManifest], &manifest)
				if fault == "undeclared" {
					manifest["artifacts"].([]any)[0].(map[string]any)["path"] = "private.txt"
				} else {
					manifest["artifacts"] = []any{}
				}
				f.outputs[artifactManifest], _ = json.Marshal(manifest)
			}
			if _, err := p.Fetch(context.Background(), *out.Remote, file, io.Discard, file.Bytes, nil); err == nil {
				t.Fatal("changed artifact or final identity accepted")
			}
		})
	}
}

func TestNativeCleanupRecoveryMissingName403IsUnknown(t *testing.T) {
	f := newNativeFixture(t, "fixture_user")
	f.fault = "missing-name-403"
	client, err := f.client([]byte("SYNTHETIC_TOKEN"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	request := cleanupRequest{Protocol: 1, Owner: f.owner, Slug: "crs-" + strings.Repeat("a", 40), Kind: "dataset", ID: "123", SHA256: provider.Digest(nil)}
	if seen := cleanup(context.Background(), client, "recover", request); seen.Status != "unknown" || f.count("DeleteDataset") != 0 || f.count("StartBlobUpload") != 0 {
		t.Fatal("missing-name 403 became deletion evidence or a mutation", seen)
	}
}

func TestNativeCleanupChecksIdentityAndNeverRepeatsUncertainDelete(t *testing.T) {
	for _, kind := range []string{"dataset", "kernel"} {
		for _, fault := range []string{"recover", "replacement", "public", "ownership-bytes", "active", "lost-ack"} {
			if kind == "dataset" && fault == "active" {
				continue
			}
			t.Run(kind+"/"+fault, func(t *testing.T) {
				f := newNativeFixture(t, "fixture_user")
				marker := []byte("original-marker")
				f.dataset, f.state = true, "COMPLETE"
				f.stage = stagingRequest{Owner: f.owner, Slug: "crs-" + strings.Repeat("a", 40), MarkerSHA256: provider.Digest(marker), License: "other"}
				f.staged["relay-stage.bin"] = marker
				f.kernel = &executionRequest{Owner: f.owner, Slug: "cre-" + strings.Repeat("b", 40), Source: "original-source", Datasets: []string{f.owner + "/" + f.stage.Slug}}
				request := cleanupRequest{Protocol: 1, Owner: f.owner, Slug: f.stage.Slug, Kind: kind, ID: f.datasetID,
					SHA256: f.stage.MarkerSHA256}
				operation := "DeleteDataset"
				if kind == "kernel" {
					request.Slug, request.ID, request.SHA256 = f.kernel.Slug, f.kernelID, provider.Digest([]byte(f.kernel.Source))
					operation = "DeleteKernel"
				}
				want, mode := "identity_mismatch", "apply"
				switch fault {
				case "recover":
					mode, want = "recover", "unknown"
				case "replacement":
					request.ID = "999"
				case "public":
					f.fault = "public-" + kind
				case "ownership-bytes":
					if kind == "dataset" {
						f.staged["relay-stage.bin"] = []byte("changed-marker")
					} else {
						f.kernel.Source = "changed-source"
					}
				case "active":
					f.state, want = "RUNNING", "active"
				case "lost-ack":
					f.fault, want = "lost-delete-ack", "unknown"
				}
				client, err := f.client([]byte("SYNTHETIC_TOKEN"))
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				seen := cleanup(context.Background(), client, mode, request)
				if seen.Status != want {
					t.Fatal("unsafe cleanup conclusion", seen, want)
				}
				if fault == "lost-ack" {
					if f.count(operation) != 1 {
						t.Fatal("cleanup did not send exactly once")
					}
					seen = cleanup(context.Background(), client, "recover", request)
					if seen.Status != "absent" || f.count(operation) != 1 {
						t.Fatal("recovery repeated deletion or ignored exact 404", seen)
					}
				} else if f.count(operation) != 0 {
					t.Fatal("refusal or recovery mutated provider")
				}
			})
		}
	}
}

func TestNativeLogsRedactReplayAndCheckFinalKernelIdentity(t *testing.T) {
	e := newExecutionFixture(t)
	f := newNativeFixture(t, e.request.Owner)
	f.kernel = &e.request
	f.logs = `[{"data":"first SYNTHETIC_TOKEN\nsecond\n"}]`
	request := monitorRequest{Protocol: 1, Owner: f.owner, Execution: &e.request, LogLimit: 1}
	request.Execution.KernelID = f.kernelID
	client, err := f.client([]byte("SYNTHETIC_TOKEN"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	first := monitor(context.Background(), client, "logs", request)
	text, err := base64.StdEncoding.DecodeString(first.TextB64)
	if err != nil || !first.valid("logs") || string(text) != "first [REDACTED]\n" || !first.Replay {
		t.Fatal("native replay exposed a token or lost the first line", first)
	}
	request.LogOffset, request.LogPrefix = first.Offset, first.Prefix
	second := monitor(context.Background(), client, "logs", request)
	text, _ = base64.StdEncoding.DecodeString(second.TextB64)
	if string(text) != "second\n" || second.Offset <= first.Offset {
		t.Fatal("native replay cursor repeated or skipped data", second)
	}
	f.logs = `[{"data":"changed\nsecond\n"}]`
	if reset := monitor(context.Background(), client, "logs", request); reset.Status != "reset" || reset.TextB64 != "" {
		t.Fatal("changed replay retained bytes", reset)
	}
	f.logs = `[{"data":"first SYNTHETIC_TOKEN\nsecond\n"}]`
	f.getKernel, f.fault = 0, "kernel-replaced"
	if changed := monitor(context.Background(), client, "logs", request); changed.Status != "invalid" || changed.TextB64 != "" {
		t.Fatal("final identity change released logs", changed)
	}
}

func TestNativeDiscoveryAndPreflightShareReadOnlyQuotaPath(t *testing.T) {
	for _, quota := range []string{`{}`, `{"gpuQuota":{"isPayToScaleEnabled":true}}`, `{"gpuQuota":{"totalTimeAllowed":"0s","timeUsed":"0s","timeReserved":"0s"}}`} {
		f := newNativeFixture(t, "fixture_user")
		f.quota = quota
		client, err := f.client([]byte("SYNTHETIC_TOKEN"))
		if err != nil {
			t.Fatal(err)
		}
		result := discover(context.Background(), client)
		report := preflight(context.Background(), client, f.owner, true)
		_ = client.Close()
		if !result.valid() || result.Status != "verified" || result.Account != f.owner || !report.valid(ReadOnly) || report.Authentication != "verified" || f.count("IntrospectToken") != 2 || f.count("GetAcceleratorQuotaStatistics") != 2 || len(f.calls) != 4 {
			t.Fatal("read-only evidence paths diverged", result, report)
		}
	}
	client, err := kaggleapi.New([]byte("SYNTHETIC_TOKEN"), kaggleapi.Options{APITransport: apiRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("SYNTHETIC_TOKEN")
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if result := discover(context.Background(), client); result.Status != "unavailable" || result.Account != "" {
		t.Fatal("failed discovery retained account", result)
	}
}

func TestNativeUnconfirmedStagingErrorsDoNotProveAbsenceOrRepeatCreation(t *testing.T) {
	for _, fault := range []string{"lookup-denied", "lookup-throttled", "missing-name-403"} {
		t.Run(fault, func(t *testing.T) {
			p, plan, _, blobs, f := nativeRuntime(t, false)
			f.fault = fault
			before := blobs.reads
			seen, err := p.ReconcilePrepare(t.Context(), plan, "prep")
			want := provider.ReconciliationUnknown
			if fault == "missing-name-403" {
				want = provider.ReconciliationNotFound
			}
			if err != nil || seen.Status != want || seen.Prepared != nil || blobs.reads != before || f.count("CreateDataset") != 0 || f.count("StartBlobUpload") != 0 || f.count("SaveKernel") != 0 || f.count("DeleteDataset") != 0 {
				t.Fatal("uncertain metadata caused a mutation or invented confirmation", seen, err, f.calls)
			}
		})
	}
}
