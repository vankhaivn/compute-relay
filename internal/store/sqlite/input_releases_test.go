package sqlite

import (
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/retention"
)

func inputReleaser(t *testing.T, f *dispatchFixture, inputs, results retention.BlobDeleter) *retention.InputReleaser {
	t.Helper()
	access, err := auth.New(f.s, f.s)
	requireNoError(t, err)
	sweeper, err := retention.NewSweeper(f.s, inputs, results)
	requireNoError(t, err)
	service, err := retention.NewInputReleaser(access, f.s, sweeper)
	requireNoError(t, err)
	return service
}
func TestInputReleaseExactPreviewReplayAndRecovery(t *testing.T) {
	ctx := t.Context()
	f, id, p, results := collectionFixture(t)
	runCollection(t, collectionEngine(t, f, p, results, f.s))
	_, actor, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	_, reader, _ := controlService(t, f, "a", auth.Read)
	inputs := &failingReleaseBlobs{Store: f.blobs, fail: true}
	service := inputReleaser(t, f, inputs, results)
	request := retention.InputReleaseRequest{Jobs: []retention.InputReleaseJob{{JobID: id.JobID, ReleaseRequest: releaseRequest(t, f)}}}
	wrong := request
	wrong.Jobs = append([]retention.InputReleaseJob(nil), request.Jobs...)
	wrong.Jobs[0].SHA256 = domain.SHA256Digest(strings.Repeat("a", 64))
	if _, err := service.Preview(ctx, actor, "a", wrong); !errors.Is(err, retention.ErrChanged) {
		t.Fatal("wrong output accepted", err)
	}
	preview, err := service.Preview(ctx, reader, "a", request)
	requireNoError(t, err)
	if preview.Files != 2 || preview.ReleasableBytes <= 0 {
		t.Fatal("exact upload inventory missing", preview)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM input_releases") != 0 {
		t.Fatal("preview recorded release")
	}
	request.PreviewSHA256 = preview.PreviewSHA256
	if _, err := service.Release(ctx, reader, "a", "input-original", request); !errors.Is(err, auth.ErrForbidden) {
		t.Fatal("read scope deleted inputs", err)
	}
	_, err = f.s.db.Exec(`CREATE TRIGGER input_release_fault BEFORE INSERT ON input_releases BEGIN SELECT RAISE(ABORT,'synthetic rollback'); END`)
	requireNoError(t, err)
	if _, err := service.Release(ctx, actor, "a", "input-original", request); err == nil {
		t.Fatal("failed intent committed")
	}
	if inputs.calls != 0 || controlCount(t, f.s, "SELECT count(*) FROM retention_inventory WHERE expired_at IS NOT NULL") != 0 {
		t.Fatal("deletion preceded durable intent")
	}
	_, err = f.s.db.Exec(`DROP TRIGGER input_release_fault`)
	requireNoError(t, err)
	receipt, err := service.Release(ctx, actor, "a", "input-original", request)
	requireNoError(t, err)
	if receipt.PendingFiles != 2 || receipt.Bytes != preview.ReleasableBytes || receipt.ReclaimedBytes != 0 {
		t.Fatal("receipt disagrees with preview", receipt, preview)
	}
	if f.state(t, id).Result != domain.ResultAvailable {
		t.Fatal("input release changed result")
	}
	f.restart(t)
	// The ordinary sweep, rather than the consumer replay, recovers both tombstones.
	sweeper, err := retention.NewSweeper(f.s, f.blobs, results)
	requireNoError(t, err)
	_, _, err = sweeper.SweepOnce(ctx, retention.DefaultPolicy(), 0, 100)
	requireNoError(t, err)
	service = inputReleaser(t, f, f.blobs, results)
	replay, err := service.Release(ctx, actor, "a", "input-original", request)
	requireNoError(t, err)
	if replay.PendingFiles != 0 || replay.ReclaimedBytes != preview.ReleasableBytes || replay.ReleasedAt != receipt.ReleasedAt {
		t.Fatal("recovery lost receipt", replay)
	}
	again, err := service.Release(ctx, actor, "a", "input-original", request)
	requireNoError(t, err)
	if !reflect.DeepEqual(again, replay) {
		t.Fatal("replay changed result", again, replay)
	}
	wrong.PreviewSHA256 = request.PreviewSHA256
	if _, err := service.Release(ctx, actor, "a", "input-original", wrong); !errors.Is(err, retention.ErrReleaseConflict) {
		t.Fatal("key reused for another target", err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM jobs") != 1 || controlCount(t, f.s, "SELECT count(*) FROM artifacts") != 5 {
		t.Fatal("input release erased history")
	}
	assertNoNewCompute(t, f)
}
func TestInputReleaseSharedActiveAndWindowGuards(t *testing.T) {
	ctx := t.Context()
	f, id, p, results := collectionFixture(t)
	runCollection(t, collectionEngine(t, f, p, results, f.s))
	_, actor, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	service := inputReleaser(t, f, f.blobs, results)
	request := retention.InputReleaseRequest{Jobs: []retention.InputReleaseJob{{JobID: id.JobID, ReleaseRequest: releaseRequest(t, f)}}}
	preview, err := service.Preview(ctx, actor, "a", request)
	requireNoError(t, err)
	other := f.seed(t, "a", 2, false)
	blocked, err := service.Preview(ctx, actor, "a", request)
	requireNoError(t, err)
	if blocked.ReleasableBytes != 0 {
		t.Fatal("shared active job did not pin", blocked)
	}
	request.PreviewSHA256 = preview.PreviewSHA256
	if _, err := service.Release(ctx, actor, "a", "stale-preview", request); !errors.Is(err, retention.ErrChanged) {
		t.Fatal("stale preview broadened deletion", err)
	}
	controls, who, _ := controlService(t, f, "a", auth.Operate)
	cancelControl(t, f, controls, who, other)
	request.PreviewSHA256 = ""
	blocked, err = service.Preview(ctx, actor, "a", request)
	requireNoError(t, err)
	if blocked.ReleasableBytes != 0 {
		t.Fatal("shared unselected job window ignored", blocked)
	}
	f.clock.advance(8 * 24 * time.Hour)
	eligible, err := service.Preview(ctx, actor, "a", request)
	requireNoError(t, err)
	if eligible.ReleasableBytes != preview.ReleasableBytes {
		t.Fatal("elapsed shared window did not unblock", eligible)
	}
	request.PreviewSHA256 = eligible.PreviewSHA256
	receipt, err := service.Release(ctx, actor, "a", "after-window", request)
	requireNoError(t, err)
	if receipt.ReclaimedBytes != eligible.ReleasableBytes {
		t.Fatal("eligible shared bytes not deleted", receipt)
	}
}
func TestInputReleaseRefusesHeldCollection(t *testing.T) {
	f, id, p, results := collectionFixture(t)
	runCollection(t, collectionEngine(t, f, p, results, f.s))
	_, actor, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	service := inputReleaser(t, f, f.blobs, results)
	request := retention.InputReleaseRequest{Jobs: []retention.InputReleaseJob{{JobID: id.JobID, ReleaseRequest: releaseRequest(t, f)}}}
	_, err := f.s.db.Exec(`UPDATE work_leases SET held=1,generation=generation+1,fence=? WHERE kind='collection'`, strings.Repeat("b", 64))
	requireNoError(t, err)
	if _, err := service.Preview(t.Context(), actor, "a", request); !errors.Is(err, retention.ErrPinned) {
		t.Fatal("held collection lost guard", err)
	}
}
func TestCancelledDiscardedInputRetentionPreservesMismatchHistory(t *testing.T) {
	scenario := fake.DefaultScenario()
	scenario.States = []domain.ExecutionState{domain.ExecutionQueued, domain.ExecutionRunning, domain.ExecutionCancelled}
	f := newDispatchFixture(t, scenario)
	id := f.seed(t, "a", 1, false)
	submittedControl(t, f, id)
	controls, actor, _ := controlService(t, f, "a", auth.Operate)
	probe := installCancelProbe(t, f, true, provider.CancellationOutcome{Status: domain.CancellationConfirmed, TerminationConfirmed: true})
	probe.execution = domain.ExecutionCancelled
	cancelControl(t, f, controls, actor, id)
	requireNoError(t, f.step(t))
	for i := 0; f.journal(t, id).Observation == nil || !f.journal(t, id).Observation.Execution.Terminal(); i++ {
		if i > 3 {
			t.Fatal("cancellation not observed")
		}
		requireNoError(t, f.step(t))
	}
	p := &collectionProvider{probeProvider: f.adapter, files: completedOutputs(t, f, f.journal(t, id).Remote.Identity), fetches: map[string]int{}, mode: "partial"}
	results, err := blobfs.New(filepath.Join(t.TempDir(), "results"), blobfs.Limits{MaxObjectBytes: 1 << 20, MaxTotalBytes: 8 << 20})
	requireNoError(t, err)
	t.Cleanup(func() { _ = results.Close() })
	if worked, e := collectionEngine(t, f, p, results, f.s).RunOnce(t.Context()); !worked || e == nil {
		t.Fatal("collection failure missing", e)
	}

	before := f.state(t, id)
	if before.Execution != domain.ExecutionCancelled || before.Orchestration != domain.OrchestrationNeedsAttention || before.Result != domain.ResultIncomplete {
		t.Fatal("fixture differs from historic live state", before)
	}
	f.clock.advance(8 * 24 * time.Hour)
	scanRetention(t, f)
	if controlCount(t, f.s, "SELECT count(*) FROM retention_inventory WHERE workspace_id='a' AND expired_at IS NOT NULL") != 0 {
		t.Fatal("unresolved cancellation expired")
	}
	requireNoError(t, f.adapter.Advance(*f.journal(t, id).Remote))
	requireNoError(t, f.adapter.Advance(*f.journal(t, id).Remote))
	_, _, _ = admitCleanup(t, f, id, "discard-cancelled", true)
	worker, _ := cleanupWorker(t, f)
	runCleanup(t, worker)
	f.restart(t)
	f.clock.advance(8 * 24 * time.Hour)
	scanRetention(t, f)
	if controlCount(t, f.s, "SELECT count(*) FROM retention_inventory WHERE workspace_id='a' AND expired_at IS NOT NULL") != 2 {
		t.Fatal("confirmed cancelled discard kept inputs pinned")
	}
	if f.state(t, id) != before || controlCount(t, f.s, "SELECT count(*) FROM collection_publications") != 0 {
		t.Fatal("retention invented result or rewrote history")
	}
	if f.backend.Stats().SubmitCalls != 1 {
		t.Fatal("retention submitted compute")
	}
}

func TestInputReleaseBatchWaivesOnlyExactCollectedAttempts(t *testing.T) {
	ctx := t.Context()
	f, first, p, results := collectionFixture(t)
	runCollection(t, collectionEngine(t, f, p, results, f.s))
	firstProof := releaseRequest(t, f)
	second := f.seed(t, "a", 2, false)
	submittedControl(t, f, second)
	for range 2 {
		requireNoError(t, f.adapter.Advance(*f.journal(t, second).Remote))
		requireNoError(t, f.step(t))
	}
	p.files = completedOutputs(t, f, f.journal(t, second).Remote.Identity)
	runCollection(t, collectionEngine(t, f, p, results, f.s))
	var proof retention.ReleaseRequest
	requireNoError(t, f.s.db.QueryRow(`SELECT attempt_id,artifact_id,sha256 FROM artifacts WHERE job_id=? AND role='output'`, second.JobID).Scan(&proof.AttemptID, &proof.ArtifactID, &proof.SHA256))
	_, actor, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	// Inputs can still be released after an earlier result release; proof is durable.
	_, err := resultReleaser(t, f, results).Release(ctx, actor, "a", first.JobID, "prior-result-release", firstProof)
	requireNoError(t, err)
	service := inputReleaser(t, f, f.blobs, results)
	request := retention.InputReleaseRequest{Jobs: []retention.InputReleaseJob{{JobID: first.JobID, ReleaseRequest: firstProof}}}
	blocked, err := service.Preview(ctx, actor, "a", request)
	requireNoError(t, err)
	if blocked.ReleasableBytes != 0 {
		t.Fatal("unselected collected attempt window waived")
	}
	request.Jobs = append(request.Jobs, retention.InputReleaseJob{JobID: second.JobID, ReleaseRequest: proof})
	preview, err := service.Preview(ctx, actor, "a", request)
	requireNoError(t, err)
	if preview.Files != 2 || preview.ReleasableBytes <= 0 {
		t.Fatal("batch failed to release shared inventory once", preview)
	}
	request.PreviewSHA256 = preview.PreviewSHA256
	receipt, err := service.Release(ctx, actor, "a", "batch-release", request)
	requireNoError(t, err)
	if receipt.Files != 2 || receipt.ReclaimedBytes != preview.ReleasableBytes || controlCount(t, f.s, "SELECT count(*) FROM input_released_jobs") != 2 {
		t.Fatal("batch receipt double counted shared files", receipt)
	}
}

func TestUnsubmittedStagingCleanupKeepsUnsafeInputReferences(t *testing.T) {
	for _, mode := range []string{"failed_cleanup", "shared_active"} {
		t.Run(mode, func(t *testing.T) {
			f := newDispatchFixture(t, fake.DefaultScenario())
			id := f.seed(t, "a", 1, false)
			f.adapter.losePrepare = true
			if err := f.step(t); err == nil {
				t.Fatal("lost preparation acknowledgement hidden")
			}
			original := f.journal(t, id)
			probe := &stagingRecoveryProbe{probeProvider: f.adapter, planDigest: original.Plan.Digest(), preparationID: original.PreparationID, unavailable: true}
			registry := provider.NewSnapshotRegistry()
			requireNoError(t, registry.Register(provider.BindingSnapshot{Binding: f.profile.Binding, AccountScope: f.profile.AccountScope, CredentialRef: f.profile.CredentialRef}, probe))
			var err error
			f.engine, err = dispatch.New(f.s, registry, f.blobs, f.clock, dispatch.DefaultConfig())
			requireNoError(t, err)
			if err = f.step(t); err == nil {
				t.Fatal("unconfirmed staging did not end")
			}
			before := f.state(t, id)
			_, actor := cleanupService(t, f, "a", auth.Read, auth.Operate)
			previewService := cleanupPreviewService(t, f).WithPreviewResolver(cleanupResolver{&lateCleanupProbe{probeProvider: f.adapter, mode: "present"}})
			preview, err := previewService.Preview(t.Context(), actor, "a", id.JobID, id.AttemptID, false)
			requireNoError(t, err)
			if !preview.Eligible || f.journal(t, id).Prepared == nil || f.journal(t, id).Prepared.Ready {
				t.Fatal("ownership-only fixture lost exact staging", preview)
			}
			f.clock.advance(8 * 24 * time.Hour)
			scanRetention(t, f)
			if controlCount(t, f.s, "SELECT count(*) FROM retention_inventory WHERE workspace_id='a' AND expired_at IS NOT NULL") != 0 {
				t.Fatal("ownership discovery alone released input recovery")
			}
			admitCleanup(t, f, id, "input-cleanup", false)
			worker, cleanupProbe := cleanupWorker(t, f)
			if mode == "failed_cleanup" {
				cleanupProbe.err = errors.New("synthetic cleanup acknowledgement lost")
			}
			runCleanup(t, worker)
			if mode == "shared_active" {
				if controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='cleanup' AND status='succeeded'") != 1 {
					t.Fatal("fixture cleanup did not succeed")
				}
				f.seed(t, "a", 2, false)
			} else if controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='cleanup' AND status='failed'") != 1 {
				t.Fatal("fixture cleanup did not fail")
			}
			f.restart(t)
			f.clock.advance(8 * 24 * time.Hour)
			scanRetention(t, f)
			if controlCount(t, f.s, "SELECT count(*) FROM retention_inventory WHERE workspace_id='a' AND expired_at IS NOT NULL") != 0 {
				t.Fatal("unsafe input reference expired", mode)
			}
			if f.state(t, id) != before || f.journal(t, id).Prepared.Ready || f.backend.Stats().SubmitCalls != 0 {
				t.Fatal("retention changed failed preparation history or submitted compute")
			}
		})
	}
}
