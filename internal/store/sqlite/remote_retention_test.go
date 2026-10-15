package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func policyJob(raw string) string {
	return strings.Replace(dispatchJSON, `"name":"dispatch-fixture"`, `"name":"dispatch-fixture","retention":`+raw, 1)
}

func terminalPolicyFixture(t *testing.T, policy string) (*dispatchFixture, scheduler.Identity, *collectionProvider, *blobfs.Store) {
	t.Helper()
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seedRaw(t, "a", 1, false, policyJob(policy))
	submittedControl(t, f, id)
	for i := 0; i < 2; i++ {
		requireNoError(t, f.adapter.Advance(*f.journal(t, id).Remote))
		requireNoError(t, f.step(t))
	}
	if !f.journal(t, id).Observation.Execution.Terminal() || f.state(t, id).RemoteActivity != domain.RemoteActivityInactive {
		t.Fatal("fixture lacks confirmed terminal execution")
	}
	p := &collectionProvider{probeProvider: f.adapter, fetches: map[string]int{}, files: completedOutputs(t, f, f.journal(t, id).Remote.Identity)}
	blobs, err := blobfs.New(filepath.Join(t.TempDir(), "results"), blobfs.Limits{MaxObjectBytes: 1 << 20, MaxTotalBytes: 8 << 20})
	requireNoError(t, err)
	t.Cleanup(func() { _ = blobs.Close() })
	return f, id, p, blobs
}

func preparedPolicyFixture(t *testing.T, policy string) (*dispatchFixture, scheduler.Identity) {
	t.Helper()
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seedRaw(t, "a", 1, false, policyJob(policy))
	for i := 0; f.journal(t, id).Phase != dispatch.Ready; i++ {
		if i == 6 {
			t.Fatal("fixture never staged")
		}
		requireNoError(t, f.step(t))
	}
	service, actor, _ := controlService(t, f, "a", auth.Operate)
	if receipt := cancelControl(t, f, service, actor, id); receipt.Effect != operations.DispatchPrevented {
		t.Fatal("prepared submission was not prevented", receipt)
	}
	return f, id
}

func remoteDeadlineAt(t *testing.T, f *dispatchFixture, kind string) int64 {
	t.Helper()
	var due int64
	requireNoError(t, f.s.db.QueryRow(`SELECT due_ms FROM remote_retention_deadlines WHERE kind=?`, kind).Scan(&due))
	return due
}

func TestRemoteRetentionAfterCollectionSurvivesRestartWithoutEarlyCleanup(t *testing.T) {
	ctx := context.Background()
	f, _, p, blobs := terminalPolicyFixture(t, `{"after_collection_seconds":3600}`)
	if count := controlCount(t, f.s, `SELECT count(*) FROM remote_retention_deadlines`); count != 0 {
		t.Fatal("collection deadline existed before verified publication", count)
	}
	runCollection(t, collectionEngine(t, f, p, blobs, f.s))
	due := remoteDeadlineAt(t, f, "collected")
	if due <= f.clock.Now().UnixMilli() {
		t.Fatal("collection lifetime did not begin at publication")
	}
	if _, err := f.s.db.Exec(`UPDATE remote_retention_deadlines SET due_ms=due_ms-1`); err == nil {
		t.Fatal("immutable deadline was movable")
	}
	f.clock.advance(time.UnixMilli(due).Sub(f.clock.Now()) - time.Millisecond)
	f.restart(t)
	if created, err := f.s.ScheduleRetention(ctx, retentionScanLimit); err != nil || created != 0 {
		t.Fatal("retention deleted before its durable deadline", created, err)
	}
	if count := controlCount(t, f.s, `SELECT count(*) FROM operations WHERE kind='cleanup'`); count != 0 {
		t.Fatal("early cleanup operation appeared", count)
	}
	f.clock.advance(time.Millisecond)
	f.restart(t)
	worker, probe := cleanupWorker(t, f)
	runCleanup(t, worker) // RunOnce schedules and claims the due cleanup.
	if count := controlCount(t, f.s, `SELECT count(*) FROM operations WHERE kind='cleanup' AND status='succeeded'`); count != 1 || len(probe.requests) == 0 {
		t.Fatal("due cleanup did not use the cleanup worker", count, len(probe.requests))
	}
}

func TestRemoteRetentionMigrationBackfillsConservativeDeadlines(t *testing.T) {
	for _, fixture := range []struct {
		name string
		make func(*testing.T) *dispatchFixture
		kind string
	}{
		{"published", func(t *testing.T) *dispatchFixture {
			f, _, p, blobs := terminalPolicyFixture(t, `{"after_collection_seconds":3600}`)
			runCollection(t, collectionEngine(t, f, p, blobs, f.s))
			return f
		}, "collected"},
		{"prepared", func(t *testing.T) *dispatchFixture {
			f, _ := preparedPolicyFixture(t, `{"uncollected_max_seconds":60}`)
			return f
		}, "uncollected"},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			f := fixture.make(t)
			original := remoteDeadlineAt(t, f, fixture.kind)
			path := copyFixtureAtSchema(t, f.s, 31)
			upgraded, err := Open(context.Background(), path, DefaultOptions())
			requireNoError(t, err)
			defer upgraded.Close()
			var due int64
			requireNoError(t, upgraded.db.QueryRow(`SELECT due_ms FROM remote_retention_deadlines WHERE kind=?`, fixture.kind).Scan(&due))
			if due < original || due > original+999 || controlCount(t, upgraded, `SELECT count(*) FROM operations WHERE kind='cleanup'`) != 0 {
				t.Fatal("upgrade moved a deadline early or fabricated cleanup", original, due)
			}
			upgraded.options.Clock = func() time.Time { return time.UnixMilli(due + 1).UTC() }
			if created, err := upgraded.ScheduleRetention(context.Background(), retentionScanLimit); err != nil || created != 1 {
				t.Fatal("upgraded due rule could not enter cleanup", created, err)
			}
		})
	}
}

func TestRemoteRetentionUncollectedAndPreparedAttempts(t *testing.T) {
	ctx := context.Background()
	for _, fixture := range []struct {
		name string
		make func(*testing.T) (*dispatchFixture, scheduler.Identity)
	}{
		{"terminal execution", func(t *testing.T) (*dispatchFixture, scheduler.Identity) {
			f, id, p, blobs := terminalPolicyFixture(t, `{"uncollected_max_seconds":60}`)
			p.mode = "corrupt"
			if worked, err := collectionEngine(t, f, p, blobs, f.s).RunOnce(context.Background()); !worked || err == nil {
				t.Fatal("fixture collection did not fail verification", worked, err)
			}
			return f, id
		}},
		{"unused preparation", func(t *testing.T) (*dispatchFixture, scheduler.Identity) {
			return preparedPolicyFixture(t, `{"uncollected_max_seconds":60}`)
		}},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			f, id := fixture.make(t)
			due := remoteDeadlineAt(t, f, "uncollected")
			if due-f.clock.Now().UnixMilli() != 60_000 {
				t.Fatal("uncollected deadline moved from first settled attempt", due, f.clock.Now())
			}
			f.clock.advance(59 * time.Second)
			f.restart(t)
			if created, err := f.s.ScheduleRetention(ctx, retentionScanLimit); err != nil || created != 0 {
				t.Fatal("uncollected cleanup started early", created, err)
			}
			f.clock.advance(time.Second)
			f.restart(t)
			worker, _ := cleanupWorker(t, f)
			runCleanup(t, worker)
			var count int
			requireNoError(t, f.s.db.QueryRow(`SELECT count(*) FROM operations WHERE kind='cleanup' AND status='succeeded' AND attempt_id=?`, string(id.AttemptID)).Scan(&count))
			if count != 1 {
				t.Fatal("due uncollected attempt was not cleaned", count)
			}
		})
	}
}

func TestRemoteRetentionCollectionWinsExpiryRace(t *testing.T) {
	ctx := context.Background()
	f, _, p, blobs := terminalPolicyFixture(t, `{"after_collection_seconds":3600,"uncollected_max_seconds":1}`)
	uncollectedDue := remoteDeadlineAt(t, f, "uncollected")
	f.clock.advance(2 * time.Second)
	if f.clock.Now().UnixMilli() <= uncollectedDue {
		t.Fatal("fixture did not cross the uncollected deadline")
	}
	runCollection(t, collectionEngine(t, f, p, blobs, f.s))
	if created, err := f.s.ScheduleRetention(ctx, retentionScanLimit); err != nil || created != 0 {
		t.Fatal("expired uncollected rule overrode verified collection", created, err)
	}
	if count := controlCount(t, f.s, `SELECT count(*) FROM operations WHERE kind='cleanup'`); count != 0 {
		t.Fatal("collection race created early cleanup", count)
	}
	f.clock.advance(time.Hour)
	if created, err := f.s.ScheduleRetention(ctx, retentionScanLimit); err != nil || created != 1 {
		t.Fatal("post-collection deadline did not schedule", created, err)
	}
	var discard bool
	if err := f.s.db.QueryRow(`SELECT json_extract(request_json,'$.discard_uncollected') FROM operations WHERE kind='cleanup'`).Scan(&discard); err != nil || discard {
		t.Fatal("collected cleanup unexpectedly discarded results", discard, err)
	}
}

func TestRemoteRetentionBusyCollectionDefersThenRuns(t *testing.T) {
	ctx := context.Background()
	f, _, p, blobs := terminalPolicyFixture(t, `{"uncollected_max_seconds":1}`)
	f.clock.advance(2 * time.Second)
	if created, err := f.s.ScheduleRetention(ctx, retentionScanLimit); err != nil || created != 0 {
		t.Fatal("pending collection was not protected at expiry", created, err)
	}
	var retry int64
	requireNoError(t, f.s.db.QueryRow(`SELECT retry_after_ms FROM remote_retention_deadlines WHERE kind='uncollected'`).Scan(&retry))
	if retry <= f.clock.Now().UnixMilli() {
		t.Fatal("busy attempt was not deferred", retry)
	}
	p.mode = "corrupt"
	if worked, err := collectionEngine(t, f, p, blobs, f.s).RunOnce(ctx); !worked || err == nil {
		t.Fatal("fixture collection did not fail verification", worked, err)
	}
	f.clock.advance(retentionBusyDelay)
	if created, err := f.s.ScheduleRetention(ctx, retentionScanLimit); err != nil || created != 1 {
		t.Fatal("deferred due deletion was lost", created, err)
	}
}

func TestRemoteRetentionNoPolicyAndCompletedExplicitCleanup(t *testing.T) {
	ctx := context.Background()
	legacy, _ := collectedFixture(t)
	legacy.clock.advance(48 * time.Hour)
	if created, err := legacy.s.ScheduleRetention(ctx, retentionScanLimit); err != nil || created != 0 || controlCount(t, legacy.s, `SELECT count(*) FROM remote_retention_deadlines`) != 0 {
		t.Fatal("legacy job acquired implicit remote cleanup", created, err)
	}
	f, id := preparedPolicyFixture(t, `{"uncollected_max_seconds":60}`)
	_, _, explicit := admitCleanup(t, f, id, "explicit-before-retention", false)
	worker, _ := cleanupWorker(t, f)
	runCleanup(t, worker)
	if explicit.Operation.Kind != domain.OperationCleanup {
		t.Fatal("explicit cleanup fixture missing")
	}
	f.clock.advance(time.Minute)
	f.restart(t)
	if created, err := f.s.ScheduleRetention(ctx, retentionScanLimit); err != nil || created != 0 || controlCount(t, f.s, `SELECT count(*) FROM operations WHERE kind='cleanup'`) != 1 {
		t.Fatal("policy recreated completed explicit cleanup", created, err)
	}
}

func TestRemoteRetentionDueAfterFailedExplicitCleanup(t *testing.T) {
	ctx := context.Background()
	f, id := preparedPolicyFixture(t, `{"uncollected_max_seconds":60}`)
	_, _, first := admitCleanup(t, f, id, "explicit-before-due", false)
	probe := &cleanupProbe{probeProvider: f.adapter, err: provider.Problem(domain.CodeProviderAccessDenied, domain.FailureStageAuthentication, "fixture access refusal")}
	engine, err := cleanup.New(f.s, cleanupResolver{probe}, cleanup.DefaultConfig())
	requireNoError(t, err)
	runCleanup(t, engine)
	if controlCount(t, f.s, `SELECT count(*) FROM operations WHERE kind='cleanup' AND status='failed'`) != 1 {
		t.Fatal("fixture explicit cleanup did not fail")
	}
	f.clock.advance(time.Minute)
	probe.err = nil
	if created, err := f.s.ScheduleRetention(ctx, retentionScanLimit); err != nil || created != 1 {
		t.Fatal("failed explicit cleanup hid the later due policy", created, err)
	}
	var linked string
	requireNoError(t, f.s.db.QueryRow(`SELECT operation_id FROM remote_retention_deadlines WHERE kind='uncollected'`).Scan(&linked))
	if linked == string(first.Operation.ID) || linked == "" {
		t.Fatal("retention did not link its own cleanup receipt", linked)
	}
	runCleanup(t, engine)
	if controlCount(t, f.s, `SELECT count(*) FROM operations WHERE kind='cleanup' AND status='succeeded'`) != 1 || len(probe.requests) != 2 {
		t.Fatal("due policy did not complete through the cleanup engine", probe.requests)
	}
}

func TestRemoteRetentionFailedPolicyRemainsReportableWithoutAutomaticReplay(t *testing.T) {
	ctx := context.Background()
	f, _ := preparedPolicyFixture(t, `{"uncollected_max_seconds":60}`)
	f.clock.advance(time.Minute)
	probe := &cleanupProbe{probeProvider: f.adapter, err: provider.Problem(domain.CodeProviderAccessDenied, domain.FailureStageAuthentication, "fixture access refusal")}
	engine, err := cleanup.New(f.s, cleanupResolver{probe}, cleanup.DefaultConfig())
	requireNoError(t, err)
	runCleanup(t, engine)
	var due int64
	var linked, status string
	requireNoError(t, f.s.db.QueryRow(`SELECT d.due_ms,d.operation_id,c.status FROM remote_retention_deadlines d JOIN operations c ON c.workspace_id=d.workspace_id AND c.operation_id=d.operation_id WHERE d.kind='uncollected'`).Scan(&due, &linked, &status))
	if due > f.clock.Now().UnixMilli() || linked == "" || status != "failed" {
		t.Fatal("failed due policy lost its durable deadline or receipt", due, linked, status)
	}
	f.clock.advance(time.Minute)
	if created, err := f.s.ScheduleRetention(ctx, retentionScanLimit); err != nil || created != 0 || controlCount(t, f.s, `SELECT count(*) FROM operations WHERE kind='cleanup'`) != 1 {
		t.Fatal("failed policy repeated a one-shot deletion", created, err)
	}
}

func TestRemoteRetentionUnknownExplicitDeletionStaysPending(t *testing.T) {
	ctx := context.Background()
	f, id := preparedPolicyFixture(t, `{"uncollected_max_seconds":60}`)
	_, _, _ = admitCleanup(t, f, id, "explicit-ambiguous-before-due", false)
	probe := &cleanupProbe{probeProvider: f.adapter, err: errors.New("ambiguous fixture outcome")}
	engine, err := cleanup.New(f.s, cleanupResolver{probe}, cleanup.DefaultConfig())
	requireNoError(t, err)
	runCleanup(t, engine)
	if controlCount(t, f.s, `SELECT count(*) FROM cleanup_resources WHERE state='unknown'`) != 1 {
		t.Fatal("fixture did not preserve unknown deletion outcome")
	}
	f.clock.advance(time.Minute)
	if created, err := f.s.ScheduleRetention(ctx, retentionScanLimit); err != nil || created != 0 {
		t.Fatal("unknown prior deletion was automatically retried", created, err)
	}
	var due, retry int64
	var linked *string
	requireNoError(t, f.s.db.QueryRow(`SELECT due_ms,retry_after_ms,operation_id FROM remote_retention_deadlines WHERE kind='uncollected'`).Scan(&due, &retry, &linked))
	if due > f.clock.Now().UnixMilli() || retry <= f.clock.Now().UnixMilli() || linked != nil || controlCount(t, f.s, `SELECT count(*) FROM operations WHERE kind='cleanup'`) != 1 {
		t.Fatal("unknown prior outcome was dropped instead of left pending", due, retry, linked)
	}
}

func TestConfirmedCleanupReceiptSurvivesLaterKaggleStyle403(t *testing.T) {
	ctx := context.Background()
	f, id := collectedFixture(t)
	service, actor, _ := admitCleanup(t, f, id, "first-partial-cleanup", false)
	first, err := f.s.ClaimCleanup(ctx, time.Minute)
	if err != nil || first == nil || len(first.Resources) != 2 {
		t.Fatal("missing first cleanup", first, err)
	}
	if first.Resources[0].Result.Purpose != "execution" || first.Resources[1].Result.Purpose != "staging" {
		t.Fatal("cleanup order changed")
	}
	requireNoError(t, f.s.BeginCleanupResource(ctx, *first, first.Resources[0]))
	deleted := first.Resources[0].Result
	deleted.State, deleted.Reason = operations.ResourceDeleted, "deletion_confirmed"
	requireNoError(t, f.s.CompleteCleanupResource(ctx, *first, deleted))
	failed := first.Resources[1].Result
	failed.State, failed.Reason = operations.ResourceFailed, "provider_access_denied"
	requireNoError(t, f.s.CompleteCleanupResource(ctx, *first, failed))
	problem := provider.Problem(domain.CodeProviderAccessDenied, domain.FailureStageAuthentication, "fixture staging refusal")
	requireNoError(t, f.s.CompleteCleanup(ctx, *first, cleanup.Outcome{Effect: operations.Failed, Problem: &problem}))
	probe := &cleanupProbe{probeProvider: f.adapter}
	probe.resourceHook = func(_ context.Context, r provider.CleanupResourceRequest) (provider.CleanupOutcome, error) {
		if r.Purpose == "execution" {
			return provider.CleanupOutcome{}, errors.New("403 private resource not visible")
		}
		return provider.CleanupOutcome{Deleted: true}, nil
	}
	preview := cleanupPreviewService(t, f).WithPreviewResolver(cleanupResolver{probe})
	view, err := preview.Preview(ctx, actor, "a", id.JobID, id.AttemptID, false)
	if err != nil || len(view.Resources) != 2 || view.Resources[0].Observation != "unknown" {
		t.Fatal("fixture did not reproduce post-delete 403 preview", view, err)
	}
	second, err := service.Submit(ctx, actor, "a", id.JobID, domain.OperationCleanup, "second-partial-cleanup", cleanupBody(id.AttemptID, false, ""))
	requireNoError(t, err)
	work, err := f.s.ClaimCleanup(ctx, time.Minute)
	if err != nil || work == nil || work.Resources[0].Result.State != operations.ResourceDeleted || work.Resources[1].Result.State != operations.ResourcePending {
		t.Fatal("new cleanup lost the final deletion receipt", work, err)
	}
	f.clock.advance(time.Minute + time.Second) // Release the claim for the real worker.
	engine, err := cleanup.New(f.s, cleanupResolver{probe}, cleanup.DefaultConfig())
	requireNoError(t, err)
	runCleanup(t, engine)
	if len(probe.requests) != 1 || probe.requests[0].LedgerID != work.Resources[1].Result.ResourceID {
		t.Fatal("confirmed execution resource was inspected or deleted again", probe.requests)
	}
	latest, err := service.Get(ctx, actor, "a", second.Operation.ID)
	if err != nil || latest.Effect != operations.RemoteDeleted {
		t.Fatal("remaining staging cleanup did not complete", latest, err)
	}
}
