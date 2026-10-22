package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func setResourceProvider(t *testing.T, f *dispatchFixture, kind string) {
	t.Helper()
	_, err := f.s.db.Exec(`INSERT INTO managed_connections(connection_id,workspace_id,provider_type,label,revision,authentication,new_work,canonical_account,account_scope,active_credential_key,updated_at) VALUES(?,'a',?,'bound fixture',1,'verified','enabled','fixture_account',?,'fixture_key',?)`, f.profile.Binding.ProviderInstanceID, kind, f.profile.AccountScope, f.clock.Now().Format(time.RFC3339Nano))
	requireNoError(t, err)
	if kind == "ssh" {
		_, err = f.s.db.Exec(`INSERT INTO managed_resource_accounts VALUES(?,'machine',?)`, f.profile.AccountScope, "resource_"+strings.Repeat("f", 64))
		requireNoError(t, err)
	}
}

func resourceCounts(t *testing.T, f *dispatchFixture, kind string) (int, int) {
	t.Helper()
	var installation domain.RuntimeInstallationID
	requireNoError(t, f.s.db.QueryRow(`SELECT installation_id FROM runtime_installation`).Scan(&installation))
	var staging, kernels int
	requireNoError(t, f.s.write(t.Context(), func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
		var err error
		staging, kernels, err = remoteResourceOccupancy(ctx, tx, installation, func() string {
			if kind == "ssh" {
				return "resource_" + strings.Repeat("f", 64)
			}
			return f.profile.AccountScope
		}(), kind)
		return err
	}))
	return staging, kernels
}

// Additional rows are durable, unresolved creation intents with no remote calls.
func seedUnknownResource(t *testing.T, f *dispatchFixture, base dispatch.Journal, w domain.WorkspaceID, n int, instance string) dispatch.Work {
	t.Helper()
	oldSeed := f.authorizeSeed
	f.authorizeSeed = false
	id := f.seed(t, w, n, false)
	f.authorizeSeed = oldSeed
	plan := base.Plan.Clone()
	plan.Job.Identity.WorkspaceID = w
	plan.Job.Identity.JobID = id.JobID
	plan.Job.Identity.AttemptID = id.AttemptID
	plan.Job.Identity.IntentID = domain.SubmissionIntentID(fmt.Sprintf("intent_bound_%d", n))
	plan.Job.Identity.InstanceID = domain.ProviderInstanceID(instance)
	plan.Job.Identity.ResourceKey = fmt.Sprintf("resource_bound_%d", n)
	_, err := f.s.db.Exec(`INSERT INTO managed_connections(connection_id,workspace_id,provider_type,label,revision,authentication,new_work,canonical_account,account_scope,active_credential_key,updated_at)
 SELECT ?,?,provider_type,'alias',1,'verified','enabled','fixture_account',account_scope,'fixture_key',updated_at FROM managed_connections WHERE connection_id=? ON CONFLICT(connection_id) DO NOTHING`, instance, w, f.profile.Binding.ProviderInstanceID)
	requireNoError(t, err)
	prep := domain.OperationID(fmt.Sprintf("prep_bound_%d", n))
	work := dispatch.Work{InstallationID: plan.Job.Identity.InstallationID}
	work.Job.Profile = f.profile
	j := dispatch.Journal{Plan: &plan, PreparationID: prep}
	requireNoError(t, f.s.write(t.Context(), func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		return insertResource(ctx, tx, work, j, "staging", string(prep), now)
	}))
	return work
}

func TestRemoteResourceBoundsRetainFutureKernelAndUnknownIntents(t *testing.T) {
	for _, kind := range []string{"kaggle", "ssh"} {
		t.Run(kind, func(t *testing.T) {
			f := newDispatchFixture(t, fake.DefaultScenario())
			id := f.seed(t, "a", 1, false)
			requireNoError(t, f.step(t))
			base := f.journal(t, id)
			setResourceProvider(t, f, kind)
			for n := 2; n <= provider.RemoteResourceCountLimit; n++ {
				w := domain.WorkspaceID("a")
				if n%2 == 0 {
					w = "b"
				}
				seedUnknownResource(t, f, base, w, n, fmt.Sprintf("alias_%d", n))
			}
			staging, kernels := resourceCounts(t, f, kind)
			if staging != 128 || kind == "kaggle" && kernels != 128 || kind == "ssh" && kernels != 0 {
				t.Fatal("lost retained resource or double counted SSH", staging, kernels)
			}
			work := dispatch.Work{InstallationID: base.Plan.Job.Identity.InstallationID}
			work.Job.Profile = f.profile
			if kind == "ssh" {
				work.Job.Profile.ResourceScope = "resource_" + strings.Repeat("f", 64)
			}
			err := f.s.write(t.Context(), func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
				return checkRemoteResourceNewWork(ctx, tx, work)
			})
			want := "remote_staging_dataset_limit"
			if kind == "ssh" {
				want = "remote_attempt_container_limit"
			}
			if !errors.Is(err, provider.ErrResourceCountLimit) || provider.Reason(err, "") != want {
				t.Fatal("new identity crossed count bound", err)
			}
			if kind == "ssh" {
				work.Job.Profile.AccountScope = "another_login_account"
				err = f.s.write(t.Context(), func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
					return checkRemoteResourceNewWork(ctx, tx, work)
				})
				if !errors.Is(err, provider.ErrResourceCountLimit) || provider.Reason(err, "") != "remote_attempt_container_limit" {
					t.Fatal("physical aliases escaped count", err)
				}
				work.Job.Profile.ResourceScope = ""
				err = f.s.write(t.Context(), func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
					return checkRemoteResourceNewWork(ctx, tx, work)
				})
				if provider.Reason(err, "") != "remote_resource_scope_unknown" {
					t.Fatal("unknown physical scope allowed growth", err)
				}
			}
			// Existing recovery is deliberately outside new-identity admission.
			work.Journal = base
			requireNoError(t, f.s.write(t.Context(), func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
				return checkRemoteResourceNewWork(ctx, tx, work)
			}))
			f.restart(t)
			if got, _ := resourceCounts(t, f, kind); got != 128 {
				t.Fatal("restart released unknown count", got)
			}
		})
	}
}

func TestRemoteResourceCountGateSerializesLastSlotAcrossWorkspaces(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	requireNoError(t, f.step(t))
	base := f.journal(t, id)
	setResourceProvider(t, f, "kaggle")
	for n := 2; n < 128; n++ {
		seedUnknownResource(t, f, base, "a", n, "another_alias")
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for n := 128; n <= 129; n++ {
		w := domain.WorkspaceID("a")
		if n == 129 {
			w = "b"
		}
		id := f.seed(t, w, n, false)
		plan := base.Plan.Clone()
		plan.Job.Identity.WorkspaceID = w
		plan.Job.Identity.JobID = id.JobID
		plan.Job.Identity.AttemptID = id.AttemptID
		plan.Job.Identity.IntentID = domain.SubmissionIntentID(fmt.Sprintf("intent_race_%d", n))
		prep := domain.OperationID(fmt.Sprintf("prep_race_%d", n))
		work := dispatch.Work{InstallationID: plan.Job.Identity.InstallationID}
		work.Job.Profile = f.profile
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- f.s.write(context.Background(), func(ctx context.Context, tx *sql.Tx, now time.Time) error {
				if err := checkRemoteResourceNewWork(ctx, tx, work); err != nil {
					return err
				}
				return insertResource(ctx, tx, work, dispatch.Journal{Plan: &plan, PreparationID: prep}, "staging", string(prep), now)
			})
		}()
	}
	wg.Wait()
	close(results)
	accepted, refused := 0, 0
	for err := range results {
		if err == nil {
			accepted++
		} else if errors.Is(err, provider.ErrResourceCountLimit) {
			refused++
		} else {
			t.Fatal(err)
		}
	}
	if accepted != 1 || refused != 1 {
		t.Fatal("count admission raced", accepted, refused)
	}
	if staging, kernels := resourceCounts(t, f, "kaggle"); staging != 128 || kernels != 128 {
		t.Fatal("cap exceeded", staging, kernels)
	}
}

func TestRemoteResourceOccupancyReleasesOnlyExactCleanupOrRejection(t *testing.T) {
	t.Run("cleanup", func(t *testing.T) {
		f, id := collectedFixture(t)
		setResourceProvider(t, f, "ssh")
		if s, k := resourceCounts(t, f, "kaggle"); s != 1 || k != 1 {
			t.Fatal(s, k)
		}
		admitCleanup(t, f, id, "bound-cleanup", false)
		work, err := f.s.ClaimCleanup(t.Context(), time.Minute)
		requireNoError(t, err)
		if work == nil {
			t.Fatal("cleanup not claimed")
		}
		first := work.Resources[0]
		requireNoError(t, f.s.BeginCleanupResource(t.Context(), *work, first))
		// A durable delete intent is uncertainty, never free capacity.
		if s, k := resourceCounts(t, f, "kaggle"); s != 1 || k != 1 {
			t.Fatal("intent freed capacity", s, k)
		}
		result := first.Result
		result.State = operations.ResourceDeleted
		result.Reason = "exact_delete_confirmed"
		requireNoError(t, f.s.CompleteCleanupResource(t.Context(), *work, result))
		if s, k := resourceCounts(t, f, "kaggle"); s != 1 || k != 0 {
			t.Fatal("independent kernel cleanup lost", s, k)
		}
		if s, k := resourceCounts(t, f, "ssh"); s != 0 || k != 0 {
			t.Fatal("same physical container charged twice", s, k)
		}
		last := work.Resources[1]
		requireNoError(t, f.s.BeginCleanupResource(t.Context(), *work, last))
		result = last.Result
		result.State = operations.ResourceAbsent
		result.Reason = "exact_absence_confirmed"
		requireNoError(t, f.s.CompleteCleanupResource(t.Context(), *work, result))
		if s, k := resourceCounts(t, f, "kaggle"); s != 0 || k != 0 {
			t.Fatal("cleanup did not free capacity", s, k)
		}
	})
	t.Run("rejection", func(t *testing.T) {
		scenario := fake.DefaultScenario()
		scenario.Mode = fake.Reject
		f := newDispatchFixture(t, scenario)
		f.seed(t, "a", 1, false)
		requireNoError(t, f.step(t))
		if s, k := resourceCounts(t, f, "kaggle"); s != 1 || k != 1 {
			t.Fatal("future kernel not held", s, k)
		}
		requireNoError(t, f.step(t))
		if s, k := resourceCounts(t, f, "kaggle"); s != 1 || k != 0 {
			t.Fatal("definitive rejection did not release only kernel", s, k)
		}
	})
}

func sharedClaimFixture(t *testing.T) (*dispatchFixture, provider.SharedResourceClaim) {
	t.Helper()
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	requireNoError(t, f.step(t))
	setResourceProvider(t, f, "kaggle")
	j := f.journal(t, id)
	input := j.Plan.Job.Inputs.Inputs[0].Object
	return f, provider.SharedResourceClaim{Identity: j.Plan.Job.Identity, PreparationID: j.PreparationID, PlanSHA256: j.Plan.Digest(), ContentSHA256: input.SHA256, Bytes: input.Bytes}
}

func TestSharedResourceCreationIsOneShotAcrossRestartAndLaterAttempt(t *testing.T) {
	f, claim := sharedClaimFixture(t)
	requireNoError(t, f.s.TrackSharedUse(t.Context(), f.profile.AccountScope, claim))
	var wg sync.WaitGroup
	results := make(chan bool, 2)
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := f.s.ClaimSharedCreation(context.Background(), f.profile.AccountScope, claim)
			results <- ok
			errs <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	count := 0
	for ok := range results {
		if ok {
			count++
		}
	}
	for err := range errs {
		requireNoError(t, err)
	}
	if count != 1 {
		t.Fatal("creation permit repeated", count)
	}
	f.restart(t)
	requireNoError(t, f.s.TrackSharedUse(t.Context(), f.profile.AccountScope, claim))
	ok, err := f.s.ClaimSharedCreation(t.Context(), f.profile.AccountScope, claim)
	requireNoError(t, err)
	if ok {
		t.Fatal("restart rearmed unknown creation")
	}
	// Another original attempt with the same content is a user, never a new creator.
	f.seed(t, "b", 2, false)
	requireNoError(t, f.step(t))
	remote := f.journal(t, scheduler.Identity{WorkspaceID: "a", JobID: "job_1", AttemptID: "attempt_1"}).Remote
	if remote == nil {
		t.Fatal("first remote missing")
	}
	requireNoError(t, f.adapter.Advance(*remote))
	requireNoError(t, f.adapter.Advance(*remote))
	for range 8 {
		if f.journal(t, scheduler.Identity{WorkspaceID: "b", JobID: "job_2", AttemptID: "attempt_2"}).Plan != nil {
			break
		}
		requireNoError(t, f.step(t))
	}
	j := f.journal(t, scheduler.Identity{WorkspaceID: "b", JobID: "job_2", AttemptID: "attempt_2"})
	if j.Plan == nil {
		t.Fatal("later plan missing", f.state(t, scheduler.Identity{WorkspaceID: "a", JobID: "job_1", AttemptID: "attempt_1"}), f.state(t, scheduler.Identity{WorkspaceID: "b", JobID: "job_2", AttemptID: "attempt_2"}), f.backend.Stats())
	}
	later := claim
	later.Identity = j.Plan.Job.Identity
	later.PreparationID = j.PreparationID
	later.PlanSHA256 = j.Plan.Digest()
	requireNoError(t, f.s.TrackSharedUse(t.Context(), f.profile.AccountScope, later))
	ok, err = f.s.ClaimSharedCreation(t.Context(), f.profile.AccountScope, later)
	requireNoError(t, err)
	if ok {
		t.Fatal("later alias repeated shared create")
	}
	if n := controlCount(t, f.s, "SELECT count(*) FROM shared_resource_claims"); n != 1 {
		t.Fatal("reuse consumed another shared slot", n)
	}
	for _, bad := range []provider.SharedResourceClaim{func() provider.SharedResourceClaim {
		v := claim
		v.ContentSHA256 = provider.Digest([]byte("not frozen"))
		return v
	}(), func() provider.SharedResourceClaim {
		v := claim
		v.PlanSHA256 = provider.Digest([]byte("changed plan"))
		return v
	}()} {
		if err := f.s.TrackSharedUse(t.Context(), f.profile.AccountScope, bad); err == nil {
			t.Fatal("accepted unrelated shared claim")
		}
	}
	if err := f.s.TrackSharedUse(t.Context(), "other_account", claim); err == nil {
		t.Fatal("accepted another account")
	}
}

func TestSharedResourceCountCapChargesUnstartedAndUnknownClaims(t *testing.T) {
	f, claim := sharedClaimFixture(t)
	identity, _ := json.Marshal(claim.Identity)
	for n := 0; n < 128; n++ {
		sha := provider.Digest([]byte(fmt.Sprint(n)))
		_, err := f.s.db.Exec(`INSERT INTO shared_resource_claims(installation_id,account_scope,resource_key,content_sha256,bytes,preparation_id,identity,plan_sha256,creation_started,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)`, claim.Identity.InstallationID, f.profile.AccountScope, provider.SharedResourceKey(sha, 0), sha, 0, claim.PreparationID, string(identity), claim.PlanSHA256, n%2, f.clock.Now().Format(time.RFC3339Nano))
		requireNoError(t, err)
	}
	err := f.s.TrackSharedUse(t.Context(), f.profile.AccountScope, claim)
	if !errors.Is(err, provider.ErrResourceCountLimit) || provider.Reason(err, "") != "remote_shared_dataset_limit" {
		t.Fatal("shared cap ignored reserved/unknown entries", err)
	}
	if _, err := f.s.db.Exec(`DELETE FROM shared_resource_claims`); err == nil {
		t.Fatal("forgot uncertain reservation")
	}
	if n := controlCount(t, f.s, "SELECT count(*) FROM shared_resource_claims"); n != 128 {
		t.Fatal("failed claim changed count", n)
	}
}

func TestRemoteResourceCountRefusalStaysQueuedAndDoesNotCreate(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	requireNoError(t, f.step(t))
	base := f.journal(t, id)
	settings := scheduler.DefaultSettings()
	settings.MaxActivePerAccount = 64
	requireNoError(t, f.s.ConfigureScheduler(t.Context(), settings))
	target := f.seed(t, "a", 129, false)
	work := f.claim(t)
	plan, prep := storePlan(t, work)
	setResourceProvider(t, f, "kaggle")
	for n := 2; n <= 128; n++ {
		seedUnknownResource(t, f, base, "a", n, "another_alias")
	}
	before := f.backend.Stats().PrepareCalls
	_, err := f.s.CommitDispatch(t.Context(), work.Handle, dispatch.Action{Kind: dispatch.BeginPreparation, Plan: &plan, PreparationID: prep})
	if !errors.Is(err, provider.ErrResourceCountLimit) {
		t.Fatal("preparation crossed bound", err)
	}
	updated, err := f.s.CommitDispatch(t.Context(), work.Handle, dispatch.Action{Kind: dispatch.RefuseNewWork, RefusalReason: "remote_staging_dataset_limit", Cause: provider.Reason(err, "")})
	requireNoError(t, err)
	if updated.Journal.Problem == nil || updated.Journal.Problem.Details[domain.ReasonDetail] != "remote_staging_dataset_limit" || updated.Job.Attempt.State.Orchestration != domain.OrchestrationQueued || updated.Journal.Plan != nil {
		t.Fatal("refusal did not retain queued original", updated.Journal)
	}
	if f.backend.Stats().PrepareCalls != before || f.state(t, target).Execution != domain.ExecutionNotSubmitted {
		t.Fatal("refusal invoked provider")
	}
}

// Plans can be frozen entirely from committed metadata; these tests exercise
// durable preparation intents without reading/uploading synthetic large bytes.
func freezeResourceClaim(t *testing.T, f *dispatchFixture, w domain.WorkspaceID, n int, sha domain.SHA256Digest, sizes ...int64) provider.SharedResourceClaim {
	t.Helper()
	size := int64(256 << 20)
	if len(sizes) > 0 {
		size = sizes[0]
	}
	object := domain.ObjectMetadata{ID: domain.ObjectID(fmt.Sprintf("large_%d", n)), WorkspaceID: w, Bytes: size, SHA256: sha}
	requireNoError(t, f.s.CommitObject(t.Context(), object))
	connection := strings.TrimPrefix(f.profile.CredentialRef, "vault:")
	_, err := f.s.db.Exec(`INSERT INTO managed_connections(connection_id,workspace_id,provider_type,label,revision,authentication,new_work,canonical_account,account_scope,active_credential_key,updated_at) VALUES(?,?,'fixture','fixture',1,'verified','enabled','fixture_account',?,'fixture_key',?) ON CONFLICT(connection_id) DO NOTHING`, connection, w, f.profile.AccountScope, f.clock.Now().Format(time.RFC3339Nano))
	requireNoError(t, err)
	raw := strings.Replace(dispatchJSON, `"object_id":"data"`, `"object_id":"`+string(object.ID)+`"`, 1)
	oldSeed := f.authorizeSeed
	f.authorizeSeed = false
	id := f.seedRaw(t, w, n, false, raw)
	f.authorizeSeed = oldSeed
	_, err = f.s.db.Exec(`DELETE FROM job_objects WHERE workspace_id=? AND job_id=? AND role='input:0'`, w, id.JobID)
	requireNoError(t, err)
	_, err = f.s.db.Exec(`INSERT INTO job_objects VALUES(?,?,'input:0',?,?,?)`, w, id.JobID, object.ID, object.Bytes, object.SHA256)
	requireNoError(t, err)
	service, actor := authorizationService(t, f, w, auth.Execute)
	_, err = service.Authorize(t.Context(), actor, w, id.JobID, "large-grant-"+string(id.AttemptID), authorizationRequest(id.AttemptID, 10))
	requireNoError(t, err)
	work := f.claim(t)
	plan, op := storePlan(t, work)
	_, err = f.s.CommitDispatch(t.Context(), work.Handle, dispatch.Action{Kind: dispatch.BeginPreparation, Plan: &plan, PreparationID: op})
	requireNoError(t, err)
	return provider.SharedResourceClaim{Identity: plan.Job.Identity, PreparationID: op, PlanSHA256: plan.Digest(), ContentSHA256: sha, Bytes: size}
}

func TestSharedResourceTrackingSerializesLastSlotAcrossOriginalPlans(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	setResourceProvider(t, f, "kaggle")
	settings := scheduler.DefaultSettings()
	settings.MaxActivePerAccount = 64
	requireNoError(t, f.s.ConfigureScheduler(t.Context(), settings))
	first := freezeResourceClaim(t, f, "a", 1, provider.Digest([]byte("first large input")), 1)
	second := freezeResourceClaim(t, f, "a", 2, provider.Digest([]byte("second large input")), 1)
	identity, _ := json.Marshal(first.Identity)
	for n := 0; n < 127; n++ {
		sha := provider.Digest([]byte(fmt.Sprintf("retained-%d", n)))
		_, err := f.s.db.Exec(`INSERT INTO shared_resource_claims(installation_id,account_scope,resource_key,content_sha256,bytes,preparation_id,identity,plan_sha256,creation_started,created_at) VALUES(?,?,?,?,?,?,?,?,1,?)`, first.Identity.InstallationID, f.profile.AccountScope, provider.SharedResourceKey(sha, 1), sha, 1, first.PreparationID, string(identity), first.PlanSHA256, f.clock.Now().Format(time.RFC3339Nano))
		requireNoError(t, err)
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, claim := range []provider.SharedResourceClaim{first, second} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- f.s.TrackSharedUse(context.Background(), f.profile.AccountScope, claim)
		}()
	}
	wg.Wait()
	close(results)
	accepted, refused := 0, 0
	for err := range results {
		if err == nil {
			accepted++
		} else if errors.Is(err, provider.ErrResourceCountLimit) {
			refused++
		} else {
			t.Fatal(err)
		}
	}
	if accepted != 1 || refused != 1 || controlCount(t, f.s, "SELECT count(*) FROM shared_resource_claims") != 128 {
		t.Fatal("shared last slot raced", accepted, refused)
	}
}

func TestSharedResourceMigrationPreservesLegacyPlansAndChargesOvercap(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	setResourceProvider(t, f, "kaggle")
	claim := freezeResourceClaim(t, f, "a", 1, provider.Digest([]byte("legacy plausible shared input")))
	original := f.journal(t, scheduler.Identity{WorkspaceID: "a", JobID: "job_1", AttemptID: "attempt_1"}).Plan.Digest()
	f.root = copyFixtureAtSchema(t, f.s, 40)
	f.restart(t)
	var key string
	var started, legacy bool
	requireNoError(t, f.s.db.QueryRow(`SELECT resource_key,creation_started,legacy FROM shared_resource_claims`).Scan(&key, &started, &legacy))
	if key != provider.SharedResourceKey(claim.ContentSHA256, claim.Bytes) || !started || !legacy {
		t.Fatal("legacy shared ownership was rearmed", key, started, legacy)
	}
	if got := f.journal(t, scheduler.Identity{WorkspaceID: "a", JobID: "job_1", AttemptID: "attempt_1"}).Plan.Digest(); got != original {
		t.Fatal("migration rewrote original plan")
	}
	requireNoError(t, f.s.TrackSharedUse(t.Context(), f.profile.AccountScope, claim))
	ok, err := f.s.ClaimSharedCreation(t.Context(), f.profile.AccountScope, claim)
	requireNoError(t, err)
	if ok {
		t.Fatal("legacy unknown creation repeated")
	}
	// Restored legacy reservations may already exceed policy; they stay retained,
	// and existing users still work. No migration silently discards old charges.
	identity, _ := json.Marshal(claim.Identity)
	for n := 0; n < 128; n++ {
		sha := provider.Digest([]byte(fmt.Sprintf("legacy-retained-%d", n)))
		_, err = f.s.db.Exec(`INSERT INTO shared_resource_claims(installation_id,account_scope,resource_key,content_sha256,bytes,preparation_id,identity,plan_sha256,creation_started,legacy,created_at) VALUES(?,?,?,?,?,?,?,?,1,1,?)`, claim.Identity.InstallationID, f.profile.AccountScope, provider.SharedResourceKey(sha, 1), sha, 1, claim.PreparationID, string(identity), claim.PlanSHA256, f.clock.Now().Format(time.RFC3339Nano))
		requireNoError(t, err)
	}
	f.restart(t)
	requireNoError(t, f.s.TrackSharedUse(t.Context(), f.profile.AccountScope, claim))
	if n := controlCount(t, f.s, "SELECT count(*) FROM shared_resource_claims"); n != 129 {
		t.Fatal("restart dropped overcap legacy obligations", n)
	}
	err = f.s.write(t.Context(), func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
		_, err := missingSharedCapacity(ctx, tx, claim.Identity.InstallationID, f.profile.AccountScope, []domain.ObjectMetadata{{SHA256: provider.Digest([]byte("new large input")), Bytes: 256 << 20}})
		return err
	})
	if !errors.Is(err, provider.ErrResourceCountLimit) {
		t.Fatal("overcap legacy state allowed growth", err)
	}
}

func pendingSharedPlan(t *testing.T, f *dispatchFixture, n int, hashes []domain.SHA256Digest) (dispatch.Work, provider.Plan, domain.OperationID) {
	t.Helper()
	var body map[string]any
	requireNoError(t, json.Unmarshal([]byte(dispatchJSON), &body))
	var inputs []map[string]any
	var objects []domain.ObjectMetadata
	for i, sha := range hashes {
		object := domain.ObjectMetadata{ID: domain.ObjectID(fmt.Sprintf("pending_%d_%d", n, i)), WorkspaceID: "a", Bytes: 256 << 20, SHA256: sha}
		requireNoError(t, f.s.CommitObject(t.Context(), object))
		objects = append(objects, object)
		inputs = append(inputs, map[string]any{"name": fmt.Sprintf("data%d", i), "target": fmt.Sprintf("data%d.bin", i), "source": map[string]any{"kind": "object", "object_id": object.ID}})
	}
	body["inputs"] = inputs
	raw, err := json.Marshal(body)
	requireNoError(t, err)
	oldSeed := f.authorizeSeed
	f.authorizeSeed = false
	id := f.seedRaw(t, "a", n, false, string(raw))
	f.authorizeSeed = oldSeed
	_, err = f.s.db.Exec(`DELETE FROM job_objects WHERE workspace_id='a' AND job_id=? AND role LIKE 'input:%'`, id.JobID)
	requireNoError(t, err)
	for i, object := range objects {
		_, err = f.s.db.Exec(`INSERT INTO job_objects VALUES('a',?,?,?,?,?)`, id.JobID, fmt.Sprintf("input:%d", i), object.ID, object.Bytes, object.SHA256)
		requireNoError(t, err)
	}
	service, actor := authorizationService(t, f, "a", auth.Execute)
	_, err = service.Authorize(t.Context(), actor, "a", id.JobID, "pending-grant-"+string(id.AttemptID), authorizationRequest(id.AttemptID, 10))
	requireNoError(t, err)
	work := f.claim(t)
	plan, prep := storePlan(t, work)
	return work, plan, prep
}

func seedSharedReservations(t *testing.T, f *dispatchFixture, owner provider.SharedResourceClaim, count int) {
	t.Helper()
	identity, _ := json.Marshal(owner.Identity)
	for n := 0; n < count; n++ {
		sha := provider.Digest([]byte(fmt.Sprintf("batch-retained-%d", n)))
		_, err := f.s.db.Exec(`INSERT INTO shared_resource_claims(installation_id,account_scope,resource_key,content_sha256,bytes,preparation_id,identity,plan_sha256,creation_started,created_at) VALUES(?,?,?,?,?,?,?,?,1,?)`, owner.Identity.InstallationID, f.profile.AccountScope, provider.SharedResourceKey(sha, 1), sha, 1, owner.PreparationID, string(identity), owner.PlanSHA256, f.clock.Now().Format(time.RFC3339Nano))
		requireNoError(t, err)
	}
}

func TestSharedPreparationCountRefusalDoesNotPartiallyReserve(t *testing.T) {
	f, owner := sharedClaimFixture(t)
	settings := scheduler.DefaultSettings()
	settings.MaxActivePerAccount = 64
	requireNoError(t, f.s.ConfigureScheduler(t.Context(), settings))
	hashes := []domain.SHA256Digest{provider.Digest([]byte("batch-first")), provider.Digest([]byte("batch-second"))}
	work, plan, prep := pendingSharedPlan(t, f, 2, hashes)
	seedSharedReservations(t, f, owner, 127)
	_, err := f.s.CommitDispatch(t.Context(), work.Handle, dispatch.Action{Kind: dispatch.BeginPreparation, Plan: &plan, PreparationID: prep})
	if !errors.Is(err, provider.ErrResourceCountLimit) || provider.Reason(err, "") != "remote_shared_dataset_limit" {
		t.Fatal("shared keyset crossed cap", err)
	}
	if n := controlCount(t, f.s, "SELECT count(*) FROM shared_resource_claims"); n != 127 {
		t.Fatal("refused keyset partially reserved", n)
	}
	if n := controlCount(t, f.s, "SELECT count(*) FROM provider_resources"); n != 1 {
		t.Fatal("refused preparation crossed intent", n)
	}
	updated, err := f.s.CommitDispatch(t.Context(), work.Handle, dispatch.Action{Kind: dispatch.RefuseNewWork, RefusalReason: "remote_shared_dataset_limit"})
	requireNoError(t, err)
	if updated.Journal.Plan != nil || updated.Job.Attempt.State.Orchestration != domain.OrchestrationQueued {
		t.Fatal("shared cap terminalized or intended preparation")
	}
}

func TestSharedPreparationBatchReservationSerializesDisjointKeysets(t *testing.T) {
	f, owner := sharedClaimFixture(t)
	settings := scheduler.DefaultSettings()
	settings.MaxActivePerAccount = 64
	requireNoError(t, f.s.ConfigureScheduler(t.Context(), settings))
	first, p1, o1 := pendingSharedPlan(t, f, 2, []domain.SHA256Digest{provider.Digest([]byte("a1")), provider.Digest([]byte("a2"))})
	second, p2, o2 := pendingSharedPlan(t, f, 3, []domain.SHA256Digest{provider.Digest([]byte("b1")), provider.Digest([]byte("b2"))})
	seedSharedReservations(t, f, owner, 126)
	type proposal struct {
		work dispatch.Work
		plan provider.Plan
		prep domain.OperationID
	}
	proposals := []proposal{{first, p1, o1}, {second, p2, o2}}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, p := range proposals {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := f.s.CommitDispatch(context.Background(), p.work.Handle, dispatch.Action{Kind: dispatch.BeginPreparation, Plan: &p.plan, PreparationID: p.prep})
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	accepted, refused := 0, 0
	for err := range results {
		if err == nil {
			accepted++
		} else if errors.Is(err, provider.ErrResourceCountLimit) {
			refused++
		} else {
			t.Fatal(err)
		}
	}
	if accepted != 1 || refused != 1 {
		t.Fatal("keyset admission raced", accepted, refused)
	}
	if n := controlCount(t, f.s, "SELECT count(*) FROM shared_resource_claims"); n != 128 {
		t.Fatal("keyset partially committed", n)
	}
	if n := controlCount(t, f.s, "SELECT count(*) FROM provider_resources"); n != 2 {
		t.Fatal("losing preparation left intent", n)
	}
}

func TestSharedPreparationCanReuseExistingKeyAtCountCap(t *testing.T) {
	f, owner := sharedClaimFixture(t)
	settings := scheduler.DefaultSettings()
	settings.MaxActivePerAccount = 64
	requireNoError(t, f.s.ConfigureScheduler(t.Context(), settings))
	sha := provider.Digest([]byte("already reserved large content"))
	work, plan, prep := pendingSharedPlan(t, f, 2, []domain.SHA256Digest{sha})
	seedSharedReservations(t, f, owner, 127)
	identity, _ := json.Marshal(owner.Identity)
	_, err := f.s.db.Exec(`INSERT INTO shared_resource_claims(installation_id,account_scope,resource_key,content_sha256,bytes,preparation_id,identity,plan_sha256,creation_started,created_at) VALUES(?,?,?,?,?,?,?,?,1,?)`, owner.Identity.InstallationID, f.profile.AccountScope, provider.SharedResourceKey(sha, 256<<20), sha, 256<<20, owner.PreparationID, string(identity), owner.PlanSHA256, f.clock.Now().Format(time.RFC3339Nano))
	requireNoError(t, err)
	_, err = f.s.CommitDispatch(t.Context(), work.Handle, dispatch.Action{Kind: dispatch.BeginPreparation, Plan: &plan, PreparationID: prep})
	requireNoError(t, err)
	if n := controlCount(t, f.s, "SELECT count(*) FROM shared_resource_claims"); n != 128 {
		t.Fatal("reuse added shared obligation", n)
	}
	claim := provider.SharedResourceClaim{Identity: plan.Job.Identity, PreparationID: prep, PlanSHA256: plan.Digest(), ContentSHA256: sha, Bytes: 256 << 20}
	requireNoError(t, f.s.TrackSharedUse(t.Context(), f.profile.AccountScope, claim))
	ok, err := f.s.ClaimSharedCreation(t.Context(), f.profile.AccountScope, claim)
	requireNoError(t, err)
	if ok {
		t.Fatal("new user rearmed original shared create")
	}
}
