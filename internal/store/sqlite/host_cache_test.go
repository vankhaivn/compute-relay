package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hostcache"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

const cacheMachine = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
const cacheNativeEngine = "synthetic-engine-01"
const cacheEngine = "32f98314e1cb06675fb34e7e86cb0ceee4a3471818a8fc21696da26b79b7e397"

func cacheAdmissionFixture(t *testing.T) admissionFixture {
	t.Helper()
	f := newAdmission(t)
	ctx := context.Background()
	for _, c := range []struct {
		workspace, connection, profile, credential string
	}{
		{"a", "con_admission_a", "cache-host-a", "vault:con_admission_a"},
		{"b", "con_admission_b", "cache-host-b", "vault:con_admission_b"},
	} {
		p := f.profile
		p.Binding.Profile = c.profile
		p.Binding.ConfigurationRevision = "rev_cache_" + c.workspace
		p.CredentialRef = c.credential
		p.ResourceScope = "resource_" + cacheMachine
		p.CostClass = admission.CostOperatorManaged
		requireNoError(t, f.s.PutProfile(ctx, p, true))
		requireNoError(t, f.s.PutWorkspace(ctx, auth.Workspace{ID: domain.WorkspaceID(c.workspace), Enabled: true, AllowedProfiles: []string{c.profile}}))
		_, err := f.s.db.Exec(`UPDATE managed_connections SET provider_type='ssh',current_profile=? WHERE connection_id=?`, c.profile, c.connection)
		requireNoError(t, err)
		_, err = f.s.db.Exec(`INSERT INTO managed_profile_runtime(profile,revision,config) VALUES(?,?,?)`, c.profile, p.Binding.ConfigurationRevision,
			[]byte(`{"schema_version":3,"image":"sha256:`+strings.Repeat("a", 64)+`","architecture":"amd64","recipe_sha256":"`+strings.Repeat("b", 64)+`","machine_id":"`+cacheMachine+`","engine_id":"`+cacheEngine+`"}`))
		requireNoError(t, err)
	}
	_, err := f.s.db.Exec(`INSERT INTO managed_resource_accounts(account_scope,resource_kind,resource_scope)
	 VALUES(?,'machine',?)`, f.profile.AccountScope, "resource_"+cacheMachine)
	requireNoError(t, err)
	// Direct store fixtures do not run the managed SSH scanner. Supply the
	// complete empty-machine observation that production admission refreshes
	// before accepting new work.
	info, err := f.s.Info(ctx)
	requireNoError(t, err)
	requireNoError(t, f.s.RecordStorageScan(ctx, hoststorage.Scan{
		Scope:      hoststorage.Scope{InstallationID: domain.RuntimeInstallationID(info.InstallationID), MachineID: cacheMachine, EngineID: cacheEngine},
		ObservedAt: time.Now().UTC(), Objects: []hoststorage.Object{},
	}))
	return f
}

func cacheJob(profile string, bundleExpectation, inputExpectation string) []byte {
	raw := strings.Replace(admissionJSON, `"profile":"default-gpu"`, `"profile":"`+profile+`"`, 1)
	raw = strings.Replace(raw, `"bundle":{"object_id":"code"}`, `"bundle":{"object_id":"code"`+bundleExpectation+`}`, 1)
	raw = strings.Replace(raw, `"source":{"kind":"object","object_id":"data"}`, `"source":{"kind":"object","object_id":"data"`+inputExpectation+`}`, 1)
	return []byte(raw)
}

func cacheOwner(w domain.WorkspaceID, r admission.Receipt) hostcache.Attempt {
	return hostcache.Attempt{WorkspaceID: w, JobID: r.JobID, AttemptID: r.AttemptID}
}

func completeCacheObservation(g hostcache.Generation) hostcache.Observation {
	return hostcache.Observation{GenerationID: g.ID, VolumeName: g.VolumeName, OperationID: g.OperationID, Key: g.Key,
		Kind: hostcache.ObservedComplete, Labels: g.Labels(), ContentSHA256: g.Key.SHA256, ContentBytes: g.Key.Bytes, WriterInactive: true}
}

func TestHostCacheNoPreparationReleaseNeedsDurableNoRemoteProof(t *testing.T) {
	f := cacheAdmissionFixture(t)
	ctx := context.Background()
	first, err := f.service.Submit(ctx, f.p, "a", "cache-cancel-before-prepare", cacheJob("cache-host-a", "", ""))
	requireNoError(t, err)
	owner := cacheOwner("a", first)
	before, err := f.s.Pins(ctx, owner)
	requireNoError(t, err)
	if len(before) == 0 {
		t.Fatal("admission did not pin cache")
	}
	requireNoError(t, f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		return releaseHostCachePinsWithoutPreparation(ctx, tx, owner.WorkspaceID, owner.JobID, owner.AttemptID, now)
	}))
	after, err := f.s.Pins(ctx, owner)
	requireNoError(t, err)
	if len(after) != 0 {
		t.Fatal("durably unprepared attempt retained cache pins")
	}
	second, err := f.service.Submit(ctx, f.p, "a", "cache-barrier-crossed", cacheJob("cache-host-a", "", ""))
	requireNoError(t, err)
	owner = cacheOwner("a", second)
	_, err = f.s.db.ExecContext(ctx, `UPDATE scheduler_queue SET dispatch_barrier=1 WHERE workspace_id=? AND job_id=? AND attempt_id=?`, owner.WorkspaceID, owner.JobID, owner.AttemptID)
	requireNoError(t, err)
	err = f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		return releaseHostCachePinsWithoutPreparation(ctx, tx, owner.WorkspaceID, owner.JobID, owner.AttemptID, now)
	})
	if !errors.Is(err, hostcache.ErrUnknown) {
		t.Fatalf("crossed preparation barrier released without cleanup proof: %v", err)
	}
	after, err = f.s.Pins(ctx, owner)
	requireNoError(t, err)
	if len(after) == 0 {
		t.Fatal("uncertain remote mount lost its pins")
	}
}

func TestHostCacheAdmissionPinsAndMetadataExpectations(t *testing.T) {
	f := cacheAdmissionFixture(t)
	ctx := context.Background()
	matching := `,"sha256":"` + strings.Repeat("a", 64) + `","bytes":7`
	badSize := `,"sha256":"` + strings.Repeat("a", 64) + `","bytes":8`
	badDigest := `,"sha256":"` + strings.Repeat("b", 64) + `","bytes":7`
	for _, tc := range []struct{ bundle, input string }{{badSize, matching}, {matching, badDigest}} {
		_, err := f.service.Submit(ctx, f.p, "a", "mismatch-"+tc.bundle+tc.input, cacheJob("cache-host-a", tc.bundle, tc.input))
		if !errors.Is(err, admission.ErrInputs) {
			t.Fatalf("metadata mismatch reached acceptance: %v", err)
		}
	}
	if tableCount(t, f.s, "jobs") != 0 {
		t.Fatal("mismatched expectation created a job")
	}
	receipt, err := f.service.Submit(ctx, f.p, "a", "cache-valid", cacheJob("cache-host-a", matching, matching))
	requireNoError(t, err)
	pins, err := f.s.Pins(ctx, cacheOwner("a", receipt))
	requireNoError(t, err)
	if len(pins) != 1 || pins[0].SHA256 != domain.SHA256Digest(strings.Repeat("a", 64)) || pins[0].Bytes != 7 ||
		pins[0].MachineID != cacheMachine || pins[0].EngineID != cacheEngine {
		t.Fatalf("admission failed to deduplicate exact frozen keys: %+v", pins)
	}
	replay, err := f.service.Submit(ctx, f.p, "a", "cache-valid", cacheJob("cache-host-a", matching, matching))
	requireNoError(t, err)
	if !replay.Replay || replay.JobID != receipt.JobID {
		t.Fatal("admission replay changed the cache-pinned job")
	}
	pins, err = f.s.Pins(ctx, cacheOwner("a", receipt))
	requireNoError(t, err)
	if len(pins) != 1 {
		t.Fatal("replay duplicated cache pins")
	}
}

func TestHostCacheSingleWriterUnknownRestartAndNewGeneration(t *testing.T) {
	f := cacheAdmissionFixture(t)
	ctx := context.Background()
	ra, err := f.service.Submit(ctx, f.p, "a", "cache-job-a", cacheJob("cache-host-a", "", ""))
	requireNoError(t, err)
	rb, err := f.service.Submit(ctx, f.other, "b", "cache-job-b", cacheJob("cache-host-b", "", ""))
	requireNoError(t, err)
	a, b := cacheOwner("a", ra), cacheOwner("b", rb)
	pins, err := f.s.Pins(ctx, a)
	requireNoError(t, err)
	if len(pins) != 1 {
		t.Fatal("expected one deduplicated key")
	}
	key := pins[0]
	var wg sync.WaitGroup
	results := make(chan hostcache.Generation, 12)
	created := make(chan bool, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			owner := a
			if i%2 == 1 {
				owner = b
			}
			g, fresh, err := f.s.Claim(ctx, owner, key)
			if err != nil {
				t.Error(err)
				return
			}
			results <- g
			created <- fresh
		}(i)
	}
	wg.Wait()
	close(results)
	close(created)
	first := hostcache.Generation{}
	for g := range results {
		if first.ID == "" {
			first = g
		}
		if g.ID != first.ID || g.OperationID != first.OperationID || g.VolumeName != first.VolumeName {
			t.Fatal("concurrent first use created different generations")
		}
	}
	count := 0
	for fresh := range created {
		if fresh {
			count++
		}
	}
	if count != 1 || first.State != hostcache.Claimed {
		t.Fatalf("expected one durable writer, got %d", count)
	}
	missing := hostcache.Observation{GenerationID: first.ID, VolumeName: first.VolumeName, OperationID: first.OperationID, Key: key,
		Kind: hostcache.ObservedMissing, WriterInactive: true}
	corrupt := hostcache.Observation{GenerationID: first.ID, VolumeName: first.VolumeName, OperationID: first.OperationID, Key: key,
		Kind: hostcache.ObservedCorrupt, WriterInactive: true}
	// A has committed Claim but has not yet made the host call. B's absence
	// observation cannot race ahead of A and free the key for a second writer.
	for _, observation := range []hostcache.Observation{missing, corrupt} {
		if err := f.s.Retire(ctx, observation); !errors.Is(err, hostcache.ErrUnknown) {
			t.Fatalf("paused claimant was retired from an absence observation: %v", err)
		}
	}
	reusedBeforeWrite, fresh, err := f.s.Claim(ctx, b, key)
	requireNoError(t, err)
	if fresh || reusedBeforeWrite.ID != first.ID {
		t.Fatal("second writer bypassed paused first claimant")
	}
	requireNoError(t, f.s.BeginWrite(ctx, first.Owner, first.ID))
	if err := f.s.BeginWrite(ctx, first.Owner, first.ID); !errors.Is(err, hostcache.ErrUnknown) {
		t.Fatalf("one-shot writer authorization replayed: %v", err)
	}
	for _, observation := range []hostcache.Observation{missing, corrupt} {
		if err := f.s.Retire(ctx, observation); !errors.Is(err, hostcache.ErrUnknown) {
			t.Fatalf("late writer was retired from an absence observation: %v", err)
		}
	}
	uncertain := completeCacheObservation(first)
	uncertain.WriterInactive = false
	if err := f.s.Verify(ctx, uncertain); !errors.Is(err, hostcache.ErrConflict) {
		t.Fatalf("unknown writer outcome promoted generation: %v", err)
	}
	active, err := f.s.Active(ctx, key)
	requireNoError(t, err)
	if active == nil || active.State != hostcache.Claimed {
		t.Fatal("unknown outcome lost the active claim")
	}
	requireNoError(t, f.s.Close())
	reopened, err := Open(ctx, f.root, DefaultOptions())
	requireNoError(t, err)
	defer reopened.Close()
	reused, fresh, err := reopened.Claim(ctx, b, key)
	requireNoError(t, err)
	if fresh || reused.ID != first.ID {
		t.Fatal("restart replayed an unresolved remote writer")
	}
	requireNoError(t, reopened.Verify(ctx, completeCacheObservation(first)))
	verified, err := reopened.Active(ctx, key)
	requireNoError(t, err)
	if verified == nil || verified.State != hostcache.Verified {
		t.Fatal("exact read-only verification did not publish generation")
	}
	_, fresh, err = reopened.Claim(ctx, b, key)
	requireNoError(t, err)
	if fresh {
		t.Fatal("verified hit requested another writer")
	}
	wrong := completeCacheObservation(first)
	wrong.ContentBytes++
	if err := reopened.Verify(ctx, wrong); !errors.Is(err, hostcache.ErrConflict) {
		t.Fatalf("wrong digest or size accepted: %v", err)
	}
	mount := hostcache.MountReceipt{Attempt: a, Generation: *verified, ContainerID: strings.Repeat("e", 64), ReadOnly: true, Labels: verified.Labels()}
	requireNoError(t, reopened.RecordMount(ctx, mount))
	if err := reopened.RecordMount(ctx, hostcache.MountReceipt{Attempt: a, Generation: *verified, ContainerID: strings.Repeat("f", 64), ReadOnly: true, Labels: verified.Labels()}); !errors.Is(err, hostcache.ErrConflict) {
		t.Fatal("mount receipt was repointed")
	}
	requireNoError(t, reopened.Retire(ctx, corrupt))
	replacement, fresh, err := reopened.Claim(ctx, b, key)
	requireNoError(t, err)
	if !fresh || replacement.ID == first.ID || replacement.VolumeName == first.VolumeName || replacement.OperationID == first.OperationID {
		t.Fatal("corrupt generation was overwritten in place")
	}
	inventory, err := reopened.Inventory(ctx, key.InstallationID, key.MachineID, key.EngineID)
	requireNoError(t, err)
	if len(inventory) != 2 {
		t.Fatalf("expected retired and fresh generations, got %d", len(inventory))
	}
	for _, g := range inventory {
		if g.Pins != 2 || g.Key.Bytes != 7 {
			t.Fatalf("inventory lost key-wide pins or physical bytes: %+v", g)
		}
		if g.ID == first.ID && (g.Mounts != 1 || g.LastUsedAt.IsZero()) {
			t.Fatal("old exact mount receipt was lost")
		}
	}
}

func TestHostCacheRetryPinsFrozenKey(t *testing.T) {
	f := cacheAdmissionFixture(t)
	ctx := context.Background()
	r, err := f.service.Submit(ctx, f.p, "a", "cache-retry", cacheJob("cache-host-a", "", ""))
	requireNoError(t, err)
	old := cacheOwner("a", r)
	newAttempt := hostcache.Attempt{WorkspaceID: "a", JobID: r.JobID, AttemptID: "att_retry_cache"}
	err = f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		state, err := json.Marshal(domain.InitialAttemptState())
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO attempts VALUES(?,?,?,?,?,?,?,?,?,?)`, old.WorkspaceID, old.JobID, newAttempt.AttemptID, 2,
			strings.Repeat("f", 64), string(state), "queued", 1, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		return pinHostCacheRetry(ctx, tx, old, newAttempt, now)
	})
	requireNoError(t, err)
	original, err := f.s.Pins(ctx, old)
	requireNoError(t, err)
	retried, err := f.s.Pins(ctx, newAttempt)
	requireNoError(t, err)
	if len(original) != 1 || len(retried) != 1 || original[0] != retried[0] {
		t.Fatal("retry did not inherit the exact frozen cache key")
	}
}

func TestHostCacheMissingClaimNeedsTerminalWriterBeforeReplacement(t *testing.T) {
	f := cacheAdmissionFixture(t)
	ctx := context.Background()
	r, err := f.service.Submit(ctx, f.p, "a", "cache-missing-claim", cacheJob("cache-host-a", "", ""))
	requireNoError(t, err)
	owner := cacheOwner("a", r)
	pins, err := f.s.Pins(ctx, owner)
	requireNoError(t, err)
	g, fresh, err := f.s.Claim(ctx, owner, pins[0])
	requireNoError(t, err)
	if !fresh {
		t.Fatal("expected first writer")
	}
	requireNoError(t, f.s.BeginWrite(ctx, owner, g.ID))
	missing := hostcache.Observation{GenerationID: g.ID, VolumeName: g.VolumeName, OperationID: g.OperationID, Key: g.Key,
		Kind: hostcache.ObservedMissing, WriterInactive: true}
	if err := f.s.Retire(ctx, missing); !errors.Is(err, hostcache.ErrUnknown) {
		t.Fatalf("a begun writer was replaced on absence alone: %v", err)
	}
	// The original claimant now knows the one-shot call has returned and can
	// durably close its ability to send before the absence becomes decisive.
	requireNoError(t, f.s.FinishWrite(ctx, owner, g.ID))
	requireNoError(t, f.s.Retire(ctx, missing))
	replacement, fresh, err := f.s.Claim(ctx, owner, pins[0])
	requireNoError(t, err)
	if !fresh || replacement.ID == g.ID || replacement.VolumeName == g.VolumeName {
		t.Fatal("terminal missing generation did not get a fresh identity")
	}
}

func TestHostCachePinsReleaseOnlyForExactConfirmedCleanup(t *testing.T) {
	f := cacheAdmissionFixture(t)
	ctx := context.Background()
	r, err := f.service.Submit(ctx, f.p, "a", "cache-cleanup", cacheJob("cache-host-a", "", ""))
	requireNoError(t, err)
	owner := cacheOwner("a", r)
	pins, err := f.s.Pins(ctx, owner)
	requireNoError(t, err)
	key := pins[0]
	g, fresh, err := f.s.Claim(ctx, owner, key)
	requireNoError(t, err)
	if !fresh {
		t.Fatal("fresh attempt did not claim generation")
	}
	requireNoError(t, f.s.BeginWrite(ctx, owner, g.ID))
	requireNoError(t, f.s.FinishWrite(ctx, owner, g.ID))
	requireNoError(t, f.s.Verify(ctx, completeCacheObservation(g)))
	active, err := f.s.Active(ctx, key)
	requireNoError(t, err)
	container := strings.Repeat("e", 64)
	requireNoError(t, f.s.RecordMount(ctx, hostcache.MountReceipt{Attempt: owner, Generation: *active, ContainerID: container, ReadOnly: true, Labels: active.Labels()}))
	op := domain.Operation{WorkspaceID: owner.WorkspaceID, JobID: owner.JobID, AttemptID: owner.AttemptID}
	staging := cleanup.ResourceWork{
		Resource: cleanup.Resource{Request: provider.CleanupResourceRequest{
			CleanupRequest: provider.CleanupRequest{Remote: provider.RemoteReference{
				Identity: provider.Identity{InstallationID: key.InstallationID},
				Resource: `{"container":"relay-job","id":"` + container + `","engine_id":"` + cacheNativeEngine + `"}`,
			}}, Purpose: "staging"}},
		Result: operations.ResourceResult{Purpose: "staging", State: operations.ResourceDeleted},
	}
	// A completed workload or an unresolved cleanup resource still holds pins.
	if err := f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		unresolved := staging
		unresolved.Result.State = operations.ResourceUnknown
		return releaseHostCachePinsOnCleanup(ctx, tx, op, []cleanup.ResourceWork{unresolved}, now)
	}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("unresolved cleanup released pins: %v", err)
	}
	wrong := staging
	wrong.Resource.Request.Remote.Resource = `{"container":"relay-job","id":"` + strings.Repeat("f", 64) + `","engine_id":"` + cacheNativeEngine + `"}`
	if err := f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		return releaseHostCachePinsOnCleanup(ctx, tx, op, []cleanup.ResourceWork{wrong}, now)
	}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("different container cleanup released pins: %v", err)
	}
	wrongEngine := staging
	wrongEngine.Resource.Request.Remote.Resource = `{"container":"relay-job","id":"` + container + `","engine_id":"another-engine-01"}`
	if err := f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		return releaseHostCachePinsOnCleanup(ctx, tx, op, []cleanup.ResourceWork{wrongEngine}, now)
	}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("different native engine released pins: %v", err)
	}
	inventory, err := f.s.Inventory(ctx, key.InstallationID, key.MachineID, key.EngineID)
	requireNoError(t, err)
	if len(inventory) != 1 || inventory[0].Pins != 1 || inventory[0].Mounts != 1 {
		t.Fatal("unconfirmed cleanup changed live cache accounting")
	}
	requireNoError(t, f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		return releaseHostCachePinsOnCleanup(ctx, tx, op, []cleanup.ResourceWork{staging}, now)
	}))
	inventory, err = f.s.Inventory(ctx, key.InstallationID, key.MachineID, key.EngineID)
	requireNoError(t, err)
	if len(inventory) != 1 || inventory[0].Pins != 0 || inventory[0].Mounts != 0 {
		t.Fatal("confirmed exact cleanup did not release live cache accounting")
	}
	pins, err = f.s.Pins(ctx, owner)
	requireNoError(t, err)
	if len(pins) != 0 {
		t.Fatal("released attempt still has active pins")
	}
}
