package kaggle

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle/kaggleapi"
)

type sharedClaimRecord struct {
	owner   provider.SharedResourceClaim
	started bool
}

// This registry survives adapter reconstruction in the tests. Its account scope
// is fixed by the fixture; the production composition root supplies that scope.
type sharedClaimsFixture struct {
	mu                             sync.Mutex
	rows                           map[string]sharedClaimRecord
	trackErr, claimErr             error
	claimGranted                   func(context.Context) error
	trackCalls, claimCalls, grants int
}

func newSharedClaimsFixture() *sharedClaimsFixture {
	return &sharedClaimsFixture{rows: map[string]sharedClaimRecord{}}
}

func sharedFixtureKey(c provider.SharedResourceClaim) string {
	return string(c.Identity.InstallationID) + ":" + provider.SharedResourceKey(c.ContentSHA256, c.Bytes)
}

func (f *sharedClaimsFixture) TrackSharedUse(ctx context.Context, c provider.SharedResourceClaim) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.trackCalls++
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.trackErr != nil {
		return f.trackErr
	}
	if !c.Valid() {
		return errors.New("invalid fixture claim")
	}
	key := sharedFixtureKey(c)
	if _, ok := f.rows[key]; ok {
		return nil
	}
	if len(f.rows) >= provider.RemoteResourceCountLimit {
		return provider.WithReason(provider.ErrResourceCountLimit, "remote_shared_dataset_limit")
	}
	f.rows[key] = sharedClaimRecord{owner: c}
	return nil
}

func (f *sharedClaimsFixture) ClaimSharedCreation(ctx context.Context, c provider.SharedResourceClaim) (bool, error) {
	f.mu.Lock()
	f.claimCalls++
	if f.claimErr != nil {
		f.mu.Unlock()
		return false, f.claimErr
	}
	key := sharedFixtureKey(c)
	row, ok := f.rows[key]
	if !ok || row.started || row.owner != c {
		f.mu.Unlock()
		return false, nil
	}
	row.started = true
	f.rows[key] = row
	f.grants++
	granted := f.claimGranted
	f.mu.Unlock()
	if granted != nil {
		if err := granted(ctx); err != nil {
			return false, err
		}
	}
	return true, nil
}

func TestSharedClaimsRequiredBeforeRemoteSharedUse(t *testing.T) {
	for _, present := range []bool{false, true} {
		t.Run(fmt.Sprint(present), func(t *testing.T) {
			p, plan, _, _, f := sharedRuntime(t)
			if present {
				if _, err := p.Prepare(t.Context(), plan, "prep"); err != nil {
					t.Fatal(err)
				}
			}
			p.stager.sharedClaims = nil
			uploads, creates := f.count("StartBlobUpload"), f.count("CreateDataset")
			_, err := p.Prepare(t.Context(), plan, "prep")
			if !errors.Is(err, ErrConfig) || provider.Reason(err, "") != "shared_claims_unavailable" || f.count("StartBlobUpload") != uploads || f.count("CreateDataset") != creates {
				t.Fatal("missing authority did not stop before shared reuse or mutation", err)
			}
		})
	}
}

func TestSharedClaimsLimitRefusesNewContentButPermitsRecordedReuse(t *testing.T) {
	p, plan, _, _, f := sharedRuntime(t)
	claims := p.stager.sharedClaims.(*sharedClaimsFixture)
	for i := 0; i < provider.RemoteResourceCountLimit; i++ {
		claims.rows[fmt.Sprint(i)] = sharedClaimRecord{}
	}
	_, err := p.Prepare(t.Context(), plan, "prep")
	if !errors.Is(err, provider.ErrResourceCountLimit) || provider.Reason(err, "") != "remote_shared_dataset_limit" || f.count("StartBlobUpload") != 0 || f.count("CreateDataset") != 0 {
		t.Fatal("shared count bound did not stop before upload/create", err)
	}
	seed, seedPlan, _, _, g := sharedRuntime(t)
	if _, err := seed.Prepare(t.Context(), seedPlan, "prep"); err != nil {
		t.Fatal(err)
	}
	seedClaims := seed.stager.sharedClaims.(*sharedClaimsFixture)
	for len(seedClaims.rows) < provider.RemoteResourceCountLimit {
		seedClaims.rows[fmt.Sprint(len(seedClaims.rows))] = sharedClaimRecord{}
	}
	q, next, _, _, h := sharedRuntime(t)
	q.stager.sharedClaims = seedClaims
	h.shared = g.shared
	if prepared, err := q.Prepare(t.Context(), next, "prep"); err != nil || !prepared.Ready || h.count("CreateDataset") != 1 || h.count("StartBlobUpload") != 2 {
		t.Fatal("recorded shared reuse consumed new capacity", prepared, err)
	}
}

func TestSharedClaimRecordsExistingReuseWithoutClaimingCreation(t *testing.T) {
	seed, plan, _, _, f := sharedRuntime(t)
	if _, err := seed.Prepare(t.Context(), plan, "prep"); err != nil {
		t.Fatal(err)
	}
	p, next, _, _, g := sharedRuntime(t)
	g.shared = f.shared
	claims := p.stager.sharedClaims.(*sharedClaimsFixture)
	if _, err := p.Prepare(t.Context(), next, "prep"); err != nil {
		t.Fatal(err)
	}
	if len(claims.rows) != 1 || claims.trackCalls == 0 || claims.claimCalls != 0 || g.count("CreateDataset") != 1 || g.count("StartBlobUpload") != 2 {
		t.Fatal("existing shared object was trusted without recording reuse or was recreated", claims)
	}
}

func TestSharedClaimRecordedObjectDisappearanceDoesNotRecreate(t *testing.T) {
	shortVisibility(t, 15*time.Millisecond, time.Millisecond)
	p, plan, _, _, f := sharedRuntime(t)
	if _, err := p.Prepare(t.Context(), plan, "prep"); err != nil {
		t.Fatal(err)
	}
	q, next, _, _, g := sharedRuntime(t)
	q.stager.sharedClaims = p.stager.sharedClaims
	// An operator/provider removal does not release the retained creation permit.
	clear(f.shared)
	g.shared = f.shared
	_, err := q.Prepare(t.Context(), next, "prep")
	if !errors.Is(err, ErrStaging) || provider.Reason(err, "") != "shared_creation_unresolved" || g.count("StartBlobUpload") != 0 || g.count("CreateDataset") != 0 || len(f.shared) != 0 {
		t.Fatal("an absent recorded identity was recreated", err, g.calls)
	}
}

// bindSharedTransport changes only the staging HTTP seam, retaining all native
// request encoding, response checks, source hashes and upload receipts.
func bindSharedTransport(p *RuntimeAdapter, f *nativeFixture, api func(*http.Request) (*http.Response, error), storage func(*http.Request) (*http.Response, error)) {
	p.stager.run = func(ctx context.Context, _ Config, mode string, token []byte, plan stagingPlan, blobs StagingBlobs) (stagingResponse, error) {
		client, err := kaggleapi.New(token, kaggleapi.Options{APITransport: apiRoundTripFunc(api), StorageTransport: apiRoundTripFunc(storage)})
		if err != nil {
			return stagingResponse{}, err
		}
		defer client.Close()
		return stage(ctx, client, mode, plan, blobs)
	}
}

func TestSharedClaimUnknownCreateSurvivesRestartAndNewAttempt(t *testing.T) {
	shortVisibility(t, 15*time.Millisecond, time.Millisecond)
	p, plan, _, _, f := sharedRuntime(t)
	claims := p.stager.sharedClaims.(*sharedClaimsFixture)
	// Drop the create response without proving that it reached the provider.
	// An absent name afterward must not permit another creation invocation.
	bindSharedTransport(p, f, func(r *http.Request) (*http.Response, error) {
		if path.Base(r.URL.Path) == "CreateDataset" {
			f.calls = append(f.calls, "CreateDataset")
			return nil, errors.New("untrusted response text")
		}
		return f.api(r)
	}, f.storage)
	_, err := p.Prepare(t.Context(), plan, "prep")
	if !errors.Is(err, ErrStaging) || provider.Reason(err, "") != "shared_create_failed" || f.count("CreateDataset") != 1 || f.count("StartBlobUpload") != 2 || claims.grants != 1 {
		t.Fatal("unknown create lost its reservation", err, f.calls)
	}
	for _, mode := range []string{"same-invocation-owner", "restart", "new-attempt"} {
		t.Run(mode, func(t *testing.T) {
			q, next, _, _, g := sharedRuntime(t)
			q.stager.sharedClaims = claims
			if mode == "same-invocation-owner" {
				q = p
				next = plan
				g = f
			}
			if mode == "new-attempt" {
				next.Job.Identity.AttemptID = "later-attempt"
				next.Job.Identity.IntentID = "later-intent"
				next.Job.Identity.ResourceKey = "later-resource"
				next.Job.Identity.Nonce = strings.Repeat("b", 32)
			}
			uploads, creates := g.count("StartBlobUpload"), g.count("CreateDataset")
			_, err := q.Prepare(t.Context(), next, "prep")
			if !errors.Is(err, ErrStaging) || provider.Reason(err, "") != "shared_creation_unresolved" || g.count("CreateDataset") != creates || g.count("StartBlobUpload") != uploads || claims.grants != 1 {
				t.Fatal("another invocation recreated an uncertain shared resource", err, g.calls)
			}
		})
	}
}

func TestSharedClaimsConcurrentCreatorHasOnePermit(t *testing.T) {
	shortVisibility(t, time.Second, time.Millisecond)
	p, plan, _, _, f := sharedRuntime(t)
	q, next, _, _, g := sharedRuntime(t)
	next.Job.Identity.AttemptID = "concurrent-attempt"
	next.Job.Identity.IntentID = "concurrent-intent"
	next.Job.Identity.ResourceKey = "concurrent-resource"
	next.Job.Identity.Nonce = strings.Repeat("b", 32)
	claims := p.stager.sharedClaims.(*sharedClaimsFixture)
	q.stager.sharedClaims = claims
	g.shared = f.shared
	granted, release := make(chan struct{}), make(chan struct{})
	claims.claimGranted = func(ctx context.Context) error {
		close(granted)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	var transportMu sync.Mutex
	bind := func(p *RuntimeAdapter, f *nativeFixture) {
		bindSharedTransport(p, f, func(r *http.Request) (*http.Response, error) {
			transportMu.Lock()
			defer transportMu.Unlock()
			return f.api(r)
		}, func(r *http.Request) (*http.Response, error) {
			transportMu.Lock()
			defer transportMu.Unlock()
			return f.storage(r)
		})
	}
	bind(p, f)
	bind(q, g)
	result := make(chan error, 2)
	go func() {
		prepared, err := p.Prepare(t.Context(), plan, "prep")
		if err == nil && !prepared.Ready {
			err = errors.New("first not ready")
		}
		result <- err
	}()
	select {
	case <-granted:
	case <-time.After(time.Second):
		t.Fatal("creator did not reserve")
	}
	go func() {
		prepared, err := q.Prepare(t.Context(), next, "prep")
		if err == nil && !prepared.Ready {
			err = errors.New("second not ready")
		}
		result <- err
	}()
	deadline := time.After(time.Second)
	for {
		claims.mu.Lock()
		calls := claims.claimCalls
		claims.mu.Unlock()
		if calls == 2 {
			break
		}
		select {
		case <-deadline:
			close(release)
			t.Fatal("second creator did not observe reservation")
		case <-time.After(time.Millisecond):
		}
	}
	close(release)
	for range 2 {
		if err := <-result; err != nil {
			t.Fatal(err)
		}
	}
	if claims.grants != 1 || f.count("CreateDataset")+g.count("CreateDataset") != 3 || f.count("StartBlobUpload")+g.count("StartBlobUpload") != 6 {
		t.Fatal("concurrent preparations repeated shared upload/create", claims.grants, f.calls, g.calls)
	}
}

func TestSharedClaimLocalFailuresRetainLocalOrigin(t *testing.T) {
	for _, step := range []string{"track", "claim"} {
		t.Run(step, func(t *testing.T) {
			p, plan, _, _, f := sharedRuntime(t)
			claims := p.stager.sharedClaims.(*sharedClaimsFixture)
			cause := errors.New("private store diagnostic")
			local := provider.LocalStateError{Err: cause}
			if step == "track" {
				claims.trackErr = local
			} else {
				claims.claimErr = local
			}
			_, err := p.Prepare(t.Context(), plan, "prep")
			assertLocalStoreFailure(t, err, cause)
			want := "shared_use_record_failed"
			if step == "claim" {
				want = "shared_creation_claim_failed"
			}
			if provider.Reason(err, "") != want || f.count("StartBlobUpload") != 0 || f.count("CreateDataset") != 0 {
				t.Fatal("local failure reached remote mutation or lost reason", err)
			}
		})
	}
}
