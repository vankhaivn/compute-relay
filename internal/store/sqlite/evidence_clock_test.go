package sqlite

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func TestQuotaEvidenceKeepsObservedTimeAndRejectsFutureEvidence(t *testing.T) {
	opts := DefaultOptions()
	opts.Clock = func() time.Time { return schedulerNow.Add(123 * time.Nanosecond) }
	s, _ := clockStore(t, opts, len(migrations))
	observed := schedulerNow.Add(-time.Second).Add(123 * time.Nanosecond)
	q := schedulerQuota(10, observed)
	if err := s.RecordQuota(testctx, "account", "gpu", q); err != nil {
		t.Fatal(err)
	}
	var raw string
	if err := s.db.QueryRow(`SELECT observation FROM scheduler_quotas WHERE account_scope='account' AND resource='gpu'`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var stored provider.QuotaObservation
	if json.Unmarshal([]byte(raw), &stored) != nil || !stored.ObservedAt.Equal(observed) {
		t.Fatal("quota observation was replaced by repository time", raw)
	}
	q.ObservedAt = schedulerNow.Add(time.Second)
	if err := s.RecordQuota(testctx, "account", "gpu", q); !errors.Is(err, scheduler.ErrInvalid) {
		t.Fatal("future quota evidence accepted", err)
	}
}

func TestProgressEvidenceKeepsObservedTimeAndRejectsFutureEvidence(t *testing.T) {
	t.Run("preparation", func(t *testing.T) {
		f := newDispatchFixture(t, fake.DefaultScenario())
		f.seed(t, "a", 1, false)
		claim, err := f.s.ClaimNext(testctx, "evidence-worker")
		if err != nil || claim.Claim == nil {
			t.Fatal(claim, err)
		}
		work, err := f.s.LoadDispatch(testctx, *claim.Claim)
		if err != nil {
			t.Fatal(err)
		}
		work, err = f.s.ConsumeAuthorization(testctx, work.Handle)
		if err != nil {
			t.Fatal(err)
		}
		plan, operation := storePlan(t, work)
		work, err = f.s.CommitDispatch(testctx, work.Handle, dispatch.Action{Kind: dispatch.BeginPreparation, Plan: &plan, PreparationID: operation})
		if err != nil {
			t.Fatal(err)
		}
		observed := f.clock.Now().Add(123 * time.Nanosecond)
		f.clock.advance(time.Second)
		sample := domain.PreparationProgress{Scope: "staged_input_bytes", Generation: work.Handle.Claim.Generation, BytesCompleted: 1, BytesTotal: 2, ObservedAt: observed}
		if err = f.s.RecordPreparationProgress(testctx, work.Handle, sample); err != nil {
			t.Fatal(err)
		}
		var raw string
		if err = f.s.db.QueryRow(`SELECT progress FROM preparation_progress WHERE workspace_id='a'`).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var stored domain.PreparationProgress
		if json.Unmarshal([]byte(raw), &stored) != nil || !stored.ObservedAt.Equal(observed) {
			t.Fatal("preparation observation was replaced by repository time", raw)
		}
		sample.ObservedAt = f.clock.Now().Add(time.Second)
		if err = f.s.RecordPreparationProgress(testctx, work.Handle, sample); !errors.Is(err, dispatch.ErrInvalid) {
			t.Fatal("future preparation evidence accepted", err)
		}
	})
	t.Run("collection", func(t *testing.T) {
		f, _, _, _ := collectionFixture(t)
		work, err := f.s.ClaimCollection(testctx, time.Minute, 1)
		if err != nil || work == nil {
			t.Fatal(work, err)
		}
		observed := f.clock.Now().Add(123 * time.Nanosecond)
		f.clock.advance(time.Second)
		sample := initialProgress(*work, observed)
		if err = f.s.RecordCollectionProgress(testctx, *work, "discovering", sample); err != nil {
			t.Fatal(err)
		}
		var raw string
		if err = f.s.db.QueryRow(`SELECT progress FROM collection_progress WHERE workspace_id='a'`).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var stored domain.CollectionProgress
		if json.Unmarshal([]byte(raw), &stored) != nil || !stored.ObservedAt.Equal(observed) {
			t.Fatal("collection observation was replaced by repository time", raw)
		}
		sample.ObservedAt = f.clock.Now().Add(time.Second)
		if err = f.s.RecordCollectionProgress(testctx, *work, "discovering", sample); !errors.Is(err, collection.ErrInvalid) {
			t.Fatal("future collection evidence accepted", err)
		}
	})
}
