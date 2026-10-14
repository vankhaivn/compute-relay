package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func workLeaseFixture(t *testing.T, kind workKind) (*dispatchFixture, workLease, func() (time.Time, error)) {
	t.Helper()
	ctx := context.Background()
	switch kind {
	case dispatchWork:
		f := newDispatchFixture(t, fake.DefaultScenario())
		f.seed(t, "a", 1, false)
		settings := scheduler.DefaultSettings()
		settings.LeaseDuration = time.Second
		if err := f.s.ConfigureScheduler(ctx, settings); err != nil {
			t.Fatal(err)
		}
		w := f.claim(t)
		return f, dispatchLease(w.Handle.Claim), func() (time.Time, error) {
			h, err := f.s.RenewDispatch(ctx, w.Handle)
			return h.Claim.ExpiresAt, err
		}
	case collectionWorkKind:
		f, _, _, _ := collectionFixture(t)
		w, err := f.s.ClaimCollection(ctx, time.Second, 2)
		if err != nil || w == nil {
			t.Fatal("claim", err)
		}
		return f, collectionLease(w.Lease), func() (time.Time, error) { return f.s.RenewCollection(ctx, *w, time.Second) }
	default:
		f, id := collectedFixture(t)
		admitCleanup(t, f, id, "lease-fixture", false)
		w, err := f.s.ClaimCleanup(ctx, time.Second)
		if err != nil || w == nil {
			t.Fatal("claim", err)
		}
		return f, cleanupLease(w.Lease), func() (time.Time, error) { return f.s.RenewCleanup(ctx, *w, time.Second) }
	}
}

func TestWorkLeaseFencingRenewalExpiryAndRestart(t *testing.T) {
	for _, kind := range []workKind{dispatchWork, collectionWorkKind, cleanupWork} {
		t.Run(string(kind), func(t *testing.T) {
			f, first, renew := workLeaseFixture(t, kind)
			ctx := context.Background()
			check := func(l workLease) error {
				return f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
					_, err := checkWorkLease(ctx, tx, l, now)
					return err
				})
			}
			f.clock.advance(500 * time.Millisecond)
			until, err := renew()
			if err != nil || !until.After(first.until) {
				t.Fatal("renewal did not extend expiry", until, err)
			}
			if same, err := renew(); err != nil || !same.Equal(until) {
				t.Fatal("renewal changed an unchanged expiry", same, err)
			}
			f.restart(t)
			f.clock.advance(600 * time.Millisecond)
			if err := check(first); err != nil {
				t.Fatal("renewal changed capability or was lost at restart", err)
			}
			for _, alter := range []func(*workLease){
				func(l *workLease) { l.generation++ },
				func(l *workLease) { l.fence = strings.Repeat("e", 64) },
				func(l *workLease) { l.owner = "other" },
				func(l *workLease) { l.operationID = sql.NullString{String: "other", Valid: true} },
				func(l *workLease) { l.attemptRevision = sql.NullInt64{Int64: 9999, Valid: true} },
			} {
				forged := first
				alter(&forged)
				if err := check(forged); !errors.Is(err, first.lost()) {
					t.Fatal("forged capability accepted", err)
				}
			}
			claim := func() (next workLease, err error) {
				err = f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
					next, err = claimWorkLease(ctx, tx, first, now, time.Second)
					return err
				})
				return
			}
			if _, err := claim(); !errors.Is(err, first.lost()) {
				t.Fatal("live lease was reclaimed", err)
			}
			f.clock.advance(until.Sub(f.clock.Now()))
			if _, err := renew(); !errors.Is(err, first.lost()) {
				t.Fatal("expired owner renewed", err)
			}
			if err := check(first); !errors.Is(err, first.lost()) {
				t.Fatal("exact expiry was accepted", err)
			}
			second, err := claim()
			if err != nil || second.generation != first.generation+1 || second.fence == first.fence {
				t.Fatal("reclaim lost fencing", second, err)
			}
			if err := check(first); !errors.Is(err, first.lost()) {
				t.Fatal("stale owner accepted", err)
			}
			if err := f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
				return releaseWorkLease(ctx, tx, second, now)
			}); err != nil {
				t.Fatal(err)
			}
			if err := check(second); !errors.Is(err, second.lost()) {
				t.Fatal("released owner accepted", err)
			}
			f.restart(t)
			third, err := claim()
			if err != nil || third.generation != second.generation+1 || third.fence == second.fence {
				t.Fatal("released generation forgotten at restart", third, err)
			}
		})
	}
}

func TestWorkLeaseGenerationOverflowFailsWithoutReset(t *testing.T) {
	f, l, _ := workLeaseFixture(t, dispatchWork)
	f.clock.advance(2 * time.Second)
	root := copyFixtureAtSchema(t, f.s, 25)
	old, err := connect(filepath.Join(root, databaseName), false)
	if err != nil {
		t.Fatal(err)
	}
	// The released scheduler schema permits any strictly greater generation.
	if _, err := old.Exec(`UPDATE scheduler_leases SET generation=?`, int64(math.MaxInt64)); err != nil {
		t.Fatal(err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	f.root = root
	f.restart(t)
	if err := f.s.write(context.Background(), func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		_, err := claimWorkLease(ctx, tx, l, now, time.Second)
		return err
	}); !errors.Is(err, ErrConflict) {
		t.Fatal("exhausted generation was reused", err)
	}
	var generation int64
	if err := f.s.db.QueryRow(`SELECT generation FROM work_leases WHERE kind='dispatch' AND key=?`, l.key).Scan(&generation); err != nil || generation != math.MaxInt64 {
		t.Fatal("overflow changed history", generation, err)
	}
}

func TestWorkLeaseMigrationPreservesEveryGeneration(t *testing.T) {
	for _, kind := range []workKind{dispatchWork, collectionWorkKind, cleanupWork} {
		for _, state := range []string{"live", "expired", "released"} {
			t.Run(string(kind)+"/"+state, func(t *testing.T) {
				f, l, _ := workLeaseFixture(t, kind)
				ctx := context.Background()
				if state == "expired" {
					f.clock.advance(2 * time.Second)
				}
				if err := f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
					if state != "released" {
						return nil
					}
					if err := releaseWorkLease(ctx, tx, l, now); err != nil {
						return err
					}
					if kind == cleanupWork {
						var w, op string
						if err := tx.QueryRow(`SELECT workspace_id,cleanup_operation_id FROM work_leases WHERE kind=? AND key=?`, kind, l.key).Scan(&w, &op); err != nil {
							return err
						}
						row, err := loadOperation(ctx, tx, domain.WorkspaceID(w), domain.OperationID(op))
						if err != nil {
							return err
						}
						return failCleanup(ctx, tx, row.Record, now)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				root := copyFixtureAtSchema(t, f.s, 25)
				old, err := connect(filepath.Join(root, databaseName), false)
				if err != nil {
					t.Fatal(err)
				}
				var query string
				switch kind {
				case dispatchWork:
					query = `SELECT 'dispatch',json_array(queue_seq),generation,owner,fence,until_ms,held,NULL,NULL FROM scheduler_leases`
				case collectionWorkKind:
					query = `SELECT 'collection',json_array(workspace_id,job_id,attempt_id),generation,'',fence,until_ms,held,operation_id,attempt_revision FROM collection_leases`
				default:
					query = `SELECT 'cleanup',json_array(l.workspace_id,l.operation_id),l.generation,'',l.fence,l.until_ms,c.status='running',NULL,NULL FROM cleanup_leases l JOIN cleanup_operations c ON c.workspace_id=l.workspace_id AND c.operation_id=l.operation_id`
				}
				before := upgradeRows(t, old, query)
				if len(before) == 0 {
					t.Fatal("empty migration fixture")
				}
				if err := old.Close(); err != nil {
					t.Fatal(err)
				}
				f.root = root
				f.restart(t)
				after := upgradeRows(t, f.s.db, `SELECT kind,key,generation,owner,fence,until_ms,held,operation_id,attempt_revision FROM work_leases WHERE kind='`+string(kind)+`'`)
				if !reflect.DeepEqual(before, after) {
					t.Fatalf("lease history changed: before=%v after=%v", before, after)
				}
				if err := integrity(ctx, f.s.db); err != nil {
					t.Fatal(err)
				}
				if state != "live" && !(kind == cleanupWork && state == "released") {
					var next workLease
					if err := f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
						var err error
						next, err = claimWorkLease(ctx, tx, l, now, time.Second)
						return err
					}); err != nil || next.generation != l.generation+1 {
						t.Fatal("migrated generation was reset", next, err)
					}
				}
			})
		}
	}
}

func TestWorkLeaseStorageRejectsForgedIdentityAndOrphans(t *testing.T) {
	for _, kind := range []workKind{dispatchWork, collectionWorkKind, cleanupWork} {
		t.Run(string(kind), func(t *testing.T) {
			f, l, renew := workLeaseFixture(t, kind)
			for _, change := range []string{"generation=generation+2", "generation=generation-1", "fence='" + strings.Repeat("e", 64) + "'", "until_ms=until_ms-1", "key='[]'", "operation_id='other'"} {
				if _, err := f.s.db.Exec(`UPDATE work_leases SET `+change+` WHERE kind=? AND key=?`, kind, l.key); err == nil {
					t.Fatal("forged transition accepted", change)
				}
			}
			if _, err := f.s.db.Exec(`DELETE FROM work_leases WHERE kind=? AND key=?`, kind, l.key); err == nil {
				t.Fatal("lease history deleted")
			}
			for _, key := range []string{"[]", "null", "[ 1]", "[1,2,3,4]", `["absent","missing","attempt"]`, `["absent","missing"]`} {
				if _, err := f.s.db.Exec(`INSERT INTO work_leases(kind,key,generation,owner,fence,until_ms,held,operation_id,attempt_revision) VALUES(?,?,1,?,?,?,1,?,?)`, kind, key, l.owner, l.fence, l.until.UnixMilli(), l.operationID, l.attemptRevision); err == nil {
					t.Fatal("noncanonical or orphan lease accepted", key)
				}
			}
			if _, err := f.s.db.Exec(`CREATE TRIGGER ignore_lease_update BEFORE UPDATE ON work_leases BEGIN SELECT RAISE(IGNORE); END;`); err != nil {
				t.Fatal(err)
			}
			if _, err := renew(); !errors.Is(err, l.lost()) {
				t.Fatal("renewal acknowledged a missing update", err)
			}
			if err := f.s.write(context.Background(), func(ctx context.Context, tx *sql.Tx, now time.Time) error {
				return releaseWorkLease(ctx, tx, l, now)
			}); !errors.Is(err, l.lost()) {
				t.Fatal("release acknowledged a missing update", err)
			}
			f.clock.advance(2 * time.Second)
			if err := f.s.write(context.Background(), func(ctx context.Context, tx *sql.Tx, now time.Time) error {
				_, err := claimWorkLease(ctx, tx, l, now, time.Second)
				return err
			}); !errors.Is(err, l.lost()) {
				t.Fatal("claim acknowledged a missing update", err)
			}
		})
	}
	if workKey("a:b", "c", "d") == workKey("a", "b:c", "d") {
		t.Fatal("composite keys alias")
	}
}

type renewedCollectionRepository struct {
	*Store
	work collection.Work
}

func (r *renewedCollectionRepository) ClaimCollection(ctx context.Context, ttl time.Duration, workers int) (*collection.Work, error) {
	w, err := r.Store.ClaimCollection(ctx, ttl, workers)
	if w != nil {
		r.work = *w
	}
	return w, err
}

func TestCollectionPublicationSurvivesRenewalDuringTransfer(t *testing.T) {
	f, _, p, blobs := collectionFixture(t)
	repo := &renewedCollectionRepository{Store: f.s}
	p.hook = func() error {
		f.clock.advance(10 * time.Second)
		_, err := f.s.RenewCollection(context.Background(), repo.work, time.Minute)
		return err
	}
	runCollection(t, collectionEngine(t, f, p, blobs, repo))
	if n := controlCount(t, f.s, "SELECT count(*) FROM collection_publications"); n != 1 {
		t.Fatal("renewal invalidated verified proof", n)
	}
}
