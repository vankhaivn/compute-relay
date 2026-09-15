package sqlite

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/retention"
)

type retentionFaultRepository struct {
	*Store
	stage string
	fired bool
}

func (r *retentionFaultRepository) ExpireRetention(ctx context.Context, now time.Time, p retention.Policy, limit int) (retention.SweepReport, error) {
	report, err := r.Store.ExpireRetention(ctx, now, p, limit)
	if err == nil && r.stage == "expiry-ack" && !r.fired {
		r.fired = true
		return retention.SweepReport{}, ErrUnavailable
	}
	return report, err
}
func (r *retentionFaultRepository) CompleteRetention(ctx context.Context, d retention.Deletion, now time.Time) error {
	err := r.Store.CompleteRetention(ctx, d, now)
	if err == nil && r.stage == "completion-ack" && !r.fired {
		r.fired = true
		return ErrUnavailable
	}
	return err
}

type retentionFaultBlobs struct {
	*blobfs.Store
	calls int
	lose  bool
	fired bool
}

func (b *retentionFaultBlobs) Delete(ctx context.Context, m domain.ObjectMetadata) (bool, error) {
	b.calls++
	absent, err := b.Store.Delete(ctx, m)
	if err == nil && b.lose && !b.fired {
		b.fired = true
		return false, ErrUnavailable
	}
	return absent, err
}

func TestRetentionLostAcknowledgementsRecoverWithoutCompute(t *testing.T) {
	for _, stage := range []string{"expiry-ack", "delete-ack", "completion-ack"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			f, _, p, results := collectionFixture(t)
			runCollection(t, collectionEngine(t, f, p, results, f.s))
			f.clock.advance(8 * 24 * time.Hour)
			repo := &retentionFaultRepository{Store: f.s, stage: stage}
			inputs := &retentionFaultBlobs{Store: f.blobs, lose: stage == "delete-ack"}
			outputs := &retentionFaultBlobs{Store: results}
			sweep, err := retention.NewSweeper(repo, inputs, outputs, f.clock)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := sweep.SweepOnce(ctx, retention.DefaultPolicy(), 0, 100); !errors.Is(err, ErrUnavailable) {
				t.Fatal("acknowledgement fault not returned", err)
			}
			if stage == "expiry-ack" && inputs.calls+outputs.calls != 0 {
				t.Fatal("uncertain expiry commit authorized deletion")
			}
			if stage == "delete-ack" && !inputs.fired {
				t.Fatal("delete fault did not execute")
			}
			f.restart(t)
			sweep, err = retention.NewSweeper(f.s, f.blobs, results, f.clock)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 3; i++ {
				if _, _, err := sweep.SweepOnce(ctx, retention.DefaultPolicy(), 0, 100); err != nil {
					t.Fatal(err)
				}
			}
			if controlCount(t, f.s, "SELECT count(*) FROM retention_inventory WHERE expired_at IS NOT NULL AND deleted_at IS NULL") != 0 {
				t.Fatal("recovery lost pending deletion")
			}
			if controlCount(t, f.s, "SELECT count(*) FROM retention_audit WHERE action='delete'") != controlCount(t, f.s, "SELECT count(*) FROM retention_inventory WHERE deleted_at IS NOT NULL") {
				t.Fatal("acknowledgement replay duplicated audit facts")
			}
			assertNoNewCompute(t, f)
		})
	}
}

func TestRetentionExpiryRollbackAndActualDiskFull(t *testing.T) {
	for _, full := range []bool{false, true} {
		t.Run(fmt.Sprint("full=", full), func(t *testing.T) {
			ctx := context.Background()
			f, id, p, blobs := collectionFixture(t)
			runCollection(t, collectionEngine(t, f, p, blobs, f.s))
			f.clock.advance(8 * 24 * time.Hour)
			before := f.state(t, id)
			if full {
				if _, err := f.s.db.Exec("CREATE TABLE retention_full_fixture(data BLOB)"); err != nil {
					t.Fatal(err)
				}
				var pages int
				if err := f.s.db.QueryRow("PRAGMA page_count").Scan(&pages); err != nil {
					t.Fatal(err)
				}
				if _, err := f.s.db.Exec(fmt.Sprintf("PRAGMA max_page_count=%d", pages+16)); err != nil {
					t.Fatal(err)
				}
				if _, err := f.s.db.Exec(`CREATE TRIGGER retention_fault BEFORE INSERT ON retention_audit BEGIN INSERT INTO retention_full_fixture VALUES(zeroblob(4194304)); END`); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := f.s.db.Exec(`CREATE TRIGGER retention_fault BEFORE INSERT ON retention_audit BEGIN SELECT RAISE(ABORT,'synthetic retention rollback'); END`); err != nil {
					t.Fatal(err)
				}
			}
			inputs := &retentionFaultBlobs{Store: f.blobs}
			outputs := &retentionFaultBlobs{Store: blobs}
			sweep, err := retention.NewSweeper(f.s, inputs, outputs, f.clock)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err = sweep.SweepOnce(ctx, retention.DefaultPolicy(), 0, 100); err == nil || full && !errors.Is(err, ErrDiskFull) {
				t.Fatal("did not exercise intended SQL fault", err)
			}
			if inputs.calls+outputs.calls != 0 || f.state(t, id) != before || controlCount(t, f.s, "SELECT count(*) FROM retention_inventory WHERE expired_at IS NOT NULL") != 0 || controlCount(t, f.s, "SELECT count(*) FROM retention_expirations") != 0 {
				t.Fatal("rolled-back expiry leaked state or byte deletion")
			}
			if _, err := f.s.db.Exec("PRAGMA max_page_count=1073741823"); err != nil {
				t.Fatal(err)
			}
			if _, err := f.s.db.Exec("DROP TRIGGER retention_fault"); err != nil {
				t.Fatal(err)
			}
			if _, _, err := sweep.SweepOnce(ctx, retention.DefaultPolicy(), 0, 100); err != nil {
				t.Fatal("explicit local recovery failed", err)
			}
			assertNoNewCompute(t, f)
		})
	}
}
