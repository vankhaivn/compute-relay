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

type retentionFaultBlobs struct {
	*blobfs.Store
	calls int
}

func (b *retentionFaultBlobs) Delete(ctx context.Context, m domain.ObjectMetadata) (bool, error) {
	b.calls++
	absent, err := b.Store.Delete(ctx, m)
	return absent, err
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
				requireNoError(t, f.s.db.QueryRow("PRAGMA page_count").Scan(&pages))
				if _, err := f.s.db.Exec(fmt.Sprintf("PRAGMA max_page_count=%d", pages+16)); err != nil {
					t.Fatal(err)
				}
				if _, err := f.s.db.Exec(`CREATE TRIGGER retention_fault BEFORE UPDATE OF expired_at ON retention_inventory BEGIN INSERT INTO retention_full_fixture VALUES(zeroblob(4194304)); END`); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := f.s.db.Exec(`CREATE TRIGGER retention_fault BEFORE UPDATE OF expired_at ON retention_inventory BEGIN SELECT RAISE(ABORT,'synthetic retention rollback'); END`); err != nil {
					t.Fatal(err)
				}
			}
			inputs := &retentionFaultBlobs{Store: f.blobs}
			outputs := &retentionFaultBlobs{Store: blobs}
			sweep, err := retention.NewSweeper(f.s, inputs, outputs)
			requireNoError(t, err)
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
