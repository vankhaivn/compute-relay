package runtimehost

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/retention"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

func TestRetentionResultExpirySweepAndRootBinding(t *testing.T) {
	ctx := t.Context()
	f := newSystemFixture(t, fake.DefaultScenario())
	job := f.collectibleJob(t, "retention-results", "automatic")
	f.collectionSource(t)
	f.collectOnce(t)
	result, err := f.host.store.ReadCollection(ctx, "app", f.actor.TokenID(), job.JobID, job.AttemptID)
	if err != nil || len(result.Files) == 0 {
		t.Fatal("missing publication", err)
	}
	before, err := f.host.store.ReadJob(ctx, "app", f.actor.TokenID(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	f.advance(t, 8*24*time.Hour)
	if _, err = f.host.store.ExpireRetention(ctx, retention.DefaultPolicy(), 100); err != nil {
		t.Fatal(err)
	}
	after, err := f.host.store.ReadJob(ctx, "app", f.actor.TokenID(), job.JobID)
	want := before.Attempt.State
	want.Result = domain.ResultExpired
	if err != nil || after.Attempt.State != want {
		t.Fatal("expiry changed remote or business outcome", after.Attempt.State, err)
	}
	if _, err = f.host.store.ReadCollection(ctx, "app", f.actor.TokenID(), job.JobID, job.AttemptID); !errors.Is(err, retention.ErrExpired) {
		t.Fatal("expiry hidden as missing", err)
	}
	sweep, err := retention.NewSweeper(f.host.store, f.host.inputs, f.host.results)
	if err != nil {
		t.Fatal(err)
	}
	var cursor int64
	for i := 0; i < 20; i++ {
		_, cursor, err = sweep.SweepOnce(ctx, retention.DefaultPolicy(), cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		if cursor == 0 {
			break
		}
		if i == 19 {
			t.Fatal("bounded sweep did not drain")
		}
	}
	for _, file := range result.Files {
		if stream, err := f.host.results.Open(ctx, "app", file.Object.ID); err == nil {
			stream.Close()
			t.Fatal("expired bytes remain")
		}
	}
	page, err := f.host.store.PendingRetention(ctx, 0, 100)
	if err != nil || len(page.Items) != 0 {
		t.Fatal("pending tombstones remain", page, err)
	}
	swapped, err := retention.NewSweeper(f.host.store, f.host.results, f.host.inputs)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = swapped.SweepOnce(ctx, retention.DefaultPolicy(), 0, 10); !errors.Is(err, retention.ErrChanged) {
		t.Fatal("swapped roots inherited authority", err)
	}
	replacement, err := blobfs.New(filepath.Join(t.TempDir(), "replacement"), blobfs.Limits{MaxObjectBytes: 1 << 20, MaxTotalBytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	changed, err := retention.NewSweeper(f.host.store, f.host.inputs, replacement)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = changed.SweepOnce(ctx, retention.DefaultPolicy(), 0, 10); !errors.Is(err, retention.ErrChanged) {
		t.Fatal("replacement inherited tombstones", err)
	}
	f.reopen(t)
	if _, err = f.host.store.ReadCollection(ctx, "app", f.actor.TokenID(), job.JobID, job.AttemptID); !errors.Is(err, retention.ErrExpired) {
		t.Fatal("restart forgot expiry", err)
	}
	f.assertSingleCompute(t, job)
}

type systemRetentionRepository struct {
	*sqlite.Store
	stage string
	fired bool
}

func (r *systemRetentionRepository) ExpireRetention(ctx context.Context, p retention.Policy, limit int) (retention.SweepReport, error) {
	report, err := r.Store.ExpireRetention(ctx, p, limit)
	if err == nil && r.stage == "expiry-ack" && !r.fired {
		r.fired = true
		return retention.SweepReport{}, sqlite.ErrUnavailable
	}
	return report, err
}
func (r *systemRetentionRepository) CompleteRetention(ctx context.Context, d retention.Deletion) error {
	err := r.Store.CompleteRetention(ctx, d)
	if err == nil && r.stage == "completion-ack" && !r.fired {
		r.fired = true
		return sqlite.ErrUnavailable
	}
	return err
}

type systemRetentionBlobs struct {
	*blobfs.Store
	calls int
	lose  bool
	fired bool
}

func (b *systemRetentionBlobs) Delete(ctx context.Context, m domain.ObjectMetadata) (bool, error) {
	b.calls++
	absent, err := b.Store.Delete(ctx, m)
	if err == nil && b.lose && !b.fired {
		b.fired = true
		return false, sqlite.ErrUnavailable
	}
	return absent, err
}

func TestRetentionLostAcknowledgementsRecoverWithoutCompute(t *testing.T) {
	for _, stage := range []string{"expiry-ack", "delete-ack", "completion-ack"} {
		t.Run(stage, func(t *testing.T) {
			ctx := t.Context()
			f := newSystemFixture(t, fake.DefaultScenario())
			job := f.collectibleJob(t, "retention-ack", "automatic")
			f.collectionSource(t)
			f.collectOnce(t)
			f.advance(t, 8*24*time.Hour)
			repo := &systemRetentionRepository{Store: f.host.store, stage: stage}
			inputs := &systemRetentionBlobs{Store: f.host.inputs, lose: stage == "delete-ack"}
			outputs := &systemRetentionBlobs{Store: f.host.results}
			sweep, err := retention.NewSweeper(repo, inputs, outputs)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err = sweep.SweepOnce(ctx, retention.DefaultPolicy(), 0, 100); !errors.Is(err, sqlite.ErrUnavailable) {
				t.Fatal("fault not observed", err)
			}
			if stage == "expiry-ack" && (!repo.fired || inputs.calls+outputs.calls != 0) {
				t.Fatal("uncertain expiry authorized deletion")
			}
			if stage == "delete-ack" && !inputs.fired || stage == "completion-ack" && !repo.fired {
				t.Fatal("fault did not fire")
			}
			f.reopen(t)
			sweep, err = retention.NewSweeper(f.host.store, f.host.inputs, f.host.results)
			if err != nil {
				t.Fatal(err)
			}
			for range 3 {
				if _, _, err = sweep.SweepOnce(ctx, retention.DefaultPolicy(), 0, 100); err != nil {
					t.Fatal(err)
				}
			}
			page, err := f.host.store.PendingRetention(ctx, 0, 100)
			if err != nil || len(page.Items) != 0 {
				t.Fatal("recovery lost tombstones", page, err)
			}
			f.assertSingleCompute(t, job)
		})
	}
}
