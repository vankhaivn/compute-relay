package sqlite

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

type retentionReadProbe struct {
	inner operations.BlobReader
	reads int
}

func (p *retentionReadProbe) Open(ctx context.Context, w domain.WorkspaceID, id domain.ObjectID) (io.ReadCloser, error) {
	p.reads++
	return p.inner.Open(ctx, w, id)
}

func TestRetentionRejectsExpiredInputsBeforeReadAndCommit(t *testing.T) {
	ctx := context.Background()
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	service, actor, _ := controlService(t, f, "a", auth.Read, auth.Write, auth.Operate)
	cancelled := cancelControl(t, f, service, actor, id)
	proof, err := f.s.RetryInputs(ctx, "a", actor.TokenID(), id.JobID, id.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	f.clock.advance(8 * 24 * time.Hour)
	scanRetention(t, f)
	// Physical bytes still exist until sweep, but expiry already forbids their reuse.
	stream, err := f.blobs.Open(ctx, "a", "code")
	if err != nil {
		t.Fatal(err)
	}
	stream.Close()
	probe := &retentionReadProbe{inner: f.blobs}
	access, err := auth.New(f.s, f.s, nil)
	if err != nil {
		t.Fatal(err)
	}
	service, err = operations.New(access, f.s, probe, f.clock, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Submit(ctx, actor, "a", id.JobID, domain.OperationRetryCompute, "expired-retry-key", controlRequest(id.AttemptID)); !errors.Is(err, operations.ErrInputs) || probe.reads != 0 {
		t.Fatal("expired snapshot read bytes or returned an unclassified SQL failure", err, probe.reads)
	}
	req, err := operations.Parse(domain.OperationRetryCompute, controlRequest(id.AttemptID))
	if err != nil {
		t.Fatal(err)
	}
	key, _ := admission.KeyDigest("stale-proof-key")
	hash, _ := req.Digest(id.JobID, domain.OperationRetryCompute)
	command := operations.Command{WorkspaceID: "a", TokenID: actor.TokenID(), JobID: id.JobID, Kind: domain.OperationRetryCompute, Request: req, KeyDigest: key, RequestHash: hash, Inputs: &proof, Limits: admission.DefaultLimits(), Now: f.clock.Now()}
	if _, err := f.s.AdmitOperation(ctx, command); !errors.Is(err, operations.ErrInputs) {
		t.Fatal("commit trusted a pre-expiry input proof", err)
	}
	job, err := admission.Parse([]byte(dispatchJSON))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.ValidateJob(ctx, "a", actor.TokenID(), job); !errors.Is(err, admission.ErrInputs) {
		t.Fatal("validation accepted expired input metadata", err)
	}
	key, _ = admission.KeyDigest("expired-new-job-key")
	if _, err := f.s.AdmitJob(ctx, "a", actor.TokenID(), key, job, admission.DefaultLimits()); !errors.Is(err, admission.ErrInputs) {
		t.Fatal("new job accepted an expired input", err)
	}
	replay, err := service.Submit(ctx, actor, "a", id.JobID, domain.OperationCancel, "cancel-original-key", controlRequest(id.AttemptID))
	if err != nil || !replay.Replay || replay.Operation.ID != cancelled.Operation.ID || replay.Effect != cancelled.Effect || probe.reads != 0 {
		t.Fatal("input expiry invalidated the original control receipt", err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM attempts") != 1 || controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='retry_compute'") != 0 {
		t.Fatal("expired inputs created new work")
	}
	if f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("input expiry caused provider execution")
	}
}

func TestRetentionAdmissionReplayOutlivesInputExpiry(t *testing.T) {
	ctx := context.Background()
	f := newDispatchFixture(t, fake.DefaultScenario())
	service, actor, _ := controlService(t, f, "a", auth.Read, auth.Write, auth.Operate)
	job, err := admission.Parse([]byte(dispatchJSON))
	if err != nil {
		t.Fatal(err)
	}
	key, _ := admission.KeyDigest("retained-receipt-key")
	original, err := f.s.AdmitJob(ctx, "a", actor.TokenID(), key, job, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	cancelControl(t, f, service, actor, scheduler.Identity{WorkspaceID: "a", JobID: original.JobID, AttemptID: original.AttemptID})
	f.clock.advance(8 * 24 * time.Hour)
	scanRetention(t, f)
	f.restart(t)
	replayed, err := f.s.AdmitJob(ctx, "a", actor.TokenID(), key, job, admission.DefaultLimits())
	if err != nil || !replayed.Replay || replayed.JobID != original.JobID || replayed.AttemptID != original.AttemptID || !replayed.CreatedAt.Equal(original.CreatedAt) {
		t.Fatal("expiry or restart lost the original job receipt", err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM attempts") != 1 {
		t.Fatal("replay re-admitted expired work")
	}
}
