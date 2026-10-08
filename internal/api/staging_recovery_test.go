package api_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func TestControlHTTPReconcilesExhaustedOriginalPreparation(t *testing.T) {
	f, blobs := setupControlHTTP(t, nil)
	job := admitControlJob(t, f)
	ctx := context.Background()
	if err := f.store.ConfigureScheduler(ctx, scheduler.DefaultSettings()); err != nil {
		t.Fatal(err)
	}
	claimed, err := f.store.ClaimNext(ctx, "staging-worker", time.Now().UTC())
	if err != nil || claimed.Claim == nil {
		t.Fatal("fixture claim failed", err)
	}
	work, err := f.store.LoadDispatch(ctx, *claimed.Claim, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	refs := map[string]domain.ObjectMetadata{}
	for _, ref := range work.Job.Objects {
		refs[ref.Role] = ref.Object
	}
	snapshot, err := dispatch.Snapshot(work.Job, refs)
	if err != nil {
		t.Fatal(err)
	}
	resolved, preparation, err := dispatch.ResolveJob(work, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	plan := provider.Plan{Job: resolved}
	work, err = f.store.CommitDispatch(ctx, work.Handle, dispatch.Action{Kind: dispatch.BeginPreparation, Plan: &plan, PreparationID: preparation}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < dispatch.MaxFailures; i++ {
		work, err = f.store.CommitDispatch(ctx, work.Handle, dispatch.Action{Kind: dispatch.Fault, Code: domain.CodeStagingFailed}, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
	}
	if work.Journal.Phase != dispatch.Attention || work.Journal.SubmitStarted || work.Journal.Prepared != nil {
		t.Fatal("fixture did not exhaust original staging")
	}
	body := `{"attempt_id":"` + string(job.AttemptID) + `","reason":"observe original staging"}`
	_, raw := f.call(t, "POST", job.Links.Self+"/reconcile", "a", "staging-reconcile", body, 202)
	accepted := decodeControl(t, raw)
	if accepted.Status != "accepted" || accepted.Attempt != string(job.AttemptID) || accepted.New != "" || accepted.Confirmed {
		t.Fatal("reconcile changed attempt or invented remote termination")
	}
	_, raw = f.call(t, "POST", job.Links.Self+"/reconcile", "a", "staging-reconcile", body, 202)
	replay := decodeControl(t, raw)
	if replay.ID != accepted.ID || !replay.Replay || blobs.opens.Load() != 0 {
		t.Fatal("control replay multiplied work or reread payload")
	}
	_, raw = f.call(t, "GET", job.Links.Self, "a", "", "", 200)
	record, err := f.store.ReadJob(ctx, "a", f.tokenIDs["a"], job.JobID)
	if err != nil || record.Attempt.State.Orchestration != domain.OrchestrationReconciling || record.Attempt.State.Execution != domain.ExecutionNotSubmitted || record.Attempt.ID != job.AttemptID {
		t.Fatal("HTTP reconcile did not persist original pre-submit state", err)
	}
	if !reflect.DeepEqual(record.Objects, work.Job.Objects) || record.Job.ActiveAttemptID != work.Job.Job.ActiveAttemptID {
		t.Fatal("reconcile changed original frozen inputs or active attempt")
	}
}
