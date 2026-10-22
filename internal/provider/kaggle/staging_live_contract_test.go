package kaggle

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/provider"
)

// shortVisibility shortens the read-only wait for a created dataset in one test.
func shortVisibility(t *testing.T, grace, poll time.Duration) {
	t.Helper()
	oldGrace, oldPoll := stagingVisibilityGrace, stagingVisibilityPoll
	stagingVisibilityGrace, stagingVisibilityPoll = grace, poll
	t.Cleanup(func() { stagingVisibilityGrace, stagingVisibilityPoll = oldGrace, oldPoll })
}

func TestStagingLiveLicenseAndVisibilityContract(t *testing.T) {
	policy := DefaultStagingPolicy()
	if policy.License != "other" || !policy.valid() {
		t.Fatal("default staging policy must use the live Kaggle license identifier", policy)
	}
	legacy := policy
	legacy.License = "copyright-authors"
	if legacy.valid() {
		t.Fatal("known rejected Kaggle license remained selectable")
	}
	if stagingVisibilityGrace < time.Minute || stagingVisibilityPoll < time.Second {
		t.Fatal("created-dataset visibility wait is too short for live Kaggle or polls too often")
	}
	shortVisibility(t, time.Minute, time.Millisecond)

	s, plan, _, _ := newTestStager(t, true)
	var modes []string
	s.run = func(_ context.Context, _ Config, mode string, _ []byte, p stagingPlan, _ StagingBlobs) (stagingResponse, error) {
		modes = append(modes, mode)
		switch len(modes) {
		case 1:
			if mode != "create" {
				t.Fatal("first call must be the one authorized creation")
			}
			return stagingResponse{Protocol: 1, Status: "pending"}, nil
		case 2:
			if mode != "observe" {
				t.Fatal("visibility wait repeated a mutation")
			}
			return stageResponse(p, "not_found"), nil
		case 3:
			if mode != "observe" {
				t.Fatal("visibility wait repeated a mutation")
			}
			return stageResponse(p, "pending"), nil
		default:
			t.Fatal("unexpected extra staging call")
			return stagingResponse{}, nil
		}
	}
	prepared, err := s.Prepare(context.Background(), plan, "prep")
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Ready || !prepared.Private || strings.Join(modes, ",") != "create,observe,observe" {
		t.Fatal("post-create visibility did not remain read-only", prepared, modes)
	}
}

func TestStagingVisibilityWaitStopsOnCancel(t *testing.T) {
	shortVisibility(t, time.Hour, time.Hour)
	s, plan, _, _ := newTestStager(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var modes []string
	s.run = func(_ context.Context, _ Config, mode string, _ []byte, p stagingPlan, _ StagingBlobs) (stagingResponse, error) {
		modes = append(modes, mode)
		if len(modes) == 1 {
			return stagingResponse{Protocol: 1, Status: "pending"}, nil
		}
		time.AfterFunc(10*time.Millisecond, cancel)
		return stageResponse(p, "not_found"), nil
	}
	_, err := s.Prepare(ctx, plan, "prep")
	assertStepReason(t, err, "staging_visibility_interrupted")
	if !errors.Is(err, context.Canceled) || strings.Join(modes, ",") != "create,observe" {
		t.Fatal("visibility wait outlived its context or repeated a call", err, modes)
	}
}

func TestLostCreateAcknowledgementWaitsForExactVisibility(t *testing.T) {
	shortVisibility(t, time.Second, time.Millisecond)
	p, plan, _, blobs, f := nativeRuntime(t, false)
	f.fault, f.hiddenReads = "lost-create-ack", 2
	prepared, err := p.Prepare(t.Context(), plan, "prep")
	if err != nil || !prepared.Ready || f.count("CreateDataset") != 1 || f.count("StartBlobUpload") != 3 {
		t.Fatal("lost create acknowledgement was not resolved by exact read-only observation", prepared, err, f.calls)
	}
	if blobs.reads != 4 {
		t.Fatal("visibility polling reopened upload sources", blobs.reads)
	}
}

func TestLostCreateAcknowledgementRejectsWrongIdentity(t *testing.T) {
	shortVisibility(t, time.Second, time.Millisecond)
	p, plan, _, _, f := nativeRuntime(t, false)
	f.fault = "lost-create-ack"
	p.stager.run = func(ctx context.Context, c Config, mode string, token []byte, plan stagingPlan, blobs StagingBlobs) (stagingResponse, error) {
		client, err := f.client(token)
		if err != nil {
			return stagingResponse{}, err
		}
		defer client.Close()
		if mode == "observe" {
			f.fault = "changed-marker"
		}
		return stage(ctx, client, mode, plan, blobs)
	}
	if _, err := p.Prepare(t.Context(), plan, "prep"); !errors.Is(err, ErrStagingIdentity) || f.count("CreateDataset") != 1 || f.count("StartBlobUpload") != 3 {
		t.Fatal("foreign staging was accepted or creation repeated", err, f.calls)
	}
}

func TestStagingCleanupObservationRequiresStrictAbsence(t *testing.T) {
	for _, mode := range []string{"absent", "missing-name-403", "lookup-denied", "changed-marker", "present"} {
		t.Run(mode, func(t *testing.T) {
			p, plan, _, blobs, f := nativeRuntime(t, false)
			if mode == "changed-marker" || mode == "present" {
				if _, err := p.Prepare(t.Context(), plan, "prep"); err != nil {
					t.Fatal(err)
				}
			}
			beforeCreate, beforeUpload, beforeReads := f.count("CreateDataset"), f.count("StartBlobUpload"), blobs.reads
			f.fault, blobs.mode = mode, "missing"
			seen, err := p.ObserveStagingCleanup(t.Context(), plan, "prep")
			switch mode {
			case "absent":
				if err != nil || seen.Status != provider.ReconciliationNotFound {
					t.Fatal(seen, err)
				}
			case "present":
				if err != nil || seen.Status != provider.ReconciliationFound {
					t.Fatal(seen, err)
				}
			case "changed-marker":
				if !errors.Is(err, ErrStagingIdentity) {
					t.Fatal(seen, err)
				}
			default:
				if seen.Cause != "staging_lookup_failed_http_403" {
					t.Fatal("cleanup observation lost lookup cause", seen.Cause)
				}
				if err != nil || seen.Status != provider.ReconciliationUnknown {
					t.Fatal(seen, err)
				}
			}
			if f.count("CreateDataset") != beforeCreate || f.count("StartBlobUpload") != beforeUpload || f.count("DeleteDataset") != 0 || blobs.reads != beforeReads {
				t.Fatal("cleanup observation mutated or uploaded", f.calls)
			}
		})
	}
}

func TestStagingVisibilityWaitToleratesTransientReadFailure(t *testing.T) {
	shortVisibility(t, time.Second, time.Millisecond)
	s, plan, blobs, _ := newTestStager(t, true)
	calls := 0
	s.run = func(_ context.Context, _ Config, mode string, _ []byte, p stagingPlan, _ StagingBlobs) (stagingResponse, error) {
		calls++
		if calls == 1 {
			return stagingResponse{Protocol: 1, Status: "pending"}, nil
		}
		if mode != "observe" {
			t.Fatal("visibility retry repeated creation")
		}
		if calls == 2 {
			return stageResponse(p, "unknown"), nil
		}
		if calls == 3 {
			return stagingResponse{}, errors.New("synthetic read unavailable")
		}
		return stageResponse(p, "ready"), nil
	}
	prepared, err := s.Prepare(t.Context(), plan, "prep")
	if err != nil || !prepared.Ready || calls != 4 || blobs.reads != 2 {
		t.Fatal("transient read ended visibility observation or reopened inputs", err, calls, blobs.reads)
	}
}
