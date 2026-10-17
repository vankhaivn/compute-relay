package kaggle

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
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
	if _, err := s.Prepare(ctx, plan, "prep"); !errors.Is(err, context.Canceled) || strings.Join(modes, ",") != "create,observe" {
		t.Fatal("visibility wait outlived its context or repeated a call", err, modes)
	}
}
