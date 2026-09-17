package kaggle

import (
	"context"
	"strings"
	"testing"
)

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
