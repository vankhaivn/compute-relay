package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

func TestDispatchCachedProblemIsAuthorizedAndObservational(t *testing.T) {
	ctx := context.Background()
	scenario := fake.DefaultScenario()
	scenario.Mode = fake.AcceptLoseResponse
	f := newDispatchFixture(t, scenario)
	id := f.seed(t, "a", 1, false)
	access, err := auth.New(f.s, f.s, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, tokenA, err := access.Issue(ctx, "a", []auth.Scope{auth.Read}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	_, tokenB, err := access.Issue(ctx, "b", []auth.Scope{auth.Read}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	initial, err := f.s.ReadJob(ctx, "a", tokenA.ID, id.JobID)
	if err != nil || initial.Problem != nil {
		t.Fatal("ordinary queued admission has no dispatch problem", err)
	}
	if err = f.step(t); err != nil {
		t.Fatal(err)
	}
	if err = f.step(t); err != nil {
		t.Fatal(err)
	}
	before := f.journal(t, id)
	stats := f.backend.Stats()
	for i := 0; i < 3; i++ {
		record, err := f.s.ReadJob(ctx, "a", tokenA.ID, id.JobID)
		if err != nil {
			t.Fatal(err)
		}
		p := record.Problem
		if p == nil || p.Code != domain.CodeProviderSubmissionUnknown || !p.ComputeMayHaveStarted || p.SafeOperationRetry || p.RecommendedAction != domain.RecommendedActionReconcile {
			t.Fatal("cached uncertainty not surfaced")
		}
		raw, _ := json.Marshal(p)
		if strings.Contains(string(raw), "canary") {
			t.Fatal("upstream diagnostic escaped into status")
		}
	}
	if f.journal(t, id).Version != before.Version || f.backend.Stats() != stats {
		t.Fatal("GET changed journal or called provider")
	}
	if _, err = f.s.ReadJob(ctx, "a", tokenB.ID, id.JobID); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("foreign token was accepted: %v", err)
	}
	if _, err = f.s.ReadJob(ctx, "b", tokenB.ID, id.JobID); !errors.Is(err, admission.ErrNotFound) {
		t.Fatalf("foreign job was disclosed: %v", err)
	}
	if err = f.s.RevokeToken(ctx, tokenA.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = f.s.ReadJob(ctx, "a", tokenA.ID, id.JobID); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("revoked status access: %v", err)
	}
}
