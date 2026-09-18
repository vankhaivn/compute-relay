package sqlite

import (
	"context"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

func TestLoadProviderAttemptReturnsOnlyOriginalDurablePlanAndPreparation(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	journal := f.journal(t, id)
	if journal.Plan == nil || journal.Prepared == nil {
		t.Fatal("fixture did not reach durable prepared state")
	}
	plan, prepared, err := f.s.LoadProviderAttempt(context.Background(), journal.Plan.Job.Identity)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Digest() != journal.Plan.Digest() || prepared != *journal.Prepared {
		t.Fatal("trusted loader changed the original provider attempt")
	}
	wrong := journal.Plan.Job.Identity
	wrong.Nonce += "x"
	if _, _, err := f.s.LoadProviderAttempt(context.Background(), wrong); err == nil {
		t.Fatal("mismatched provider identity was accepted")
	}
}
