package sqlite

import (
	"reflect"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

func TestDispatchCachedProblemReadDoesNotWrite(t *testing.T) {
	scenario := fake.DefaultScenario()
	scenario.Mode = fake.AcceptLoseResponse
	f := newDispatchFixture(t, scenario)
	id := f.seed(t, "a", 1, false)
	_, p := authorizationService(t, f, "a", auth.Read)
	requireNoError(t, f.step(t))
	requireNoError(t, f.step(t))
	before, state, events := f.journal(t, id), f.state(t, id), controlCount(t, f.s, "SELECT count(*) FROM events")
	for range 3 {
		if _, err := f.s.ReadJob(t.Context(), "a", p.TokenID(), id.JobID); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(f.journal(t, id), before) || f.state(t, id) != state || controlCount(t, f.s, "SELECT count(*) FROM events") != events {
		t.Fatal("GET wrote journal, state or events")
	}
}
