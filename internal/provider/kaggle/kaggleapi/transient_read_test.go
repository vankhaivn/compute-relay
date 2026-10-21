package kaggleapi

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestReadRetriesTransientStatusAndTransport(t *testing.T) {
	for _, status := range []int{0, 500, 503} {
		calls := 0
		c := newFixtureClient(t, func(*http.Request) (*http.Response, error) {
			calls++
			if calls == 1 {
				if status == 0 {
					return nil, errors.New("transport unavailable")
				}
				return response(status, "text/plain", "private body"), nil
			}
			return response(200, "application/json", `{"id":123,"ref":"fixture_user/fixture-stage"}`), nil
		}, nil)
		gate, waits := fakeCooldowns()
		c.state.pacing = gate
		got, err := c.GetDataset(context.Background(), fixtureDataset)
		if err != nil || got.ID != "123" || calls != 2 || len(*waits) != 1 || (*waits)[0] != time.Second {
			t.Fatal("read did not recover", status, calls, err, *waits)
		}
	}
}
func TestMutation5xxAndTransportAreNeverRetried(t *testing.T) {
	for _, status := range []int{0, 503} {
		calls := 0
		c := newFixtureClient(t, func(*http.Request) (*http.Response, error) {
			calls++
			if status == 0 {
				return nil, errors.New("transport unavailable")
			}
			return response(status, "text/plain", "private body"), nil
		}, nil)
		_, _ = c.rpc(context.Background(), datasetService+"CreateDataset", struct{}{}, maxAccountBytes)
		if calls != 1 {
			t.Fatal("mutation repeated", calls)
		}
	}
}
