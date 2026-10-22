package api_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

type managedCauseAdapter struct {
	managedAdapter
	failure error
}

func (a *managedCauseAdapter) Verify(context.Context, []byte) (connections.Verification, error) {
	return connections.Verification{}, a.failure
}

func TestManagedHTTPRetainsClosedFailureCauseInOperationContract(t *testing.T) {
	for _, tc := range []struct{ name, cause string }{{"typed", "ssh_channel_open_failed"}, {"untyped", ""}} {
		t.Run(tc.name, func(t *testing.T) {
			failure := errors.New("private provider diagnostic")
			a := &managedCauseAdapter{failure: provider.WithReason(failure, tc.cause)}
			f := setupManagedHTTP(t, true, a)
			_, raw := f.call(t, "POST", "/v1/workspaces/a/connections", "manage", "http-cause-create", managedBody, 202)
			initial := readManagedOperation(t, raw)
			if initial.Cause != nil || strings.Contains(string(raw), `"cause"`) {
				t.Fatal("accepted operation manufactured a cause")
			}
			if worked, err := f.service.RunOnce(t.Context()); !worked || err != nil {
				t.Fatal("worker failed", worked, err)
			}
			_, raw = f.call(t, "GET", "/v1/workspaces/a/connection-operations/"+string(initial.ID), "manage", "", "", 200)
			saved := readManagedOperation(t, raw)
			if saved.Problem == nil || *saved.Problem != "provider_unavailable" || (saved.Cause == nil) != (tc.cause == "") || saved.Cause != nil && *saved.Cause != tc.cause {
				t.Fatal("HTTP lost problem or cause", saved)
			}
			if strings.Contains(string(raw), failure.Error()) || tc.cause == "" && strings.Contains(string(raw), `"cause"`) {
				t.Fatal("HTTP leaked raw diagnostic or manufactured a cause")
			}
		})
	}
}
