package appclient

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

func TestControlRoutesKeepWireKindsAndOriginalAttempt(t *testing.T) {
	for _, action := range []string{"cancel", "retry", "reconcile", "collect"} {
		t.Run(action, func(t *testing.T) {
			r := Request{Action: action, Workspace: "workspace", ID: "job_original", Attempt: "att_original", Key: "explicit-key", Reason: "explicit action"}
			kind := action
			if action == "retry" {
				kind = "retry_compute"
			}
			raw := `{"operation_id":"op_original","workspace_id":"workspace","job_id":"job_original","attempt_id":"att_original","kind":"` + kind + `","status":"succeeded","revision":2,"created_at":"2026-09-16T00:00:00Z","updated_at":"2026-09-16T00:00:00Z","effect":"fixture_effect","remote_termination_confirmed":false,"replay":false}`
			if action == "retry" {
				raw = strings.TrimSuffix(raw, "}") + `,"new_attempt_id":"att_new"}`
			}
			var calls atomic.Int32
			server := serveJSON(t, func(w http.ResponseWriter, q *http.Request) {
				calls.Add(1)
				body, err := io.ReadAll(q.Body)
				var request map[string]string
				if err != nil || json.Unmarshal(body, &request) != nil || request["attempt_id"] != r.Attempt || request["reason"] != r.Reason || q.URL.Path != "/v1/workspaces/workspace/jobs/job_original/"+action || q.Header.Get("Idempotency-Key") != r.Key {
					t.Error("control identity changed")
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(202)
				_, _ = io.WriteString(w, raw)
			})
			got, err := Exchange(context.Background(), server.URL, token, r)
			if err != nil || string(got) != raw || calls.Load() != 1 {
				t.Fatal(string(got), err, calls.Load())
			}
			if action == "retry" {
				for _, bad := range []string{strings.Replace(raw, `"new_attempt_id":"att_new"`, `"new_attempt_id":"att_original"`, 1), strings.Replace(raw, `"new_attempt_id":"att_new"`, `"new_attempt_id":null`, 1), strings.Replace(raw, `"retry_compute"`, `"retry"`, 1)} {
					if _, err := receive(response(202, bad), 202, r, token, true); err == nil {
						t.Fatal("false retry identity accepted")
					}
				}
			}
		})
	}
}
