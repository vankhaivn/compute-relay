package api_test

import (
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
)

func TestDeadlineHTTPRejectionAndStatusContract(t *testing.T) {
	f := setupJobHTTP(t)
	makeBody := func(deadline string) string {
		return strings.Replace(jobBody, `"timeouts":{`, `"timeouts":{"execution_deadline":"`+deadline+`",`, 1)
	}
	_, raw := f.call(t, "POST", "/v1/workspaces/a/jobs", "a", "deadline-expired", makeBody("2020-01-01T00:00:00Z"), 422)
	if !strings.Contains(string(raw), "REMOTE_TIMEOUT") {
		t.Fatal("unstable expiry error", string(raw))
	}
	deadline := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	_, raw = f.call(t, "POST", "/v1/workspaces/a/jobs", "a", "deadline-future", makeBody(deadline), 202)
	receipt := decodeHTTPJSON[admission.Receipt](t, raw)
	_, raw = f.call(t, "GET", receipt.Links.Self, "a", "", "", 200)
	wireSchema(t, "job-status", raw)
	status := decodeHTTPJSON[map[string]any](t, raw)
	if status["execution_deadline"] != deadline {
		t.Fatal("status lost the frozen absolute bound")
	}
	f.call(t, "GET", receipt.Links.Self, "b", "", "", 403)
}
