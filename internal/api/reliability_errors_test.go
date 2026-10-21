package api

import (
	"context"
	"encoding/json"
	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"net/http/httptest"
	"testing"
)

func TestReliabilityErrorHints(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
		code   domain.ErrorCode
		safe   bool
		after  string
	}{{admission.ErrSelectionPending, 429, domain.CodeRequestLimitExceeded, true, "1"}, {admission.ErrSelectionUnavailable, 503, domain.CodeProviderUnreachable, true, "5"}, {context.DeadlineExceeded, 408, domain.CodeRequestTimeout, false, ""}} {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("POST", "http://localhost/", nil)
		jobError(w, r, tc.err)
		var e ErrorEnvelope
		if err := json.Unmarshal(w.Body.Bytes(), &e); err != nil {
			t.Fatal(err)
		}
		if w.Code != tc.status || e.Error.Code != tc.code || e.Error.SafeOperationRetry != tc.safe || w.Header().Get("Retry-After") != tc.after {
			t.Fatal("untruthful error hint", w.Code, e, w.Header())
		}
	}
	w := httptest.NewRecorder()
	respondError(w, httptest.NewRequest("GET", "http://localhost/", nil), 503, domain.CodeRuntimeUnavailable, domain.FailureStageLocalRuntime, "runtime request failed")
	if w.Header().Get("Retry-After") != "5" {
		t.Fatal("503 lacks backpressure hint")
	}
}
