package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hostimages"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func TestResourceCountRefusalsExposeOnlyClosedPolicyDetails(t *testing.T) {
	for _, reason := range []string{"remote_staging_dataset_limit", "remote_kernel_limit", "remote_shared_dataset_limit", "remote_attempt_container_limit", "runner_image_count_limit", "runner_image_scope_unknown", "host_image_count_limit"} {
		t.Run(reason, func(t *testing.T) {
			cause := provider.ErrResourceCountLimit
			if reason == "host_image_count_limit" {
				cause = hostimages.ErrCountLimit
			}
			err := provider.WithReason(errors.Join(cause, errors.New("private-host-account-path-canary")), reason)
			responders := []func(http.ResponseWriter, *http.Request, error){jobError}
			if reason != "host_image_count_limit" {
				responders = append(responders, connectionError)
			}
			for _, responder := range responders {
				w := httptest.NewRecorder()
				responder(w, httptest.NewRequest(http.MethodPost, "http://localhost/", nil), err)
				var envelope ErrorEnvelope
				if w.Code != 422 || json.Unmarshal(w.Body.Bytes(), &envelope) != nil {
					t.Fatal("count refusal lost its public envelope", w.Code, w.Body.String())
				}
				p := envelope.Error
				if p.Code != domain.CodeResourceRequirementUnsatisfied || p.Stage != domain.FailureStageValidation || p.RecommendedAction != domain.RecommendedActionWait || p.ComputeMayHaveStarted || p.Details[domain.ReasonDetail] != reason || strings.Contains(w.Body.String(), "canary") {
					t.Fatal("unsafe or missing count refusal detail", p)
				}
			}
		})
	}
}
