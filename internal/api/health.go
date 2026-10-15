package api

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hosthealth"
)

// ConnectionHealth is independent of existing connection and operation projections.
// Stop is an observation of the guard decision; this read handler never stops work.
type ConnectionHealth struct {
	SchemaVersion int                   `json:"schema_version"`
	ResourceID    string                `json:"resource_id"`
	Latest        *hosthealth.Snapshot  `json:"latest"`
	Guard         hosthealth.Decision   `json:"guard"`
	History       []hosthealth.Snapshot `json:"history"`
	Policy        hosthealth.Policy     `json:"policy"`
}

var healthResourcePattern = regexp.MustCompile(`^resource_[0-9a-f]{64}$`)

func (h *handler) health(w http.ResponseWriter, r *http.Request, p auth.Principal, workspace domain.WorkspaceID) {
	if h.config.Health == nil {
		respondError(w, r, 404, domain.CodeInvalidRequest, domain.FailureStageValidation, "host health is not configured")
		return
	}
	if !managedScope(w, r, p, workspace, auth.Read) {
		return
	}
	id := r.PathValue("connection")
	if !domain.ObjectID(id).Valid() {
		connectionError(w, r, connections.ErrRequest)
		return
	}
	view, err := h.config.Health.Get(r.Context(), p, workspace, id)
	if err != nil {
		healthError(w, r, err)
		return
	}
	now := h.config.Now().UTC()
	response, err := projectConnectionHealth(view, now)
	if err != nil {
		healthError(w, r, err)
		return
	}
	respond(w, http.StatusOK, response)
}
func projectConnectionHealth(view hosthealth.View, now time.Time) (ConnectionHealth, error) {
	if !healthResourcePattern.MatchString(view.ResourceID) || view.Policy == nil || view.Policy.Validate() != nil || len(view.History) > hosthealth.HistoryMaxRows {
		return ConnectionHealth{}, hosthealth.ErrUnavailable
	}
	history := make([]hosthealth.Snapshot, 0, len(view.History))
	for _, s := range view.History {
		if s.Validate() != nil || s.ResourceID != view.ResourceID {
			return ConnectionHealth{}, hosthealth.ErrUnavailable
		}
		if len(history) > 0 && s.ObservedAt.After(history[len(history)-1].ObservedAt) {
			return ConnectionHealth{}, hosthealth.ErrUnavailable
		}
		if now.Sub(s.ObservedAt) <= hosthealth.HistoryMaxAge {
			history = append(history, s)
		}
	}
	if view.Latest != nil && (view.Latest.Validate() != nil || view.Latest.ResourceID != view.ResourceID) {
		return ConnectionHealth{}, hosthealth.ErrUnavailable
	}
	return ConnectionHealth{SchemaVersion: 1, ResourceID: view.ResourceID, Latest: view.Latest, Guard: hosthealth.Evaluate(view, *view.Policy, now), History: history, Policy: *view.Policy}, nil
}
func healthError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, auth.ErrUnauthenticated), errors.Is(err, auth.ErrForbidden), errors.Is(err, connections.ErrNotFound), errors.Is(err, connections.ErrRequest):
		connectionError(w, r, err)
	case errors.Is(err, hosthealth.ErrUnsupported):
		respondError(w, r, 400, domain.CodeUnsupportedCapability, domain.FailureStageValidation, "host health is unsupported for this provider")
	case errors.Is(err, hosthealth.ErrUnverified):
		respondError(w, r, 409, domain.CodeIllegalStateTransition, domain.FailureStageObservation, "host health requires a verified machine; check the connection")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		respondError(w, r, 503, domain.CodeStateStoreUnavailable, domain.FailureStageObservation, "host health observation did not complete")
	default:
		respondError(w, r, 503, domain.CodeStateStoreUnavailable, domain.FailureStageObservation, "host health is unavailable")
	}
}
