package api

import (
	"encoding/json"
	"errors"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hostwatchdog"
	"github.com/vankhaivn/compute-relay/internal/jsonwire"
	"net/http"
)

func (h *handler) watchdog(w http.ResponseWriter, r *http.Request, p auth.Principal, workspace domain.WorkspaceID) {
	fail := func(err error) {
		switch {
		case errors.Is(err, hostwatchdog.ErrUnsupported):
			respondError(w, r, 422, domain.CodeUnsupportedCapability, domain.FailureStageValidation, "host watchdog requires native macOS Intel")
		case errors.Is(err, hostwatchdog.ErrIntegrity):
			respondError(w, r, 503, domain.CodeStateStoreUnavailable, domain.FailureStageLocalRuntime, "host watchdog integrity or observation failed")
		default:
			connectionError(w, r, err)
		}
	}
	if r.Method == http.MethodPost {
		raw, key, ok := readManagedRequest(w, r, 1024)
		if !ok {
			return
		}
		var body struct {
			Revision int64 `json:"expected_revision"`
		}
		if json.Unmarshal(raw, &body) != nil {
			fail(connections.ErrRequest)
			return
		}
		fields, err := jsonwire.Object(raw, 1024)
		if err != nil || jsonwire.Fields(fields, []string{"expected_revision"}, nil) != nil {
			fail(connections.ErrRequest)
			return
		}

		op, err := h.config.HostWatchdog.Submit(r.Context(), p, workspace, r.PathValue("connection"), key, r.PathValue("watchdogAction"), body.Revision)
		if err != nil {
			fail(err)
			return
		}
		w.Header().Set("Location", "/v1/workspaces/"+string(workspace)+"/host-watchdog-operations/"+string(op.ID))
		respond(w, 202, op)
		return
	}
	if id := r.PathValue("watchdogOperation"); id != "" {
		op, err := h.config.HostWatchdog.Operation(r.Context(), p, workspace, domain.OperationID(id))
		if err != nil {
			fail(err)
			return
		}
		respond(w, 200, op)
		return
	}
	status, err := h.config.HostWatchdog.Status(r.Context(), p, workspace, r.PathValue("connection"))
	if err != nil {
		fail(err)
		return
	}
	respond(w, 200, map[string]any{"status": status})
}
