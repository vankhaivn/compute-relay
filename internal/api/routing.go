package api

import (
	"net/http"
	"strings"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

type principalKey struct{}

type routePolicy struct {
	public, upload, download, query, logs bool
	remote                                bool // handler makes bounded read-only provider calls
	bodyLimit                             int64
}

type workspaceHandler func(http.ResponseWriter, *http.Request, auth.Principal, domain.WorkspaceID)

func (h *handler) routes() {
	h.mux = http.NewServeMux()
	h.policies = make(map[string]routePolicy)
	h.mux.HandleFunc("/", h.routeNotFound)
	register := func(methods, path string, policy routePolicy, endpoint http.HandlerFunc) {
		reject := func(w http.ResponseWriter, r *http.Request) {
			if workspace := r.PathValue("workspace"); workspace != "" {
				if _, _, ok := routeWorkspace(w, r, workspace); !ok {
					return
				}
			}
			w.Header().Set("Allow", methods)
			respondError(w, r, http.StatusMethodNotAllowed, domain.CodeInvalidRequest, domain.FailureStageValidation, "method not implemented for this route")
		}
		fallback := policy
		fallback.public, fallback.upload, fallback.download, fallback.remote, fallback.bodyLimit = false, false, false, false, 0
		// Cleanup query parameters are accepted only by its GET preview.
		if strings.HasSuffix(path, "/cleanup") {
			fallback.query = false
		}
		for _, method := range strings.Split(methods, ", ") {
			pattern := method + " " + path
			h.mux.HandleFunc(pattern, endpoint)
			h.policies[pattern] = policy
			if method == http.MethodGet {
				// ServeMux normally lets HEAD run GET. Relay's bounded read endpoints are GET-only.
				h.mux.HandleFunc("HEAD "+path, reject)
				h.policies["HEAD "+path] = fallback
			}
		}
	}
	register("GET", "/healthz", routePolicy{public: true}, func(w http.ResponseWriter, r *http.Request) { respond(w, 200, map[string]string{"status": "ok"}) })
	register("GET", "/readyz", routePolicy{}, h.readiness)
	register("GET", "/v1/info", routePolicy{}, h.info)
	const base = "/v1/workspaces/{workspace}/"
	add := func(methods, path string, policy routePolicy, endpoint workspaceHandler) {
		register(methods, base+path, policy, func(w http.ResponseWriter, r *http.Request) {
			p, workspace, ok := routeWorkspace(w, r, r.PathValue("workspace"))
			if !ok {
				return
			}
			endpoint(w, r, p, workspace)
		})
	}
	add("POST", "objects", routePolicy{upload: true}, h.upload)
	add("GET", "objects/{object}", routePolicy{}, h.object)
	if h.config.Jobs != nil {
		add("POST", "jobs", routePolicy{bodyLimit: admission.MaxRequestBytes}, h.jobs)
		add("POST", "jobs/validate", routePolicy{bodyLimit: admission.MaxRequestBytes}, h.jobs)
		add("GET", "jobs/{job}", routePolicy{}, h.jobs)
	}
	if h.config.Connections != nil {
		add("GET", "providers", routePolicy{}, h.connections)
		add("GET, POST", "connections", routePolicy{bodyLimit: connections.MaxRequestBytes}, h.connections)
		add("GET", "connections/{connection}", routePolicy{}, h.connections)
		add("POST", "connections/{connection}/actions", routePolicy{bodyLimit: connections.MaxRequestBytes}, h.connections)
		add("GET", "connection-operations/{connectionOperation}", routePolicy{}, h.connections)
	}
	if h.config.Authorizations != nil {
		add("POST", "jobs/{job}/authorize", routePolicy{bodyLimit: executionauth.MaxRequestBytes}, h.authorizations)
		add("GET", "jobs/{job}/authorizations/{authorization}", routePolicy{}, h.authorizations)
		for _, action := range []string{"prepare", "start"} {
			add("POST", "jobs/{job}/"+action, routePolicy{bodyLimit: executionauth.MaxRequestBytes}, h.executionGate)
		}
		add("GET", "jobs/{job}/readiness", routePolicy{}, h.executionGate)
		add("GET", "jobs/{job}/gate-authorizations/{authorization}", routePolicy{}, h.executionGate)
	}
	if h.config.Secrets != nil {
		add("POST", "jobs/{job}/workload-secrets", routePolicy{bodyLimit: workloadsecret.MaxRequestBytes}, h.workloadSecrets)
		add("GET", "jobs/{job}/workload-secrets/{authorization}", routePolicy{}, h.workloadSecrets)
		add("POST", "jobs/{job}/workload-secrets/{authorization}/revoke", routePolicy{bodyLimit: 4096}, h.workloadSecrets)
	}
	add("GET", "jobs/{job}/logs", routePolicy{query: true, logs: true}, h.logs)
	add("GET", "jobs/{job}/artifacts", routePolicy{query: true}, h.artifacts)
	add("GET", "jobs/{job}/artifacts/{artifact}", routePolicy{query: true}, h.artifacts)
	add("GET", "jobs/{job}/artifacts/{artifact}/content", routePolicy{query: true, download: true}, h.artifacts)
	if h.config.Operations != nil {
		for _, action := range []string{"cancel", "retry", "reconcile", "collect"} {
			add("POST", "jobs/{job}/"+action, routePolicy{bodyLimit: operations.MaxRequestBytes}, h.controls)
		}
		cleanupMethods := "POST"
		if h.config.Cleanup != nil {
			cleanupMethods = "GET, POST"
		}
		add(cleanupMethods, "jobs/{job}/cleanup", routePolicy{query: true, remote: true, bodyLimit: operations.MaxRequestBytes}, h.controls)
		h.policies["POST "+base+"jobs/{job}/cleanup"] = routePolicy{bodyLimit: operations.MaxRequestBytes}
		add("GET", "operations/{operation}", routePolicy{}, h.controls)
	}

}

func (h *handler) object(w http.ResponseWriter, r *http.Request, p auth.Principal, workspace domain.WorkspaceID) {
	meta, err := h.objects.Get(r.Context(), p, workspace, domain.ObjectID(r.PathValue("object")))
	if err != nil {
		mapError(w, r, err)
		return
	}
	respond(w, 200, meta)
}

// Probe the same mux for supported methods so its path matching also owns Allow.
// The catch-all never exposes ServeMux's plain-text 404/405 responses.
func (h *handler) allowed(r *http.Request) (string, routePolicy) {
	var methods []string
	var policy routePolicy
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		probe := r.Clone(r.Context())
		probe.Method = method
		_, pattern := h.mux.Handler(probe)
		if pattern != "/" && pattern != "" {
			methods = append(methods, method)
			candidate := h.policies[pattern]
			if candidate.query {
				policy = candidate
			}
		}
	}
	policy.public, policy.upload, policy.download, policy.bodyLimit = false, false, false, 0
	if strings.HasSuffix(r.URL.Path, "/cleanup") {
		policy.query = false
	}
	return strings.Join(methods, ", "), policy
}

func (h *handler) routeNotFound(w http.ResponseWriter, r *http.Request) {
	methods, _ := h.allowed(r)
	if methods == "" {
		managedNotFound(w, r)
		return
	}
	// Matching a known workspace route retains the workspace guard before 405.
	if strings.HasPrefix(r.URL.Path, "/v1/workspaces/") {
		workspace, _, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/v1/workspaces/"), "/")
		if _, _, ok := routeWorkspace(w, r, workspace); !ok {
			return
		}
	}
	w.Header().Set("Allow", methods)
	respondError(w, r, 405, domain.CodeInvalidRequest, domain.FailureStageValidation, "method not implemented for this route")
}

func routeWorkspace(w http.ResponseWriter, r *http.Request, id string) (auth.Principal, domain.WorkspaceID, bool) {
	p, _ := r.Context().Value(principalKey{}).(auth.Principal)
	workspace, err := domain.ParseWorkspaceID(id)
	if err != nil {
		respondError(w, r, 400, domain.CodeInvalidRequest, domain.FailureStageValidation, "invalid workspace identifier")
		return p, workspace, false
	}
	if workspace != p.WorkspaceID() {
		mapError(w, r, auth.ErrForbidden)
		return p, workspace, false
	}
	return p, workspace, true
}
