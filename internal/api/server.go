// Package api supplies an authenticated loopback HTTP foundation. It has no scheduler,
// provider calls, runtime deployment, or nondurable job-admission fallback.
package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/buildinfo"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/objects"
	"github.com/vankhaivn/compute-relay/internal/operations"
)

type Config struct {
	Results        *collection.Reader // nil disables published artifact reads; no provider fallback.
	Operations     *operations.Service // nil disables durable controls; never use an in-memory fallback.
	Jobs           *admission.Service  // nil disables durable job routes; never use a memory fallback.
	HTTPSInputs    *objects.Ingestor   // nil disables HTTPS ingestion; operator composition supplies the guarded client.
	LocalImports   *objects.Importer   // nil disables local import; configured by the operator composition root.
	Listen         string
	MaxJSONBytes   int64
	MaxUploadBytes int64
	MaxInFlight    int
	MaxUploads     int
	MaxRateKeys    int
	RequestTimeout time.Duration
	UploadTimeout  time.Duration
	HeaderTimeout  time.Duration
	IdleTimeout    time.Duration
	GlobalRate     Rate
	WorkspaceRate  Rate
	Now            func() time.Time // Clock seam for deterministic rate-limit tests.
}

func DefaultConfig() Config {
	return Config{
		Listen: "127.0.0.1:7331", MaxJSONBytes: 1 << 20, MaxUploadBytes: 2 << 30,
		MaxInFlight: 32, MaxUploads: 4, MaxRateKeys: 1024,
		RequestTimeout: 10 * time.Second, UploadTimeout: 2 * time.Minute,
		HeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second,
		GlobalRate: Rate{PerSecond: 50, Burst: 100}, WorkspaceRate: Rate{PerSecond: 2, Burst: 20},
		Now: time.Now,
	}
}

// NewServer deliberately refuses non-loopback binds in this foundation. Explicit TLS /
// reverse-proxy deployment belongs to the operational configuration work, not a silent
// switch to 0.0.0.0. ready must check local dependencies only; nil means not ready.
func NewServer(cfg Config, access *auth.Service, objectService *objects.Service, ready func(context.Context) error) (*http.Server, error) {
	host, port, err := net.SplitHostPort(cfg.Listen)
	ip, ipErr := netip.ParseAddr(host)
	portNumber, portErr := strconv.Atoi(port)
	if err != nil || ipErr != nil || !ip.IsLoopback() || portErr != nil || portNumber < 0 || portNumber > 65535 ||
		cfg.MaxJSONBytes <= 0 || cfg.MaxUploadBytes <= 0 || cfg.MaxInFlight <= 0 || cfg.MaxInFlight > 1024 ||
		cfg.MaxUploads <= 0 || cfg.MaxUploads > cfg.MaxInFlight || cfg.MaxRateKeys <= 0 || cfg.MaxRateKeys > 100000 ||
		cfg.RequestTimeout > time.Hour || cfg.UploadTimeout > 24*time.Hour || cfg.RequestTimeout <= 0 || cfg.UploadTimeout < cfg.RequestTimeout || cfg.HeaderTimeout <= 0 || cfg.IdleTimeout <= 0 ||
		!cfg.GlobalRate.valid() || !cfg.WorkspaceRate.valid() || access == nil || objectService == nil {
		return nil, errors.New("invalid authenticated loopback server configuration")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	h := &handler{config: cfg, auth: access, objects: objectService, ready: ready,
		global: newLimiter(cfg.GlobalRate, 1), spaces: newLimiter(cfg.WorkspaceRate, cfg.MaxRateKeys),
		inFlight: make(chan struct{}, cfg.MaxInFlight), uploads: make(chan struct{}, cfg.MaxUploads)}
	return &http.Server{
		Addr: cfg.Listen, Handler: h, MaxHeaderBytes: 32 << 10,
		ReadHeaderTimeout: cfg.HeaderTimeout, ReadTimeout: cfg.UploadTimeout,
		WriteTimeout: cfg.UploadTimeout + cfg.RequestTimeout, IdleTimeout: cfg.IdleTimeout,
	}, nil
}

type handler struct {
	config   Config
	auth     *auth.Service
	objects  *objects.Service
	ready    func(context.Context) error
	global   *limiter
	spaces   *limiter
	inFlight chan struct{}
	uploads  chan struct{}
}

type requestIDKey struct{}

func RequestID(ctx context.Context) string {
	value, _ := ctx.Value(requestIDKey{}).(string)
	return value
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		http.Error(w, "runtime unavailable", http.StatusServiceUnavailable)
		return
	}
	requestID := "req_" + hex.EncodeToString(id[:])
	r = r.WithContext(context.WithValue(r.Context(), requestIDKey{}, requestID))
	w.Header().Set("X-Request-ID", requestID)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	defer func() {
		if value := recover(); value != nil {
			if value == http.ErrAbortHandler {
				panic(http.ErrAbortHandler) // Preserve a failed binary stream's missing acknowledgement.
			}
			// No panic value, token, request dump, or private host path is returned/logged.
			respondError(w, r, http.StatusServiceUnavailable, domain.CodeStateStoreUnavailable, domain.FailureStageLocalRuntime, "runtime request failed")
		}
	}()
	if !h.safeAuthority(r) {
		respondError(w, r, http.StatusForbidden, domain.CodeWorkspaceForbidden, domain.FailureStageAuthentication, "Host or Origin is not permitted")
		return
	}
	if ok, wait := h.global.allow("global", h.config.Now()); !ok {
		h.limited(w, r, wait)
		return
	}
	select {
	case h.inFlight <- struct{}{}:
		defer func() { <-h.inFlight }()
	default:
		h.limited(w, r, 1)
		return
	}
	isArtifact := artifactPath(r.URL.Path)
	if r.URL.RawQuery != "" && !isArtifact || len(r.URL.Path) > 2048 || r.URL.RawPath != "" || len(r.URL.RawQuery) > 512 {
		respondError(w, r, 400, domain.CodeInvalidRequest, domain.FailureStageValidation, "unexpected query or encoded path")
		return
	}
	if r.URL.Path == "/healthz" && r.Method == http.MethodGet {
		respond(w, 200, map[string]string{"status": "ok"})
		return
	}

	// Authentication itself has the short control-request deadline, even for uploads.
	ctx, cancel := context.WithTimeout(r.Context(), h.config.RequestTimeout)
	principal, err := h.authenticate(ctx, r)
	cancel()
	if err != nil {
		mapError(w, r, err)
		return
	}
	if ok, wait := h.spaces.allow(string(principal.WorkspaceID()), h.config.Now()); !ok {
		h.limited(w, r, wait)
		return
	}
	segments := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	isUpload := len(segments) == 4 && segments[0] == "v1" && segments[1] == "workspaces" && segments[3] == "objects" && r.Method == http.MethodPost
	isImport := len(segments) == 5 && segments[0] == "v1" && segments[1] == "workspaces" && segments[3] == "objects" && segments[4] == "import" && r.Method == http.MethodPost
	isIngest := len(segments) == 5 && segments[0] == "v1" && segments[1] == "workspaces" && segments[3] == "objects" && segments[4] == "ingest" && r.Method == http.MethodPost
	isDownload := isArtifact && len(segments) == 8 && r.Method == http.MethodGet
	timeout, maxBody := h.config.RequestTimeout, h.config.MaxJSONBytes
	if isUpload {
		timeout, maxBody = h.config.UploadTimeout, h.config.MaxUploadBytes
	}
	if isImport || isIngest || isDownload {
		timeout = h.config.UploadTimeout // JSON body stays bounded by MaxJSONBytes.
	}
	if r.ContentLength > maxBody {
		mapError(w, r, blobfs.ErrTooLarge)
		return
	}
	if r.Body != nil {
		r.Body = http.MaxBytesReader(w, r.Body, maxBody)
		defer r.Body.Close()
	}
	// A context alone cannot interrupt a blocked body Read. Bound the connection's read
	// deadline too. Unsupported is normal for ResponseRecorder in unit tests.
	if err := http.NewResponseController(w).SetReadDeadline(time.Now().Add(timeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		mapError(w, r, objects.ErrUnavailable)
		return
	}
	if isDownload {
		if err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(timeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
			artifactError(w, r, collection.ErrUnavailable)
			return
		}
	}
	ctx, cancel = context.WithTimeout(r.Context(), timeout)
	defer cancel()
	r = r.WithContext(ctx)
	if r.URL.Path == "/readyz" && r.Method == http.MethodGet {
		if h.ready == nil || h.ready(ctx) != nil {
			respondError(w, r, 503, domain.CodeStateStoreUnavailable, domain.FailureStageLocalRuntime, "local runtime dependencies are not ready")
		} else {
			respond(w, 200, map[string]string{"status": "ready"})
		}
		return
	}
	if r.URL.Path == "/v1/info" && r.Method == http.MethodGet {
		features := []string{"workspace_auth", "object_upload", "object_metadata"}
		admissionStatus := "not_implemented"
		if h.config.Jobs != nil {
			features = append(features, "job_admission", "job_validation", "job_status")
			admissionStatus = "implemented-offline"
		}
		if h.config.Operations != nil {
			features = append(features, "job_cancel", "job_retry", "job_reconcile", "job_collect", "operation_status")
		}
		if h.config.Results != nil {
			features = append(features, "artifact_list", "artifact_metadata", "artifact_download")
		}
		respond(w, 200, map[string]any{"api_version": "compute-connector/v1alpha1", "runtime_version": buildinfo.Current().Version, "implementation_status": "implemented-offline", "features": features, "job_admission": admissionStatus})
		return
	}
	if len(segments) < 4 || segments[0] != "v1" || segments[1] != "workspaces" || (segments[3] != "objects" && segments[3] != "jobs" && segments[3] != "operations") {
		respondError(w, r, 404, domain.CodeInvalidRequest, domain.FailureStageValidation, "route not implemented")
		return
	}
	workspace, err := domain.ParseWorkspaceID(segments[2])
	if err != nil {
		respondError(w, r, 400, domain.CodeInvalidRequest, domain.FailureStageValidation, "invalid workspace identifier")
		return
	}
	if workspace != principal.WorkspaceID() {
		mapError(w, r, auth.ErrForbidden)
		return
	}
	if isArtifact {
		h.artifacts(w, r, principal, workspace, segments)
		return
	}
	if segments[3] == "operations" || segments[3] == "jobs" && len(segments) == 6 {
		h.controls(w, r, principal, workspace, segments)
		return
	}
	if segments[3] == "jobs" {
		h.jobs(w, r, principal, workspace, segments)
		return
	}
	if isIngest {
		h.ingestObject(w, r, principal, workspace)
		return
	}
	if isImport {
		h.importObject(w, r, principal, workspace)
		return
	}
	if isUpload {
		h.upload(w, r, principal, workspace)
		return
	}
	if len(segments) == 5 && r.Method == http.MethodGet {
		meta, err := h.objects.Get(ctx, principal, workspace, domain.ObjectID(segments[4]))
		if err != nil {
			mapError(w, r, err)
			return
		}
		respond(w, 200, meta)
		return
	}
	if len(segments) == 4 {
		w.Header().Set("Allow", "POST")
	} else if len(segments) == 5 {
		w.Header().Set("Allow", "GET")
	}
	respondError(w, r, 405, domain.CodeInvalidRequest, domain.FailureStageValidation, "method not implemented for this route")
}

func (h *handler) authenticate(ctx context.Context, r *http.Request) (auth.Principal, error) {
	values := r.Header.Values("Authorization")
	if len(values) != 1 {
		return auth.Principal{}, auth.ErrUnauthenticated
	}
	scheme, token, found := strings.Cut(values[0], " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return auth.Principal{}, auth.ErrUnauthenticated
	}
	return h.auth.Authenticate(ctx, token)
}

func (h *handler) safeAuthority(r *http.Request) bool {
	host, port, err := net.SplitHostPort(r.Host)
	ip, ipErr := netip.ParseAddr(host)
	configuredHost, configuredPort, _ := net.SplitHostPort(h.config.Listen)
	configuredIP, _ := netip.ParseAddr(configuredHost)
	number, portErr := strconv.Atoi(port)
	if err != nil || ipErr != nil || !ip.IsLoopback() || ip != configuredIP || portErr != nil || number < 1 || number > 65535 ||
		(configuredPort != "0" && port != configuredPort) {
		return false
	}
	if r.Header.Get("Sec-Fetch-Site") == "cross-site" {
		return false
	}
	origins := r.Header.Values("Origin")
	if len(origins) == 0 {
		return true
	}
	if len(origins) != 1 {
		return false
	}
	origin, err := url.Parse(origins[0])
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return err == nil && origin.Scheme == scheme && origin.Host == r.Host && origin.User == nil && origin.Path == "" && origin.RawQuery == "" && origin.Fragment == ""
}

func (h *handler) upload(w http.ResponseWriter, r *http.Request, p auth.Principal, workspace domain.WorkspaceID) {
	if err := auth.Require(p, workspace, auth.Write); err != nil {
		mapError(w, r, err)
		return
	}
	media, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || (media != "application/octet-stream" && media != "application/gzip") || len(params) != 0 || len(r.Header.Values("Content-Type")) != 1 || r.Header.Get("Content-Encoding") != "" {
		respondError(w, r, 415, domain.CodeInvalidRequest, domain.FailureStageValidation, "send an unencoded binary object")
		return
	}
	digest := domain.SHA256Digest(r.Header.Get("X-Content-SHA256"))
	if len(r.Header.Values("X-Content-SHA256")) > 1 || (digest != "" && !digest.Valid()) {
		respondError(w, r, 400, domain.CodeInvalidRequest, domain.FailureStageValidation, "invalid SHA-256 declaration")
		return
	}
	select {
	case h.uploads <- struct{}{}:
		defer func() { <-h.uploads }()
	default:
		h.limited(w, r, 1)
		return
	}
	meta, err := h.objects.Upload(r.Context(), p, workspace, r.ContentLength, digest, r.Body)
	if err != nil {
		mapError(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/workspaces/"+url.PathEscape(string(workspace))+"/objects/"+string(meta.ID))
	respond(w, http.StatusCreated, meta)
}

func (h *handler) limited(w http.ResponseWriter, r *http.Request, seconds int) {
	w.Header().Set("Retry-After", strconv.Itoa(seconds))
	respondError(w, r, 429, domain.CodeRequestLimitExceeded, domain.FailureStageLocalRuntime, "local request limit reached")
}

func respond(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

// DecodeJSON applies the ordinary JSON-body bound and rejects unknown fields and trailing
// JSON values. Services still perform semantic validation. It never runs workload code.
func DecodeJSON(w http.ResponseWriter, r *http.Request, limit int64, destination any) error {
	if limit <= 0 || r.Body == nil {
		return objects.ErrInvalid
	}
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		if err != nil {
			return err
		}
		return objects.ErrInvalid
	}
	return nil
}
