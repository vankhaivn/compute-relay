package api

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/joblogs"
	"github.com/vankhaivn/compute-relay/internal/operations"
)

func TestMuxRoutingRejectsUnsupportedMethodsWithEnvelope(t *testing.T) {
	f := setupHTTP(t, nil, nil)
	for _, tc := range []struct {
		method, path, who, allow string
		status                   int
	}{
		{"GET", "/missing", "a", "", 404},
		{"GET", "/missing", "", "", 401},
		{"DELETE", "/v1/workspaces/a/objects", "a", "POST", 405},
		{"POST", "/v1/workspaces/a/objects/obj_missing", "a", "GET", 405},
		{"PUT", "/v1/workspaces/a/objects/obj_missing", "a", "GET", 405},
		{"POST", "/v1/workspaces/a/objects/obj_missing", "", "", 401},
		{"POST", "/v1/workspaces/b/objects/obj_missing", "a", "", 403},
		{"POST", "/v1/info", "a", "GET", 405},
		{"HEAD", "/v1/info", "a", "GET", 405},
		{"HEAD", "/v1/workspaces/b/objects/obj_missing", "a", "", 403},
	} {
		t.Run(tc.method+tc.path+tc.who, func(t *testing.T) {
			resp, raw := f.request(t, tc.method, tc.path, tc.who, nil, nil)
			if resp.StatusCode != tc.status || resp.Header.Get("Allow") != tc.allow || resp.Header.Get("Location") != "" {
				t.Fatalf("status=%d allow=%q location=%q body=%s", resp.StatusCode, resp.Header.Get("Allow"), resp.Header.Get("Location"), raw)
			}
			if tc.method == http.MethodHead {
				if len(raw) != 0 {
					t.Fatal("HEAD returned a response body")
				}
				return
			}
			var envelope ErrorEnvelope
			if err := json.Unmarshal(raw, &envelope); err != nil || envelope.Error.RequestID == "" || envelope.Error.Code == "" {
				t.Fatalf("missing structured error: %s", raw)
			}
		})
	}
}

func TestMuxNeverRedirectsOrAcceptsPathAndQueryAliases(t *testing.T) {
	f := setupHTTP(t, nil, nil)
	// Redirects and decoded slashes must never turn an invalid request into another route.
	for _, path := range []string{
		"/v1//info", "/v1/./info", "/v1/workspaces/a/../a/objects", "/v1/info/",
		"/v1/%69nfo", "/v1/workspaces/a%2Fb/objects", "/v1/workspaces/a/objects/obj%2Fmissing",
		"/v1/info?", "/v1/info?unused=1", "/healthz?", "/v1/workspaces/a/objects?unused=1",
	} {
		t.Run(path, func(t *testing.T) {
			resp, raw := f.request(t, "GET", path, "a", nil, nil)
			if resp.StatusCode != 400 || resp.Header.Get("Location") != "" {
				t.Fatalf("status=%d location=%q body=%s", resp.StatusCode, resp.Header.Get("Location"), raw)
			}
			var envelope ErrorEnvelope
			if err := json.Unmarshal(raw, &envelope); err != nil || envelope.Error.Code != "INVALID_REQUEST" {
				t.Fatalf("missing invalid-request envelope: %s", raw)
			}
		})
	}
}

// Verified provider reads have longer bounded deadlines; mutations keep control deadlines.
func TestObservationalRoutesGetTheirBoundedReadDeadline(t *testing.T) {
	h := &handler{config: Config{RequestTimeout: 10 * time.Second, UploadTimeout: 2 * time.Minute, Operations: new(operations.Service), Cleanup: new(cleanup.Service)}}
	h.routes()
	if got := h.timeout(h.policies["GET /v1/workspaces/{workspace}/jobs/{job}/logs"]); got != joblogs.ReadTimeout {
		t.Fatal("log replay deadline", got)
	}
	const path = "/v1/workspaces/{workspace}/jobs/{job}/cleanup"
	if got := h.timeout(h.policies["GET "+path]); got != cleanup.PreviewTimeout {
		t.Fatal("cleanup preview deadline", got)
	}
	for _, pattern := range []string{"POST " + path, "HEAD " + path, "GET /v1/workspaces/{workspace}/operations/{operation}"} {
		if got := h.timeout(h.policies[pattern]); got != 10*time.Second {
			t.Fatal(pattern, got)
		}
	}
}
