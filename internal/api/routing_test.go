package api

import (
	"encoding/json"
	"net/http"
	"testing"
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
