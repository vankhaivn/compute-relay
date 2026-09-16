package runtimehost

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
)

func startHost(t *testing.T, h *Host) (string, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	announced, done := make(chan Listening, 1), make(chan error, 1)
	go func() { done <- h.Serve(ctx, "127.0.0.1:0", func(v Listening) error { announced <- v; return nil }) }()
	var event Listening
	select {
	case event = <-announced:
	case err := <-done:
		cancel()
		t.Fatal("serve failed", err)
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("serve startup deadline")
	}
	if event.DispatchEnabled || event.Mode != "local-admission-only" {
		t.Fatal("misleading serve mode")
	}
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error("shutdown failed", err)
			}
		case <-time.After(15 * time.Second):
			t.Error("shutdown did not join")
		}
	}
	t.Cleanup(stop)
	return "http://" + event.Address, stop
}
func requestHost(t *testing.T, url, method, token, body string, headers map[string]string, want int) []byte {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		if k == "Host" {
			req.Host = v
		} else {
			req.Header.Set(k, v)
		}
	}
	resp, err := boundedClient().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	closeErr := resp.Body.Close()
	if readErr != nil || closeErr != nil || resp.StatusCode != want || resp.Header.Get("X-Compute-Relay-Mode") != "local-admission-only" {
		t.Fatalf("HTTP %d want %d body=%s read=%v close=%v", resp.StatusCode, want, raw, readErr, closeErr)
	}
	return raw
}
func TestLocalHTTPAuthUploadAdmissionAndReopenWithoutDispatch(t *testing.T) {
	ctx := context.Background()
	path := initialized(t)
	h, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	if _, err := h.CreateWorkspace(ctx, "app"); err != nil {
		t.Fatal(err)
	}
	tokenPath := privateTokenPath(t)
	receipt, err := h.IssueToken(ctx, "app", []auth.Scope{auth.Read, auth.Write, auth.Operate}, time.Hour, tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	rawToken, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	token := strings.TrimSuffix(string(rawToken), "\n")
	url, stop := startHost(t, h)
	requestHost(t, url+"/healthz", "GET", "", "", nil, 200)
	requestHost(t, url+"/readyz", "GET", "", "", nil, 401)
	requestHost(t, url+"/readyz", "GET", token, "", nil, 200)
	requestHost(t, url+"/v1/info", "GET", token, "", map[string]string{"Host": "evil.invalid:7331"}, 403)
	requestHost(t, url+"/v1/info", "GET", token, "", map[string]string{"Origin": "https://evil.invalid"}, 403)
	requestHost(t, url+"/v1/workspaces/foreign/objects/x", "GET", token, "", nil, 403)
	input := "opaque admission bytes; MUST_NOT_EXECUTE"
	sum := sha256.Sum256([]byte(input))
	raw := requestHost(t, url+"/v1/workspaces/app/objects", "POST", token, input, map[string]string{"Content-Type": "application/octet-stream", "X-Content-SHA256": hex.EncodeToString(sum[:])}, 201)
	var object domain.ObjectMetadata
	if json.Unmarshal(raw, &object) != nil || !object.Valid() {
		t.Fatal("invalid object receipt")
	}
	requestHost(t, url+"/v1/workspaces/app/objects/"+string(object.ID), "GET", token, "", nil, 200)
	spec := fmt.Sprintf(`{"api_version":"compute-connector/v1alpha1","name":"local fixture","profile":"local-test","bundle":{"object_id":%q},"execution":{"kind":"python","command":["python","MUST_NOT_EXECUTE.py"]},"inputs":[],"outputs":[{"path":"answer.json","required":true}],"resources":{"accelerator":"cpu"},"network":{"remote_internet":"disabled"},"timeouts":{"remote_wall_seconds":10,"setup_seconds":5,"finalization_grace_seconds":2}}`, object.ID)
	// New installations have no profiles. Explicit TEST-ONLY profile seeding below
	// proves handler composition without inventing a production provider setup path.
	requestHost(t, url+"/v1/workspaces/app/jobs", "POST", token, spec, map[string]string{"Content-Type": "application/json", "Idempotency-Key": "local-idempotency-001"}, 403)
	if err := h.store.PutWorkspace(ctx, auth.Workspace{ID: "app", Enabled: true, AllowedProfiles: []string{"local-test"}}); err != nil {
		t.Fatal(err)
	}
	profile := admission.DefaultProfile(domain.ProviderBinding{Profile: "local-test", ProviderInstanceID: "fixture", ConfigurationRevision: "one"}, "offline_account")
	if err := h.store.PutProfile(ctx, profile, true); err != nil {
		t.Fatal(err)
	}
	raw = requestHost(t, url+"/v1/workspaces/app/jobs", "POST", token, spec, map[string]string{"Content-Type": "application/json", "Idempotency-Key": "local-idempotency-001"}, 202)
	var job admission.Receipt
	if json.Unmarshal(raw, &job) != nil || !job.JobID.Valid() {
		t.Fatal("invalid admission receipt")
	}
	stop()
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	h, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	url, stop = startHost(t, h)
	raw = requestHost(t, url+"/v1/workspaces/app/jobs", "POST", token, spec, map[string]string{"Content-Type": "application/json", "Idempotency-Key": "local-idempotency-001"}, 202)
	var replay admission.Receipt
	if json.Unmarshal(raw, &replay) != nil || !replay.Replay || replay.JobID != job.JobID || replay.AttemptID != job.AttemptID {
		t.Fatal("reopen lost original receipt")
	}
	payload, err := h.inputs.Open(ctx, "app", object.ID)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := io.ReadAll(payload)
	closeErr := payload.Close()
	if err != nil || closeErr != nil || !bytes.Equal(actual, []byte(input)) {
		t.Fatal("input bytes changed on reopen")
	}
	requestHost(t, url+"/v1/workspaces/app/jobs/"+string(job.JobID), "GET", token, "", nil, 200)
	requestHost(t, url+"/v1/workspaces/app/jobs/"+string(job.JobID)+"/cancel", "POST", token, fmt.Sprintf(`{"attempt_id":%q}`, job.AttemptID), map[string]string{"Content-Type": "application/json", "Idempotency-Key": "local-cancel-001"}, 202)
	stop()
	if err := h.RevokeToken(ctx, receipt.ID); err != nil {
		t.Fatal(err)
	}
	url, stop = startHost(t, h)
	requestHost(t, url+"/v1/info", "GET", token, "", nil, 401)
	stop()
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(path, "state", "runtime.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, table := range []string{"submission_intents", "provider_resources", "collection_publications"} {
		var count int
		if err := db.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Fatal("local HTTP caused provider work", table, count, err)
		}
	}
}
