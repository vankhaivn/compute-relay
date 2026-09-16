package appclient

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var token = []byte("cr1_" + strings.Repeat("A", 43)) // Synthetic, never an issued credential.
const receipt = `{"job_id":"job_original","attempt_id":"att_original","status":"queued","created_at":"2026-09-16T00:00:00Z","idempotency_replay":false,"links":{"self":"/v1/workspaces/workspace/jobs/job_original"}}`

func submitRequest() Request {
	return Request{Action: "submit", Workspace: "workspace", Key: "original-key", Body: strings.NewReader(`{}`), Bytes: 2}
}
func serveJSON(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}
func TestEndpointRejectsCredentialAndAuthorityChanges(t *testing.T) {
	for _, url := range []string{"http://localhost:7331", "https://127.0.0.1:7331", "http://0.0.0.0:7331", "http://192.168.1.1:7331", "http://127.0.0.1:0", "http://127.0.0.1:07331", "http://127.0.0.1:7331/x", "http://127.0.0.1:7331?x", "http://127.0.0.1:7331?", "http://127.0.0.1:7331#", "http://user:secret@127.0.0.1:7331", "http://[::ffff:127.0.0.1]:7331", "http://[::1%25zone]:7331", "http://[0:0:0:0:0:0:0:1]:7331", "http://127.0.0.1:65536"} {
		if _, err := Endpoint(url); err == nil {
			t.Errorf("accepted unsafe endpoint %q", url)
		}
	}
	for _, url := range []string{"http://127.0.0.1:7331", "http://127.0.0.2:7331/", "http://[::1]:7331"} {
		if _, err := Endpoint(url); err != nil {
			t.Error(url, err)
		}
	}
	if !ValidToken(token) || ValidToken(append(append([]byte{}, token...), '\n')) {
		t.Fatal("token framing")
	}
}
func TestActionRulesRejectUnknownTargetsAndImplicitRetry(t *testing.T) {
	for _, r := range []Request{
		{Action: "delete", Workspace: "workspace"}, {Action: "status", Workspace: "../secret", ID: "job"},
		{Action: "status", Workspace: "workspace", ID: "job", Key: "key-unused"},
		{Action: "cancel", Workspace: "workspace", ID: "job", Key: "key-unused"},
		{Action: "retry", Workspace: "workspace", ID: "job", Attempt: "att", Key: "key-unused"},
		{Action: "submit", Workspace: "workspace", Body: strings.NewReader(`{}`), Bytes: 2},
		{Action: "upload", Workspace: "workspace", Body: strings.NewReader("x"), Bytes: 1},
	} {
		if Validate(r) == nil {
			t.Error("invalid action accepted", r.Action)
		}
	}
}
func TestSubmitSendsOneOriginalRequestWithoutAmbientProxy(t *testing.T) {
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	t.Setenv("ALL_PROXY", "http://127.0.0.1:1")
	var calls atomic.Int32
	server := serveJSON(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		raw, err := io.ReadAll(r.Body)
		if err != nil || r.Method != "POST" || r.URL.Path != "/v1/workspaces/workspace/jobs" || r.Header.Get("Authorization") != "Bearer "+string(token) || r.Header.Get("Idempotency-Key") != "original-key" || string(raw) != `{}` || !r.Close || r.Header.Get("Cookie") != "" {
			t.Error("changed request authority/body")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(202)
		_, _ = io.WriteString(w, receipt)
	})
	raw, err := Exchange(context.Background(), server.URL, token, submitRequest())
	if err != nil || string(raw) != receipt || calls.Load() != 1 {
		t.Fatal(string(raw), err, calls.Load())
	}
}
func TestLostAdmissionAcknowledgementNeverRepeatsPOST(t *testing.T) {
	for _, fault := range []string{"before-header", "after-body"} {
		t.Run(fault, func(t *testing.T) {
			var calls atomic.Int32
			server := serveJSON(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				_, _ = io.Copy(io.Discard, r.Body)
				c, b, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				defer c.Close()
				if fault == "after-body" {
					_, _ = b.WriteString("HTTP/1.1 202 Accepted\r\nContent-Type: application/json\r\nContent-Length: 9999\r\n\r\n" + receipt)
					_ = b.Flush()
				}
			})
			raw, err := Exchange(context.Background(), server.URL, token, submitRequest())
			var f *Failure
			if len(raw) != 0 || !errors.As(err, &f) || !f.RequestMayHaveCommitted || calls.Load() != 1 {
				t.Fatal(string(raw), err, calls.Load())
			}
		})
	}
}
func TestRedirectDoesNotForwardTokenOrReplay(t *testing.T) {
	var targetCalls, sourceCalls atomic.Int32
	target := serveJSON(t, func(http.ResponseWriter, *http.Request) { targetCalls.Add(1) })
	source := serveJSON(t, func(w http.ResponseWriter, r *http.Request) {
		sourceCalls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Location", target.URL)
		w.WriteHeader(307)
	})
	raw, err := Exchange(context.Background(), source.URL, token, submitRequest())
	if err == nil || len(raw) != 0 || sourceCalls.Load() != 1 || targetCalls.Load() != 0 {
		t.Fatal("redirect replayed", err)
	}
}
func TestUploadChecksReceiptDigestAndWorkspace(t *testing.T) {
	data := bytes.Repeat([]byte("source"), 1<<18)
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	server := serveJSON(t, func(w http.ResponseWriter, r *http.Request) {
		h := sha256.New()
		n, err := io.Copy(h, r.Body)
		if err != nil || n != int64(len(data)) || hex.EncodeToString(h.Sum(nil)) != digest || r.Header.Get("X-Content-SHA256") != digest || r.URL.Path != "/v1/workspaces/workspace/objects" {
			t.Error("upload changed")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(201)
		_, _ = io.WriteString(w, `{"id":"obj_result","workspace_id":"workspace","bytes":1572864,"sha256":"`+digest+`"}`)
	})
	r := Request{Action: "upload", Workspace: "workspace", Body: bytes.NewReader(data), Bytes: int64(len(data)), SHA256: digest}
	if _, err := Exchange(context.Background(), server.URL, token, r); err != nil {
		t.Fatal(err)
	}
}
func TestResponseRejectsFalseReceiptsAndPreservesFutureInformation(t *testing.T) {
	for _, raw := range []string{`{}`, strings.Replace(receipt, "workspace", "foreign", 1), strings.Replace(receipt, `"idempotency_replay":false`, `"idempotency_replay":null`, 1), strings.Replace(receipt, `"job_id":`, `"job_id":"other","job_id":`, 1), receipt + `{}`, strings.Replace(receipt, `"queued"`, `"running"`, 1)} {
		if _, err := receive(response(202, raw), 202, submitRequest(), token, true); err == nil {
			t.Fatal("invalid receipt passed", raw)
		}
	}
	raw := strings.TrimSuffix(receipt, "}") + `,"future":{"field":9007199254740993}}`
	got, err := receive(response(202, raw), 202, submitRequest(), token, true)
	if err != nil || string(got) != raw {
		t.Fatal("future information changed", err)
	}
}
func TestResponseErrorsStaySanitizedAndDoNotClaimRollback(t *testing.T) {
	raw := `{"error":{"code":"IDEMPOTENCY_CONFLICT","message":"` + string(token) + `","details":{"secret":"private"}}}`
	_, err := receive(response(409, raw), 202, submitRequest(), token, true)
	var f *Failure
	if !errors.As(err, &f) || strings.Contains(err.Error(), string(token)) || !f.RequestMayHaveCommitted {
		t.Fatal(err)
	}
	raw = `{"error":{"code":"IDEMPOTENCY_CONFLICT","message":"private"}}`
	_, err = receive(response(409, raw), 202, submitRequest(), token, true)
	if !errors.As(err, &f) || f.Code != "IDEMPOTENCY_CONFLICT" {
		t.Fatal(err)
	}
	encoded := strings.TrimSuffix(receipt, "}") + `,"future":"\u0063r1_` + strings.Repeat("A", 43) + `"}`
	if _, err := receive(response(202, encoded), 202, submitRequest(), token, true); err == nil {
		t.Fatal("escaped token reflected")
	}
}
func TestStatusDoesNotInventTerminalMeaningForUnknownState(t *testing.T) {
	raw := `{"api_version":"compute-connector/v1alpha1","workspace_id":"workspace","job_id":"job","active_attempt_id":"att","revision":2,"updated_at":"2026-09-16T00:00:00Z","state":{"orchestration":"needs_attention","execution":"future_state","result":"not_available","cancellation":"not_requested","remote_activity":"possible","release_evidence":"not_observable","deadline_exceeded":false}}`
	r := Request{Action: "status", Workspace: "workspace", ID: "job"}
	got, err := receive(response(200, raw), 200, r, token, false)
	if err != nil || string(got) != raw {
		t.Fatal("status changed", err)
	}
}

type closeFailure struct{ io.Reader }

func (closeFailure) Close() error { return errors.New("private close failure") }
func response(status int, raw string) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(raw)), ContentLength: int64(len(raw))}
}
func TestLateCloseAndOversizeNeverExposeSuccessfulPrefix(t *testing.T) {
	r := response(202, receipt)
	r.Body = closeFailure{strings.NewReader(receipt)}
	if raw, err := receive(r, 202, submitRequest(), token, true); err == nil || len(raw) != 0 {
		t.Fatal("Close failure passed")
	}
	r = response(202, receipt)
	r.Header.Set("Content-Encoding", "gzip")
	if _, err := receive(r, 202, submitRequest(), token, true); err == nil {
		t.Fatal("encoded response passed")
	}
	r = response(202, strings.Repeat(" ", MaxResponseBytes+1))
	r.ContentLength = -1
	if _, err := receive(r, 202, submitRequest(), token, true); err == nil {
		t.Fatal("unbounded response")
	}
}
func TestRequestBodyCloseJoinsActiveReadAndIsConcurrentSafe(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	b := &requestBody{reader: readerFunc(func(p []byte) (int, error) { close(entered); <-release; return 0, io.EOF }), closed: make(chan struct{})}
	readDone := make(chan struct{})
	go func() { defer close(readDone); _, _ = b.Read(make([]byte, 1)) }()
	<-entered
	var wg sync.WaitGroup
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() { defer wg.Done(); _ = b.Close() }()
	}
	select {
	case <-b.closed:
		t.Fatal("released active reader")
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	<-readDone
	wg.Wait()
	if _, err := b.Read(make([]byte, 1)); err != io.ErrClosedPipe {
		t.Fatal("read after release", err)
	}
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }
func TestCancellationDoesNotRetryOrReturnReceipt(t *testing.T) {
	entered := make(chan struct{})
	server := serveJSON(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(entered)
		<-r.Context().Done()
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { <-entered; cancel() }()
	raw, err := Exchange(ctx, server.URL, token, submitRequest())
	if err == nil || len(raw) != 0 {
		t.Fatal("cancelled request passed")
	}
}
