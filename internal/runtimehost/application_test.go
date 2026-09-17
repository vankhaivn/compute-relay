package runtimehost

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/appcli"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operatorcli"
	"github.com/vankhaivn/compute-relay/internal/statefs"
)

func TestApplicationCLIRealProfileUploadLostReceiptRemapAndControls(t *testing.T) {
	ctx := context.Background()
	root := initialized(t)
	local := func(args ...string) []byte {
		t.Helper()
		var out, diagnostic bytes.Buffer
		args = append(args, "--root", root)
		if code := operatorcli.Run(ctx, args, &out, &diagnostic, Command); code != 0 {
			t.Fatal(code, diagnostic.String())
		}
		return out.Bytes()
	}
	local("workspace", "create", "--id", "app")
	tokenFile := filepath.Join(root, "application.token")
	raw := local("token", "issue", "--workspace", "app", "--scope", "read", "--scope", "write", "--scope", "operate", "--output", tokenFile)
	var tokenReceipt TokenReceipt
	if json.Unmarshal(raw, &tokenReceipt) != nil || tokenReceipt.ID == "" {
		t.Fatal("missing local token receipt")
	}
	d := profileDocument()
	original := d.profile()
	profileFile := filepath.Join(root, "profile.json")
	writeProfile := func() {
		t.Helper()
		raw, _ := json.Marshal(d)
		if err := os.WriteFile(profileFile, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	writeProfile()
	local("profile", "apply", "--file", profileFile)
	local("profile", "grant", "--workspace", "app", "--name", d.Name)
	h, err := Open(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	url, stop := startHost(t, h)
	call := func(endpoint string, want int, args ...string) []byte {
		t.Helper()
		args = append(args, "--url", endpoint, "--workspace", "app", "--token-file", tokenFile)
		var out, diagnostic bytes.Buffer
		code := appcli.Run(ctx, args, &out, &diagnostic)
		if code != want {
			t.Fatal("application command", args[0:2], code, diagnostic.String())
		}
		if code == 0 && diagnostic.Len() != 0 || code != 0 && out.Len() != 0 {
			t.Fatal("mixed successful and failed command output")
		}
		if code != 0 {
			return diagnostic.Bytes()
		}
		return out.Bytes()
	}
	input := filepath.Join(root, "bundle.bin")
	payload := []byte("opaque admission fixture; MUST_NOT_EXECUTE")
	if statefs.WriteNew(input, payload) != nil {
		t.Fatal("input fixture")
	}
	raw = call(url, 0, "object", "upload", "--file", input)
	var object domain.ObjectMetadata
	if json.Unmarshal(raw, &object) != nil || !object.Valid() {
		t.Fatal("bad uploaded object")
	}
	jobFile := filepath.Join(root, "job.json")
	spec := fmt.Sprintf(`{"api_version":"compute-connector/v1alpha1","name":"application fixture","profile":"local-test","bundle":{"object_id":%q},"execution":{"kind":"python","command":["python","MUST_NOT_EXECUTE.py"]},"inputs":[],"outputs":[{"path":"answer.json","required":true}],"resources":{"accelerator":"cpu"},"network":{"remote_internet":"disabled"},"timeouts":{"remote_wall_seconds":10,"setup_seconds":5,"finalization_grace_seconds":2}}`, object.ID)
	if statefs.WriteNew(jobFile, []byte(spec)) != nil {
		t.Fatal("job fixture")
	}
	call(url, 0, "job", "validate", "--file", jobFile)
	// The proxy loses only the response after the real runtime commits admission.
	// It is test-only loopback transport, not a production proxy or provider call.
	var sends atomic.Int32
	committed := make(chan admission.Receipt, 1)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sends.Add(1)
		forward := r.Clone(r.Context())
		forward.RequestURI = ""
		forward.URL.Scheme = "http"
		forward.URL.Host = strings.TrimPrefix(url, "http://")
		forward.Host = forward.URL.Host
		tr := &http.Transport{Proxy: nil, DisableKeepAlives: true}
		defer tr.CloseIdleConnections()
		response, err := tr.RoundTrip(forward)
		if err != nil {
			t.Error(err)
			return
		}
		body, readErr := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		var receipt admission.Receipt
		if readErr != nil || closeErr != nil || response.StatusCode != 202 || json.Unmarshal(body, &receipt) != nil {
			t.Error("admission was not committed")
			return
		}
		committed <- receipt
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_ = conn.Close()
	}))
	diagnostic := call(proxy.URL, 1, "job", "submit", "--file", jobFile, "--idempotency-key", "original-admission")
	proxy.Close()
	if sends.Load() != 1 || !bytes.Contains(diagnostic, []byte(`"request_may_have_committed":true`)) {
		t.Fatal("lost response silently replayed or reported rollback")
	}
	var first admission.Receipt
	select {
	case first = <-committed:
	default:
		t.Fatal("missing real admission")
	}
	raw = call(url, 0, "job", "submit", "--file", jobFile, "--idempotency-key", "original-admission")
	var replay admission.Receipt
	if json.Unmarshal(raw, &replay) != nil || !replay.Replay || replay.JobID != first.JobID || replay.AttemptID != first.AttemptID {
		t.Fatal("manual replay changed original admission")
	}
	stop()
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	d.Revision, d.AccountScope, d.InstanceID, d.Enabled = "rev2", "remapped_account", "remapped_instance", false
	writeProfile()
	local("profile", "apply", "--file", profileFile)
	local("profile", "revoke", "--workspace", "app", "--name", d.Name)
	h, err = Open(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	url, stop = startHost(t, h)
	raw = call(url, 0, "job", "submit", "--file", jobFile, "--idempotency-key", "original-admission")
	if json.Unmarshal(raw, &replay) != nil || !replay.Replay || replay.JobID != first.JobID {
		t.Fatal("remap/revoke rewrote original receipt")
	}
	call(url, 1, "job", "submit", "--file", jobFile, "--idempotency-key", "different-admission")
	call(url, 0, "job", "status", "--id", string(first.JobID))
	raw = call(url, 0, "job", "cancel", "--id", string(first.JobID), "--attempt", string(first.AttemptID), "--idempotency-key", "original-control")
	var control map[string]any
	if json.Unmarshal(raw, &control) != nil || control["kind"] != "cancel" || control["remote_termination_confirmed"] != false {
		t.Fatal("invalid original control")
	}
	raw = call(url, 0, "job", "cancel", "--id", string(first.JobID), "--attempt", string(first.AttemptID), "--idempotency-key", "original-control")
	var repeated map[string]any
	if json.Unmarshal(raw, &repeated) != nil || repeated["replay"] != true {
		t.Fatal("control replay lost")
	}
	delete(control, "replay")
	delete(repeated, "replay")
	if !reflect.DeepEqual(control, repeated) {
		t.Fatal("control replay rewrote receipt")
	}
	call(url, 0, "operation", "status", "--id", control["operation_id"].(string))
	stop()
	// Private inspection verifies that a current alias never retargets the accepted job.
	record, err := h.store.ReadJob(ctx, "app", tokenReceipt.ID, first.JobID)
	if err != nil || !reflect.DeepEqual(record.Profile, original) {
		t.Fatal("accepted profile retargeted", err)
	}
	stream, err := h.inputs.Open(ctx, "app", object.ID)
	if err != nil {
		t.Fatal(err)
	}
	actual, readErr := io.ReadAll(stream)
	closeErr := stream.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(actual, payload) {
		t.Fatal("upload bytes changed")
	}
	if err := h.RevokeToken(ctx, tokenReceipt.ID); err != nil {
		t.Fatal(err)
	}
	url, stop = startHost(t, h)
	call(url, 1, "job", "status", "--id", string(first.JobID))
	stop()
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(root, "state", "runtime.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for table, want := range map[string]int{"jobs": 1, "attempts": 1, "profile_revisions": 2, "submission_intents": 0, "provider_resources": 0, "collection_publications": 0} {
		var n int
		if err := db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil || n != want {
			t.Fatal(table, n, want, err)
		}
	}
}
