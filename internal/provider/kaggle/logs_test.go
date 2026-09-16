package kaggle

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/vankhaivn/compute-relay/internal/provider"
)

func logResponse(text, availability string) monitorResponse {
	return monitorResponse{Protocol: 1, Status: "logs", Reason: "none", TextB64: base64.StdEncoding.EncodeToString([]byte(text)), Availability: availability}
}

func logFixture(t *testing.T, reply func() monitorResponse) (*LogReader, provider.RemoteReference, *int) {
	t.Helper()
	e := newExecutionFixture(t)
	m, err := NewMonitor(e.stager.config, e.stager.credentials, &stagingClock{})
	if err != nil {
		t.Fatal(err)
	}
	m.local = e.stager.local
	count := new(int)
	m.run = func(_ context.Context, _ Config, mode string, token []byte, r monitorRequest) (monitorResponse, error) {
		*count++
		if mode != "logs" || string(token) != "SYNTHETIC_TOKEN" || r.Execution == nil || r.Execution.Source != "" || r.Execution.KernelID != "42" {
			t.Fatal("log read lost exact reference or retransmitted source")
		}
		return reply(), nil
	}
	reader, err := NewLogReader(e, m)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := e.remote(executionFound(e.request, "RUNNING"))
	if err != nil {
		t.Fatal(err)
	}
	return reader, ref, count
}

func TestLogSnapshotsPageWithoutMixingChangedOrForeignEvidence(t *testing.T) {
	text := "one\nhai\n三\nfour\n"
	reader, ref, calls := logFixture(t, func() monitorResponse { return logResponse(text, "delayed") })
	page, err := reader.ReadLogs(context.Background(), ref, provider.PageRequest{Limit: 2})
	if err != nil || len(page.Lines) != 2 || page.Lines[0] != "one" || page.NextCursor == "" || page.Source != "provider" || page.Availability != "delayed" || page.Truncated {
		t.Fatal(page, err)
	}
	request := provider.PageRequest{Limit: 2, Cursor: page.NextCursor}
	last, err := reader.ReadLogs(context.Background(), ref, request)
	if err != nil || last.Validate(request) != nil || len(last.Lines) != 2 || last.Lines[0] != "三" || last.NextCursor != "" {
		t.Fatal(last, err)
	}
	text += "newly appended\n"
	if _, err := reader.ReadLogs(context.Background(), ref, request); !errors.Is(err, ErrLogChanged) {
		t.Fatal("mixed snapshots", err)
	}
	before := *calls
	foreign := ref
	foreign.Identity.WorkspaceID = "foreign"
	if _, err := reader.ReadLogs(context.Background(), foreign, request); !errors.Is(err, ErrExecutionIdentity) || *calls != before {
		t.Fatal("foreign reference reached helper", err)
	}
	var cursor logCursor
	raw, _ := base64.RawURLEncoding.DecodeString(request.Cursor)
	_ = json.Unmarshal(raw, &cursor)
	cursor.Reference = provider.Digest([]byte("foreign"))
	raw, _ = json.Marshal(cursor)
	request.Cursor = base64.RawURLEncoding.EncodeToString(raw)
	if _, err := reader.ReadLogs(context.Background(), ref, request); !errors.Is(err, ErrLogCursor) || *calls != before {
		t.Fatal("foreign cursor reached helper", err)
	}
}

func TestLogSnapshotsBoundLinesAndDistinguishEmptyUnavailableAndIdentityFailure(t *testing.T) {
	reply := logResponse(strings.Repeat("界", 6000), "after_completion")
	reader, ref, _ := logFixture(t, func() monitorResponse { return reply })
	page, err := reader.ReadLogs(context.Background(), ref, provider.PageRequest{Limit: 1})
	if err != nil || !page.Truncated || len(page.Lines) != 1 || len(page.Lines[0]) > 16<<10 || !utf8.ValidString(page.Lines[0]) {
		t.Fatal(page, err)
	}
	reply = logResponse("", "after_completion")
	page, err = reader.ReadLogs(context.Background(), ref, provider.PageRequest{Limit: 1})
	if err != nil || page.Availability != "after_completion" || len(page.Lines) != 0 || page.Truncated || page.NextCursor != "" {
		t.Fatal("empty log misrepresented", err)
	}
	reply = monitorResponse{Protocol: 1, Status: "unavailable", Reason: "missing_log"}
	page, err = reader.ReadLogs(context.Background(), ref, provider.PageRequest{Limit: 1})
	if err != nil || page.Availability != "unavailable" || len(page.Lines) != 0 || page.Source != "provider" {
		t.Fatal("invented fallback logs", err)
	}
	reply = monitorResponse{Protocol: 1, Status: "invalid", Reason: "identity_mismatch"}
	if _, err := reader.ReadLogs(context.Background(), ref, provider.PageRequest{Limit: 1}); !errors.Is(err, ErrExecutionIdentity) {
		t.Fatal("identity failure concealed", err)
	}
	reply = logResponse("prefix", "delayed")
	reply.Truncated = true
	page, err = reader.ReadLogs(context.Background(), ref, provider.PageRequest{Limit: 1})
	if err != nil || !page.Truncated {
		t.Fatal("snapshot truncation hidden", err)
	}
}

func TestLogSnapshotsValidatePageAndCursorBeforeIO(t *testing.T) {
	reader, ref, count := logFixture(t, func() monitorResponse { t.Fatal("invalid page invoked helper"); return monitorResponse{} })
	for _, req := range []provider.PageRequest{{Limit: 0}, {Limit: 101}, {Limit: 1, Cursor: "!bad"}, {Limit: 1, Cursor: strings.Repeat("x", 513)}, {Limit: 1, Cursor: base64.RawURLEncoding.EncodeToString([]byte(`{}`))}} {
		if _, err := reader.ReadLogs(context.Background(), ref, req); !errors.Is(err, ErrLogCursor) {
			t.Fatal("invalid pagination accepted", err)
		}
	}
	if *count != 0 {
		t.Fatal("unexpected read")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := reader.ReadLogs(ctx, ref, provider.PageRequest{Limit: 1}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	m := monitorFixture(t)
	if _, err := NewLogReader(reader.executor, m); !errors.Is(err, ErrConfig) {
		t.Fatal("foreign monitor binding accepted")
	}
}
