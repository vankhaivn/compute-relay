package kaggle

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"unicode/utf8"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

var (
	ErrLogCursor  = provider.ErrLogCursorInvalid
	ErrLogChanged = provider.ErrLogCursorReset
)

// LogReader is bound to the original attempt and shares an instance's serialized
// monitor. It returns bounded provider replay pages, never runtime/payload artifact fallback.
type LogReader struct {
	executor *Executor
	monitor  *Monitor
}

func NewLogReader(e *Executor, m *Monitor) (*LogReader, error) {
	if e == nil || m == nil || e.stager.config != m.config {
		return nil, ErrConfig
	}
	return &LogReader{executor: e, monitor: m}, nil
}

type logCursor struct {
	Reference domain.SHA256Digest `json:"reference"`
	Prefix    domain.SHA256Digest `json:"prefix"`
	Offset    int                 `json:"offset"`
}

func (l *LogReader) Logs(ctx context.Context, ref provider.RemoteReference, page provider.PageRequest) (logPage provider.LogPage, retErr error) {
	defer func() {
		if retErr != nil {
			retErr = provider.WithReason(retErr, "logs_response_invalid")
		}
	}()
	var none provider.LogPage
	if l == nil || page.Validate() != nil {
		return none, provider.WithReason(ErrLogCursor, "logs_cursor_invalid")
	}
	if err := ctx.Err(); err != nil {
		return none, contextStep(err, "logs")
	}
	if err := l.executor.checkOperationalReference(ref); err != nil {
		return none, err
	}
	identity, _ := json.Marshal(ref)
	expected := provider.Digest(identity)
	var cursor logCursor
	if page.Cursor != "" {
		raw, err := base64.RawURLEncoding.Strict().DecodeString(page.Cursor)
		if err != nil || closedObject(raw, &cursor) != nil || cursor.Reference != expected || !cursor.Prefix.Valid() || cursor.Offset < 0 || cursor.Offset > 32<<20 || base64.RawURLEncoding.EncodeToString(raw) != page.Cursor {
			return none, provider.WithReason(ErrLogCursor, "logs_cursor_invalid")
		}
	}
	request := l.executor.request
	request.Source = ""
	var recorded kernelReference
	if closedObject([]byte(ref.Resource), &recorded) != nil {
		return none, provider.WithReason(ErrExecutionIdentity, "logs_identity_mismatch")
	}
	request.KernelID = recorded.KernelID
	r, err := l.monitor.callRequest(ctx, "logs", monitorRequest{Protocol: 1, Owner: l.monitor.config.AccountName, Execution: &request, LogOffset: cursor.Offset, LogPrefix: string(cursor.Prefix), LogLimit: page.Limit})
	if err != nil {
		return none, err
	}
	if r.Status == "reset" {
		return none, provider.WithReason(ErrLogChanged, "logs_cursor_changed")
	}
	if r.Status == "invalid" {
		return none, provider.WithReason(ErrExecutionIdentity, "logs_identity_mismatch")
	}
	if r.Status == "unavailable" {
		return none, provider.WithReason(provider.LogReadFailure{Reason: r.Reason}, r.Reason)
	}
	raw, err := base64.StdEncoding.Strict().DecodeString(r.TextB64)
	if err != nil {
		return none, ErrProtocol
	}
	if r.Replay && r.Offset < cursor.Offset {
		return none, ErrProtocol
	}
	digest := provider.Digest(raw)
	if !r.Replay && page.Cursor != "" && (cursor.Offset > len(raw) || cursor.Prefix != provider.Digest(raw[:cursor.Offset])) {
		return none, provider.WithReason(ErrLogChanged, "logs_cursor_changed")
	}
	lines := []string{}
	if len(raw) > 0 {
		lines = strings.Split(string(raw), "\n")
		if lines[len(lines)-1] == "" {
			lines = lines[:len(lines)-1]
		}
	}
	start := 0
	if !r.Replay {
		if cursor.Offset > len(raw) {
			return none, provider.WithReason(ErrLogChanged, "logs_cursor_changed")
		}
		start = len(strings.Split(string(raw[:cursor.Offset]), "\n")) - 1
	}
	if start > len(lines) {
		return none, provider.WithReason(ErrLogCursor, "logs_cursor_invalid")
	}
	end := min(start+page.Limit, len(lines))
	result := provider.LogPage{Source: "provider", Availability: r.Availability, Lines: append([]string{}, lines[start:end]...), Truncated: r.Truncated}
	for i, line := range result.Lines {
		if len(line) > 16<<10 {
			n := 16 << 10
			for !utf8.ValidString(line[:n]) {
				n--
			}
			result.Lines[i] = line[:n]
			result.Truncated = true
		}
	}
	if r.Replay && (len(raw) > 0 || r.Availability != "after_completion") || end < len(lines) || r.Availability == "live" || r.Availability == "delayed" {
		offset := 0
		for _, line := range lines[:end] {
			offset += len(line) + 1
		}
		offset = min(offset, len(raw))
		digest = provider.Digest(raw[:offset])
		if r.Replay {
			offset, digest = r.Offset, domain.SHA256Digest(r.Prefix)
		}
		raw, _ := json.Marshal(logCursor{Reference: expected, Prefix: digest, Offset: offset})
		result.NextCursor = base64.RawURLEncoding.EncodeToString(raw)
	}
	if err := result.Validate(page); err != nil {
		return none, ErrProtocol
	}
	return result, nil
}
