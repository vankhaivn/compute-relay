package kaggle

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

var (
	ErrLogCursor  = errors.New("invalid or foreign log snapshot cursor")
	ErrLogChanged = errors.New("provider log snapshot changed; restart with an empty cursor")
)

// LogReader is bound to the original attempt and shares an instance's serialized
// monitor. It returns provider snapshots, never runtime/payload artifact fallback.
type LogReader struct {
	executor *Executor
	monitor  *Monitor
}

var _ provider.LogReader = (*LogReader)(nil)

func NewLogReader(e *Executor, m *Monitor) (*LogReader, error) {
	if e == nil || m == nil || e.stager.config != m.config {
		return nil, ErrConfig
	}
	return &LogReader{executor: e, monitor: m}, nil
}

type logCursor struct {
	Reference domain.SHA256Digest `json:"reference"`
	Snapshot  domain.SHA256Digest `json:"snapshot"`
	Offset    int                 `json:"offset"`
}

func (l *LogReader) ReadLogs(ctx context.Context, ref provider.RemoteReference, page provider.PageRequest) (provider.LogPage, error) {
	var none provider.LogPage
	if l == nil || page.Validate() != nil {
		return none, ErrLogCursor
	}
	if err := ctx.Err(); err != nil {
		return none, err
	}
	if err := l.executor.checkOperationalReference(ref); err != nil {
		return none, err
	}
	identity, _ := json.Marshal(ref)
	expected := provider.Digest(identity)
	var cursor logCursor
	if page.Cursor != "" {
		raw, err := base64.RawURLEncoding.Strict().DecodeString(page.Cursor)
		if err != nil || closedObject(raw, &cursor) != nil || cursor.Reference != expected || !cursor.Snapshot.Valid() || cursor.Offset < 0 || cursor.Offset > maxLogSnapshot || base64.RawURLEncoding.EncodeToString(raw) != page.Cursor {
			return none, ErrLogCursor
		}
	}
	request := l.executor.request
	request.Source = ""
	var recorded kernelReference
	if closedObject([]byte(ref.Resource), &recorded) != nil {
		return none, ErrExecutionIdentity
	}
	request.KernelID = recorded.KernelID
	r, err := l.monitor.call(ctx, "logs", &request)
	if err != nil {
		return none, err
	}
	if r.Status == "invalid" {
		return none, ErrExecutionIdentity
	}
	if r.Status == "unavailable" {
		return provider.LogPage{Source: "provider", Availability: "unavailable", Lines: []string{}}, nil
	}
	raw, err := base64.StdEncoding.Strict().DecodeString(r.TextB64)
	if err != nil {
		return none, ErrProtocol
	}
	digest := provider.Digest(raw)
	if page.Cursor != "" && cursor.Snapshot != digest {
		return none, ErrLogChanged
	}
	lines := []string{}
	if len(raw) > 0 {
		lines = strings.Split(string(raw), "\n")
		if lines[len(lines)-1] == "" {
			lines = lines[:len(lines)-1]
		}
	}
	if cursor.Offset > len(lines) {
		return none, ErrLogCursor
	}
	end := min(cursor.Offset+page.Limit, len(lines))
	result := provider.LogPage{Source: "provider", Availability: r.Availability, Lines: append([]string{}, lines[cursor.Offset:end]...), Truncated: r.Truncated}
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
	if end < len(lines) {
		raw, _ := json.Marshal(logCursor{Reference: expected, Snapshot: digest, Offset: end})
		result.NextCursor = base64.RawURLEncoding.EncodeToString(raw)
	}
	if err := result.Validate(page); err != nil {
		return none, ErrProtocol
	}
	return result, nil
}
