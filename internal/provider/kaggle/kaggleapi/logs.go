package kaggleapi

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"net/http"
	"regexp"
	"strings"
)

const (
	logOperation = "kernels.LogStream"
	MaxLogBytes  = 64 << 10
	maxLogInput  = 32 << 20
	maxLogEvent  = 64 << 10
	maxLogEvents = 100000
)

var ErrLogChanged = errors.New("Kaggle log replay changed")

type LogRequest struct {
	Offset    int64
	Prefix    string
	LineLimit int
}

// LogReplay's offset/prefix cover redacted complete lines, allowing ordered replay
// without skipping the first line that exceeds a page bound. Ended is stream EOF
// evidence only; Availability derives from the separately observed kernel state.
type LogReplay struct {
	Text         []byte
	Offset       int64
	Prefix       string
	Availability string
	Truncated    bool
	Ended        bool
}

// ReadLogs replays one bounded authenticated SSE/completed JSON stream. Caller
// must verify original kernel identity before and after releasing these bytes.
func (c *Client) ReadLogs(parent context.Context, resource Resource, state KernelState, page LogRequest) (LogReplay, error) {
	if page.LineLimit == 0 {
		page.LineLimit = 100
	}
	if c == nil || c.state == nil || !resource.valid() || page.Offset < 0 || page.Offset > maxLogInput ||
		page.LineLimit < 1 || page.LineLimit > 100 || page.Offset != 0 && page.Prefix == "" ||
		page.Prefix != "" && !validDigest(page.Prefix) {
		return LogReplay{}, failure(logOperation, ErrConfig, 0, false)
	}
	token := c.credential()
	defer clear(token)
	if len(token) == 0 {
		return LogReplay{}, failure(logOperation, ErrConfig, 0, false)
	}
	ctx, cancel := context.WithTimeout(parent, c.state.logs)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, apiBase+"kernels/logs/stream/"+resource.Owner+"/"+resource.Slug, nil)
	if err != nil {
		return LogReplay{}, failure(logOperation, ErrConfig, 0, false)
	}
	request.Header.Set("Authorization", "Bearer "+string(token))
	request.Header.Set("Accept", "text/event-stream, */*")
	request.Header.Set("Accept-Encoding", "identity")
	response, err := roundTrip(ctx, c.state.api, request, logOperation)
	if err != nil {
		return LogReplay{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return LogReplay{}, failure(logOperation, ErrHTTP, response.StatusCode, true)
	}
	if _, err := bodyLimit(response, maxLogInput); err != nil {
		return LogReplay{}, failure(logOperation, err, response.StatusCode, true)
	}
	reader := newLogDecoder(page, string(token))
	if contentType(response, "text/event-stream") {
		err = reader.sse(ctx, response.Body)
	} else if contentType(response, "application/json") {
		var raw []byte
		raw, err = readBounded(ctx, response, maxLogInput)
		if err == nil {
			err = reader.completed(raw)
		}
	} else {
		err = ErrResponse
	}
	if parent.Err() != nil {
		return LogReplay{}, failure(logOperation, parent.Err(), response.StatusCode, true)
	}
	if err != nil {
		return LogReplay{}, failure(logOperation, err, response.StatusCode, true)
	}
	if !reader.matched {
		if reader.ended {
			return LogReplay{}, failure(logOperation, ErrLogChanged, response.StatusCode, true)
		}
		return LogReplay{}, failure(logOperation, ErrTransport, response.StatusCode, true)
	}
	availability := "live"
	if state == KernelComplete || state == KernelError {
		availability = "after_completion"
		if !reader.ended && len(reader.output) == 0 {
			availability = "delayed"
		}
	}
	return LogReplay{Text: reader.output, Offset: reader.consumed, Prefix: hex.EncodeToString(reader.digest.Sum(nil)),
		Availability: availability, Truncated: reader.truncated, Ended: reader.ended}, nil
}

func validDigest(text string) bool {
	if len(text) != sha256.Size*2 {
		return false
	}
	for _, b := range []byte(text) {
		if b < '0' || b > '9' {
			if b < 'a' || b > 'f' {
				return false
			}
		}
	}
	return true
}

var logSecret = regexp.MustCompile(`(?i)cr1_[A-Za-z0-9_-]{43}|gh[pousr]_[A-Za-z0-9]{20,255}|(?:authorization[=: ]+bearer[ ]+|(?:api[_-]?key|token|password|secret)[=:][ ]*)[^\s"']{1,512}`)

type logDecoder struct {
	page      LogRequest
	token     string
	digest    hash.Hash
	consumed  int64
	matched   bool
	output    []byte
	pending   string
	events    int
	lines     int
	ended     bool
	truncated bool
}

func newLogDecoder(page LogRequest, token string) *logDecoder {
	digest := sha256.New()
	return &logDecoder{page: page, token: token, digest: digest, output: make([]byte, 0),
		matched: page.Offset == 0 && (page.Prefix == "" || page.Prefix == hex.EncodeToString(digest.Sum(nil)))}
}

func (d *logDecoder) accept(text string) (bool, error) {
	data := []byte(logSecret.ReplaceAllString(strings.ReplaceAll(text, d.token, "[REDACTED]"), "[REDACTED]"))
	if d.consumed >= d.page.Offset && len(d.output)+len(data) > MaxLogBytes || int64(len(data)) > maxLogInput-d.consumed {
		d.truncated = true
		return false, nil
	}
	_, _ = d.digest.Write(data)
	d.consumed += int64(len(data))
	if d.consumed <= d.page.Offset {
		if d.consumed == d.page.Offset {
			if hex.EncodeToString(d.digest.Sum(nil)) != d.page.Prefix {
				return false, ErrLogChanged
			}
			d.matched = true
		}
		return true, nil
	}
	if !d.matched {
		return false, ErrLogChanged
	}
	d.output = append(d.output, data...)
	d.lines += bytes.Count(data, []byte("\n"))
	return d.lines < d.page.LineLimit, nil
}

func (d *logDecoder) event(raw []byte) (bool, error) {
	if d.events >= maxLogEvents {
		d.truncated = true
		return false, nil
	}
	object, err := strictObject(raw)
	var text string
	if err != nil || len(object["data"]) == 0 || bytes.Equal(object["data"], []byte("null")) || json.Unmarshal(object["data"], &text) != nil {
		return false, ErrResponse
	}
	d.events++
	d.pending += strings.ReplaceAll(strings.ReplaceAll(text, "\r\n", "\n"), "\r", "\n")
	if len(d.pending) > maxLogEvent {
		return false, ErrResponse
	}
	for {
		line, pending, found := strings.Cut(d.pending, "\n")
		if !found {
			return true, nil
		}
		d.pending = pending
		more, err := d.accept(line + "\n")
		if !more || err != nil {
			return more, err
		}
	}
}

func (d *logDecoder) finish() error {
	d.ended = true
	if d.pending != "" {
		_, err := d.accept(d.pending + "\n")
		d.pending = ""
		return err
	}
	return nil
}

func (d *logDecoder) completed(raw []byte) error {
	if strictJSON(raw) != nil {
		return ErrResponse
	}
	raw = bytes.TrimSpace(raw)
	if raw[0] != '[' {
		more, err := d.event(raw)
		if !more || err != nil {
			return err
		}
		return d.finish()
	}
	// Enforce cardinality while decoding each event, before allocating a slice
	// for every value in the provider's bounded but potentially dense JSON array.
	decoder := json.NewDecoder(bytes.NewReader(raw))
	_, _ = decoder.Token() // strictJSON has already verified the opening array.
	stopped := false
	count := 0
	for decoder.More() {
		if count >= maxLogEvents {
			return ErrLimit
		}
		var event json.RawMessage
		if decoder.Decode(&event) != nil {
			return ErrResponse
		}
		count++
		if stopped {
			// The completed array's event shapes remain checked after the page
			// boundary, without returning or hashing any subsequent log bytes.
			object, err := strictObject(event)
			data := object["data"]
			if err != nil || len(data) == 0 || data[0] != '"' {
				return ErrResponse
			}
			continue
		}
		more, err := d.event(event)
		if err != nil {
			return err
		}
		stopped = !more
	}
	if stopped {
		return nil
	}
	return d.finish()
}

func (d *logDecoder) sse(ctx context.Context, body io.Reader) error {
	limited := &io.LimitedReader{R: body, N: maxLogInput}
	reader := bufio.NewReaderSize(limited, maxLogEvent+1)
	for {
		if ctx.Err() != nil {
			return nil
		}
		line, readErr := reader.ReadSlice('\n')
		if readErr != nil {
			if readErr == bufio.ErrBufferFull || len(line) > 0 && readErr == io.EOF {
				return ErrResponse
			}
			if limited.N == 0 {
				d.truncated = true
				return nil
			}
			if readErr == io.EOF && ctx.Err() == nil {
				return d.finish()
			}
			// An idle/interrupted live stream can yield its already verified complete
			// lines, but cannot manufacture an EOF or flush an unfinished line.
			return nil
		}
		if len(line) > maxLogEvent {
			return ErrResponse
		}
		line = bytes.TrimSuffix(bytes.TrimSuffix(line, []byte("\n")), []byte("\r"))
		if len(line) == 0 || line[0] == ':' || bytes.HasPrefix(line, []byte("event:")) || bytes.HasPrefix(line, []byte("id:")) || bytes.HasPrefix(line, []byte("retry:")) {
			continue
		}
		if !bytes.HasPrefix(line, []byte("data:")) {
			return ErrResponse
		}
		payload := bytes.TrimLeft(line[5:], " \t")
		if bytes.Equal(payload, []byte("END_OF_LOG")) {
			return d.finish()
		}
		more, err := d.event(payload)
		if !more || err != nil {
			return err
		}
	}
}
