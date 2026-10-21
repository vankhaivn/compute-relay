package kaggleapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func logClient(t *testing.T, contentType, body string) *Client {
	t.Helper()
	return newFixtureClient(t, func(request *http.Request) (*http.Response, error) {
		if request.Method != "GET" || request.URL.String() != apiBase+"kernels/logs/stream/fixture_user/fixture-run" ||
			request.Header.Get("Authorization") != "Bearer "+fixtureToken || request.Header.Get("Content-Type") != "" {
			t.Fatal("wrong log request")
		}
		return response(200, contentType, body), nil
	}, nil)
}

func TestLogFixturesAndOrderedRedactedReplay(t *testing.T) {
	for _, format := range []string{"sse", "json"} {
		t.Run(format, func(t *testing.T) {
			data, err := os.ReadFile("testdata/logs." + format)
			requireOK(t, err)
			contentType := "application/json"
			if format == "sse" {
				contentType = "text/event-stream"
			}
			c := logClient(t, contentType, string(data))
			full, err := c.ReadLogs(context.Background(), fixtureKernel, KernelRunning, LogRequest{})
			requireOK(t, err)
			if string(full.Text) != "first\n[REDACTED]\nlast\n" || !full.Ended || full.Availability != "live" || full.Truncated {
				t.Fatal(full)
			}
			digest := sha256.Sum256(full.Text)
			if full.Prefix != hex.EncodeToString(digest[:]) || full.Offset != int64(len(full.Text)) {
				t.Fatal("wrong redacted prefix")
			}
			var ordered []byte
			page := LogRequest{LineLimit: 1}
			for i := 0; i < 3; i++ {
				result, err := c.ReadLogs(context.Background(), fixtureKernel, KernelComplete, page)
				requireOK(t, err)
				if result.Availability != "after_completion" {
					t.Fatal(result.Availability)
				}
				ordered = append(ordered, result.Text...)
				page.Offset, page.Prefix = result.Offset, result.Prefix
			}
			if string(ordered) != string(full.Text) {
				t.Fatalf("skipped/reordered replay: %q", ordered)
			}
			idle, err := c.ReadLogs(context.Background(), fixtureKernel, KernelComplete, page)
			requireOK(t, err)
			if len(idle.Text) != 0 || idle.Offset != page.Offset || idle.Prefix != page.Prefix || !idle.Ended {
				t.Fatal(idle)
			}
		})
	}
}

func TestLogReplayResetAndBoundaryChanges(t *testing.T) {
	good := `data: {"data":"one\ntwo\n"}` + "\n\n"
	c := logClient(t, "text/event-stream", good)
	first, err := c.ReadLogs(context.Background(), fixtureKernel, KernelRunning, LogRequest{LineLimit: 1})
	requireOK(t, err)
	for _, body := range []string{`data: {"data":"bad\ntwo\n"}` + "\n\n", ""} {
		changed := logClient(t, "text/event-stream", body)
		_, err := changed.ReadLogs(context.Background(), fixtureKernel, KernelRunning, LogRequest{Offset: first.Offset, Prefix: first.Prefix, LineLimit: 1})
		if !errors.Is(err, ErrLogChanged) {
			t.Fatal(err)
		}
	}
	digest := sha256.Sum256([]byte("o"))
	_, err = c.ReadLogs(context.Background(), fixtureKernel, KernelRunning, LogRequest{Offset: 1, Prefix: hex.EncodeToString(digest[:]), LineLimit: 1})
	if !errors.Is(err, ErrLogChanged) {
		t.Fatal("partial-line cursor accepted")
	}
	// A later page must have time to replay the verified prefix before tailing.
	slow := newFixtureClient(t, func(request *http.Request) (*http.Response, error) {
		r := response(200, "text/event-stream", good)
		r.Body = &delayedLogBody{ctx: request.Context(), body: r.Body, delay: 2100 * time.Millisecond}
		return r, nil
	}, nil)
	next, err := slow.ReadLogs(context.Background(), fixtureKernel, KernelRunning, LogRequest{Offset: first.Offset, Prefix: first.Prefix})
	requireOK(t, err)
	if string(next.Text) != "two\n" || !next.Ended {
		t.Fatal("slow prefix replay lost the next line", next)
	}
}

func TestLogPageByteCapDoesNotSkipNextLine(t *testing.T) {
	first, second := strings.Repeat("a", 40000)+"\n", strings.Repeat("b", 40000)+"\n"
	events, err := json.Marshal([]map[string]string{{"data": first}, {"data": second}})
	requireOK(t, err)
	c := logClient(t, "application/json", string(events))
	page, err := c.ReadLogs(context.Background(), fixtureKernel, KernelRunning, LogRequest{})
	requireOK(t, err)
	if string(page.Text) != first || !page.Truncated || page.Offset != int64(len(first)) {
		t.Fatal("wrong bounded page")
	}
	next, err := c.ReadLogs(context.Background(), fixtureKernel, KernelRunning, LogRequest{Offset: page.Offset, Prefix: page.Prefix})
	requireOK(t, err)
	if string(next.Text) != second {
		t.Fatal("next line skipped")
	}
}

type delayedLogBody struct {
	ctx   context.Context
	body  io.ReadCloser
	delay time.Duration
}

func (b *delayedLogBody) Read(p []byte) (int, error) {
	if b.delay > 0 {
		timer := time.NewTimer(b.delay)
		defer timer.Stop()
		b.delay = 0
		select {
		case <-timer.C:
		case <-b.ctx.Done():
			return 0, b.ctx.Err()
		}
	}
	return b.body.Read(p)
}
func (b *delayedLogBody) Close() error { return b.body.Close() }

type logReadError struct{ err error }

func (b logReadError) Read([]byte) (int, error) { return 0, b.err }
func (logReadError) Close() error               { return nil }

func TestIdleInterruptedLogsNeverClaimExhaustion(t *testing.T) {
	for _, state := range []KernelState{KernelRunning, KernelComplete} {
		c := newFixtureClient(t, func(*http.Request) (*http.Response, error) {
			r := response(200, "text/event-stream", "")
			r.Body = logReadError{errors.New(fixtureToken)}
			return r, nil
		}, nil)
		result, err := c.ReadLogs(context.Background(), fixtureKernel, state, LogRequest{})
		if !errors.Is(err, ErrTransport) || result.Ended || len(result.Text) != 0 {
			t.Fatal("unreadable stream was reported as quiet", result, err)
		}
		quiet := newFixtureClient(t, func(request *http.Request) (*http.Response, error) {
			r := response(200, "text/event-stream", "")
			r.Body = &delayedLogBody{ctx: request.Context(), body: r.Body, delay: 3 * time.Second}
			return r, nil
		}, nil)
		result, err = quiet.ReadLogs(context.Background(), fixtureKernel, state, LogRequest{})
		requireOK(t, err)
		want := "live"
		if state == KernelComplete {
			want = "delayed"
		}
		if result.Availability != want || result.Ended || len(result.Text) != 0 {
			t.Fatal("idle tail invented exhaustion", result)
		}
	}
	c := logClient(t, "text/event-stream", "data: END_OF_LOG\n\n")
	result, err := c.ReadLogs(context.Background(), fixtureKernel, KernelRunning, LogRequest{})
	requireOK(t, err)
	if !result.Ended || result.Availability != "live" {
		t.Fatal("sentinel manufactured completion")
	}
}

func TestMalformedOrUnboundedLogsFailWithoutRawDetails(t *testing.T) {
	for _, body := range []string{`data: {bad}` + "\n\n", `data: {}` + "\n\n", `foreign: payload` + "\n",
		`data: {"data":"x"}`, `data: {"data":null}` + "\n\n", `data: {"data":"\ud800"}` + "\n\n",
		`data: {"data":"one","data":"SYNTHETIC_TOKEN"}` + "\n\n", "data: " + strings.Repeat("a", maxLogEvent+1) + "\n"} {
		c := logClient(t, "text/event-stream", body)
		_, err := c.ReadLogs(context.Background(), fixtureKernel, KernelRunning, LogRequest{})
		if err == nil || strings.Contains(err.Error(), fixtureToken) {
			t.Fatal("invalid SSE succeeded or leaked")
		}
	}
	for _, body := range []string{`null`, `[{}]`, `[{"data":false}]`, `[{"data":"\ud800"}]`, `[1]`} {
		c := logClient(t, "application/json", body)
		if _, err := c.ReadLogs(context.Background(), fixtureKernel, KernelComplete, LogRequest{}); err == nil {
			t.Fatalf("invalid completed log accepted: %s", body)
		}
	}
	for _, status := range []int{401, 403, 404, 429, 302, 503} {
		c := newFixtureClient(t, func(*http.Request) (*http.Response, error) {
			r := response(status, "text/html", fixtureToken)
			r.Body = &unreadBody{t}
			return r, nil
		}, nil)
		c.state.pacing, _ = fakeCooldowns()
		_, err := c.ReadLogs(context.Background(), fixtureKernel, KernelRunning, LogRequest{})
		var failure *Error
		if !errors.As(err, &failure) || failure.StatusCode != status || strings.Contains(fmt.Sprint(err), fixtureToken) {
			t.Fatal(err)
		}
	}
}

func TestLogRedactionAcrossEventsAndSecretFormats(t *testing.T) {
	text := "authorization: bearer abc-secret\npassword=fixture-private\ncr1_" + strings.Repeat("a", 43) + "\n"
	body, err := json.Marshal([]map[string]string{{"data": text}})
	requireOK(t, err)
	c := logClient(t, "application/json", string(body))
	result, err := c.ReadLogs(context.Background(), fixtureKernel, KernelComplete, LogRequest{})
	requireOK(t, err)
	if string(result.Text) != "[REDACTED]\n[REDACTED]\n[REDACTED]\n" {
		t.Fatalf("unsafe redaction: %q", result.Text)
	}
}

func TestCompletedLogArrayCardinalityAndMalformedTail(t *testing.T) {
	tooMany := "[" + strings.TrimSuffix(strings.Repeat(`{"data":""},`, maxLogEvents+1), ",") + "]"
	c := logClient(t, "application/json", tooMany)
	if _, err := c.ReadLogs(context.Background(), fixtureKernel, KernelComplete, LogRequest{}); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	c = logClient(t, "application/json", `[{"data":"one\n"},{"data":null}]`)
	if _, err := c.ReadLogs(context.Background(), fixtureKernel, KernelComplete, LogRequest{LineLimit: 1}); !errors.Is(err, ErrResponse) {
		t.Fatal("malformed completed tail escaped validation")
	}
	c = newFixtureClient(t, func(*http.Request) (*http.Response, error) {
		r := response(200, "application/json", "")
		r.Header.Set("Content-Length", "33554433")
		r.Body = &unreadBody{t}
		return r, nil
	}, nil)
	if _, err := c.ReadLogs(context.Background(), fixtureKernel, KernelComplete, LogRequest{}); !errors.Is(err, ErrLimit) {
		t.Fatal("unbounded log body")
	}
}
