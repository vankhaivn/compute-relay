package localinput

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/packaging"
)

func fixture(t *testing.T) (*Manager, string) {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("immutable input"), 0o600); err != nil {
		t.Fatal(err)
	}
	m, err := New([]RootSpec{{Name: "project", Path: root, Workspaces: []string{"a"}}}, packaging.DefaultLimits(), 2<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m, root
}
func TestRawFileAndBundleStreams(t *testing.T) {
	m, _ := fixture(t)
	for _, request := range []Request{{Root: "project", Kind: "file", Path: "file.txt"}, {Root: "project", Kind: "bundle", Includes: []string{"file.txt"}}} {
		stream, err := m.Open(context.Background(), "a", request)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(stream)
		_ = stream.Close()
		if err != nil {
			t.Fatal(err)
		}
		if request.Kind == "file" {
			if string(data) != "immutable input" || stream.Bytes != int64(len(data)) || len(stream.SHA256) != 64 {
				t.Fatal("invalid raw snapshot")
			}
		} else {
			if _, err := packaging.Inspect(context.Background(), bytes.NewReader(data), packaging.DefaultLimits()); err != nil {
				t.Fatal(err)
			}
		}
	}
}
func TestDisabledUnknownAndForeignRoots(t *testing.T) {
	m, _ := fixture(t)
	for _, tc := range []struct{ root, workspace string }{{"missing", "a"}, {"project", "b"}, {"/etc", "a"}} {
		if s, err := m.Open(context.Background(), tc.workspace, Request{Root: tc.root, Kind: "file", Path: "file.txt"}); !errors.Is(err, ErrForbidden) {
			if s != nil {
				s.Close()
			}
			t.Fatal(err)
		}
	}
	disabled, err := New(nil, packaging.DefaultLimits(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer disabled.Close()
	if _, err := disabled.Open(context.Background(), "a", Request{Root: "project", Kind: "file", Path: "file.txt"}); !errors.Is(err, ErrForbidden) {
		t.Fatal("disabled import allowed", err)
	}
}
func TestInvalidAndChangedSourcesNeverReachSuccessfulEOF(t *testing.T) {
	m, root := fixture(t)
	for _, r := range []Request{{Root: "project", Kind: "file", Path: "../x"}, {Root: "project", Kind: "file", Path: "/etc/passwd"}, {Root: "project", Kind: "bundle", Includes: []string{"."}}, {Root: "project", Kind: "file", Path: "file.txt", Includes: []string{"file.txt"}}, {Root: "project", Kind: "unknown"}, {Root: "project", Kind: "file", Path: "file.txt", SHA256: strings.Repeat("0", 64)}} {
		s, err := m.Open(context.Background(), "a", r)
		if s != nil {
			s.Close()
		}
		if err == nil {
			t.Fatalf("unsafe source accepted: %+v", r)
		}
	}
	// io.Pipe blocks the producer before final verification; replacing the file between
	// prepare and EOF must be surfaced to the consumer, not silently published as success.
	large := strings.Repeat("a", 300000)
	if err := os.WriteFile(filepath.Join(root, "large"), []byte(large), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := m.Open(context.Background(), "a", Request{Root: "project", Kind: "file", Path: "large"})
	if err != nil {
		t.Fatal(err)
	}
	first := make([]byte, 1024)
	if _, err := io.ReadFull(s, first); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "large"), []byte(strings.Repeat("b", len(large))), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = io.Copy(io.Discard, s)
	s.Close()
	if !errors.Is(err, packaging.ErrChanged) {
		t.Fatalf("inconsistent snapshot: %v", err)
	}
}
func TestConsumerFailureAndCancellationReleaseProducer(t *testing.T) {
	m, _ := fixture(t)
	s, err := m.Open(context.Background(), "a", Request{Root: "project", Kind: "bundle", Includes: []string{"file.txt"}})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = s.Close(); _ = s.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("blocked producer leaked after consumer close")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.Open(ctx, "a", Request{Root: "project", Kind: "file", Path: "file.txt"}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Open(context.Background(), "a", Request{Root: "project"}); !errors.Is(err, ErrForbidden) {
		t.Fatal("closed manager allowed read")
	}
}
func TestConfigurationAndRawFileLimits(t *testing.T) {
	root := t.TempDir()
	cases := [][]RootSpec{{{Name: "x", Path: root}}, {{Name: "bad/name", Path: root, Workspaces: []string{"a"}}}, {{Name: "x", Path: root, Workspaces: []string{"a", "a"}}}, {{Name: "x", Path: "relative", Workspaces: []string{"a"}}}, {{Name: "x", Path: root, Workspaces: []string{"a"}}, {Name: "x", Path: root, Workspaces: []string{"b"}}}}
	for _, spec := range cases {
		m, err := New(spec, packaging.DefaultLimits(), 1024)
		if m != nil {
			m.Close()
		}
		if err == nil {
			t.Fatal("unsafe configuration accepted")
		}
	}
	m, _ := fixture(t)
	m.maxFile = 1
	if _, err := m.Open(context.Background(), "a", Request{Root: "project", Kind: "file", Path: "file.txt"}); !errors.Is(err, packaging.ErrLimit) {
		t.Fatal("raw limit ignored", err)
	}
}

func TestRequestJSONRejectsAmbiguousFields(t *testing.T) {
	for _, raw := range []string{`null`, `[]`, `{"root":null}`, `{"root":"a","root":"b"}`, `{"Root":"a"}`, `{"unexpected":true}`, `{"includes":null}`, `{"root":5}`, `{"includes":[4]}`, `{} {}`} {
		var request Request
		if err := json.Unmarshal([]byte(raw), &request); err == nil {
			t.Fatalf("ambiguous request accepted: %s", raw)
		}
	}
	var request Request
	if err := json.Unmarshal([]byte(`{"root":"project","kind":"file","path":"file.txt"}`), &request); err != nil || request.Path != "file.txt" {
		t.Fatal(err)
	}
}
func TestBundleExpectedDigestMismatchFailsBeforeEOF(t *testing.T) {
	m, _ := fixture(t)
	s, err := m.Open(context.Background(), "a", Request{Root: "project", Kind: "bundle", Includes: []string{"file.txt"}, SHA256: strings.Repeat("0", 64)})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := io.Copy(io.Discard, s); !errors.Is(err, ErrDigest) {
		t.Fatalf("expected digest mismatch: %v", err)
	}
}
