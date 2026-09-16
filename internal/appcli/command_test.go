package appcli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/statefs"
)

func TestApplicationArgumentsAreExplicitAndClosed(t *testing.T) {
	common := []string{"--workspace", "app", "--token-file", "private-token"}
	valid := [][]string{
		{"object", "upload", "--file", "input"}, {"job", "validate", "--file", "job"},
		{"job", "submit", "--file", "job", "--idempotency-key", "stable-key"},
		{"job", "status", "--id", "job"}, {"operation", "status", "--id", "op"},
	}
	for _, action := range []string{"cancel", "retry", "reconcile", "collect"} {
		valid = append(valid, []string{"job", action, "--id", "job", "--attempt", "att", "--idempotency-key", "stable-key", "--reason", "explicit action"})
	}
	for _, args := range valid {
		if _, err := Parse(append(args, common...)); err != nil {
			t.Fatal(args, err)
		}
	}
	for _, args := range [][]string{
		{"job", "submit", "--file", "job"}, {"job", "retry", "--id", "job", "--attempt", "att", "--idempotency-key", "stable-key"},
		{"job", "cancel", "--id", "job", "--idempotency-key", "stable-key"},
		{"job", "status", "--id", "job", "--id", "other"}, {"job", "status", "--id", "job", "--reason", "unused"},
		{"job", "validate", "--file", "job", "--idempotency-key", "unused-key"},
		{"job", "status", "--id", "job", "--url", "http://user:secret@127.0.0.1:7331"},
		{"object", "upload", "--file", "input", "--root", "state"}, {"job", "delete", "--id", "job"},
	} {
		if _, err := Parse(append(args, common...)); err == nil {
			t.Fatal("accepted invalid arguments", args)
		}
	}
}
func syntheticFiles(t *testing.T) (string, string) {
	t.Helper()
	root, err := statefs.PrivateDir(filepath.Join(t.TempDir(), "private"), true)
	if err != nil {
		t.Fatal(err)
	}
	tokenFile := filepath.Join(root, "token")
	if statefs.WriteNew(tokenFile, []byte("cr1_"+strings.Repeat("A", 43)+"\n")) != nil {
		t.Fatal("synthetic token fixture")
	}
	return root, tokenFile
}
func TestApplicationHelpAndBadArgumentsDoNotReadFiles(t *testing.T) {
	var out, diagnostic bytes.Buffer
	if Run(context.Background(), []string{"job", "--help"}, &out, &diagnostic) != 0 || diagnostic.Len() != 0 {
		t.Fatal("help performed work")
	}
	out.Reset()
	if Run(context.Background(), []string{"job", "status", "--token", "SYNTHETIC_SECRET"}, &out, &diagnostic) != 2 || out.Len() != 0 || strings.Contains(diagnostic.String(), "SYNTHETIC_SECRET") {
		t.Fatal("invalid arguments leaked or executed")
	}
}
func TestApplicationRejectsMalformedTokenAndJobBeforeHTTP(t *testing.T) {
	root, tokenFile := syntheticFiles(t)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer server.Close()
	job := filepath.Join(root, "job.json")
	if err := os.WriteFile(job, []byte(`{"unknown":"SYNTHETIC_SECRET"}`), 0600); err != nil {
		t.Fatal(err)
	}
	var out, diagnostic bytes.Buffer
	args := []string{"job", "submit", "--workspace", "app", "--url", server.URL, "--token-file", tokenFile, "--file", job, "--idempotency-key", "original-key"}
	if Run(context.Background(), args, &out, &diagnostic) != 1 || out.Len() != 0 || calls.Load() != 0 {
		t.Fatal("malformed job reached API")
	}
	if err := os.WriteFile(tokenFile, []byte("SYNTHETIC_SECRET\n"), 0600); err != nil {
		t.Fatal(err)
	}
	diagnostic.Reset()
	args = []string{"job", "status", "--workspace", "app", "--url", server.URL, "--token-file", tokenFile, "--id", "job"}
	if Run(context.Background(), args, &out, &diagnostic) != 1 || calls.Load() != 0 || strings.Contains(diagnostic.String(), "SYNTHETIC_SECRET") || strings.Contains(diagnostic.String(), root) {
		t.Fatal("invalid token was used or reflected")
	}
}
func TestApplicationUploadChecksActualSourceIncludingEmptyFile(t *testing.T) {
	for _, data := range []string{"", strings.Repeat("data", 1<<18)} {
		root, tokenFile := syntheticFiles(t)
		input := filepath.Join(root, "input")
		if err := os.WriteFile(input, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256([]byte(data))
		digest := hex.EncodeToString(sum[:])
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			h := sha256.New()
			n, err := io.Copy(h, r.Body)
			if err != nil || n != int64(len(data)) || hex.EncodeToString(h.Sum(nil)) != digest || r.Header.Get("Authorization") != "Bearer cr1_"+strings.Repeat("A", 43) {
				t.Error("source or token changed")
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(201)
			_, _ = fmt.Fprintf(w, `{"object_id":"obj_original","workspace_id":"app","bytes":%d,"sha256":%q}`, len(data), digest)
		}))
		var out, diagnostic bytes.Buffer
		args := []string{"object", "upload", "--workspace", "app", "--url", server.URL, "--token-file", tokenFile, "--file", input}
		code := Run(context.Background(), args, &out, &diagnostic)
		server.Close()
		if code != 0 || calls.Load() != 1 || diagnostic.Len() != 0 || !strings.Contains(out.String(), "obj_original") {
			t.Fatal(code, diagnostic.String(), calls.Load())
		}
	}
}

type failedOutput struct{}

func (failedOutput) Write([]byte) (int, error) { return 0, errors.New("private output failure") }
func TestApplicationOutputFailureDoesNotRepeatMutation(t *testing.T) {
	root, tokenFile := syntheticFiles(t)
	input := filepath.Join(root, "empty")
	if err := os.WriteFile(input, nil, 0600); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(201)
		_, _ = io.WriteString(w, `{"object_id":"obj_original","workspace_id":"app","bytes":0,"sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}`)
	}))
	defer server.Close()
	var diagnostic bytes.Buffer
	args := []string{"object", "upload", "--workspace", "app", "--url", server.URL, "--token-file", tokenFile, "--file", input}
	if Run(context.Background(), args, failedOutput{}, &diagnostic) != 1 || calls.Load() != 1 || !strings.Contains(diagnostic.String(), `"request_may_have_committed":true`) {
		t.Fatal("output error replayed or concealed mutation", diagnostic.String())
	}
}
