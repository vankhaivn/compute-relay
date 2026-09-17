package appclient

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/artifactwire"
)

func downloadPin(data []byte) artifactwire.Metadata {
	h := sha256.Sum256(data)
	return artifactwire.Metadata{
		Target: artifactwire.Target{WorkspaceID: "app", JobID: "job_one", AttemptID: "att_one"},
		ResultPhase: "completed", VerifiedAt: time.Date(2026, 9, 17, 0, 0, 0, 0, time.UTC),
		Artifact: artifactwire.File{ID: "art_one", Path: "outputs/answer", Role: "output", Bytes: int64(len(data)), SHA256: hex.EncodeToString(h[:])},
	}
}
func downloadHeaders(w http.ResponseWriter, pin artifactwire.Metadata) {
	for key, value := range map[string]string{
		"Content-Type": "application/octet-stream", "X-Artifact-ID": pin.Artifact.ID,
		"X-Workspace-ID": pin.WorkspaceID, "X-Job-ID": pin.JobID, "X-Attempt-ID": pin.AttemptID,
		"X-Content-Bytes": strconv.FormatInt(pin.Artifact.Bytes, 10), "X-Content-SHA256": pin.Artifact.SHA256,
		"Trailer": artifactwire.VerifiedTrailer,
	} {
		w.Header().Set(key, value)
	}
}
func TestArtifactDownloadRequiresFinalAcknowledgementAndExactPin(t *testing.T) {
	data := bytes.Repeat([]byte("exact bytes"), 10000)
	pin := downloadPin(data)
	token := []byte("cr1_" + strings.Repeat("A", 43))
	for _, mode := range []string{"valid", "missing-trailer", "false-trailer", "duplicate-trailer", "early-header", "wrong-attempt", "wrong-hash", "short", "long", "late-abort", "content-length", "encoding"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != "GET" || r.URL.Path != "/v1/workspaces/app/jobs/job_one/artifacts/art_one/content" || r.URL.Query().Get("attempt_id") != "att_one" || r.Header.Get("Authorization") != "Bearer "+string(token) || r.Header.Get("Accept-Encoding") != "identity" {
					t.Error("request changed target or credential framing")
				}
				downloadHeaders(w, pin)
				switch mode {
				case "early-header":
					w.Header().Set(artifactwire.VerifiedTrailer, "true")
				case "wrong-attempt":
					w.Header().Set("X-Attempt-ID", "att_two")
				case "wrong-hash":
					w.Header().Set("X-Content-SHA256", strings.Repeat("f", 64))
				case "content-length":
					w.Header().Del("Trailer")
					w.Header().Set("Content-Length", strconv.Itoa(len(data)))
				case "encoding":
					w.Header().Set("Content-Encoding", "gzip")
				}
				payload := data
				if mode == "short" {
					payload = data[:len(data)/2]
				}
				_, _ = w.Write(payload)
				if mode == "long" {
					_, _ = w.Write([]byte("x"))
				}
				if mode == "late-abort" {
					w.(http.Flusher).Flush()
					panic(http.ErrAbortHandler)
				}
				if mode != "missing-trailer" {
					w.Header().Set(artifactwire.VerifiedTrailer, "true")
				}
				if mode == "false-trailer" {
					w.Header().Set(artifactwire.VerifiedTrailer, "false")
				}
				if mode == "duplicate-trailer" {
					w.Header().Add(artifactwire.VerifiedTrailer, "true")
				}
			}))
			defer server.Close()
			var dst bytes.Buffer
			err := DownloadArtifact(context.Background(), server.URL, token, pin, &dst)
			if (err == nil) != (mode == "valid") || calls.Load() != 1 || int64(dst.Len()) > pin.Artifact.Bytes {
				t.Fatal("invalid transfer result or automatic retry", mode, err, calls.Load())
			}
			if err != nil {
				var failure *Failure
				if !errors.As(err, &failure) || failure.RequestMayHaveCommitted {
					t.Fatal("read failure claimed a mutation", err)
				}
			}
		})
	}
}

func TestArtifactClientRefusesRedirectAndPreservesExpiryStatus(t *testing.T) {
	token := []byte("cr1_" + strings.Repeat("A", 43))
	pin := downloadPin([]byte("x"))
	var forwarded atomic.Int64
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { forwarded.Add(1) }))
	defer other.Close()
	for _, status := range []int{302, 410, 503} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if status == 302 {
				w.Header().Set("Location", other.URL)
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = io.WriteString(w, `{"error":{"code":"ARTIFACT_MISSING","message":"private provider response"}}`)
		}))
		_, err := ArtifactMetadata(context.Background(), server.URL, token, pin.Target, pin.Artifact.ID)
		server.Close()
		var failure *Failure
		if !errors.As(err, &failure) || failure.HTTPStatus != status || failure.RequestMayHaveCommitted || strings.Contains(err.Error(), "private") || forwarded.Load() != 0 {
			t.Fatal("read error lost status or leaked/followed response", err)
		}
	}
}

func TestArtifactClientDecodesOnlyExactMetadataAndExplicitPages(t *testing.T) {
	token := []byte("cr1_" + strings.Repeat("A", 43))
	pin := downloadPin(nil)
	for _, mode := range []string{"valid", "wrong-target", "null-bytes", "token", "bad-page"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				raw, _ := json.Marshal(pin)
				switch mode {
				case "wrong-target":
					raw = bytes.Replace(raw, []byte("att_one"), []byte("att_two"), 1)
				case "null-bytes":
					raw = bytes.Replace(raw, []byte(`"bytes":0`), []byte(`"bytes":null`), 1)
				case "token":
					raw = bytes.Replace(raw, []byte(`"result_phase":"completed"`), []byte(`"result_phase":"`+string(token)+`"`), 1)
				}
				_, _ = w.Write(raw)
			}))
			defer server.Close()
			if mode == "bad-page" {
				if _, err := ListArtifacts(context.Background(), server.URL, token, pin.Target, "", 100); err == nil {
					t.Fatal("metadata became a complete page")
				}
				return
			}
			_, err := ArtifactMetadata(context.Background(), server.URL, token, pin.Target, pin.Artifact.ID)
			if (err == nil) != (mode == "valid") {
				t.Fatal("invalid metadata qualified", mode, err)
			}
		})
	}
}
