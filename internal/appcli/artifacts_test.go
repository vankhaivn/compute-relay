package appcli

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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/artifactwire"
)

func TestArtifactArgumentsAndHelpAreInert(t *testing.T) {
	common := []string{"--workspace", "app", "--token-file", "missing", "--attempt", "att_one"}
	for _, command := range [][]string{
		{"job", "artifacts", "--id", "job_one", "--limit", "2"},
		{"artifact", "show", "--job", "job_one", "--id", "art_one"},
		{"artifact", "download", "--job", "job_one", "--id", "art_one", "--output", "new-file"},
	} {
		if _, err := parseArtifacts(append(command, common...)); err != nil {
			t.Fatal(command, err)
		}
	}
	for _, command := range [][]string{
		{"job", "artifacts", "--id", "job_one", "--limit", "01"},
		{"job", "artifacts", "--id", "job_one", "--limit", "101"},
		{"job", "artifacts", "--id", "job_one", "--cursor", "bad"},
		{"job", "artifacts", "--id", "job_one", "--idempotency-key", "not-a-mutation"},
		{"artifact", "show", "--job", "job_one", "--id", "art_one", "--output", "forbidden"},
		{"artifact", "download", "--job", "job_one", "--id", "art_one"},
		{"artifact", "download", "--job", "job_one", "--id", "art_one", "--output", "x", "--output", "y"},
	} {
		if _, err := parseArtifacts(append(command, common...)); err == nil {
			t.Fatal("accepted cross-command or ambiguous options")
		}
	}
	var out, diagnostic bytes.Buffer
	if RunArtifacts(context.Background(), []string{"artifact", "--help"}, &out, &diagnostic) != 0 || diagnostic.Len() != 0 {
		t.Fatal("help performed work")
	}
	out.Reset()
	if RunArtifacts(context.Background(), []string{"artifact", "download", "--token", "SECRET_CANARY"}, &out, &diagnostic) != 2 || out.Len() != 0 || strings.Contains(diagnostic.String(), "SECRET_CANARY") {
		t.Fatal("invalid arguments echoed secrets or executed")
	}
}

type artifactBrokenOutput struct{}

func (artifactBrokenOutput) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestArtifactCommandPublishesOnlyVerifiedNewFiles(t *testing.T) {
	for _, mode := range []string{"valid", "late", "existing", "race", "stdout"} {
		t.Run(mode, func(t *testing.T) {
			root, tokenFile := syntheticFiles(t)
			output := filepath.Join(root, "chosen-by-user")
			data := bytes.Repeat([]byte("original payload"), 10000)
			digest := sha256.Sum256(data)
			pin := artifactwire.Metadata{
				Target:      artifactwire.Target{WorkspaceID: "app", JobID: "job_one", AttemptID: "att_one"},
				ResultPhase: "completed", VerifiedAt: time.Now().UTC(),
				Artifact: artifactwire.File{ID: "art_one", Path: "outputs/not-the-destination", Role: "output", Bytes: int64(len(data)), SHA256: hex.EncodeToString(digest[:])},
			}
			if mode == "existing" {
				if err := os.WriteFile(output, []byte("preserve-existing"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			var calls atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != "GET" || r.URL.Query().Get("attempt_id") != pin.AttemptID {
					t.Error("download invoked a mutation or changed attempt")
				}
				if !strings.HasSuffix(r.URL.Path, "/content") {
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(pin)
					return
				}
				if mode == "race" {
					if err := os.WriteFile(output, []byte("preserve-existing"), 0600); err != nil {
						t.Error(err)
					}
				}
				for key, value := range map[string]string{
					"Content-Type": "application/octet-stream", "X-Artifact-ID": pin.Artifact.ID,
					"X-Workspace-ID": pin.WorkspaceID, "X-Job-ID": pin.JobID, "X-Attempt-ID": pin.AttemptID,
					"X-Content-Bytes": strconv.Itoa(len(data)), "X-Content-SHA256": pin.Artifact.SHA256,
					"Trailer": artifactwire.VerifiedTrailer,
				} {
					w.Header().Set(key, value)
				}
				_, _ = w.Write(data)
				if mode != "late" {
					w.Header().Set(artifactwire.VerifiedTrailer, "true")
				}
			}))
			defer server.Close()
			args := []string{"artifact", "download", "--url", server.URL, "--token-file", tokenFile, "--workspace", "app", "--job", "job_one", "--attempt", "att_one", "--id", "art_one", "--output", output}
			var out, diagnostic bytes.Buffer
			var writer io.Writer = &out
			if mode == "stdout" {
				writer = artifactBrokenOutput{}
			}
			code := RunArtifacts(context.Background(), args, writer, &diagnostic)
			if (code == 0) != (mode == "valid") {
				t.Fatal("incorrect exit status", mode, code, diagnostic.String())
			}
			wantCalls := int64(2)
			if mode == "existing" {
				wantCalls = 0
			}
			if calls.Load() != wantCalls {
				t.Fatal("automatic replay or existing destination contacted server", calls.Load())
			}
			actual, err := os.ReadFile(output)
			switch mode {
			case "late":
				if !errors.Is(err, os.ErrNotExist) {
					t.Fatal("unacknowledged bytes were published")
				}
			case "existing", "race":
				if err != nil || string(actual) != "preserve-existing" {
					t.Fatal("destination was overwritten", err)
				}
			default:
				if err != nil || !bytes.Equal(actual, data) {
					t.Fatal("verified bytes missing", err)
				}
			}
			if mode == "stdout" && !strings.Contains(diagnostic.String(), `"download_may_be_published":true`) {
				t.Fatal("stdout failure pretended to roll back file publication")
			}
			entries, err := filepath.Glob(filepath.Join(root, ".compute-relay-download-*"))
			if err != nil || len(entries) != 0 {
				t.Fatal("ordinary completion leaked temporary file")
			}
		})
	}
}

func TestArtifactLocalRehashRejectsFalseFetchAcknowledgement(t *testing.T) {
	root, _ := syntheticFiles(t)
	digest := sha256.Sum256([]byte("right"))
	pin := artifactwire.File{ID: "art_one", Path: "outputs/result", Role: "output", Bytes: 5, SHA256: hex.EncodeToString(digest[:])}
	output := filepath.Join(root, "new")
	if err := publishArtifact(context.Background(), output, pin, func(dst io.Writer) error {
		_, err := dst.Write([]byte("wrong"))
		return err
	}); err == nil {
		t.Fatal("local bytes were not independently verified")
	}
	if _, err := os.Lstat(output); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("invalid local bytes received a final name")
	}
}
