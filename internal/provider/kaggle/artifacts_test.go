package kaggle

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func artifactFixture(t *testing.T) (*ArtifactReader, provider.RemoteReference, map[string][]byte) {
	t.Helper()
	e := newExecutionFixture(t)
	a, err := NewArtifactReader(e, DefaultArtifactPolicy())
	if err != nil {
		t.Fatal(err)
	}
	ref, err := e.remote(executionFound(e.request, "COMPLETE"))
	if err != nil {
		t.Fatal(err)
	}
	id := ref.Identity
	raw, _ := json.Marshal(map[string]any{"manifest_version": "1", "job_id": id.JobID, "attempt_id": id.AttemptID, "attempt_nonce": id.Nonce, "runner_version": "fixture", "bundle_sha256": id.BundleSHA256, "input_manifest_sha256": id.InputManifestSHA256, "started_at": "2026-10-14T00:00:00Z", "finished_at": "2026-10-14T00:00:01Z", "phase": "completed", "exit_code": 0, "timed_out": false, "resource_check": map[string]any{"gpu_required": false, "gpu_verified": false}, "artifacts": []any{}, "error": nil})
	return a, ref, map[string][]byte{artifactManifest: raw, "control/stdout.log": []byte("log\n"), "outputs/answer.txt": []byte("yes")}
}
func artifactCatalog(data map[string][]byte) []byte {
	files := make([]artifactEntry, 0, len(data))
	for path, raw := range data {
		files = append(files, artifactEntry{path, int64(len(raw)), provider.Digest(raw)})
	}
	raw, _ := json.Marshal(struct {
		Protocol int             `json:"protocol"`
		Files    []artifactEntry `json:"files"`
	}{1, files})
	return raw
}
func artifactFor(ref provider.RemoteReference, path string, raw []byte) provider.Artifact {
	return provider.Artifact{Remote: ref, Path: path, Bytes: int64(len(raw)), SHA256: provider.Digest(raw)}
}

func TestArtifactCatalogReturnsCompleteIndependentLists(t *testing.T) {
	a, ref, data := artifactFixture(t)
	reads := 0
	a.run = func(_ context.Context, c Config, mode string, token []byte, r artifactRequest, dst io.Writer) ([]byte, error) {
		reads++
		if c != a.executor.stager.config || mode != "catalog" || r.Execution.Source != "" || r.Execution.KernelID != "42" || r.Identity.Nonce != ref.Identity.Nonce || r.Target != nil || dst != nil || string(token) != "SYNTHETIC_TOKEN" {
			t.Fatal("unbound catalog invocation")
		}
		return artifactCatalog(data), nil
	}
	catalog, err := a.ListArtifacts(context.Background(), ref)
	if err != nil || len(catalog) != len(data) || reads != 1 {
		t.Fatal("partial catalog or hidden pagination", len(catalog), reads, err)
	}
	for _, file := range catalog {
		if file != artifactFor(ref, file.Path, data[file.Path]) {
			t.Fatal("catalog altered file identity")
		}
	}
	catalog[0].Path = "caller change"
	data["control/stdout.log"] = []byte("changed log")
	fresh, err := a.ListArtifacts(context.Background(), ref)
	if err != nil || len(fresh) != len(data) || reads != 2 {
		t.Fatal("fresh catalog not read", err)
	}
	for _, file := range fresh {
		if file != artifactFor(ref, file.Path, data[file.Path]) {
			t.Fatal("catalog retained stale/caller-owned metadata")
		}
	}
}

func TestArtifactReferenceAndUnselectedFilesFailBeforeCredentialIO(t *testing.T) {
	a, ref, data := artifactFixture(t)
	a.executor.stager.credentials = stagingCredentialFunc(func(context.Context, ports.CredentialRef, func([]byte) error) error {
		t.Fatal("invalid authority caused credential/provider work")
		return nil
	})
	foreign := ref
	foreign.Identity.WorkspaceID = "foreign"
	if _, err := a.ListArtifacts(context.Background(), foreign); err == nil {
		t.Fatal("foreign catalog accepted")
	}
	for _, path := range []string{"code/main.py", "inputs/input.bin", "scratch/private", "outputs/undeclared", "outputs/../secret"} {
		file := artifactFor(ref, path, []byte("x"))
		if _, err := a.Fetch(context.Background(), ref, file, io.Discard, file.Bytes, nil); err == nil {
			t.Fatal("unselected bytes accepted", path)
		}
	}
	file := artifactFor(ref, "outputs/answer.txt", data["outputs/answer.txt"])
	if _, err := a.Fetch(context.Background(), ref, file, io.Discard, file.Bytes-1, nil); err == nil {
		t.Fatal("destination limit ignored")
	}
}

func TestArtifactCatalogRejectsMalformedConflictingAndOversizedEvidence(t *testing.T) {
	for _, fault := range []string{"missing", "unselected", "duplicate", "control-limit", "total", "file-count", "unknown-field", "null", "duplicate-key", "case-key", "trailing"} {
		t.Run(fault, func(t *testing.T) {
			a, ref, data := artifactFixture(t)
			switch fault {
			case "missing":
				delete(data, artifactManifest)
			case "unselected":
				data["scratch/private"] = []byte("x")
			case "total":
				a.policy.MaxBytes = 1
			case "file-count":
				a.policy.MaxFiles = 2
			}
			raw := artifactCatalog(data)
			switch fault {
			case "duplicate":
				var v map[string]any
				_ = json.Unmarshal(raw, &v)
				f := v["files"].([]any)
				v["files"] = append(f, f[0])
				raw, _ = json.Marshal(v)
			case "control-limit":
				raw = bytes.Replace(raw, []byte(`"bytes":4`), []byte(`"bytes":20971521`), 1)
			case "unknown-field":
				raw = bytes.Replace(raw, []byte(`"protocol":1`), []byte(`"protocol":1,"secret":"x"`), 1)
			case "null":
				raw = bytes.Replace(raw, []byte(`"bytes":3`), []byte(`"bytes":null`), 1)
			case "duplicate-key":
				raw = bytes.Replace(raw, []byte(`"protocol":1`), []byte(`"protocol":1,"protocol":1`), 1)
			case "case-key":
				raw = bytes.Replace(raw, []byte(`"path":`), []byte(`"Path":`), 1)
			case "trailing":
				raw = append(raw, []byte(` {}`)...)
			}
			a.run = func(context.Context, Config, string, []byte, artifactRequest, io.Writer) ([]byte, error) {
				return raw, nil
			}
			if page, err := a.ListArtifacts(context.Background(), ref); err == nil || len(page) != 0 {
				t.Fatal("invalid catalog exposed", fault, err)
			}
		})
	}
}

func TestArtifactTransferIndependentlyChecksBytesAndLateFailure(t *testing.T) {
	for _, fault := range []string{"none", "short", "overflow", "wrong", "late", "panic", "report"} {
		t.Run(fault, func(t *testing.T) {
			a, ref, data := artifactFixture(t)
			payload := data["outputs/answer.txt"]
			file := artifactFor(ref, "outputs/answer.txt", payload)
			a.run = func(_ context.Context, _ Config, mode string, _ []byte, r artifactRequest, dst io.Writer) ([]byte, error) {
				if mode != "fetch" || r.Target == nil || r.Execution.Source != "" || r.Target.SHA256 != file.SHA256 {
					t.Fatal("transfer replaced original pin")
				}
				switch fault {
				case "short":
					payload = payload[:2]
				case "overflow":
					payload = append(payload, 'x')
				case "wrong":
					payload = []byte("bad")
				}
				if _, err := dst.Write(payload); err != nil {
					return nil, err
				}
				switch fault {
				case "late":
					return nil, errors.New("SYNTHETIC_TOKEN")
				case "panic":
					panic("SYNTHETIC_TOKEN")
				case "report":
					return []byte("extra acknowledgement"), nil
				}
				return nil, nil
			}
			var dst bytes.Buffer
			result, err := a.Fetch(context.Background(), ref, file, &dst, file.Bytes, nil)
			if fault == "none" {
				if err != nil || result.Bytes != file.Bytes || result.SHA256 != file.SHA256 || dst.String() != "yes" {
					t.Fatal(result, err)
				}
			} else if err == nil || result != (provider.TransferResult{}) || strings.Contains(err.Error(), "SYNTHETIC_TOKEN") {
				t.Fatal("failed bytes qualified", fault, result, err)
			}
		})
	}
}

type brokenArtifactWriter struct{ mode string }

func (w brokenArtifactWriter) Write(p []byte) (int, error) {
	switch w.mode {
	case "panic":
		panic("SYNTHETIC_SECRET")
	case "invalid":
		return len(p) + 1, nil
	case "error":
		return len(p), io.ErrClosedPipe
	default:
		return len(p) - 1, nil
	}
}
func TestArtifactDestinationFailureCancelsWithoutPublishing(t *testing.T) {
	for _, mode := range []string{"short", "invalid", "error", "panic"} {
		a, ref, data := artifactFixture(t)
		file := artifactFor(ref, "outputs/answer.txt", data["outputs/answer.txt"])
		a.run = func(ctx context.Context, _ Config, _ string, _ []byte, _ artifactRequest, dst io.Writer) ([]byte, error) {
			_, err := dst.Write([]byte("yes"))
			if err == nil || ctx.Err() == nil {
				t.Fatal("writer failure did not cancel producer")
			}
			return nil, nil
		}
		if result, err := a.Fetch(context.Background(), ref, file, brokenArtifactWriter{mode}, file.Bytes, nil); err == nil || result != (provider.TransferResult{}) {
			t.Fatal("failed destination reported success", mode)
		}
	}
}

func TestArtifactManifestFetchRejectsInvalidResultEvenWithMatchingDigest(t *testing.T) {
	a, ref, _ := artifactFixture(t)
	raw := []byte(`{"phase":"completed","artifacts":[]}`)
	a.run = func(_ context.Context, _ Config, _ string, _ []byte, _ artifactRequest, dst io.Writer) ([]byte, error) {
		_, err := dst.Write(raw)
		return nil, err
	}
	var dst bytes.Buffer
	if _, err := a.Fetch(context.Background(), ref, artifactFor(ref, artifactManifest, raw), &dst, int64(len(raw)), nil); !errors.Is(err, ErrArtifactTransfer) {
		t.Fatalf("invalid result accepted: %v", err)
	}
}
