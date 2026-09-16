package kaggle

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

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
	return a, ref, map[string][]byte{artifactManifest: []byte(`{"unit_fixture":true}`), "control/stdout.log": []byte("log\n"), "outputs/answer.txt": []byte("yes")}
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

func TestArtifactCatalogPaginationKeepsOneCompleteSnapshot(t *testing.T) {
	a, ref, data := artifactFixture(t)
	reads := 0
	a.run = func(_ context.Context, c Config, mode string, token []byte, r artifactRequest, dst io.Writer) ([]byte, error) {
		reads++
		if c != a.executor.stager.config || mode != "catalog" || r.Execution.Source != "" || r.Execution.KernelID != "42" || r.Identity.Nonce != ref.Identity.Nonce || r.Target != nil || dst != nil || string(token) != "SYNTHETIC_TOKEN" {
			t.Fatal("unbound catalog invocation")
		}
		return artifactCatalog(data), nil
	}
	cursor := ""
	got := []provider.Artifact{}
	for {
		page, err := a.ListArtifacts(context.Background(), ref, provider.PageRequest{Cursor: cursor, Limit: 1})
		if err != nil || len(page.Artifacts) != 1 {
			t.Fatal(page, err)
		}
		f := page.Artifacts[0]
		if f != artifactFor(ref, f.Path, data[f.Path]) {
			t.Fatal("catalog altered file identity")
		}
		got = append(got, f)
		page.Artifacts[0].Path = "caller change"
		cursor = page.NextCursor
		if cursor == "" {
			break
		}
	}
	if reads != 1 || len(got) != len(data) {
		t.Fatal("partial catalog or hidden remote pagination", reads, len(got))
	}
	first, _ := a.ListArtifacts(context.Background(), ref, provider.PageRequest{Limit: 1})
	old := first.NextCursor
	data["control/stdout.log"] = []byte("changed log")
	if _, err := a.ListArtifacts(context.Background(), ref, provider.PageRequest{Limit: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ListArtifacts(context.Background(), ref, provider.PageRequest{Cursor: old, Limit: 1}); err != ErrArtifactCursor {
		t.Fatal("mixed different catalogs", err)
	}
	fresh, _ := NewArtifactReader(a.executor, DefaultArtifactPolicy())
	fresh.run = func(context.Context, Config, string, []byte, artifactRequest, io.Writer) ([]byte, error) {
		t.Fatal("expired cursor triggered a remote read")
		return nil, nil
	}
	if _, err := fresh.ListArtifacts(context.Background(), ref, provider.PageRequest{Cursor: old, Limit: 1}); err != ErrArtifactCursor {
		t.Fatal("in-memory cursor survived reconstruction", err)
	}
}

func TestArtifactReferenceAndUnselectedFilesFailBeforeCredentialIO(t *testing.T) {
	a, ref, data := artifactFixture(t)
	a.executor.stager.local = func(context.Context, Config, Mode, []byte) (Report, error) {
		t.Fatal("invalid authority caused local/provider work")
		return Report{}, nil
	}
	foreign := ref
	foreign.Identity.WorkspaceID = "foreign"
	if _, err := a.ListArtifacts(context.Background(), foreign, provider.PageRequest{Limit: 1}); err == nil {
		t.Fatal("foreign catalog accepted")
	}
	if _, err := a.ListArtifacts(context.Background(), ref, provider.PageRequest{Cursor: "malformed", Limit: 1}); err == nil {
		t.Fatal("invalid cursor accepted")
	}
	for _, path := range []string{"code/main.py", "inputs/input.bin", "scratch/private", "outputs/undeclared", "outputs/../secret"} {
		file := artifactFor(ref, path, []byte("x"))
		if _, err := a.FetchArtifact(context.Background(), ref, file, io.Discard, file.Bytes); err == nil {
			t.Fatal("unselected bytes accepted", path)
		}
	}
	file := artifactFor(ref, "outputs/answer.txt", data["outputs/answer.txt"])
	if _, err := a.FetchArtifact(context.Background(), ref, file, io.Discard, file.Bytes-1); err == nil {
		t.Fatal("destination limit ignored")
	}
}

func TestArtifactCatalogRejectsMalformedConflictingAndOversizedEvidence(t *testing.T) {
	for _, fault := range []string{"missing", "unselected", "duplicate", "control-limit", "total", "unknown-field", "null", "duplicate-key", "case-key", "trailing"} {
		t.Run(fault, func(t *testing.T) {
			a, ref, data := artifactFixture(t)
			switch fault {
			case "missing":
				delete(data, artifactManifest)
			case "unselected":
				data["scratch/private"] = []byte("x")
			case "total":
				a.policy.MaxBytes = 1
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
			a.run = func(context.Context, Config, string, []byte, artifactRequest, io.Writer) ([]byte, error) { return raw, nil }
			if page, err := a.ListArtifacts(context.Background(), ref, provider.PageRequest{Limit: 100}); err == nil || len(page.Artifacts) != 0 || len(a.catalog) != 0 {
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
			result, err := a.FetchArtifact(context.Background(), ref, file, &dst, file.Bytes)
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
		if result, err := a.FetchArtifact(context.Background(), ref, file, brokenArtifactWriter{mode}, file.Bytes); err == nil || result != (provider.TransferResult{}) {
			t.Fatal("failed destination reported success", mode)
		}
	}
}
