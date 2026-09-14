package packaging

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func fixture(t *testing.T) (*Project, string) {
	t.Helper()
	dir := t.TempDir()
	for name, content := range map[string]string{"main.py": "print('synthetic')\n", "src/helper.py": "value = 42\n", "src/empty": "", "src/.env": "excluded-secret-canary", "src/generated.txt": "ignored", ".computeignore": "**/generated.txt\n"} {
		writeFile(t, dir, name, content)
	}
	p, err := OpenProject(dir, DefaultLimits(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p, dir
}
func writeFile(t *testing.T, root, name, content string) {
	t.Helper()
	full := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
func bundle(t *testing.T, p *Project) ([]byte, Report) {
	t.Helper()
	plan, err := p.Prepare(context.Background(), []string{"main.py", "src"})
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	report, err := plan.Write(context.Background(), &b)
	if err != nil {
		t.Fatal(err)
	}
	return b.Bytes(), report
}
func TestRoundTripDeterminismAndPreview(t *testing.T) {
	p, _ := fixture(t)
	data, report := bundle(t, p)
	again, second := bundle(t, p)
	if !bytes.Equal(data, again) || !reflect.DeepEqual(report, second) {
		t.Fatal("same snapshot is nondeterministic")
	}
	inspected, err := Inspect(context.Background(), bytes.NewReader(data), DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(inspected, report) || report.Bytes != int64(len(data)) || report.SHA256 != digest(data) {
		t.Fatal("roundtrip identity differs")
	}
	if len(report.Manifest.Files) != 3 {
		t.Fatal("default/custom exclusions ignored")
	}
	plan, err := p.Prepare(context.Background(), []string{"main.py", "src"})
	if err != nil {
		t.Fatal(err)
	}
	preview := plan.Preview()
	if preview.ExcludedEntries != 2 || preview.SecretScan != "heuristic-passed" {
		t.Fatal(preview)
	}
	preview.Manifest.Files[0].Path = "mutated"
	if plan.Preview().Manifest.Files[0].Path == "mutated" {
		t.Fatal("mutable plan leaked")
	}
}
func TestSelectionsAndSourceLimits(t *testing.T) {
	p, _ := fixture(t)
	for _, includes := range [][]string{nil, {"."}, {".."}, {"/etc/passwd"}, {"main.py", "main.py"}, {"src", "src/helper.py"}, {"src/.env"}, {"missing"}, {"main.py/child"}} {
		if _, err := p.Prepare(context.Background(), includes); err == nil {
			t.Fatalf("unsafe selection accepted: %v", includes)
		}
	}
	for _, change := range []func(*Limits){func(l *Limits) { l.MaxFiles = 1 }, func(l *Limits) { l.MaxExpandedBytes = 1 }, func(l *Limits) { l.MaxDepth = 1 }, func(l *Limits) { l.MaxEntries = 1; l.MaxFiles = 1 }} {
		old := p.limits
		change(&p.limits)
		_, err := p.Prepare(context.Background(), []string{"src"})
		p.limits = old
		if err == nil {
			t.Fatal("limit ignored")
		}
	}
}
func TestModifiedFileAfterPreviewFailsEvenWithRestoredTimestamp(t *testing.T) {
	p, root := fixture(t)
	plan, err := p.Prepare(context.Background(), []string{"main.py"})
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(root, "main.py")
	info, _ := os.Stat(file)
	data, _ := os.ReadFile(file)
	data[0] = 'x'
	if err := os.WriteFile(file, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(file, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if _, err := plan.Write(context.Background(), &out); !errors.Is(err, ErrChanged) {
		t.Fatalf("changed bytes accepted: %v", err)
	}
	if _, err := Inspect(context.Background(), &out, DefaultLimits()); err == nil {
		t.Fatal("failed producer made a valid bundle")
	}
}
func TestDirectoryAndIgnoreChangesFail(t *testing.T) {
	for _, which := range []string{"new-file", "ignore", "replace"} {
		t.Run(which, func(t *testing.T) {
			p, root := fixture(t)
			plan, err := p.Prepare(context.Background(), []string{"src"})
			if err != nil {
				t.Fatal(err)
			}
			switch which {
			case "new-file":
				writeFile(t, root, "src/new.py", "new")
			case "ignore":
				writeFile(t, root, ".computeignore", "main.py\n")
			case "replace":
				_ = os.Rename(filepath.Join(root, "src"), filepath.Join(root, "old"))
				writeFile(t, root, "src/helper.py", "replacement")
			}
			if _, err := plan.Write(context.Background(), io.Discard); !errors.Is(err, ErrChanged) {
				t.Fatalf("changed tree accepted: %v", err)
			}
		})
	}
}

type mutateWriter struct {
	once   bool
	mutate func()
}

func (w *mutateWriter) Write(b []byte) (int, error) {
	if !w.once {
		w.once = true
		w.mutate()
	}
	return len(b), nil
}
func TestRawSnapshotMutationDuringRead(t *testing.T) {
	p, root := fixture(t)
	writeFile(t, root, "data.bin", strings.Repeat("a", BufferBytes*3))
	snapshot, err := p.SnapshotFile(context.Background(), "data.bin", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	err = snapshot.Write(context.Background(), &mutateWriter{mutate: func() { writeFile(t, root, "data.bin", strings.Repeat("b", BufferBytes*3)) }})
	if !errors.Is(err, ErrChanged) {
		t.Fatal("mid-copy mutation accepted", err)
	}
}
func TestHeuristicScannerAcrossBoundaryAndNoLeak(t *testing.T) {
	p, root := fixture(t)
	canary := "cr1_" + strings.Repeat("X", 43)
	for _, prefix := range []int{0, BufferBytes - 10, BufferBytes - 1} {
		writeFile(t, root, "data.bin", strings.Repeat("a", prefix)+canary)
		_, err := p.Prepare(context.Background(), []string{"data.bin"})
		if !errors.Is(err, ErrSecret) || strings.Contains(err.Error(), canary) {
			t.Fatal("unsafe scanner result", err)
		}
	}
}

func encodeArchive(t *testing.T, m Manifest, modify func(int, *tar.Header, []byte) (*tar.Header, []byte)) []byte {
	t.Helper()
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	tw := tar.NewWriter(gz)
	encoded, _ := json.Marshal(m)
	hs := []*tar.Header{header(ManifestPath, int64(len(encoded)), false)}
	bodies := [][]byte{encoded}
	for _, f := range m.Files {
		hs = append(hs, header("code/"+f.Path, f.Bytes, f.Executable))
		bodies = append(bodies, bytes.Repeat([]byte{'a'}, int(f.Bytes)))
	}
	for i, h := range hs {
		body := bodies[i]
		if modify != nil {
			h, body = modify(i, h, body)
		}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(body); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}
func oneFile() Manifest {
	return Manifest{Version: Version, Files: []File{{Path: "a.py", Bytes: 4, SHA256: digest([]byte("aaaa"))}}}
}
func TestArchiveMaliciousPathCorpus(t *testing.T) {
	names := []string{"../escape", "/absolute", "C:/drive", `a\b`, "a//b", "a/./b", "a/../b", "a/", "a:", "a\x00b", "AUX.txt", "com1.txt", "a. ", "control/../../x", ".compute-relay/state", ".git/config", ".env", "a/CON", "é.py", strings.Repeat("a", 101)}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			m := oneFile()
			m.Files[0].Path = name
			// Use a normal wire name; the independently validated manifest must reject the path.
			data := encodeArchive(t, m, func(i int, h *tar.Header, b []byte) (*tar.Header, []byte) {
				if i > 0 {
					h.Name = "code/a.py"
				}
				return h, b
			})
			if _, err := Inspect(context.Background(), bytes.NewReader(data), DefaultLimits()); err == nil {
				t.Fatal("unsafe manifest accepted")
			}
		})
	}
}
func TestArchiveCollisionAndTypeCorpus(t *testing.T) {
	for _, names := range [][]string{{"a", "a"}, {"a", "a/x"}, {"A/x", "a/y"}, {"b", "a"}} {
		m := Manifest{Version: Version}
		for _, name := range names {
			m.Files = append(m.Files, File{Path: name, Bytes: 4, SHA256: digest([]byte("aaaa"))})
		}
		data := encodeArchive(t, m, nil)
		if _, err := Inspect(context.Background(), bytes.NewReader(data), DefaultLimits()); err == nil {
			t.Fatalf("collision/order accepted: %v", names)
		}
	}
	for _, kind := range []byte{tar.TypeSymlink, tar.TypeLink, tar.TypeDir, tar.TypeFifo, tar.TypeChar, tar.TypeBlock} {
		data := encodeArchive(t, oneFile(), func(i int, h *tar.Header, b []byte) (*tar.Header, []byte) {
			if i > 0 {
				h.Typeflag = kind
				h.Size = 0
				h.Linkname = "outside"
				b = nil
			}
			return h, b
		})
		if _, err := Inspect(context.Background(), bytes.NewReader(data), DefaultLimits()); err == nil {
			t.Fatalf("type %v accepted", kind)
		}
	}
}
func unpack(t *testing.T, data []byte) []byte {
	t.Helper()
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	b, err := io.ReadAll(gz)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func zip(data []byte) []byte {
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	_, _ = gz.Write(data)
	_ = gz.Close()
	return b.Bytes()
}
func TestHiddenHeadersMalformedAndTrailingArchives(t *testing.T) {
	valid := encodeArchive(t, oneFile(), nil)
	for _, kind := range []byte{'x', 'g', 'L', 'K', 'S'} {
		raw := unpack(t, valid)
		raw[156] = kind
		if _, err := Inspect(context.Background(), bytes.NewReader(zip(raw)), DefaultLimits()); err == nil {
			t.Fatal("hidden metadata accepted")
		}
	}
	cases := [][]byte{valid[:len(valid)-1], valid[:10], append(append([]byte(nil), valid...), valid...), append(append([]byte(nil), valid...), []byte("junk")...)}
	raw := unpack(t, valid)
	cases = append(cases, zip(append(raw, make([]byte, 512)...)))
	corrupt := append([]byte(nil), valid...)
	corrupt[len(corrupt)-8] ^= 1
	cases = append(cases, corrupt)
	for i, data := range cases {
		if _, err := Inspect(context.Background(), bytes.NewReader(data), DefaultLimits()); err == nil {
			t.Fatalf("malformed archive %d accepted", i)
		}
	}
}
func TestArchiveBoundsAndManifestValidation(t *testing.T) {
	valid := encodeArchive(t, oneFile(), nil)
	l := DefaultLimits()
	l.MaxCompressedBytes = int64(len(valid))
	if _, err := Inspect(context.Background(), bytes.NewReader(valid), l); err != nil {
		t.Fatal("exact limit rejected", err)
	}
	l.MaxCompressedBytes--
	if _, err := Inspect(context.Background(), bytes.NewReader(valid), l); err == nil {
		t.Fatal("compressed limit bypass")
	}
	l = DefaultLimits()
	l.MaxExpandedBytes = 3
	if _, err := Inspect(context.Background(), bytes.NewReader(valid), l); err == nil {
		t.Fatal("expanded limit bypass")
	}
	l = DefaultLimits()
	l.MaxManifestBytes = 128
	if _, err := Inspect(context.Background(), bytes.NewReader(valid), l); err == nil {
		t.Fatal("manifest size bypass")
	}
	for _, bad := range []func(*Manifest){func(m *Manifest) { m.Version = "future" }, func(m *Manifest) { m.Files[0].SHA256 = strings.Repeat("0", 64) }, func(m *Manifest) { m.Files[0].Bytes = 1000000 }} {
		m := oneFile()
		bad(&m)
		data := encodeArchive(t, m, nil)
		limit := DefaultLimits()
		limit.MaxExpandedBytes = 1024
		if _, err := Inspect(context.Background(), bytes.NewReader(data), limit); err == nil {
			t.Fatal("invalid manifest/bomb accepted")
		}
	}
	duplicate := encodeArchive(t, oneFile(), func(i int, h *tar.Header, b []byte) (*tar.Header, []byte) {
		if i == 0 {
			b = []byte(strings.Replace(string(b), `"bundle_version":`, `"bundle_version":"wrong","bundle_version":`, 1))
			h.Size = int64(len(b))
		}
		return h, b
	})
	if _, err := Inspect(context.Background(), bytes.NewReader(duplicate), DefaultLimits()); err == nil {
		t.Fatal("duplicate JSON key accepted")
	}
}
func TestCancellationAndWriteFailure(t *testing.T) {
	p, _ := fixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Prepare(ctx, []string{"main.py"}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	plan, err := p.Prepare(context.Background(), []string{"main.py"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := plan.Write(ctx, io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := plan.Write(context.Background(), shortWriter{}); err == nil {
		t.Fatal("short write accepted")
	}
	p.limits.MaxCompressedBytes = 1
	if _, err := plan.Write(context.Background(), io.Discard); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
}

type shortWriter struct{}

func (shortWriter) Write(b []byte) (int, error) { return 0, nil }
func TestIgnoreGrammarAndProtectedPaths(t *testing.T) {
	rules, err := parseIgnore("# comment\n*.log\nassets/**/draft?.txt\ncache/\n")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"a.log", "src/a.log", "assets/draft1.txt", "assets/a/b/draft2.txt", "cache/x", "nested/cache/x"} {
		if !rules.excludes(p) {
			t.Fatal("not excluded", p)
		}
	}
	for _, bad := range []string{"!secret", "/absolute", "../escape", "a/**b", "[", "a\\b"} {
		if _, err := parseIgnore(bad); err == nil {
			t.Fatal("invalid ignore accepted", bad)
		}
	}
	_, root := fixture(t)
	writeFile(t, root, "private-state/runtime.db", "private")
	p, err := OpenProject(root, DefaultLimits(), []string{"private-state"})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Prepare(context.Background(), []string{"private-state"}); err == nil {
		t.Fatal("protected state packaged")
	}
}

func TestMissingRequiredManifestFields(t *testing.T) {
	for _, field := range []string{"bytes", "executable"} {
		data := encodeArchive(t, oneFile(), func(i int, h *tar.Header, b []byte) (*tar.Header, []byte) {
			if i == 0 {
				var m map[string]any
				_ = json.Unmarshal(b, &m)
				file := m["files"].([]any)[0].(map[string]any)
				delete(file, field)
				b, _ = json.Marshal(m)
				h.Size = int64(len(b))
			}
			return h, b
		})
		if _, err := Inspect(context.Background(), bytes.NewReader(data), DefaultLimits()); err == nil {
			t.Fatalf("missing %s accepted", field)
		}
	}
}

func FuzzInspectBounded(f *testing.F) {
	f.Add([]byte("not an archive"))
	f.Add(zip(make([]byte, 1024)))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 4096 {
			return
		}
		l := Limits{MaxCompressedBytes: 4096, MaxExpandedBytes: 4096, MaxManifestBytes: 4096, MaxFiles: 8, MaxEntries: 64, MaxDepth: 8}
		report, err := Inspect(context.Background(), bytes.NewReader(b), l)
		if err == nil && (report.Bytes != int64(len(b)) || report.SHA256 != digest(b)) {
			t.Fatal("accepted inconsistent identity")
		}
	})
}
