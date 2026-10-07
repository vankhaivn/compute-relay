package companion

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func testBuild() Build {
	return Build{Platform: "darwin-arm64", Revision: strings.Repeat("a", 40), SourceSHA256: strings.Repeat("b", 64),
		GoVersion: "go1.27.1", PythonVersion: "3.11.16", PythonDistribution: "cpython-3.11.16-macos-aarch64-none",
		UVVersion: "0.12.13", KaggleVersion: "2.2.4", KaggleSDKVersion: "0.1.35"}
}

func fixture(t *testing.T) (string, string, string) {
	t.Helper()
	base := t.TempDir()
	stage := filepath.Join(base, "stage")
	for _, name := range []string{"bin/compute-relay", "bin/companionpack", "client/python/bin/python3.11", "share/LICENSE", "share/python-dependencies.json", "share/go-dependencies.json", "share/uv.lock"} {
		p := filepath.Join(stage, name)
		if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
			t.Fatal(err)
		}
		mode := os.FileMode(0644)
		if strings.Contains(name, "bin/") {
			mode = 0755
		}
		if err := os.WriteFile(p, []byte(name), mode); err != nil {
			t.Fatal(err)
		}
	}
	archive := filepath.Join(base, "bundle.tar.gz")
	if runtime.GOOS == "windows" {
		// Windows does not preserve POSIX executable bits. Parsing and extraction
		// still consume the exact native archive format without pretending that a
		// Windows filesystem can produce a qualified macOS executable package.
		m := Manifest{Schema: Schema, Build: testBuild()}
		var body bytes.Buffer
		for _, name := range []string{"bin/companionpack", "bin/compute-relay", "client/python/bin/python3.11", "share/LICENSE", "share/go-dependencies.json", "share/python-dependencies.json", "share/uv.lock"} {
			mode := int64(0644)
			if strings.Contains(name, "bin/") {
				mode = 0755
			}
			hash := sha256.Sum256([]byte(name))
			m.Files = append(m.Files, File{name, int64(len(name)), mode, hex.EncodeToString(hash[:])})
		}
		if err := m.Validate(); err != nil {
			t.Fatal(err)
		}
		raw, err := manifestBytes(m)
		if err != nil {
			t.Fatal(err)
		}
		gz := gzip.NewWriter(&body)
		writer := tar.NewWriter(gz)
		write := func(name string, data []byte, mode int64) {
			t.Helper()
			if err := writer.WriteHeader(&tar.Header{Name: name, Size: int64(len(data)), Mode: mode, Typeflag: tar.TypeReg, ModTime: time.Unix(0, 0), Format: tar.FormatPAX}); err != nil {
				t.Fatal(err)
			}
			if _, err := writer.Write(data); err != nil {
				t.Fatal(err)
			}
		}
		write(ManifestName, raw, 0644)
		for _, f := range m.Files {
			write(f.Path, []byte(f.Path), f.Mode)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		if err := gz.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(archive, body.Bytes(), 0600); err != nil {
			t.Fatal(err)
		}
		hash := sha256.Sum256(body.Bytes())
		return stage, archive, hex.EncodeToString(hash[:])
	}
	sha, err := Pack(stage, archive, testBuild())
	if err != nil {
		t.Fatal(err)
	}
	return stage, archive, sha
}

func TestDeterministicArchiveInstallAndRelocation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("native packing requires POSIX executable mode bits")
	}
	stage, archive, sha := fixture(t)
	second := filepath.Join(t.TempDir(), "second.tar.gz")
	other, err := Pack(stage, second, testBuild())
	if err != nil || other != sha {
		t.Fatal("non-deterministic archive", err)
	}
	dest := filepath.Join(t.TempDir(), "installed")
	if _, err := Install(archive, sha, dest); err != nil {
		t.Fatal(err)
	}
	if _, err := Install(archive, sha, dest); err == nil {
		t.Fatal("overwrote installation")
	}
	if runtime.GOOS == "darwin" && runtime.GOARCH == "arm64" {
		d, err := Discover(dest)
		if err != nil || d.Executable != filepath.Join(dest, "bin/compute-relay") {
			t.Fatal(err)
		}
		moved := filepath.Join(t.TempDir(), "moved")
		if err := os.Rename(dest, moved); err != nil {
			t.Fatal(err)
		}
		if _, err := Discover(moved); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(moved, "bin/compute-relay"), []byte("tamper"), 0755); err != nil {
			t.Fatal(err)
		}
		if _, err := Discover(moved); err == nil {
			t.Fatal("accepted modified binary")
		}
	}
}

func TestInstallerRejectsWrongChecksumWithoutDestination(t *testing.T) {
	_, archive, _ := fixture(t)
	dest := filepath.Join(t.TempDir(), "absent")
	if _, err := Install(archive, strings.Repeat("0", 64), dest); err == nil {
		t.Fatal("checksum accepted")
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatal("destination created before checksum verified")
	}
}

func rewriteArchive(t *testing.T, source string, change func(int, *tar.Header, []byte) (*tar.Header, []byte)) (string, string) {
	t.Helper()
	f, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	z, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close()
	reader := tar.NewReader(z)
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	writer := tar.NewWriter(gz)
	for i := 0; ; i++ {
		h, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		h, body = change(i, h, body)
		if err := writer.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(body); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(t.TempDir(), "bad.tar.gz")
	if err := os.WriteFile(archive, buf.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(buf.Bytes())
	return archive, hex.EncodeToString(h[:])
}

func TestInstallerRejectsUntrustedArchiveMembers(t *testing.T) {
	_, original, _ := fixture(t)
	for _, tc := range []string{"traversal", "absolute", "symlink", "hardlink", "device", "duplicate", "digest", "mode", "truncated", "manifest-duplicate"} {
		t.Run(tc, func(t *testing.T) {
			archive, sha := rewriteArchive(t, original, func(i int, h *tar.Header, body []byte) (*tar.Header, []byte) {
				if tc == "manifest-duplicate" && i == 0 {
					body = bytes.Replace(body, []byte(`"schema":`), []byte(`"schema":"evil","schema":`), 1)
					h.Size = int64(len(body))
				}
				if i != 1 {
					return h, body
				}
				switch tc {
				case "traversal":
					h.Name = "../escape"
				case "absolute":
					h.Name = "/tmp/escape"
				case "symlink":
					h.Typeflag = tar.TypeSymlink
					h.Linkname = "../../escape"
					h.Size = 0
					body = nil
				case "hardlink":
					h.Typeflag = tar.TypeLink
					h.Linkname = "../../escape"
					h.Size = 0
					body = nil
				case "device":
					h.Typeflag = tar.TypeChar
					h.Size = 0
					body = nil
				case "duplicate":
					h.Name = ManifestName
				case "digest":
					body[0] ^= 1
				case "mode":
					h.Mode = 04755
				case "truncated":
					body = body[:len(body)-1]
					h.Size = int64(len(body))
				}
				return h, body
			})
			dest := filepath.Join(t.TempDir(), "dest")
			if _, err := Install(archive, sha, dest); err == nil {
				t.Fatal("accepted malicious member")
			}
			if _, err := os.Stat(filepath.Join(dest, ManifestName)); !os.IsNotExist(err) {
				t.Fatal("published completed manifest")
			}
		})
	}
}

func TestPackRejectsSymlinksAndCaseCollisions(t *testing.T) {
	stage, _, _ := fixture(t)
	t.Run("native-source-symlink", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("native packing and source symlinks require POSIX host")
		}
		if err := os.Symlink("compute-relay", filepath.Join(stage, "bin/linked")); err != nil {
			t.Fatal(err)
		}
		if _, err := Pack(stage, filepath.Join(t.TempDir(), "symlink.tar.gz"), testBuild()); err == nil {
			t.Fatal("packed link")
		}
	})
	m := Manifest{Schema: Schema, Build: testBuild(), Files: []File{{Path: "a", Mode: 0644, SHA256: strings.Repeat("0", 64)}, {Path: "a/b", Mode: 0644, SHA256: strings.Repeat("0", 64)}}}
	if m.Validate() == nil {
		t.Fatal("file prefix accepted")
	}
	for _, bad := range []string{"../x", "/x", "x//y", "x/./y", "x\\y", "x:y", "x/..", "x\x00y"} {
		if validPath(bad) {
			t.Fatalf("unsafe path %q", bad)
		}
	}
}

func TestInstallCreateNewPreservesExistingData(t *testing.T) {
	_, archive, sha := fixture(t)
	dest := filepath.Join(t.TempDir(), "installed")
	if _, err := Install(archive, sha, dest); err != nil {
		t.Fatal(err)
	}
	private := filepath.Join(dest, "keep")
	if err := os.WriteFile(private, []byte("original state"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Install(archive, sha, dest); err == nil {
		t.Fatal("overwrote existing directory")
	}
	if data, err := os.ReadFile(private); err != nil || string(data) != "original state" {
		t.Fatal("modified existing contents")
	}
}

func TestGzipCorruptionNeverPublishesManifest(t *testing.T) {
	_, original, _ := fixture(t)
	data, err := os.ReadFile(original)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-5] ^= 1
	archive := filepath.Join(t.TempDir(), "corrupt.tar.gz")
	if err := os.WriteFile(archive, data, 0600); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(data)
	dest := filepath.Join(t.TempDir(), "dest")
	if _, err := Install(archive, hex.EncodeToString(hash[:]), dest); err == nil {
		t.Fatal("gzip checksum accepted")
	}
	if _, err := os.Stat(filepath.Join(dest, ManifestName)); !os.IsNotExist(err) {
		t.Fatal("published corrupted installation")
	}
}

func TestManifestRejectsCaseCollisionAndBounds(t *testing.T) {
	_, archive, _ := fixture(t)
	f, err := os.Open(archive)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	z, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close()
	r := tar.NewReader(z)
	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	original, err := decodeManifest(r)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"case", "size", "missing", "pin"} {
		t.Run(kind, func(t *testing.T) {
			m := original
			m.Files = append([]File(nil), original.Files...)
			switch kind {
			case "case":
				duplicate := m.Files[0]
				duplicate.Path = strings.ToUpper(duplicate.Path)
				m.Files = append([]File{duplicate}, m.Files...)
			case "size":
				m.Files[0].Size = MaxPayloadBytes + 1
			case "missing":
				m.Files = m.Files[1:]
			case "pin":
				m.Build.PythonVersion = "3.11.0"
			}
			if m.Validate() == nil {
				t.Fatal("invalid manifest accepted")
			}
		})
	}
}
