// Package companion distributes a self-contained native runtime separately from private state.
package companion

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
)

const (
	Schema           = "compute-relay/companion/v1alpha1"
	ManifestName     = "manifest.json"
	MaxManifestBytes = 16 << 20
	MaxArchiveBytes  = 1 << 30
	MaxPayloadBytes  = 2 << 30
	MaxFiles         = 50000
)

var ErrBundle = errors.New("invalid or incomplete companion bundle")

type Build struct {
	Platform           string `json:"platform"`
	Revision           string `json:"revision"`
	SourceSHA256       string `json:"source_sha256"`
	WorktreeModified   bool   `json:"worktree_modified"`
	GoVersion          string `json:"go_version"`
	PythonVersion      string `json:"python_version"`
	PythonDistribution string `json:"python_distribution"`
	UVVersion          string `json:"uv_version"`
	KaggleVersion      string `json:"kaggle_version"`
	KaggleSDKVersion   string `json:"kagglesdk_version"`
}

type File struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	Mode   int64  `json:"mode"`
	SHA256 string `json:"sha256"`
}

type Manifest struct {
	Schema string `json:"schema"`
	Build  Build  `json:"build"`
	Files  []File `json:"files"`
}

type Discovery struct {
	Manifest      Manifest `json:"manifest"`
	Executable    string   `json:"executable"`
	ManagedPython string   `json:"managed_python"`
}

func validHex(s string, size int) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == size && strings.ToLower(s) == s
}

func validPath(s string) bool {
	if len(s) == 0 || len(s) > 1024 || path.Clean(s) != s || strings.HasPrefix(s, "/") {
		return false
	}
	for _, r := range s {
		if r < 32 || r > 126 || r == '\\' || r == ':' {
			return false
		}
	}
	for _, part := range strings.Split(s, "/") {
		if part == "." || part == ".." || len(part) > 255 || strings.TrimSpace(part) != part {
			return false
		}
	}
	return true
}

func (m Manifest) Validate() error {
	b := m.Build
	if m.Schema != Schema || b.Platform != "darwin-arm64" || !validHex(b.Revision, 20) || !validHex(b.SourceSHA256, 32) ||
		b.GoVersion != "go1.27.1" || b.PythonVersion != "3.11.16" || b.UVVersion != "0.12.13" ||
		b.PythonDistribution != "cpython-3.11.16-macos-aarch64-none" || b.KaggleVersion != "2.2.4" || b.KaggleSDKVersion != "0.1.35" ||
		len(m.Files) == 0 || len(m.Files) > MaxFiles {
		return ErrBundle
	}
	seen := map[string]bool{}
	var total int64
	last := ""
	for _, f := range m.Files {
		lower := strings.ToLower(f.Path)
		if !validPath(f.Path) || lower == ManifestName || f.Path <= last || seen[lower] || f.Size < 0 || f.Size > MaxPayloadBytes ||
			(f.Mode != 0644 && f.Mode != 0755) || !validHex(f.SHA256, 32) {
			return ErrBundle
		}
		seen[lower] = true
		last = f.Path
		total += f.Size
		if total > MaxPayloadBytes {
			return ErrBundle
		}
	}
	for name := range seen {
		for p := path.Dir(name); p != "."; p = path.Dir(p) {
			if seen[p] {
				return ErrBundle
			}
		}
	}
	for _, required := range []string{"bin/compute-relay", "bin/companionpack", "client/python/bin/python3.11", "share/LICENSE", "share/python-dependencies.json", "share/go-dependencies.json", "share/uv.lock"} {
		found := false
		for _, f := range m.Files {
			if f.Path == required {
				found = !strings.HasPrefix(required, "bin/") && required != "client/python/bin/python3.11" || f.Mode == 0755
				break
			}
		}
		if !found {
			return ErrBundle
		}
	}
	return nil
}

func decodeManifest(reader io.Reader) (Manifest, error) {
	var m Manifest
	data, err := io.ReadAll(io.LimitReader(reader, MaxManifestBytes+1))
	if err != nil || len(data) > MaxManifestBytes {
		return m, ErrBundle
	}
	d := json.NewDecoder(strings.NewReader(string(data)))
	d.DisallowUnknownFields()
	if d.Decode(&m) != nil || d.Decode(new(any)) != io.EOF {
		return m, ErrBundle
	}
	// Canonical encoding also rejects duplicate keys and case aliases accepted by encoding/json.
	canonical, err := manifestBytes(m)
	if err != nil || string(data) != string(canonical) {
		return m, ErrBundle
	}
	return m, m.Validate()
}

func manifestBytes(m Manifest) ([]byte, error) {
	b, err := json.MarshalIndent(m, "", "  ")
	return append(b, '\n'), err
}

func digest(reader io.Reader) (string, int64, error) {
	h := sha256.New()
	n, err := io.Copy(h, reader)
	return hex.EncodeToString(h.Sum(nil)), n, err
}

// Discover verifies the complete installed payload before returning absolute lifecycle paths.
// Runtime data must live elsewhere: additional files fail verification.
func Discover(directory string) (Discovery, error) {
	var result Discovery
	rootPath, err := filepath.Abs(directory)
	if err != nil {
		return result, err
	}
	info, err := os.Lstat(rootPath)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return result, ErrBundle
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return result, err
	}
	defer root.Close()
	manifest, err := root.Open(ManifestName)
	if err != nil {
		return result, err
	}
	m, err := decodeManifest(manifest)
	manifest.Close()
	if err != nil {
		return result, err
	}
	if m.Build.Platform != runtime.GOOS+"-"+runtime.GOARCH {
		return result, ErrBundle
	}
	expected := map[string]File{}
	for _, f := range m.Files {
		expected[f.Path] = f
	}
	err = filepath.WalkDir(rootPath, func(p string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		name, err := filepath.Rel(rootPath, p)
		if err != nil {
			return err
		}
		name = filepath.ToSlash(name)
		if entry.Type()&os.ModeSymlink != 0 || !entry.Type().IsRegular() {
			return ErrBundle
		}
		if name == ManifestName {
			return nil
		}
		f, ok := expected[name]
		if !ok {
			return ErrBundle
		}
		actual, err := root.Open(name)
		if err != nil {
			return err
		}
		info, statErr := actual.Stat()
		sha, size, hashErr := digest(io.LimitReader(actual, f.Size+1))
		actual.Close()
		if statErr != nil || hashErr != nil || size != f.Size || sha != f.SHA256 || int64(info.Mode().Perm()) != f.Mode {
			return ErrBundle
		}
		delete(expected, name)
		return nil
	})
	if err != nil {
		return result, err
	}
	if len(expected) != 0 {
		return result, ErrBundle
	}
	return Discovery{m, filepath.Join(rootPath, "bin", "compute-relay"), filepath.Join(rootPath, "client", "python", "bin", "python3.11")}, nil
}
