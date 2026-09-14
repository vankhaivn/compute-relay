// Package packaging creates and inspects finite, deterministic, manifest-bound bundles.
// It never executes workload code or extracts an untrusted archive on the control host.
package packaging

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"

	"github.com/vankhaivn/compute-relay/internal/safefs"
)

const (
	Version      = "compute-relay/bundle/v1"
	ManifestPath = ".compute-relay/bundle.json"
	BufferBytes  = 64 * 1024
)

var (
	ErrInvalid     = errors.New("invalid bundle or selection")
	ErrLimit       = errors.New("bundle limit exceeded")
	ErrSecret      = errors.New("possible credential material in selected input")
	ErrChanged     = safefs.ErrChanged
	ErrPath        = safefs.ErrPath
	ErrUnavailable = safefs.ErrUnavailable
)

type Limits struct {
	MaxCompressedBytes int64
	MaxExpandedBytes   int64 // Sum of file payloads; wire header overhead is separately bounded.
	MaxManifestBytes   int64
	MaxFiles           int
	MaxEntries         int // Visited source entries, including directories and ignored entries.
	MaxDepth           int
}

func DefaultLimits() Limits {
	return Limits{100 << 20, 500 << 20, 4 << 20, 10000, 100000, 32}
}
func (l Limits) Validate() error {
	if l.MaxCompressedBytes < 1 || l.MaxCompressedBytes > 2<<30 || l.MaxExpandedBytes < 1 || l.MaxExpandedBytes > 4<<30 ||
		l.MaxManifestBytes < 128 || l.MaxManifestBytes > 16<<20 || l.MaxFiles < 1 || l.MaxFiles > 100000 || l.MaxEntries < l.MaxFiles || l.MaxEntries > 1000000 || l.MaxDepth < 1 || l.MaxDepth > 64 {
		return ErrLimit
	}
	return nil
}

type File struct {
	Path       string `json:"path"`
	Bytes      int64  `json:"bytes"`
	SHA256     string `json:"sha256"`
	Executable bool   `json:"executable"`
}
type Manifest struct {
	Version string `json:"bundle_version"`
	Files   []File `json:"files"`
}
type Preview struct {
	Manifest        Manifest `json:"manifest"`
	ManifestSHA256  string   `json:"manifest_sha256"`
	ExpandedBytes   int64    `json:"expanded_bytes"`
	ExcludedEntries int      `json:"excluded_entries"`
	SecretScan      string   `json:"secret_scan"`
}
type Report struct {
	Manifest       Manifest `json:"manifest"`
	ManifestSHA256 string   `json:"manifest_sha256"`
	Bytes          int64    `json:"bytes"`
	SHA256         string   `json:"sha256"`
}

func digest(b []byte) string { d := sha256.Sum256(b); return hex.EncodeToString(d[:]) }
func validDigest(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
func manifestBytes(m Manifest) ([]byte, error) { return json.Marshal(m) }

// paths tracks implicit directories too: A/x and a/y collide even though their file names
// differ. Rejecting collisions everywhere preserves the same interpretation on all hosts.
type paths struct {
	names map[string]string
	files map[string]bool
}

func newPaths() *paths { return &paths{map[string]string{}, map[string]bool{}} }
func (p *paths) add(name string) error {
	if name == "." || len(name) > 240 || !safefs.ValidPath(name) || defaultExcluded(name) {
		return ErrPath
	}
	parts := strings.Split(name, "/")
	prefix := ""
	for i, part := range parts {
		if prefix != "" {
			prefix += "/"
		}
		prefix += part
		key := strings.ToLower(prefix)
		if original, ok := p.names[key]; ok && original != prefix {
			return ErrPath
		}
		if p.files[key] {
			return ErrPath
		}
		if i == len(parts)-1 {
			if _, ok := p.names[key]; ok {
				return ErrPath
			}
			p.files[key] = true
		}
		p.names[key] = prefix
	}
	return nil
}

// boundReader distinguishes a real EOF at the limit from truncated input. The extra probe
// byte is never returned as accepted data; callers must not treat ErrLimit as EOF.
type boundReader struct {
	r    io.Reader
	left int64
	n    int64
}

func (r *boundReader) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	if r.left == 0 {
		var one [1]byte
		n, err := r.r.Read(one[:])
		if n > 0 {
			return 0, ErrLimit
		}
		return 0, err
	}
	if int64(len(b)) > r.left {
		b = b[:r.left]
	}
	n, err := r.r.Read(b)
	r.left -= int64(n)
	r.n += int64(n)
	return n, err
}

type boundWriter struct {
	w    io.Writer
	left int64
	n    int64
}

func (w *boundWriter) Write(b []byte) (int, error) {
	if int64(len(b)) > w.left {
		return 0, ErrLimit
	}
	n, err := w.w.Write(b)
	w.left -= int64(n)
	w.n += int64(n)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	return n, err
}
