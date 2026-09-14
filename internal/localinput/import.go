// Package localinput exposes only explicitly configured, workspace-allowed local roots.
// It produces verified streams; objects.Service remains the ownership/publication boundary.
package localinput

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"regexp"
	"sync"

	"github.com/vankhaivn/compute-relay/internal/packaging"
)

var (
	ErrForbidden = errors.New("local import root is disabled or not allowed")
	ErrDigest    = errors.New("local input digest does not match declaration")
)
var rootName = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)
var workspaceName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

// RootSpec is operator configuration, never HTTP input. Empty roots disable local import.
// Protected contains relative runtime state/cache paths that must never be imported.
type RootSpec struct {
	Name, Path string
	Workspaces []string
	Protected  []string
}
type Request struct {
	Root     string   `json:"root"`
	Kind     string   `json:"kind"` // file or bundle
	Path     string   `json:"path,omitempty"`
	Includes []string `json:"includes,omitempty"`
	SHA256   string   `json:"sha256,omitempty"` // Optional expected immutable object digest.
}

type allowedRoot struct {
	project    *packaging.Project
	workspaces map[string]bool
}
type Manager struct {
	mu      sync.RWMutex
	roots   map[string]allowedRoot
	maxFile int64
	closed  bool
}

func New(specs []RootSpec, limits packaging.Limits, maxFileBytes int64) (*Manager, error) {
	if len(specs) > 128 || maxFileBytes < 1 || maxFileBytes > 2<<30 {
		return nil, packaging.ErrLimit
	}
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	m := &Manager{roots: map[string]allowedRoot{}, maxFile: maxFileBytes}
	for _, s := range specs {
		if !rootName.MatchString(s.Name) || !filepath.IsAbs(s.Path) || len(s.Workspaces) == 0 || len(s.Workspaces) > 10000 {
			_ = m.Close()
			return nil, packaging.ErrInvalid
		}
		if _, exists := m.roots[s.Name]; exists {
			_ = m.Close()
			return nil, packaging.ErrInvalid
		}
		ws := map[string]bool{}
		for _, w := range s.Workspaces {
			if !workspaceName.MatchString(w) || ws[w] {
				_ = m.Close()
				return nil, packaging.ErrInvalid
			}
			ws[w] = true
		}
		p, err := packaging.OpenProject(s.Path, limits, s.Protected)
		if err != nil {
			_ = m.Close()
			return nil, err
		}
		m.roots[s.Name] = allowedRoot{p, ws}
	}
	return m, nil
}

// Close waits for opened streams. Callers must Close streams and stop requests first.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	var result error
	for _, r := range m.roots {
		result = errors.Join(result, r.project.Close())
	}
	return result
}

// Stream uses io.Pipe for backpressure, never unbounded buffering. Closing either consumer
// path cancels the producer and waits for it. A snapshot error reaches the reader before
// successful EOF, so BlobStore.Put cannot publish an incomplete/inconsistent import.
type Stream struct {
	*io.PipeReader
	Bytes   int64
	SHA256  string
	cancel  context.CancelFunc
	done    chan struct{}
	release func()
	once    sync.Once
}

func (s *Stream) Close() error {
	s.once.Do(func() { s.cancel(); _ = s.PipeReader.Close(); <-s.done; s.release() })
	return nil
}
func (m *Manager) Open(ctx context.Context, workspace string, request Request) (*Stream, error) {
	m.mu.RLock()
	release := true
	defer func() {
		if release {
			m.mu.RUnlock()
		}
	}()
	r, ok := m.roots[request.Root]
	if m.closed || !ok || !r.workspaces[workspace] {
		return nil, ErrForbidden
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.SHA256 != "" && !digestSyntax(request.SHA256) {
		return nil, packaging.ErrInvalid
	}
	var write func(context.Context, io.Writer) error
	size := int64(-1)
	sha := request.SHA256
	switch request.Kind {
	case "file":
		if request.Path == "" || len(request.Includes) != 0 {
			return nil, packaging.ErrInvalid
		}
		snapshot, err := r.project.SnapshotFile(ctx, request.Path, m.maxFile)
		if err != nil {
			return nil, err
		}
		meta := snapshot.Metadata()
		if sha != "" && sha != meta.SHA256 {
			return nil, ErrDigest
		}
		size = meta.Bytes
		sha = meta.SHA256
		write = snapshot.Write
	case "bundle":
		if request.Path != "" || len(request.Includes) == 0 {
			return nil, packaging.ErrInvalid
		}
		plan, err := r.project.Prepare(ctx, request.Includes)
		if err != nil {
			return nil, err
		}
		write = func(ctx context.Context, w io.Writer) error {
			report, err := plan.Write(ctx, w)
			if err != nil {
				return err
			}
			if request.SHA256 != "" && request.SHA256 != report.SHA256 {
				return ErrDigest
			}
			return nil
		}
	default:
		return nil, packaging.ErrInvalid
	}
	child, cancel := context.WithCancel(ctx)
	reader, writer := io.Pipe()
	stream := &Stream{PipeReader: reader, Bytes: size, SHA256: sha, cancel: cancel, done: make(chan struct{}), release: m.mu.RUnlock}
	go func() {
		defer close(stream.done)
		defer writer.Close()
		_ = writer.CloseWithError(write(child, writer))
	}()
	release = false
	return stream, nil
}
func digestSyntax(s string) bool {
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
