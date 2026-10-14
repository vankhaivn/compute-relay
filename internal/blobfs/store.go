// Package blobfs implements workspace-scoped immutable filesystem blobs. It does not
// replace SQLite ownership metadata: a published blob is not an admitted API object.
package blobfs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/statefs"
)

var (
	ErrInvalid        = errors.New("invalid object declaration")
	ErrTooLarge       = errors.New("object exceeds byte limit")
	ErrLengthMismatch = errors.New("object length does not match declaration")
	ErrDigestMismatch = errors.New("object digest does not match declaration")
	ErrExists         = errors.New("object identity already published")
	ErrNotFound       = errors.New("object not found")
	ErrCorrupt        = errors.New("object storage integrity failure")
	ErrStorageFull    = errors.New("local storage limit or free reserve reached")
	ErrUnavailable    = errors.New("object storage unavailable")
	ErrClosed         = errors.New("blob store is closed")
	ErrLocked         = errors.New("blob directory already in use")
)

const bufferBytes = 64 * 1024
const formatMarker = "compute-relay/blob-v1\n"

type Limits struct {
	MaxObjectBytes int64
	MaxTotalBytes  int64
	MinFreeBytes   uint64
	MaxObjects     int // Includes in-flight reservations; zero selects 100,000.
}

func DefaultLimits() Limits {
	return Limits{MaxObjectBytes: 2 << 30, MaxTotalBytes: 20 << 30, MinFreeBytes: 2 << 30, MaxObjects: 100000}
}

// Store pins its standalone or host-owned root lock while operations and recovery run.
// The operator must supply a dedicated private directory.
type Store struct {
	root   string
	limits Limits
	lock   *statefs.Root
	life   sync.RWMutex
	closed bool
	mu     sync.Mutex
	count  int
	used   int64 // Includes in-flight data writes. Metadata overhead is covered by free space.

	// Narrow syscall seams for deterministic fault tests; not configurable by workloads.
	freeBytes func(string) (uint64, error)
	write     func(*os.File, []byte) (int, error)
	syncFile  func(*os.File) error
	rename    func(string, string) error
}

func (l Limits) valid() bool {
	return l.MaxObjects >= 0 && l.MaxObjectBytes > 0 && l.MaxTotalBytes >= l.MaxObjectBytes
}

func New(path string, limits Limits) (*Store, error) {
	if path == "" || !limits.valid() {
		return nil, ErrInvalid
	}
	// Standalone blob roots historically create missing parents.
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, ErrUnavailable
	}
	if err := os.MkdirAll(filepath.Dir(absolute), 0o700); err != nil {
		return nil, ErrUnavailable
	}
	root, err := statefs.OpenNamed(absolute, ".lock")
	if err != nil {
		return nil, rootError(err)
	}
	return openRoot(root, limits)
}

// NewRoot retains a locked child capability. The caller may close its own handle
// immediately; this store keeps the host lock pinned until Close.
func NewRoot(root *statefs.Root, limits Limits) (*Store, error) {
	if !limits.valid() {
		return nil, ErrInvalid
	}
	retained, err := root.Retain()
	if err != nil {
		return nil, rootError(err)
	}
	return openRoot(retained, limits)
}

func rootError(err error) error {
	switch {
	case errors.Is(err, statefs.ErrLocked):
		return ErrLocked
	case errors.Is(err, statefs.ErrUnsafe):
		return ErrCorrupt
	case errors.Is(err, statefs.ErrClosed):
		return ErrClosed
	default:
		return ErrUnavailable
	}
}

func openRoot(root *statefs.Root, limits Limits) (_ *Store, err error) {
	if limits.MaxObjects == 0 {
		limits.MaxObjects = 100000
	}
	s := &Store{root: root.Path, limits: limits, lock: root, freeBytes: diskAvailable,
		write:    func(f *os.File, b []byte) (int, error) { return f.Write(b) },
		syncFile: (*os.File).Sync, rename: os.Rename}
	defer func() {
		if err != nil {
			_ = root.Close()
		}
	}()
	if err = s.initialize(); err != nil {
		return nil, err
	}
	if err = s.recover(); err != nil {
		return nil, err
	}
	return s, nil
}

// Close waits for in-flight store operations. Callers must bound/cancel their Readers;
// arbitrary io.Reader implementations cannot be forcibly interrupted by this package.
func (s *Store) Close() error {
	s.life.Lock()
	defer s.life.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.lock.Close()
}

func (s *Store) Put(ctx context.Context, declaration domain.ObjectMetadata, source io.Reader) (result domain.ObjectMetadata, err error) {
	s.life.RLock()
	defer s.life.RUnlock()
	if s.closed {
		return result, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if !declaration.ID.Valid() || !declaration.WorkspaceID.Valid() || declaration.Bytes < -1 ||
		(declaration.SHA256 != "" && !declaration.SHA256.Valid()) || source == nil {
		return result, ErrInvalid
	}
	if declaration.Bytes > s.limits.MaxObjectBytes {
		return result, ErrTooLarge
	}
	s.mu.Lock()
	if s.count >= s.limits.MaxObjects {
		s.mu.Unlock()
		return result, ErrStorageFull
	}
	free, freeErr := s.freeBytes(s.root)
	if freeErr != nil {
		s.mu.Unlock()
		return result, ErrUnavailable
	}
	// Reserve headroom even for empty uploads, which still consume metadata/inodes.
	if free < s.limits.MinFreeBytes || free-s.limits.MinFreeBytes < 4096 {
		s.mu.Unlock()
		return result, ErrStorageFull
	}
	s.count++
	workspace, err := s.workspace(declaration.WorkspaceID)
	var temporary string
	if err == nil {
		temporary, err = os.MkdirTemp(filepath.Join(workspace, "temporary"), "upload-")
	}
	if err != nil {
		s.count--
	}
	s.mu.Unlock()
	if err != nil {
		return result, ErrUnavailable
	}
	var staged int64
	published := false
	defer func() {
		if !published {
			s.mu.Lock()
			removeErr := os.RemoveAll(temporary)
			if removeErr == nil {
				s.used -= staged
				s.count--
			}
			s.mu.Unlock()
			if removeErr != nil {
				err = errors.Join(err, ErrUnavailable)
			}
		}
	}()
	data, err := os.OpenFile(filepath.Join(temporary, "data"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return result, ErrUnavailable
	}
	defer data.Close()
	hash := sha256.New()
	writer := &diskWriter{store: s, file: data, ctx: ctx, count: &staged}
	_, err = io.CopyBuffer(io.MultiWriter(writer, hash), &contextReader{ctx: ctx, reader: source}, make([]byte, bufferBytes))
	if err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if declaration.Bytes >= 0 && declaration.Bytes != staged {
		return result, ErrLengthMismatch
	}
	digest := domain.SHA256Digest(hex.EncodeToString(hash.Sum(nil)))
	if declaration.SHA256 != "" && declaration.SHA256 != digest {
		return result, ErrDigestMismatch
	}
	if err := s.syncFile(data); err != nil {
		return result, ErrUnavailable
	}
	if err := data.Close(); err != nil {
		return result, ErrUnavailable
	}
	result = declaration
	result.Bytes, result.SHA256 = staged, digest
	encoded, err := json.Marshal(result)
	if err != nil {
		return domain.ObjectMetadata{}, ErrInvalid
	}
	if err := statefs.WriteNew(filepath.Join(temporary, "metadata.json"), encoded); err != nil {
		return domain.ObjectMetadata{}, ErrUnavailable
	}
	if err := statefs.SyncDir(temporary); err != nil {
		return domain.ObjectMetadata{}, ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return domain.ObjectMetadata{}, err
	}
	final := filepath.Join(workspace, "objects", encodedID(string(declaration.ID)))
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := os.Lstat(final); err == nil {
		return domain.ObjectMetadata{}, ErrExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return domain.ObjectMetadata{}, ErrUnavailable
	}
	// One directory rename publishes data + self-describing metadata together. Root lock
	// and mu serialize no-overwrite checks; destinations are never replaced.
	if err := s.rename(temporary, final); err != nil {
		return domain.ObjectMetadata{}, ErrUnavailable
	}
	published = true
	if err := statefs.SyncDir(filepath.Dir(final)); err != nil {
		// Publication may have happened. Keep the complete orphan for reconciliation;
		// never delete data after an uncertain publication/metadata commit.
		return domain.ObjectMetadata{}, ErrUnavailable
	}
	return result, nil
}

func (s *Store) Open(ctx context.Context, workspace domain.WorkspaceID, id domain.ObjectID) (io.ReadCloser, error) {
	s.life.RLock()
	defer s.life.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	if !workspace.Valid() || !id.Valid() {
		return nil, ErrNotFound
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	base := filepath.Join(s.root, "workspaces", encodedID(string(workspace)))
	for _, dir := range []string{base, filepath.Join(base, "objects")} {
		if err := privateDirectory(dir); err != nil {
			return nil, ErrNotFound
		}
	}
	directory := filepath.Join(base, "objects", encodedID(string(id)))
	meta, err := readMetadata(directory)
	if err != nil {
		return nil, err
	}
	if meta.ID != id || meta.WorkspaceID != workspace {
		return nil, ErrCorrupt
	}
	data, err := os.Open(filepath.Join(directory, "data"))
	if err != nil {
		return nil, ErrUnavailable
	}
	hash := sha256.New()
	n, err := io.CopyBuffer(hash, &contextReader{ctx: ctx, reader: data}, make([]byte, bufferBytes))
	if err != nil || n != meta.Bytes || hex.EncodeToString(hash.Sum(nil)) != string(meta.SHA256) {
		_ = data.Close()
		if err != nil {
			return nil, err
		}
		return nil, ErrCorrupt
	}
	if _, err := data.Seek(0, io.SeekStart); err != nil {
		_ = data.Close()
		return nil, ErrUnavailable
	}
	return data, nil
}

type diskWriter struct {
	store *Store
	file  *os.File
	ctx   context.Context
	count *int64
}

func (w *diskWriter) Write(b []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	if int64(len(b)) > w.store.limits.MaxObjectBytes-*w.count {
		return 0, ErrTooLarge
	}
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	if int64(len(b)) > w.store.limits.MaxTotalBytes-w.store.used {
		return 0, ErrStorageFull
	}
	free, err := w.store.freeBytes(w.store.root)
	if err != nil {
		return 0, ErrUnavailable
	}
	if free < w.store.limits.MinFreeBytes || uint64(len(b)) > free-w.store.limits.MinFreeBytes {
		return 0, ErrStorageFull
	}
	n, err := w.store.write(w.file, b)
	*w.count += int64(n)
	w.store.used += int64(n)
	if err != nil {
		return n, ErrUnavailable
	}
	return n, nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
	empty  int
}

func (r *contextReader) Read(b []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.reader.Read(b)
	if n == 0 && err == nil {
		r.empty++
		if r.empty >= 100 {
			return 0, io.ErrNoProgress
		}
	} else {
		r.empty = 0
	}
	return n, err
}

func encodedID(raw string) string {
	value := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(value[:])
}

func privateDirectory(path string) error {
	err := statefs.CheckDir(path)
	if errors.Is(err, os.ErrNotExist) {
		return ErrNotFound
	}
	if err != nil {
		return rootError(err)
	}
	return nil
}

func ensureDirectory(path string) error {
	if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return ErrUnavailable
	}
	return privateDirectory(path)
}

func (s *Store) workspace(id domain.WorkspaceID) (string, error) {
	base := filepath.Join(s.root, "workspaces", encodedID(string(id)))
	for _, path := range []string{base, filepath.Join(base, "temporary"), filepath.Join(base, "objects")} {
		if err := ensureDirectory(path); err != nil {
			return "", err
		}
	}
	if err := statefs.SyncDir(filepath.Dir(base)); err != nil {
		return "", err
	}
	if err := statefs.SyncDir(base); err != nil {
		return "", err
	}
	return base, nil
}

func readMetadata(directory string) (domain.ObjectMetadata, error) {
	var meta domain.ObjectMetadata
	if err := privateDirectory(directory); err != nil {
		return meta, err
	}
	for _, name := range []string{"metadata.json", "data"} {
		info, err := os.Lstat(filepath.Join(directory, name))
		if err != nil || !info.Mode().IsRegular() {
			return meta, ErrCorrupt
		}
		if statefs.CheckFile(filepath.Join(directory, name)) != nil {
			return meta, ErrCorrupt
		}
		if name == "metadata.json" && info.Size() > 4096 {
			return meta, ErrCorrupt
		}
	}
	content, err := os.ReadFile(filepath.Join(directory, "metadata.json"))
	if err != nil {
		return meta, ErrUnavailable
	}
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&meta); err != nil || !meta.Valid() {
		return domain.ObjectMetadata{}, ErrCorrupt
	}
	if decoder.Decode(new(any)) != io.EOF {
		return domain.ObjectMetadata{}, ErrCorrupt
	}
	info, err := os.Stat(filepath.Join(directory, "data"))
	if err != nil || info.Size() != meta.Bytes {
		return domain.ObjectMetadata{}, ErrCorrupt
	}
	return meta, nil
}

func (s *Store) initialize() error {
	marker := filepath.Join(s.root, ".format")
	info, err := os.Lstat(marker)
	if errors.Is(err, os.ErrNotExist) {
		entries, err := os.ReadDir(s.root)
		if err != nil {
			return ErrUnavailable
		}
		for _, entry := range entries {
			if entry.Name() != ".lock" {
				return fmt.Errorf("%w: dedicated empty blob root required", ErrInvalid)
			}
		}
		if err := statefs.WriteNew(marker, []byte(formatMarker)); err != nil {
			return ErrUnavailable
		}
	} else if err != nil || !info.Mode().IsRegular() || info.Size() > 64 {
		return ErrCorrupt
	}
	content, err := os.ReadFile(marker)
	if err != nil || string(content) != formatMarker {
		return ErrCorrupt
	}
	if err := ensureDirectory(filepath.Join(s.root, "workspaces")); err != nil {
		return err
	}
	return statefs.SyncDir(s.root)
}

// recover removes only unfinished staging in this versioned, exclusively locked root.
// Complete blobs are preserved even when SQLite ownership has not yet been committed.
func (s *Store) recover() error {
	root := filepath.Join(s.root, "workspaces")
	workspaces, err := os.ReadDir(root)
	if err != nil {
		return ErrUnavailable
	}
	for _, workspace := range workspaces {
		if !domain.SHA256Digest(workspace.Name()).Valid() {
			return ErrCorrupt
		}
		base := filepath.Join(root, workspace.Name())
		if err := privateDirectory(base); err != nil {
			return err
		}
		for _, name := range []string{"temporary", "objects"} {
			if err := ensureDirectory(filepath.Join(base, name)); err != nil {
				return err
			}
		}
		temporary := filepath.Join(base, "temporary")
		entries, err := os.ReadDir(temporary)
		if err != nil {
			return ErrUnavailable
		}
		for _, entry := range entries {
			if !strings.HasPrefix(entry.Name(), "upload-") || !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
				return ErrCorrupt
			}
			if err := os.RemoveAll(filepath.Join(temporary, entry.Name())); err != nil {
				return ErrUnavailable
			}
		}
		if err := statefs.SyncDir(temporary); err != nil {
			return err
		}
		objects, err := os.ReadDir(filepath.Join(base, "objects"))
		if err != nil {
			return ErrUnavailable
		}
		for _, object := range objects {
			meta, err := readMetadata(filepath.Join(base, "objects", object.Name()))
			if err != nil {
				return err
			}
			if encodedID(string(meta.WorkspaceID)) != workspace.Name() || encodedID(string(meta.ID)) != object.Name() {
				return ErrCorrupt
			}
			if meta.Bytes > s.limits.MaxTotalBytes-s.used {
				return ErrStorageFull
			}
			s.used += meta.Bytes
			s.count++
			if s.count > s.limits.MaxObjects {
				return ErrStorageFull
			}
		}
	}
	return nil
}
