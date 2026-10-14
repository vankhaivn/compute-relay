// Package statefs protects operator-owned state roots with OS locks and private
// permissions. It is not a sandbox against the host administrator or same-user writes.
package statefs

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	ErrClosed      = errors.New("state root is closed")
	ErrLocked      = errors.New("state directory is already in use")
	ErrUnsafe      = errors.New("state path or permissions are unsafe")
	ErrUnavailable = errors.New("state filesystem unavailable")
)

// Root pins an OS lock until its owner and all retained child roots close.
// Never unlink the lock file: replacing its inode permits competing locks.
type Root struct {
	Path   string
	path   string
	lock   *rootLock
	owner  bool
	closed bool // guarded by lock.mu
}

type rootLock struct {
	mu      sync.Mutex
	file    *os.File
	refs    int
	closing bool
}

// PrivateDir creates only the requested final directory (parents must exist), or
// validates an existing one. exclusive refuses existing paths without modifying them.
func PrivateDir(path string, exclusive bool) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil || path == "" {
		return "", ErrUnsafe
	}
	err = os.Mkdir(absolute, 0o700)
	created := err == nil
	if err != nil && (exclusive || !errors.Is(err, os.ErrExist)) {
		return "", ErrUnavailable
	}
	if err := secureNewRoot(absolute, created); err != nil {
		return "", err
	}
	if err := CheckDir(absolute); err != nil {
		return "", err
	}
	absolute, err = filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", ErrUnavailable
	}
	return absolute, nil
}

func Open(path string) (*Root, error) { return OpenNamed(path, "runtime.lock") }

// OpenNamed preserves the standalone caller's historical lock filename. Hosted
// descendants must use Child so they cannot bypass their host's lock.
func OpenNamed(path, name string) (*Root, error) {
	if !component(name) {
		return nil, ErrUnsafe
	}
	absolute, err := filepath.Abs(path)
	if err != nil || path == "" {
		return nil, ErrUnsafe
	}
	marker := filepath.Join(filepath.Dir(absolute), ".compute-relay-runtime")
	if _, err := os.Lstat(marker); !errors.Is(err, os.ErrNotExist) {
		return nil, ErrUnsafe
	}

	absolute, err = PrivateDir(path, false)
	if err != nil {
		return nil, err
	}
	lockPath := filepath.Join(absolute, name)
	if err := CheckFile(lockPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, ErrUnavailable
	}
	if err := CheckFile(lockPath); err != nil {
		f.Close()
		return nil, err
	}
	if err := lock(f); err != nil {
		f.Close()
		return nil, err
	}
	return &Root{Path: absolute, path: absolute, lock: &rootLock{file: f, refs: 1}, owner: true}, nil
}

func component(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.ContainsAny(name, `/\`)
}

// Child creates or validates one direct private child and pins the existing lock.
func (r *Root) Child(name string) (*Root, error) {
	if !component(name) {
		return nil, ErrUnsafe
	}
	if r == nil || r.lock == nil {
		return nil, ErrClosed
	}
	r.lock.mu.Lock()
	defer r.lock.mu.Unlock()
	if r.closed || r.lock.closing {
		return nil, ErrClosed
	}
	if r.Path != r.path {
		return nil, ErrUnsafe
	}
	path, err := PrivateDir(filepath.Join(r.path, name), false)
	if err != nil {
		return nil, err
	}
	r.lock.refs++
	return &Root{Path: path, path: path, lock: r.lock}, nil
}

// Retain gives a store its own lifetime pin; closing the supplied capability cannot
// release the lock while the store is still using it.
func (r *Root) Retain() (*Root, error) {
	if r == nil || r.lock == nil {
		return nil, ErrClosed
	}
	r.lock.mu.Lock()
	defer r.lock.mu.Unlock()
	if r.closed || r.lock.closing {
		return nil, ErrClosed
	}
	if r.Path != r.path {
		return nil, ErrUnsafe
	}
	if err := CheckDir(r.path); err != nil {
		return nil, err
	}
	r.lock.refs++
	return &Root{Path: r.path, path: r.path, lock: r.lock}, nil
}

func (r *Root) Close() error {
	if r == nil || r.lock == nil {
		return nil
	}
	r.lock.mu.Lock()
	defer r.lock.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	if r.owner {
		r.lock.closing = true
	}
	r.lock.refs--
	if r.lock.refs != 0 {
		return nil
	}
	return errors.Join(unlock(r.lock.file), r.lock.file.Close())
}

func CheckDir(path string) error  { return check(path, true) }
func CheckFile(path string) error { return check(path, false) }
func check(path string, directory bool) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return os.ErrNotExist
	}
	if err != nil {
		return ErrUnavailable
	}
	if info.Mode()&os.ModeSymlink != 0 || (directory && !info.IsDir()) || (!directory && !info.Mode().IsRegular()) || !privateMode(info.Mode()) {
		return ErrUnsafe
	}
	return checkACL(path)
}

// WriteNew publishes only small internal files. Callers own the parent and keep
// incomplete files private until their separate commit/manifest boundary.
func WriteNew(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return ErrUnavailable
	}
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		return ErrUnavailable
	}
	if err := f.Sync(); err != nil {
		return ErrUnavailable
	}
	if err := f.Close(); err != nil {
		return ErrUnavailable
	}
	return nil
}

func SyncFile(path string) error {
	if err := CheckFile(path); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return ErrUnavailable
	}
	defer f.Close()
	return f.Sync()
}
