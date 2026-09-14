//go:build linux || darwin

package statefs

import (
	"errors"
	"os"
	"syscall"
)

func privateMode(m os.FileMode) bool   { return m.Perm()&0o077 == 0 }
func secureNewRoot(string, bool) error { return nil }
func checkACL(string) error            { return nil }
func lock(f *os.File) error {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return ErrLocked
		}
		return ErrUnavailable
	}
	return nil
}
func unlock(f *os.File) error { return syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }
func SyncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return ErrUnavailable
	}
	defer f.Close()
	return f.Sync()
}
