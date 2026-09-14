//go:build linux || darwin

package blobfs

import (
	"errors"
	"os"
	"syscall"
)

func privateMode(mode os.FileMode) bool { return mode.Perm()&0o077 == 0 }

func acquireLock(file *os.File) error {
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return ErrLocked
		}
		return ErrUnavailable
	}
	return nil
}

func releaseLock(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
}

func diskAvailable(path string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, err
	}
	return uint64(stat.Bavail) * uint64(stat.Bsize), nil
}

func syncDirectory(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

func prepareRootPermissions(string, bool) error { return nil }
func checkPrivatePath(string) error             { return nil }
