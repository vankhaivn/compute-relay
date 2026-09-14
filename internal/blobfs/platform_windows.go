//go:build windows

package blobfs

import (
	"os"
	"syscall"
	"unsafe"
)

var (
	kernel32     = syscall.NewLazyDLL("kernel32.dll")
	lockFileEx   = kernel32.NewProc("LockFileEx")
	unlockFileEx = kernel32.NewProc("UnlockFileEx")
	getDiskFree  = kernel32.NewProc("GetDiskFreeSpaceExW")
)

// Windows uses DACL validation in checkPrivatePath, not Unix mode bits.
func privateMode(os.FileMode) bool { return true }

func acquireLock(file *os.File) error {
	var overlap syscall.Overlapped
	ok, _, err := lockFileEx.Call(file.Fd(), 3, 0, 1, 0, uintptr(unsafe.Pointer(&overlap)))
	if ok == 0 {
		if err == syscall.Errno(33) { // ERROR_LOCK_VIOLATION
			return ErrLocked
		}
		return ErrUnavailable
	}
	return nil
}

func releaseLock(file *os.File) error {
	var overlap syscall.Overlapped
	ok, _, err := unlockFileEx.Call(file.Fd(), 0, 1, 0, uintptr(unsafe.Pointer(&overlap)))
	if ok == 0 {
		return err
	}
	return nil
}

func diskAvailable(path string) (uint64, error) {
	name, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	var available uint64
	ok, _, err := getDiskFree.Call(uintptr(unsafe.Pointer(name)), uintptr(unsafe.Pointer(&available)), 0, 0)
	if ok == 0 {
		return 0, err
	}
	return available, nil
}

// File.Sync flushes both files before rename. Portable Go has no directory-sync guarantee
// on Windows: process-restart safety is tested, power-loss durability is not claimed.
func syncDirectory(string) error { return nil }
