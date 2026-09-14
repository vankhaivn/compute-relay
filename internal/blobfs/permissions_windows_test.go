//go:build windows

package blobfs

import (
	"errors"
	"syscall"
	"testing"
	"unsafe"
)

func TestWindowsRejectsExistingWorldReadableRoot(t *testing.T) {
	store, root := newStore(t)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	// Only a newly created disposable test root is modified, never an operator path.
	sddl, err := syscall.UTF16PtrFromString("D:P(A;OICI;FA;;;WD)")
	if err != nil {
		t.Fatal(err)
	}
	var descriptor unsafe.Pointer
	ok, _, _ := convertDescriptor.Call(uintptr(unsafe.Pointer(sddl)), 1, uintptr(unsafe.Pointer(&descriptor)), 0)
	if ok == 0 {
		t.Fatal("create test DACL")
	}
	defer localFree.Call(uintptr(descriptor))
	var dacl unsafe.Pointer
	var present, defaulted uint32
	ok, _, _ = getDescriptorDACL.Call(uintptr(descriptor), uintptr(unsafe.Pointer(&present)), uintptr(unsafe.Pointer(&dacl)), uintptr(unsafe.Pointer(&defaulted)))
	if ok == 0 || present == 0 || dacl == nil {
		t.Fatal("read test DACL")
	}
	path, err := securityPath(root)
	if err != nil {
		t.Fatal(err)
	}
	code, _, _ := setNamedSecurity.Call(uintptr(unsafe.Pointer(path)), 1, 0x80000004, 0, 0, uintptr(dacl), 0)
	if code != 0 {
		t.Fatal("apply disposable test DACL")
	}
	if unexpected, err := New(root, store.limits); !errors.Is(err, ErrCorrupt) {
		if unexpected != nil {
			unexpected.Close()
		}
		t.Fatalf("unsafe DACL accepted: %v", err)
	}
}
