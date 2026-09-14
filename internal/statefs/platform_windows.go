//go:build windows

package statefs

import (
	"os"
	"strings"
	"syscall"
	"unsafe"
)

// Same conservative DACL policy as blobfs: current user, SYSTEM and administrators.
// Reject unknown/object/conditional grants rather than interpret them optimistically.
var (
	kernel32          = syscall.NewLazyDLL("kernel32.dll")
	lockFileEx        = kernel32.NewProc("LockFileEx")
	unlockFileEx      = kernel32.NewProc("UnlockFileEx")
	localFree         = kernel32.NewProc("LocalFree")
	advapi            = syscall.NewLazyDLL("advapi32.dll")
	getNamedSecurity  = advapi.NewProc("GetNamedSecurityInfoW")
	setNamedSecurity  = advapi.NewProc("SetNamedSecurityInfoW")
	convertDescriptor = advapi.NewProc("ConvertStringSecurityDescriptorToSecurityDescriptorW")
	getDescriptorDACL = advapi.NewProc("GetSecurityDescriptorDacl")
	getACE            = advapi.NewProc("GetAce")
)

func privateMode(os.FileMode) bool { return true } // DACLs, not Unix mode bits.
func lock(f *os.File) error {
	var o syscall.Overlapped
	ok, _, err := lockFileEx.Call(f.Fd(), 3, 0, 1, 0, uintptr(unsafe.Pointer(&o)))
	if ok == 0 {
		if err == syscall.Errno(33) {
			return ErrLocked
		}
		return ErrUnavailable
	}
	return nil
}
func unlock(f *os.File) error {
	var o syscall.Overlapped
	ok, _, _ := unlockFileEx.Call(f.Fd(), 0, 1, 0, uintptr(unsafe.Pointer(&o)))
	if ok == 0 {
		return ErrUnavailable
	}
	return nil
}

// Windows files are flushed; no portable directory-sync/power-loss guarantee is claimed.
func SyncDir(string) error { return nil }
func currentSID() (string, error) {
	token, err := syscall.OpenCurrentProcessToken()
	if err != nil {
		return "", ErrUnavailable
	}
	defer token.Close()
	user, err := token.GetTokenUser()
	if err != nil {
		return "", ErrUnavailable
	}
	sid, err := user.User.Sid.String()
	if err != nil {
		return "", ErrUnavailable
	}
	return sid, nil
}
func securityPath(path string) (*uint16, error) {
	if !strings.HasPrefix(path, `\\?\`) {
		if strings.HasPrefix(path, `\\`) {
			path = `\\?\UNC\` + path[2:]
		} else {
			path = `\\?\` + path
		}
	}
	return syscall.UTF16PtrFromString(path)
}
func secureNewRoot(path string, created bool) error {
	if !created {
		return nil
	}
	sid, err := currentSID()
	if err != nil {
		return err
	}
	return setDACL(path, "D:P(A;OICI;FA;;;"+sid+")(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)")
}
func setDACL(path, sddl string) error {
	raw, err := syscall.UTF16PtrFromString(sddl)
	if err != nil {
		return ErrUnavailable
	}
	var sd, dacl unsafe.Pointer
	var present, defaulted uint32
	ok, _, _ := convertDescriptor.Call(uintptr(unsafe.Pointer(raw)), 1, uintptr(unsafe.Pointer(&sd)), 0)
	if ok == 0 {
		return ErrUnavailable
	}
	defer localFree.Call(uintptr(sd))
	ok, _, _ = getDescriptorDACL.Call(uintptr(sd), uintptr(unsafe.Pointer(&present)), uintptr(unsafe.Pointer(&dacl)), uintptr(unsafe.Pointer(&defaulted)))
	if ok == 0 || present == 0 || dacl == nil {
		return ErrUnsafe
	}
	name, err := securityPath(path)
	if err != nil {
		return ErrUnavailable
	}
	code, _, _ := setNamedSecurity.Call(uintptr(unsafe.Pointer(name)), 1, 0x80000004, 0, 0, uintptr(dacl), 0)
	if code != 0 {
		return ErrUnavailable
	}
	return nil
}
func checkACL(path string) error {
	sid, err := currentSID()
	if err != nil {
		return err
	}
	name, err := securityPath(path)
	if err != nil {
		return ErrUnavailable
	}
	var sd, dacl unsafe.Pointer
	code, _, _ := getNamedSecurity.Call(uintptr(unsafe.Pointer(name)), 1, 4, 0, 0, uintptr(unsafe.Pointer(&dacl)), 0, uintptr(unsafe.Pointer(&sd)))
	if code != 0 {
		return ErrUnavailable
	}
	defer localFree.Call(uintptr(sd))
	if dacl == nil {
		return ErrUnsafe
	}
	type aclHeader struct {
		Revision, Reserved     byte
		Size, Count, Reserved2 uint16
	}
	header := (*aclHeader)(dacl)
	if header.Size < 8 {
		return ErrUnsafe
	}
	for i := uint16(0); i < header.Count; i++ {
		var ace unsafe.Pointer
		ok, _, _ := getACE.Call(uintptr(dacl), uintptr(i), uintptr(unsafe.Pointer(&ace)))
		if ok == 0 || ace == nil {
			return ErrUnsafe
		}
		type aceHeader struct {
			Kind, Flags byte
			Size        uint16
		}
		h := (*aceHeader)(ace)
		if h.Kind == 1 {
			continue
		} // Deny grants no permission.
		if h.Kind != 0 || h.Size < 16 {
			return ErrUnsafe
		}
		trustee := (*syscall.SID)(unsafe.Add(ace, 8))
		value, err := trustee.String()
		if err != nil {
			return ErrUnsafe
		}
		if value == sid || value == "S-1-5-18" || value == "S-1-5-32-544" {
			continue
		}
		if value == "S-1-3-0" && h.Flags&8 != 0 {
			continue
		} // Inherit-only CREATOR OWNER.
		return ErrUnsafe
	}
	return nil
}
