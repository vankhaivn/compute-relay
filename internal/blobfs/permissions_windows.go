//go:build windows

package blobfs

import (
	"strings"
	"syscall"
	"unsafe"
)

var (
	advapi            = syscall.NewLazyDLL("advapi32.dll")
	getNamedSecurity  = advapi.NewProc("GetNamedSecurityInfoW")
	setNamedSecurity  = advapi.NewProc("SetNamedSecurityInfoW")
	convertDescriptor = advapi.NewProc("ConvertStringSecurityDescriptorToSecurityDescriptorW")
	getDescriptorDACL = advapi.NewProc("GetSecurityDescriptorDacl")
	getACE            = advapi.NewProc("GetAce")
	localFree         = kernel32.NewProc("LocalFree")
)

func currentUserSID() (string, error) {
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
	// Security APIs need extended paths when hashed workspace/object directories exceed
	// legacy MAX_PATH. The path is already absolute and operator-controlled.
	if !strings.HasPrefix(path, `\\?\`) {
		if strings.HasPrefix(path, `\\`) {
			path = `\\?\UNC\` + path[2:]
		} else {
			path = `\\?\` + path
		}
	}
	return syscall.UTF16PtrFromString(path)
}

// Only newly created dedicated roots receive an explicit protected inheritable DACL.
// Existing operator paths are checked, never silently rewritten.
func prepareRootPermissions(path string, created bool) error {
	if !created {
		return nil
	}
	sid, err := currentUserSID()
	if err != nil {
		return err
	}
	sddl, err := syscall.UTF16PtrFromString("D:P(A;OICI;FA;;;" + sid + ")(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)")
	if err != nil {
		return ErrUnavailable
	}
	var descriptor unsafe.Pointer
	ok, _, _ := convertDescriptor.Call(uintptr(unsafe.Pointer(sddl)), 1, uintptr(unsafe.Pointer(&descriptor)), 0)
	if ok == 0 {
		return ErrUnavailable
	}
	defer localFree.Call(uintptr(descriptor))
	var dacl unsafe.Pointer
	var present, defaulted uint32
	ok, _, _ = getDescriptorDACL.Call(uintptr(descriptor), uintptr(unsafe.Pointer(&present)), uintptr(unsafe.Pointer(&dacl)), uintptr(unsafe.Pointer(&defaulted)))
	if ok == 0 || present == 0 || dacl == nil {
		return ErrCorrupt
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

func checkPrivatePath(path string) error {
	sid, err := currentUserSID()
	if err != nil {
		return err
	}
	name, err := securityPath(path)
	if err != nil {
		return ErrUnavailable
	}
	var descriptor, dacl unsafe.Pointer
	code, _, _ := getNamedSecurity.Call(uintptr(unsafe.Pointer(name)), 1, 4, 0, 0, uintptr(unsafe.Pointer(&dacl)), 0, uintptr(unsafe.Pointer(&descriptor)))
	if code != 0 {
		return ErrUnavailable
	}
	defer localFree.Call(uintptr(descriptor))
	if dacl == nil {
		return ErrCorrupt
	} // NULL DACL grants unrestricted access.
	type aclHeader struct {
		Revision, Reserved     byte
		Size, Count, Reserved2 uint16
	}
	header := (*aclHeader)(dacl)
	if header.Size < 8 {
		return ErrCorrupt
	}
	for i := uint16(0); i < header.Count; i++ {
		var ace unsafe.Pointer
		ok, _, _ := getACE.Call(uintptr(dacl), uintptr(i), uintptr(unsafe.Pointer(&ace)))
		if ok == 0 || ace == nil {
			return ErrCorrupt
		}
		type aceHeader struct {
			Kind, Flags byte
			Size        uint16
		}
		h := (*aceHeader)(ace)
		if h.Kind == 1 {
			continue
		} // A deny entry cannot grant access.
		if h.Kind != 0 || h.Size < 16 {
			return ErrCorrupt
		} // Unknown conditional/object ACE: fail closed.
		trustee := (*syscall.SID)(unsafe.Add(ace, 8))
		value, err := trustee.String()
		if err != nil {
			return ErrCorrupt
		}
		if value == sid || value == "S-1-5-18" || value == "S-1-5-32-544" {
			continue
		}
		// CREATOR OWNER is safe only as an inheritance template. Other broad inherit-only
		// grants are rejected too, because children must remain private after creation.
		if value == "S-1-3-0" && h.Flags&8 != 0 {
			continue
		}
		return ErrCorrupt
	}
	return nil
}
