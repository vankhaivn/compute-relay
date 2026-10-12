package credentials

import (
	"runtime"
	"sync"
	"unsafe"

	"github.com/ebitengine/purego"
)

// The handles live for the process lifetime. All calls use fixed system framework
// paths and symbols; no caller chooses a library, function or arbitrary keychain query.
type securityFramework struct {
	stringCreate func(uintptr, *byte, uint32) uintptr
	dataCreate   func(uintptr, *byte, int64) uintptr
	dataLength   func(uintptr) int64
	dataBytes    func(uintptr) *byte
	typeID       func(uintptr) uint64
	dataTypeID   func() uint64
	dictCreate   func(uintptr, int64, uintptr, uintptr) uintptr
	dictSet      func(uintptr, uintptr, uintptr)
	release      func(uintptr)
	add          func(uintptr, *uintptr) int32
	read         func(uintptr, *uintptr) int32
	remove       func(uintptr) int32
	interaction  func(bool) int32
	constants    map[string]uintptr
}

var loadSecurity = sync.OnceValues(func() (*securityFramework, error) {
	cf, err := purego.Dlopen("/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation", purego.RTLD_NOW|purego.RTLD_LOCAL)
	if err != nil {
		return nil, ErrUnavailable
	}
	sec, err := purego.Dlopen("/System/Library/Frameworks/Security.framework/Security", purego.RTLD_NOW|purego.RTLD_LOCAL)
	if err != nil {
		return nil, ErrUnavailable
	}
	f := &securityFramework{constants: make(map[string]uintptr)}
	for name, target := range map[string]any{
		"CFStringCreateWithCString": &f.stringCreate, "CFDataCreate": &f.dataCreate,
		"CFDataGetLength": &f.dataLength, "CFDataGetBytePtr": &f.dataBytes,
		"CFGetTypeID": &f.typeID, "CFDataGetTypeID": &f.dataTypeID,
		"CFDictionaryCreateMutable": &f.dictCreate, "CFDictionarySetValue": &f.dictSet,
		"CFRelease": &f.release,
	} {
		address, err := purego.Dlsym(cf, name)
		if err != nil {
			return nil, ErrUnavailable
		}
		purego.RegisterFunc(target, address)
	}
	for name, target := range map[string]any{
		"SecItemAdd": &f.add, "SecItemCopyMatching": &f.read, "SecItemDelete": &f.remove,
		"SecKeychainSetUserInteractionAllowed": &f.interaction,
	} {
		address, err := purego.Dlsym(sec, name)
		if err != nil {
			return nil, ErrUnavailable
		}
		purego.RegisterFunc(target, address)
	}
	for _, name := range []string{"kSecClass", "kSecClassGenericPassword", "kSecAttrService", "kSecAttrAccount", "kSecValueData", "kSecReturnData", "kSecMatchLimit", "kSecMatchLimitOne"} {
		address, err := purego.Dlsym(sec, name)
		if err != nil {
			return nil, ErrUnavailable
		}
		f.constants[name] = nativeConstant(address)
	}
	address, err := purego.Dlsym(cf, "kCFBooleanTrue")
	if err != nil {
		return nil, ErrUnavailable
	}
	f.constants["kCFBooleanTrue"] = nativeConstant(address)
	// The runtime owns this process and must fail closed instead of opening UI.
	if f.interaction(false) != 0 {
		return nil, ErrUnavailable
	}
	return f, nil
})

// Dlsym returns the address of a fixed framework's exported CF object pointer.
// Reinterpret that native address; it is not a Go heap address or pointer arithmetic.
func nativeConstant(address uintptr) uintptr {
	pointer := *(*unsafe.Pointer)(unsafe.Pointer(&address))
	return *(*uintptr)(pointer)
}

type securityStore struct {
	framework *securityFramework
	service   string
}

func newNativeKeychainStore(installation string) (nativeKeychainStore, error) {
	f, err := loadSecurity()
	if err != nil {
		return nil, err
	}
	return &securityStore{framework: f, service: "org.compute-relay.vault." + installation}, nil
}

// Null dictionary callbacks keep ownership explicit. Created CF values outlive the
// synchronous Security call and are released on every path.
func (s *securityStore) query(key string, use func(uintptr) error) error {
	f := s.framework
	owned := make([]uintptr, 0, 3)
	defer func() {
		for i := len(owned) - 1; i >= 0; i-- {
			f.release(owned[i])
		}
	}()
	query := f.dictCreate(0, 0, 0, 0)
	if query == 0 {
		return ErrUnavailable
	}
	owned = append(owned, query)
	f.dictSet(query, f.constants["kSecClass"], f.constants["kSecClassGenericPassword"])
	for name, value := range map[string]string{"kSecAttrService": s.service, "kSecAttrAccount": key} {
		cstring := append([]byte(value), 0)
		ref := f.stringCreate(0, &cstring[0], 0x08000100)
		runtime.KeepAlive(cstring)
		if ref == 0 {
			return ErrUnavailable
		}
		owned = append(owned, ref)
		f.dictSet(query, f.constants[name], ref)
	}
	return use(query)
}

func securityStatus(status int32) error {
	switch status {
	case 0:
		return nil
	case -25299:
		return ErrConflict
	case -25300:
		return ErrUnconfigured
	default:
		return ErrUnavailable
	}
}

func (s *securityStore) add(key string, secret []byte) error {
	return s.query(key, func(query uintptr) error {
		f := s.framework
		data := f.dataCreate(0, &secret[0], int64(len(secret)))
		runtime.KeepAlive(secret)
		if data == 0 {
			return ErrUnavailable
		}
		defer f.release(data)
		f.dictSet(query, f.constants["kSecValueData"], data)
		return securityStatus(f.add(query, nil))
	})
}

func (s *securityStore) read(key string) ([]byte, error) {
	var secret []byte
	err := s.query(key, func(query uintptr) error {
		f := s.framework
		f.dictSet(query, f.constants["kSecReturnData"], f.constants["kCFBooleanTrue"])
		f.dictSet(query, f.constants["kSecMatchLimit"], f.constants["kSecMatchLimitOne"])
		var data uintptr
		status := f.read(query, &data)
		if data != 0 {
			defer f.release(data)
		}
		if err := securityStatus(status); err != nil {
			return err
		}
		if data == 0 || f.typeID(data) != f.dataTypeID() {
			return ErrUnavailable
		}
		size, bytes := f.dataLength(data), f.dataBytes(data)
		if size <= 0 || size > MaxVaultBytes || bytes == nil {
			return ErrUnavailable
		}
		secret = append([]byte(nil), unsafe.Slice(bytes, int(size))...)
		return nil
	})
	return secret, err
}

func (s *securityStore) remove(key string) error {
	return s.query(key, func(query uintptr) error { return securityStatus(s.framework.remove(query)) })
}
