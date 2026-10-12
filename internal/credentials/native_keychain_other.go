//go:build !darwin

package credentials

func newNativeKeychainStore(string) (nativeKeychainStore, error) {
	return nil, ErrUnsupported
}
