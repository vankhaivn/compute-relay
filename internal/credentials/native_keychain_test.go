package credentials

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"os"
	"runtime"
	"testing"
)

type nativeVaultProbe struct {
	value, lastRead []byte
	calls           int
}

func (p *nativeVaultProbe) add(_ string, value []byte) error {
	p.calls++
	if p.value != nil {
		return ErrConflict
	}
	p.value = bytes.Clone(value)
	return nil
}
func (p *nativeVaultProbe) read(string) ([]byte, error) {
	p.calls++
	if p.value == nil {
		return nil, ErrUnconfigured
	}
	p.lastRead = bytes.Clone(p.value)
	return p.lastRead, nil
}
func (p *nativeVaultProbe) remove(string) error {
	p.calls++
	clear(p.value)
	p.value = nil
	return nil
}

func TestNativeVaultPreservesImmutableBytesAndClearsReads(t *testing.T) {
	p := &nativeVaultProbe{}
	k := &nativeKeychain{store: p}
	ctx := context.Background()
	value := []byte{0, 255, 1, 2}
	if err := k.Create(ctx, "key", value); err != nil {
		t.Fatal(err)
	}
	if err := k.Create(ctx, "key", value); err != nil || !bytes.Equal(p.lastRead, make([]byte, len(value))) {
		t.Fatal("replay changed bytes or left the comparison buffer", err)
	}
	if err := k.Create(ctx, "key", []byte("other")); err != ErrConflict || !bytes.Equal(p.value, value) {
		t.Fatal("conflicting write replaced the original", err)
	}
	for _, panicExit := range []bool{false, true} {
		func() {
			defer func() {
				if got := recover(); (got != nil) != panicExit {
					t.Fatal("callback panic changed")
				}
			}()
			err := k.WithSecret(ctx, "key", func(raw []byte) error {
				if !bytes.Equal(raw, value) {
					t.Fatal("binary secret changed")
				}
				if panicExit {
					panic("fixture")
				}
				return ErrUnavailable
			})
			if err != ErrUnavailable {
				t.Fatal("callback error changed", err)
			}
		}()
		if !bytes.Equal(p.lastRead, make([]byte, len(value))) {
			t.Fatal("owned read buffer was not cleared")
		}
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	calls := p.calls
	if err := k.WithSecret(cancelled, "key", func([]byte) error { t.Fatal("cancelled callback"); return nil }); !errors.Is(err, context.Canceled) || p.calls != calls {
		t.Fatal("cancelled read reached the store", err)
	}
	if err := k.Create(ctx, "key", make([]byte, MaxVaultBytes+1)); err != ErrInvalid || p.calls != calls {
		t.Fatal("oversized write reached the store", err)
	}
}

// Opt-in local qualification creates and removes only synthetic entries in a new
// random installation namespace. Ordinary CI never reads or writes a native vault.
func TestNativeKeychainWithoutPython(t *testing.T) {
	if os.Getenv("COMPUTE_RELAY_TEST_KEYCHAIN") != "1" {
		t.Skip("explicit native Keychain qualification only")
	}
	if runtime.GOOS != "darwin" {
		t.Fatal("native qualification requires macOS")
	}
	t.Setenv("PATH", t.TempDir())
	installation := "test_" + rand.Text()
	k, err := NewNativeKeychain(installation)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	const key = "fixture_credential"
	value := bytes.Repeat([]byte{0, 255, 'a', 'b'}, MaxVaultBytes/4)
	defer clear(value)
	t.Cleanup(func() {
		if err := k.Delete(ctx, key); err != nil {
			t.Error("exact fixture cleanup failed", err)
		}
	})
	if err := k.Create(ctx, key, value); err != nil {
		t.Fatal("native create", err)
	}
	if err := k.Create(ctx, key, value); err != nil {
		t.Fatal("same-value replay", err)
	}
	if err := k.Create(ctx, key, []byte("other")); err != ErrConflict {
		t.Fatal("conflicting write", err)
	}
	reopened, err := NewNativeKeychain(installation)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.WithSecret(ctx, key, func(raw []byte) error {
		if !bytes.Equal(raw, value) {
			t.Fatal("reopened binary value changed")
		}
		return nil
	}); err != nil {
		t.Fatal("native read", err)
	}
	if err := reopened.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if err := reopened.WithSecret(ctx, key, func([]byte) error { t.Fatal("deleted value returned"); return nil }); err != ErrUnconfigured {
		t.Fatal("missing entry", err)
	}
}
