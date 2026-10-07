package credentials

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/ports"
)

type fingerprintVault struct {
	key     []byte
	err     error
	creates int
	reads   int
}

func (v *fingerprintVault) Create(context.Context, string, []byte) error { v.creates++; return nil }
func (v *fingerprintVault) Delete(context.Context, string) error         { return nil }
func (v *fingerprintVault) WithSecret(_ context.Context, _ string, use func([]byte) error) error {
	v.reads++
	if v.err != nil {
		return v.err
	}
	b := append([]byte(nil), v.key...)
	defer clear(b)
	return use(b)
}

func TestVaultReferenceRequiresOnlyOpaqueConnectionSlot(t *testing.T) {
	for _, id := range []string{"con_fixture", "con_0123456789abcdef", "con_A.b-c_1", "con_" + strings.Repeat("a", 124)} {
		got, ok := VaultReference(ports.CredentialRef("vault:" + id))
		if !ok || got != id {
			t.Fatal("valid connection slot rejected")
		}
	}
	for _, ref := range []string{"", "con_fixture", "env:con_fixture", "VAULT:con_fixture", "vault:", "vault:con_", "vault:item", "vault:con__item", "vault:con_.item", "vault:con_fixture:gen_1", "vault:con_fixture/gen_1", "vault:vault:con_fixture", "vault:con_fixture\n", " vault:con_fixture", "vault:con_fixture\x00", "vault:con_" + strings.Repeat("a", 125)} {
		got, ok := VaultReference(ports.CredentialRef(ref))
		if ok || got != "" {
			t.Fatal("invalid connection slot accepted")
		}
	}
}

func TestFingerprintRejectsUnavailableInvalidAndCancelledKeys(t *testing.T) {
	for _, size := range []int{0, 31, 33} {
		v := &fingerprintVault{key: make([]byte, size)}
		if result, err := Fingerprint(context.Background(), v, "request_key", []byte("fixture")); !errors.Is(err, ErrUnavailable) || result != "" || v.creates != 0 {
			t.Fatal("invalid protected key yielded a fingerprint")
		}
	}
	for _, denied := range []error{ErrUnconfigured, ErrUnavailable, ErrUnsupported} {
		v := &fingerprintVault{err: denied}
		if result, err := Fingerprint(context.Background(), v, "request_key", []byte("fixture")); !errors.Is(err, denied) || result != "" || v.creates != 0 {
			t.Fatal("unavailable key silently recreated")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	v := &fingerprintVault{key: make([]byte, 32)}
	if result, err := Fingerprint(ctx, v, "request_key", []byte("fixture")); !errors.Is(err, context.Canceled) || result != "" || v.reads != 0 || v.creates != 0 {
		t.Fatal("cancelled fingerprint accessed the protected key")
	}
}
func TestFingerprintRequiresOriginalProtectedKey(t *testing.T) {
	v := &fingerprintVault{key: make([]byte, 32)}
	first, err := Fingerprint(context.Background(), v, "request_key", []byte(`{"api_token":"fixture-one"}`))
	if err != nil || len(first) != 64 {
		t.Fatal(err)
	}
	replay, err := Fingerprint(context.Background(), v, "request_key", []byte(`{"api_token":"fixture-one"}`))
	if err != nil || first != replay {
		t.Fatal("replay changed")
	}
	changed, _ := Fingerprint(context.Background(), v, "request_key", []byte(`{"api_token":"fixture-two"}`))
	if first == changed {
		t.Fatal("different secret collided")
	}
	v.key[0] = 1
	other, _ := Fingerprint(context.Background(), v, "request_key", []byte(`{"api_token":"fixture-one"}`))
	if first == other {
		t.Fatal("fingerprint ignores protected key")
	}
	v.err = ErrUnconfigured
	if _, err = Fingerprint(context.Background(), v, "request_key", []byte("fixture")); !errors.Is(err, ErrUnconfigured) || v.creates != 0 {
		t.Fatal("lost key silently replaced")
	}
}
