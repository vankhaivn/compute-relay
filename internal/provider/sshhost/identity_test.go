package sshhost

import (
	"errors"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

const testFingerprint = "SHA256:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU"

func TestEncodeFieldsRoundTripsPasswordAndKeyLogins(t *testing.T) {
	raw, err := EncodeFields(map[string]string{FieldHost: "100.64.0.7", FieldUsername: "kenji", FieldPassword: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := ParseEndpoint(raw)
	if err != nil {
		t.Fatal(err)
	}
	if endpoint.Target != (Target{Host: "100.64.0.7", Port: 22, User: "kenji"}) || string(endpoint.Credential.Password) != "secret" ||
		endpoint.DockerPath != "" || endpoint.CPUs != "" || endpoint.MemoryBytes != 0 {
		t.Fatalf("endpoint = %+v", endpoint)
	}

	key, _ := newClientKey(t, []byte("phrase"))
	raw, err = EncodeFields(map[string]string{FieldHost: "mac.tailnet.ts.net", FieldPort: "2222", FieldUsername: "relay",
		FieldHostKey: testFingerprint, FieldPrivateKey: string(key), FieldPassphrase: "phrase",
		FieldDockerPath: "/usr/local/bin/docker", FieldCPUs: "2.5", FieldMemoryMiB: "4096"})
	if err != nil {
		t.Fatal(err)
	}
	if endpoint, err = ParseEndpoint(raw); err != nil {
		t.Fatal(err)
	}
	if endpoint.Target.Port != 2222 || endpoint.Target.HostKey != testFingerprint || endpoint.DockerPath != "/usr/local/bin/docker" ||
		endpoint.CPUs != "2.5" || endpoint.MemoryBytes != 4096<<20 || string(endpoint.Credential.Passphrase) != "phrase" {
		t.Fatalf("endpoint = %+v", endpoint)
	}
}

func TestEncodeFieldsRejectsAmbiguousOrUnsafeConnections(t *testing.T) {
	key, _ := newClientKey(t, nil)
	base := func() map[string]string {
		return map[string]string{FieldHost: "100.64.0.7", FieldUsername: "kenji", FieldPassword: "secret"}
	}
	for name, mutate := range map[string]func(map[string]string){
		"unknown field":      func(f map[string]string) { f["shell"] = "zsh" },
		"empty value":        func(f map[string]string) { f[FieldDockerPath] = "" },
		"no login":           func(f map[string]string) { delete(f, FieldPassword) },
		"two logins":         func(f map[string]string) { f[FieldPrivateKey] = string(key) },
		"passphrase alone":   func(f map[string]string) { f[FieldPassphrase] = "phrase" },
		"bad key":            func(f map[string]string) { delete(f, FieldPassword); f[FieldPrivateKey] = "not a key" },
		"option host":        func(f map[string]string) { f[FieldHost] = "-oProxyCommand=x" },
		"bad user":           func(f map[string]string) { f[FieldUsername] = "kenji;id" },
		"padded port":        func(f map[string]string) { f[FieldPort] = "022" },
		"port range":         func(f map[string]string) { f[FieldPort] = "70000" },
		"bad fingerprint":    func(f map[string]string) { f[FieldHostKey] = "MD5:00" },
		"relative docker":    func(f map[string]string) { f[FieldDockerPath] = "bin/docker" },
		"unclean docker":     func(f map[string]string) { f[FieldDockerPath] = "/usr/local/../bin/docker" },
		"zero cpus":          func(f map[string]string) { f[FieldCPUs] = "0" },
		"cpu precision":      func(f map[string]string) { f[FieldCPUs] = "1.255" },
		"small memory":       func(f map[string]string) { f[FieldMemoryMiB] = "32" },
		"memory not decimal": func(f map[string]string) { f[FieldMemoryMiB] = "4e3" },
		"oversized field":    func(f map[string]string) { f[FieldPassword] = strings.Repeat("x", maxShortField+1) },
	} {
		fields := base()
		mutate(fields)
		if _, err := EncodeFields(fields); !errors.Is(err, ErrRequest) {
			t.Errorf("%s: %v", name, err)
		}
	}
	for _, raw := range []string{`{}`, `{"version":2,"host":"h","port":22,"username":"u","password":"p"}`,
		`{"version":1,"host":"h","port":22,"username":"u","password":"p","extra":1}`,
		`{"version":1,"host":"h","port":22,"username":"u","password":"p"} {}`} {
		if _, err := ParseEndpoint([]byte(raw)); !errors.Is(err, ErrRequest) {
			t.Errorf("%s: %v", raw, err)
		}
	}
}

func TestAccountPinsTheHostKeyAndLoginEndpoint(t *testing.T) {
	target := Target{Host: "100.64.0.7", Port: 22, User: "kenji", HostKey: testFingerprint}
	account, err := Account(target)
	if err != nil {
		t.Fatal(err)
	}
	if !domain.ObjectID(account).Valid() || !strings.HasPrefix(account, "ssh-") || len(account) > 128 {
		t.Fatalf("account %q is not a store identifier", account)
	}
	if key, err := HostKeyOf(account); err != nil || key != testFingerprint {
		t.Fatalf("host key = %q, %v", key, err)
	}
	other := target
	other.User = "root"
	if second, _ := Account(other); second == account {
		t.Fatal("login endpoint is not part of the account")
	}
	if _, err = Account(Target{Host: "100.64.0.7", Port: 22, User: "kenji"}); !errors.Is(err, ErrRequest) {
		t.Fatalf("unpinned account: %v", err)
	}
	for _, bad := range []string{"kaggle-user", "ssh-00", "ssh-" + strings.Repeat("G", 64) + "-" + strings.Repeat("0", 32),
		"ssh-" + strings.Repeat("A", 64) + "-" + strings.Repeat("0", 32), account + "0"} {
		if _, err := HostKeyOf(bad); !errors.Is(err, ErrRequest) {
			t.Errorf("%q: %v", bad, err)
		}
	}
}

func TestPinRefusesAnotherHostKeyOrEndpoint(t *testing.T) {
	target := Target{Host: "100.64.0.7", Port: 22, User: "kenji", HostKey: testFingerprint}
	account, _ := Account(target)
	endpoint := Endpoint{Target: Target{Host: "100.64.0.7", Port: 22, User: "kenji"}, Credential: Credential{Password: []byte("x")}}
	pinned, err := endpoint.Pin(account)
	if err != nil || pinned.Target != target {
		t.Fatalf("pinned = %+v, %v", pinned.Target, err)
	}
	if _, err = pinned.Pin(account); err != nil {
		t.Fatalf("matching configured fingerprint: %v", err)
	}
	other := endpoint
	other.Target.HostKey = "SHA256:" + strings.Repeat("A", 43)
	if _, err = other.Pin(account); !errors.Is(err, ErrHostKeyMismatch) {
		t.Fatalf("configured fingerprint differs: %v", err)
	}
	moved := endpoint
	moved.Target.Host = "100.64.0.8"
	if _, err = moved.Pin(account); !errors.Is(err, ErrHostKeyMismatch) {
		t.Fatalf("moved endpoint: %v", err)
	}
}
