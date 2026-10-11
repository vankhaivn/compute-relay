package sshhost

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
)

func TestVerifyHostPinsThePresentedKeyAndRequiresALinuxEngine(t *testing.T) {
	engine := "linux"
	server := newTestServer(t)
	server.password = "secret"
	server.handle = func(command string, _ io.Reader, stdout, _ io.Writer) int {
		switch {
		case strings.HasPrefix(command, "'test' '-x' "):
			if command == "'test' '-x' '/usr/local/bin/docker'" {
				return 0
			}
			return 1
		case strings.HasSuffix(command, "'/usr/local/bin/docker' 'version' '--format' '{{.Server.Os}}'"):
			_, _ = io.WriteString(stdout, engine+"\n")
			return 0
		}
		return 127
	}
	target := server.target()
	fields := func(extra map[string]string) []byte {
		values := map[string]string{FieldHost: target.Host, FieldPort: strconv.Itoa(target.Port), FieldUsername: target.User, FieldPassword: "secret"}
		for k, v := range extra {
			values[k] = v
		}
		raw, err := EncodeFields(values)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	ctx := context.Background()
	account, err := VerifyHost(ctx, fields(nil))
	if err != nil {
		t.Fatal(err)
	}
	if key, _ := HostKeyOf(account); key != target.HostKey {
		t.Fatalf("pinned %q, presented %q", key, target.HostKey)
	}
	if again, err := VerifyHost(ctx, fields(map[string]string{FieldHostKey: target.HostKey})); err != nil || again != account {
		t.Fatalf("configured fingerprint: %q, %v", again, err)
	}
	if _, err = VerifyHost(ctx, fields(map[string]string{FieldHostKey: "SHA256:" + strings.Repeat("A", 43)})); !errors.Is(err, ErrHostKeyMismatch) {
		t.Fatalf("other fingerprint: %v", err)
	}
	if _, err = VerifyHost(ctx, fields(map[string]string{FieldPassword: "wrong"})); !errors.Is(err, ErrCredentialReject) {
		t.Fatalf("wrong password: %v", err)
	}
	if _, err = VerifyHost(ctx, fields(map[string]string{FieldDockerPath: "/opt/bin/docker"})); !errors.Is(err, ErrRemoteDocker) {
		t.Fatalf("missing docker: %v", err)
	}
	engine = "windows"
	if _, err = VerifyHost(ctx, fields(nil)); !errors.Is(err, ErrRemoteDocker) {
		t.Fatalf("windows engine: %v", err)
	}
}
