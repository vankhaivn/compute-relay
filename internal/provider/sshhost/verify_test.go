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
	engine := "linux/amd64"
	server := newTestServer(t)
	server.Password = "secret"
	server.Handle = func(command string, _ io.Reader, stdout, _ io.Writer) int {
		switch {
		case strings.HasPrefix(command, "'test' '-x' "):
			if command == "'test' '-x' '/usr/local/bin/docker'" {
				return 0
			}
			return 1
		case strings.HasSuffix(command, "'/usr/local/bin/docker' 'version' '--format' '{{.Server.Os}}/{{.Server.Arch}}'"):
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
	engine = "windows/amd64"
	if _, err = VerifyHost(ctx, fields(nil)); !errors.Is(err, ErrRemoteDocker) {
		t.Fatalf("windows engine: %v", err)
	}
}

func TestVerifyDockerRejectsUnknownPlatforms(t *testing.T) {
	for _, platform := range []string{"linux/amd64", "linux/arm64", "linux/386", "linux/", "windows/amd64", "linux/amd64/extra"} {
		t.Run(platform, func(t *testing.T) {
			exec := &scriptedExec{answer: func(args []string, _ io.Reader, out io.Writer) (Result, error) {
				if args[0] == "test" {
					return Result{}, nil
				}
				_, _ = io.WriteString(out, platform)
				return Result{}, nil
			}}
			endpoint := Endpoint{Target: Target{Host: "host.example", Port: 22, User: "operator"}}
			v, err := verifyDocker(context.Background(), exec, endpoint, "SHA256:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU")
			want := platform == "linux/amd64" || platform == "linux/arm64"
			if (err == nil) != want || want && (v.OS != "linux" || v.HostKey == "" || v.Account == "") {
				t.Fatalf("verification = %+v, %v", v, err)
			}
		})
	}
}
