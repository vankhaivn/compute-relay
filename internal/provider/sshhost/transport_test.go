package sshhost

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestDialAuthenticatesWithKeyPasswordAndInteractivePassword(t *testing.T) {
	key, public := newClientKey(t, nil)
	encrypted, encryptedPublic := newClientKey(t, []byte("phrase"))
	for name, tc := range map[string]struct {
		credential  Credential
		clientKey   []byte
		interactive bool
	}{
		"key":                  {credential: Credential{PrivateKey: key}},
		"encrypted key":        {credential: Credential{PrivateKey: encrypted, Passphrase: []byte("phrase")}},
		"password":             {credential: Credential{Password: []byte("secret")}},
		"interactive password": {credential: Credential{Password: []byte("secret")}, interactive: true},
	} {
		t.Run(name, func(t *testing.T) {
			server := newTestServer(t)
			server.password = "secret"
			server.interactiveOnly = tc.interactive
			server.clientPK = public
			if name == "encrypted key" {
				server.clientPK = encryptedPublic
			}
			client, err := Dial(context.Background(), server.target(), tc.credential)
			if err != nil {
				t.Fatalf("Dial: %v", err)
			}
			defer client.Close()
			if client.HostKey != server.target().HostKey {
				t.Fatalf("host key = %q", client.HostKey)
			}
		})
	}
}

func TestDialPinsPresentedHostKeyOnlyWhenNoneIsSupplied(t *testing.T) {
	server := newTestServer(t)
	server.password = "secret"
	target := server.target()
	pinned := target.HostKey
	target.HostKey = ""
	client, err := Dial(context.Background(), target, Credential{Password: []byte("secret")})
	if err != nil {
		t.Fatal(err)
	}
	_ = client.Close()
	if client.HostKey != pinned {
		t.Fatalf("presented %q, want %q", client.HostKey, pinned)
	}
	target.HostKey = "SHA256:" + strings.Repeat("A", 43)
	if _, err = Dial(context.Background(), target, Credential{Password: []byte("secret")}); !errors.Is(err, ErrHostKeyMismatch) {
		t.Fatalf("changed host key: %v", err)
	}
}

func TestDialClassifiesRejectedAndUnreachableHosts(t *testing.T) {
	server := newTestServer(t)
	server.password = "secret"
	if _, err := Dial(context.Background(), server.target(), Credential{Password: []byte("wrong")}); !errors.Is(err, ErrCredentialReject) {
		t.Fatalf("wrong password: %v", err)
	}
	other, _ := newClientKey(t, nil)
	if _, err := Dial(context.Background(), server.target(), Credential{PrivateKey: other}); !errors.Is(err, ErrCredentialReject) {
		t.Fatalf("unknown key: %v", err)
	}
	closed := server.target()
	_ = server.listener.Close()
	if _, err := Dial(context.Background(), closed, Credential{Password: []byte("secret")}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("closed listener: %v", err)
	}
}

func TestDialRejectsAmbiguousOrMalformedRequests(t *testing.T) {
	server := newTestServer(t)
	key, _ := newClientKey(t, nil)
	for name, tc := range map[string]struct {
		target     Target
		credential Credential
	}{
		"both methods":    {server.target(), Credential{PrivateKey: key, Password: []byte("x")}},
		"no method":       {server.target(), Credential{}},
		"bad key":         {server.target(), Credential{PrivateKey: []byte("not a key")}},
		"bad user":        {Target{Host: "127.0.0.1", Port: 22, User: "a b"}, Credential{Password: []byte("x")}},
		"bad host":        {Target{Host: "-oProxyCommand=x", Port: 22, User: "relay"}, Credential{Password: []byte("x")}},
		"bad port":        {Target{Host: "127.0.0.1", Port: 0, User: "relay"}, Credential{Password: []byte("x")}},
		"bad host key":    {Target{Host: "127.0.0.1", Port: 22, User: "relay", HostKey: "MD5:aa"}, Credential{Password: []byte("x")}},
		"passphrase only": {server.target(), Credential{Password: []byte("x"), Passphrase: []byte("y")}},
	} {
		if _, err := Dial(context.Background(), tc.target, tc.credential); !errors.Is(err, ErrRequest) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestRunQuotesArgumentsStreamsDataAndReportsExitStatus(t *testing.T) {
	server := newTestServer(t)
	server.password = "secret"
	server.handle = func(command string, stdin io.Reader, stdout, stderr io.Writer) int {
		data, _ := io.ReadAll(stdin)
		_, _ = stdout.Write(append([]byte("got:"), data...))
		_, _ = stderr.Write([]byte("diagnostic"))
		return 3
	}
	client, err := Dial(context.Background(), server.target(), Credential{Password: []byte("secret")})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var out bytes.Buffer
	result, err := client.Run(context.Background(), map[string]string{"PATH": "/usr/local/bin:/usr/bin"},
		[]string{"/usr/local/bin/docker", "inspect", "--format", "json", "relay-a1"}, strings.NewReader("payload"), &out)
	if err != nil {
		t.Fatal(err)
	}
	if result.ExitCode != 3 || out.String() != "got:payload" || string(result.Stderr) != "diagnostic" {
		t.Fatalf("result=%+v stdout=%q", result, out.String())
	}
	want := "PATH='/usr/local/bin:/usr/bin' '/usr/local/bin/docker' 'inspect' '--format' 'json' 'relay-a1'"
	if got := server.recorded(); len(got) != 1 || got[0] != want {
		t.Fatalf("commands = %q", got)
	}
}

func TestShellCommandQuotesEveryPrintableArgument(t *testing.T) {
	got, err := shellCommand(nil, []string{"echo", "it's", "$(id)", "a b", "`x`;|&"})
	want := `'echo' 'it'\''s' '$(id)' 'a b' '` + "`x`" + `;|&'`
	if err != nil || got != want {
		t.Fatalf("command = %q, %v; want %q", got, err, want)
	}
}

func TestRunRefusesUnquotableArgumentsBeforeSending(t *testing.T) {
	server := newTestServer(t)
	server.password = "secret"
	client, err := Dial(context.Background(), server.target(), Credential{Password: []byte("secret")})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	for _, argv := range [][]string{nil, {"echo", ""}, {"echo", "a\nb"}, {"echo", "tab\there"}, {"echo", "caf\u00e9"}} {
		if _, err := client.Run(context.Background(), nil, argv, nil, nil); !errors.Is(err, ErrRequest) {
			t.Errorf("%q: %v", argv, err)
		}
	}
	for _, env := range []map[string]string{{"path": "/bin"}, {"PATH": "/bin dir"}, {"PATH": "$(id)"}} {
		if _, err := client.Run(context.Background(), env, []string{"true"}, nil, nil); !errors.Is(err, ErrRequest) {
			t.Errorf("env %q: %v", env, err)
		}
	}
	if len(server.recorded()) != 0 {
		t.Fatalf("sent %q", server.recorded())
	}
}

func TestRunStopsWhenContextEnds(t *testing.T) {
	server := newTestServer(t)
	server.password = "secret"
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	server.handle = func(string, io.Reader, io.Writer, io.Writer) int { <-release; return 0 }
	client, err := Dial(context.Background(), server.target(), Credential{Password: []byte("secret")})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err = client.Run(ctx, nil, []string{"sleep", "60"}, nil, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run: %v", err)
	}
}
