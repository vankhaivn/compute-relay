// Package sshhost runs finite attempts in Docker on an operator-owned host reached over
// SSH. Credentials come from the Relay vault for each call; the operator's agent, keys
// and SSH configuration are never consulted. The pinned host key is the host identity.
package sshhost

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
)

var (
	ErrRequest          = errors.New("invalid SSH host request")
	ErrCredentialReject = errors.New("SSH host rejected the credential")
	ErrHostKeyMismatch  = errors.New("SSH host key does not match the pinned identity")
	ErrUnavailable      = errors.New("SSH host unavailable")
)

const (
	handshakeTimeout = 30 * time.Second
	maxStderrBytes   = 16 << 10
)

var (
	hostName    = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$`)
	userName    = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9._-]{0,63}$`)
	fingerprint = regexp.MustCompile(`^SHA256:[A-Za-z0-9+/]{43}$`)
	safeArg     = regexp.MustCompile(`^[A-Za-z0-9_@%+=:,./-]+$`)
	printable   = regexp.MustCompile(`^[\x20-\x7e]+$`)
	envName     = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)
)

// Target is the non-secret endpoint. HostKey is an OpenSSH SHA256 fingerprint; it is
// empty only while a connection is first verified and the presented key is pinned.
type Target struct {
	Host    string
	Port    int
	User    string
	HostKey string
}

func (t Target) Validate(requireHostKey bool) error {
	host := strings.TrimSuffix(strings.TrimPrefix(t.Host, "["), "]")
	if (!hostName.MatchString(t.Host) && net.ParseIP(host) == nil) || t.Port < 1 || t.Port > 65535 || !userName.MatchString(t.User) ||
		(t.HostKey != "" || requireHostKey) && !fingerprint.MatchString(t.HostKey) {
		return ErrRequest
	}
	return nil
}

func (t Target) address() string {
	return net.JoinHostPort(strings.TrimSuffix(strings.TrimPrefix(t.Host, "["), "]"), strconv.Itoa(t.Port))
}

// Credential holds exactly one login method. Callers own and clear the buffers.
type Credential struct {
	PrivateKey []byte
	Passphrase []byte
	Password   []byte
}

func (c Credential) methods() ([]ssh.AuthMethod, error) {
	switch {
	case len(c.PrivateKey) > 0 && len(c.Password) == 0:
		var signer ssh.Signer
		var err error
		if len(c.Passphrase) > 0 {
			signer, err = ssh.ParsePrivateKeyWithPassphrase(c.PrivateKey, c.Passphrase)
		} else {
			signer, err = ssh.ParsePrivateKey(c.PrivateKey)
		}
		if err != nil {
			return nil, ErrRequest
		}
		return []ssh.AuthMethod{ssh.PublicKeys(signer)}, nil
	case len(c.Password) > 0 && len(c.PrivateKey) == 0 && len(c.Passphrase) == 0:
		password := string(c.Password)
		// macOS sshd normally asks through keyboard-interactive PAM prompts. Answer only
		// a single hidden prompt; anything else is not a password login.
		interactive := ssh.KeyboardInteractive(func(_, _ string, questions []string, echos []bool) ([]string, error) {
			switch {
			case len(questions) == 0:
				return nil, nil
			case len(questions) == 1 && !echos[0]:
				return []string{password}, nil
			}
			return nil, ErrCredentialReject
		})
		return []ssh.AuthMethod{ssh.Password(password), interactive}, nil
	}
	return nil, ErrRequest
}

// Client is one authenticated connection. Each provider step dials its own client.
type Client struct {
	conn    *ssh.Client
	HostKey string
	stop    func() bool
}

// Dial authenticates against the pinned host key, or pins the presented key when the
// target has none. Context cancellation closes the connection.
func Dial(ctx context.Context, target Target, credential Credential) (*Client, error) {
	if err := target.Validate(false); err != nil {
		return nil, err
	}
	methods, err := credential.methods()
	if err != nil {
		return nil, err
	}
	var presented string
	mismatch := false
	config := &ssh.ClientConfig{
		User: target.User,
		Auth: methods,
		HostKeyCallback: func(_ string, _ net.Addr, key ssh.PublicKey) error {
			presented = ssh.FingerprintSHA256(key)
			if target.HostKey != "" && presented != target.HostKey {
				mismatch = true
				return ErrHostKeyMismatch
			}
			return nil
		},
		Timeout: handshakeTimeout,
	}
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", target.address())
	if err != nil {
		return nil, ErrUnavailable
	}
	deadline := time.Now().Add(handshakeTimeout)
	if limit, ok := ctx.Deadline(); ok && limit.Before(deadline) {
		deadline = limit
	}
	_ = conn.SetDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	sshConn, channels, requests, err := ssh.NewClientConn(conn, target.address(), config)
	if err != nil {
		stop()
		_ = conn.Close()
		switch {
		case mismatch:
			return nil, ErrHostKeyMismatch
		case presented != "" && strings.Contains(err.Error(), "unable to authenticate"):
			return nil, ErrCredentialReject
		}
		return nil, ErrUnavailable
	}
	_ = conn.SetDeadline(time.Time{})
	return &Client{conn: ssh.NewClient(sshConn, channels, requests), HostKey: presented, stop: stop}, nil
}

func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	c.stop()
	return c.conn.Close()
}

// Result is a finished remote command. Stderr is bounded diagnostic evidence only.
type Result struct {
	ExitCode int
	Stderr   []byte
}

// Run executes argv through the login shell with each argument single-quoted. Arguments
// must be non-empty printable ASCII; environment values use a conservative character set.
// A zero exit code is not implied by a nil error; callers check ExitCode.
func (c *Client) Run(ctx context.Context, env map[string]string, argv []string, stdin io.Reader, stdout io.Writer) (Result, error) {
	command, err := shellCommand(env, argv)
	if err != nil {
		return Result{}, err
	}
	session, err := c.conn.NewSession()
	if err != nil {
		return Result{}, ErrUnavailable
	}
	defer session.Close()
	stderr := &boundedBuffer{limit: maxStderrBytes}
	session.Stdin, session.Stdout, session.Stderr = stdin, stdout, stderr
	if stdout == nil {
		session.Stdout = io.Discard
	}
	done := make(chan error, 1)
	if err = session.Start(command); err != nil {
		return Result{}, ErrUnavailable
	}
	go func() { done <- session.Wait() }()
	select {
	case <-ctx.Done():
		_ = session.Signal(ssh.SIGKILL)
		_ = session.Close()
		<-done
		return Result{}, ctx.Err()
	case err = <-done:
	}
	var exit *ssh.ExitError
	switch {
	case err == nil:
		return Result{Stderr: stderr.Bytes()}, nil
	case errors.As(err, &exit):
		return Result{ExitCode: exit.ExitStatus(), Stderr: stderr.Bytes()}, nil
	}
	return Result{Stderr: stderr.Bytes()}, ErrUnavailable
}

func shellCommand(env map[string]string, argv []string) (string, error) {
	if len(argv) == 0 {
		return "", ErrRequest
	}
	var b strings.Builder
	for _, name := range slices.Sorted(maps.Keys(env)) {
		if !envName.MatchString(name) || !safeArg.MatchString(env[name]) {
			return "", ErrRequest
		}
		fmt.Fprintf(&b, "%s='%s' ", name, env[name])
	}
	for i, arg := range argv {
		if !printable.MatchString(arg) {
			return "", ErrRequest
		}
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString("'" + strings.ReplaceAll(arg, "'", `'\''`) + "'")
	}
	return b.String(), nil
}

type boundedBuffer struct {
	bytes.Buffer
	limit int
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if room := b.limit - b.Len(); room > 0 {
		if len(p) > room {
			b.Buffer.Write(p[:room])
		} else {
			b.Buffer.Write(p)
		}
	}
	return len(p), nil
}
