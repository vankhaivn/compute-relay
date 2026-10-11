package sshhost

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"encoding/pem"
	"io"
	"net"
	"sync"
	"testing"

	"golang.org/x/crypto/ssh"
)

// testServer is an in-process SSH server. Exec requests are answered by handle, which
// receives the exact remote command string and the session stdin. Configure fields before
// the first target() call; that call starts serving.
type testServer struct {
	t        *testing.T
	listener net.Listener
	hostKey  ssh.Signer
	user     string
	password string
	clientPK ssh.PublicKey
	// interactiveOnly rejects the "password" method, as macOS sshd usually does.
	interactiveOnly bool

	start    sync.Once
	mu       sync.Mutex
	commands []string
	handle   func(command string, stdin io.Reader, stdout, stderr io.Writer) int
}

func newTestServer(t *testing.T) *testServer {
	t.Helper()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(private)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &testServer{t: t, listener: listener, hostKey: signer, user: "relay"}
	s.handle = func(string, io.Reader, io.Writer, io.Writer) int { return 0 }
	t.Cleanup(func() { _ = listener.Close() })
	return s
}

func (s *testServer) target() Target {
	s.start.Do(func() { go s.serve() })
	return Target{Host: "127.0.0.1", Port: s.listener.Addr().(*net.TCPAddr).Port, User: s.user, HostKey: ssh.FingerprintSHA256(s.hostKey.PublicKey())}
}

func (s *testServer) recorded() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.commands...)
}

func (s *testServer) config() *ssh.ServerConfig {
	config := &ssh.ServerConfig{
		PublicKeyCallback: func(meta ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			if s.clientPK != nil && meta.User() == s.user && bytes.Equal(key.Marshal(), s.clientPK.Marshal()) {
				return nil, nil
			}
			return nil, errReject
		},
		KeyboardInteractiveCallback: func(meta ssh.ConnMetadata, challenge ssh.KeyboardInteractiveChallenge) (*ssh.Permissions, error) {
			answers, err := challenge("", "", []string{"Password:"}, []bool{false})
			if err == nil && s.password != "" && meta.User() == s.user && len(answers) == 1 && answers[0] == s.password {
				return nil, nil
			}
			return nil, errReject
		},
	}
	if !s.interactiveOnly {
		config.PasswordCallback = func(meta ssh.ConnMetadata, password []byte) (*ssh.Permissions, error) {
			if s.password != "" && meta.User() == s.user && string(password) == s.password {
				return nil, nil
			}
			return nil, errReject
		}
	}
	config.AddHostKey(s.hostKey)
	return config
}

var errReject = io.ErrUnexpectedEOF

func (s *testServer) serve() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.serveConn(conn)
	}
}

func (s *testServer) serveConn(raw net.Conn) {
	defer raw.Close()
	conn, channels, requests, err := ssh.NewServerConn(raw, s.config())
	if err != nil {
		return
	}
	defer conn.Close()
	go ssh.DiscardRequests(requests)
	for newChannel := range channels {
		if newChannel.ChannelType() != "session" {
			_ = newChannel.Reject(ssh.UnknownChannelType, "")
			continue
		}
		channel, channelRequests, err := newChannel.Accept()
		if err != nil {
			return
		}
		go s.serveSession(channel, channelRequests)
	}
}

func (s *testServer) serveSession(channel ssh.Channel, requests <-chan *ssh.Request) {
	defer channel.Close()
	for request := range requests {
		if request.Type != "exec" || len(request.Payload) < 4 {
			_ = request.Reply(false, nil)
			continue
		}
		length := binary.BigEndian.Uint32(request.Payload)
		command := string(request.Payload[4 : 4+length])
		_ = request.Reply(true, nil)
		s.mu.Lock()
		s.commands = append(s.commands, command)
		handle := s.handle
		s.mu.Unlock()
		code := handle(command, channel, channel, channel.Stderr())
		status := make([]byte, 4)
		binary.BigEndian.PutUint32(status, uint32(code))
		_, _ = channel.SendRequest("exit-status", false, status)
		return
	}
}

func newClientKey(t *testing.T, passphrase []byte) ([]byte, ssh.PublicKey) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	var block *pem.Block
	if passphrase != nil {
		block, err = ssh.MarshalPrivateKeyWithPassphrase(private, "", passphrase)
	} else {
		block, err = ssh.MarshalPrivateKey(private, "")
	}
	if err != nil {
		t.Fatal(err)
	}
	key, err := ssh.NewPublicKey(public)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(block), key
}
