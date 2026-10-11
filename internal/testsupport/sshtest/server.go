// Package sshtest runs an in-process SSH server for offline adapter tests. Exec
// requests are answered by Handle; no shell, port forwarding or file transfer exists.
package sshtest

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"

	"golang.org/x/crypto/ssh"
)

// Server accepts User with Password or ClientKey. Configure the exported fields
// before the first Start call; that call begins serving.
type Server struct {
	User      string
	Password  string
	ClientKey ssh.PublicKey
	// InteractiveOnly rejects the "password" method, as macOS sshd usually does.
	InteractiveOnly bool
	// Handle receives the exact remote command string and the session stdin.
	Handle func(command string, stdin io.Reader, stdout, stderr io.Writer) int

	listener net.Listener
	hostKey  ssh.Signer
	start    sync.Once
	mu       sync.Mutex
	commands []string
}

func New(t testing.TB) *Server {
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
	s := &Server{User: "relay", listener: listener, hostKey: signer}
	s.Handle = func(string, io.Reader, io.Writer, io.Writer) int { return 0 }
	t.Cleanup(func() { _ = listener.Close() })
	return s
}

// Start serves once and returns the loopback address and SHA256 host-key fingerprint.
func (s *Server) Start() (host string, port int, fingerprint string) {
	s.start.Do(func() { go s.serve() })
	return "127.0.0.1", s.listener.Addr().(*net.TCPAddr).Port, ssh.FingerprintSHA256(s.hostKey.PublicKey())
}

// Close stops accepting connections.
func (s *Server) Close() error { return s.listener.Close() }

// Commands returns every exec command received so far.
func (s *Server) Commands() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.commands...)
}

var errReject = errors.New("rejected")

func (s *Server) config() *ssh.ServerConfig {
	config := &ssh.ServerConfig{
		PublicKeyCallback: func(meta ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			if s.ClientKey != nil && meta.User() == s.User && bytes.Equal(key.Marshal(), s.ClientKey.Marshal()) {
				return nil, nil
			}
			return nil, errReject
		},
		KeyboardInteractiveCallback: func(meta ssh.ConnMetadata, challenge ssh.KeyboardInteractiveChallenge) (*ssh.Permissions, error) {
			answers, err := challenge("", "", []string{"Password:"}, []bool{false})
			if err == nil && s.Password != "" && meta.User() == s.User && len(answers) == 1 && answers[0] == s.Password {
				return nil, nil
			}
			return nil, errReject
		},
	}
	if !s.InteractiveOnly {
		config.PasswordCallback = func(meta ssh.ConnMetadata, password []byte) (*ssh.Permissions, error) {
			if s.Password != "" && meta.User() == s.User && string(password) == s.Password {
				return nil, nil
			}
			return nil, errReject
		}
	}
	config.AddHostKey(s.hostKey)
	return config
}

func (s *Server) serve() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.serveConn(conn)
	}
}

func (s *Server) serveConn(raw net.Conn) {
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

func (s *Server) serveSession(channel ssh.Channel, requests <-chan *ssh.Request) {
	defer channel.Close()
	for request := range requests {
		if request.Type != "exec" || len(request.Payload) < 4 {
			_ = request.Reply(false, nil)
			continue
		}
		length := binary.BigEndian.Uint32(request.Payload)
		if int(length) > len(request.Payload)-4 {
			_ = request.Reply(false, nil)
			continue
		}
		command := string(request.Payload[4 : 4+length])
		_ = request.Reply(true, nil)
		s.mu.Lock()
		s.commands = append(s.commands, command)
		handle := s.Handle
		s.mu.Unlock()
		code := handle(command, channel, channel, channel.Stderr())
		status := make([]byte, 4)
		binary.BigEndian.PutUint32(status, uint32(code))
		_, _ = channel.SendRequest("exit-status", false, status)
		return
	}
}
