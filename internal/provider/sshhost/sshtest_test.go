package sshhost

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/testsupport/sshtest"
	"golang.org/x/crypto/ssh"
)

// testServer adapts the shared in-process SSH server to this package's Target.
type testServer struct{ *sshtest.Server }

func newTestServer(t *testing.T) *testServer { return &testServer{sshtest.New(t)} }

func (s *testServer) target() Target {
	host, port, fingerprint := s.Start()
	return Target{Host: host, Port: port, User: s.User, HostKey: fingerprint}
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
