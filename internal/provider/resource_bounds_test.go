package provider

import (
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

func TestSharedResourceKeyPreservesContentIdentity(t *testing.T) {
	sha := domain.SHA256Digest(strings.Repeat("a", 64))
	if SharedResourceKey(sha, 0) == "" || SharedResourceKey(sha, 0) == SharedResourceKey(sha, 1) || SharedResourceKey(sha, -1) != "" || SharedResourceKey("bad", 1) != "" {
		t.Fatal("invalid content key or collapsed byte identity")
	}
}
