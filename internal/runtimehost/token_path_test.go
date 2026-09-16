package runtimehost

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
)

func TestTokenAncestorAliasCannotEnterProtectedStores(t *testing.T) {
	ctx := context.Background()
	root := initialized(t)
	h, err := Open(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(h.root.Path, alias); err != nil {
		t.Skip("host does not permit test symlink creation")
	}
	if _, err := h.CreateWorkspace(ctx, "app"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"state", "inputs", "results"} {
		output := filepath.Join(alias, name, "must-not-create")
		if _, err := h.IssueToken(ctx, "app", []auth.Scope{auth.Read}, time.Hour, output); err == nil {
			t.Fatal("ancestor alias bypassed protected storage")
		}
		if _, err := os.Lstat(output); !os.IsNotExist(err) {
			t.Fatal("alias wrote a token file")
		}
	}
}

func TestTokenDeliveryPanicStillRevokesTheIssuedDigest(t *testing.T) {
	ctx := context.Background()
	h, err := Open(ctx, initialized(t))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if _, err := h.CreateWorkspace(ctx, "app"); err != nil {
		t.Fatal(err)
	}
	var token string
	receipt, err := h.issueToken(ctx, "app", []auth.Scope{auth.Read}, time.Hour, privateTokenPath(t), func(_ string, raw []byte) error {
		token = strings.TrimSuffix(string(raw), "\n")
		panic("SYNTHETIC_SECRET")
	})
	if err != ErrTokenDelivery || receipt.Delivery != "revoked-after-delivery-error" {
		t.Fatal("delivery panic escaped compensation", err)
	}
	if _, err := h.access.Authenticate(ctx, token); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("panicked delivery token remained usable")
	}
}
