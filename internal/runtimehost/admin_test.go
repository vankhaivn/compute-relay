package runtimehost

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/statefs"
)

func TestLocalWorkspaceAndTokenAuthorityPersistAcrossReopen(t *testing.T) {
	ctx := context.Background()
	path := initialized(t)
	h, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	w, err := h.CreateWorkspace(ctx, "app")
	if err != nil || !w.Enabled || len(w.AllowedProfiles) != 0 {
		t.Fatal(w, err)
	}
	if _, err := h.CreateWorkspace(ctx, "app"); err == nil {
		t.Fatal("create overwrote workspace")
	}
	tokenPath := privateTokenPath(t)
	receipt, err := h.IssueToken(ctx, "app", []auth.Scope{auth.Read, auth.Write}, time.Hour, tokenPath)
	if err != nil || receipt.Delivery != "written-once" {
		t.Fatal(receipt, err)
	}
	data, err := readPrivate(tokenPath, 100)
	if err != nil {
		t.Fatal(err)
	}
	token := strings.TrimSuffix(string(data), "\n")
	if len(token) != 47 {
		t.Fatal("invalid issued secret")
	}
	encoded, _ := json.Marshal(receipt)
	if strings.Contains(string(encoded), token) || strings.Contains(string(encoded), "Digest") {
		t.Fatal("secret/digest in receipt")
	}
	if _, err := h.IssueToken(ctx, "app", []auth.Scope{auth.Read}, time.Hour, tokenPath); err == nil {
		t.Fatal("token file overwritten")
	}
	if _, err := h.EnableWorkspace(ctx, "app", false); err != nil {
		t.Fatal(err)
	}
	if _, err := h.access.Authenticate(ctx, token); !errors.Is(err, auth.ErrForbidden) {
		t.Fatal("disabled authority accepted", err)
	}
	h.Close()
	h, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	w, err = h.Workspace(ctx, "app")
	if err != nil || w.Enabled {
		t.Fatal("disabled workspace not durable", err)
	}
	if _, err := h.EnableWorkspace(ctx, "app", true); err != nil {
		t.Fatal(err)
	}
	principal, err := h.access.Authenticate(ctx, token)
	if err != nil || principal.WorkspaceID() != "app" {
		t.Fatal("token did not survive reopen", err)
	}
	if err := auth.Require(principal, "app", auth.Operate); !errors.Is(err, auth.ErrForbidden) {
		t.Fatal("scope escalation")
	}
	if err := h.RevokeToken(ctx, receipt.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.access.Authenticate(ctx, token); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("revocation ignored", err)
	}
}

func TestLocalTokenDeliveryFailureRevokesAndClearsOwnedBuffer(t *testing.T) {
	ctx := context.Background()
	h, err := Open(ctx, initialized(t))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if _, err := h.CreateWorkspace(ctx, "app"); err != nil {
		t.Fatal(err)
	}
	var retained []byte
	var token string
	out := privateTokenPath(t)
	receipt, err := h.issueToken(ctx, "app", []auth.Scope{auth.Read}, time.Hour, out, func(_ string, raw []byte) error {
		retained = raw
		token = strings.TrimSuffix(string(raw), "\n")
		return errors.New("SYNTHETIC_SECRET")
	})
	if !errors.Is(err, ErrTokenDelivery) || receipt.Delivery != "revoked-after-delivery-error" || receipt.ID == "" {
		t.Fatal(receipt, err)
	}
	for _, b := range retained {
		if b != 0 {
			t.Fatal("owned token bytes retained")
		}
	}
	if _, err := h.access.Authenticate(ctx, token); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("failed delivery still authorized", err)
	}
	if strings.Contains(err.Error(), "SYNTHETIC") {
		t.Fatal("raw delivery error reflected")
	}
}
func TestLocalTokenRejectsProtectedDestinationsAndInvalidAuthority(t *testing.T) {
	ctx := context.Background()
	path := initialized(t)
	h, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	h.CreateWorkspace(ctx, "app")
	for _, name := range []string{"state/secret", "inputs/secret", "results/secret", "STATE/secret"} {
		dst := filepath.Join(path, filepath.FromSlash(name))
		if _, err := h.IssueToken(ctx, "app", []auth.Scope{auth.Read}, time.Hour, dst); err == nil {
			t.Fatal("credential entered store root")
		}
		if _, err := os.Lstat(dst); !os.IsNotExist(err) {
			t.Fatal("invalid request wrote bytes")
		}
	}
	for _, scopes := range [][]auth.Scope{nil, {auth.Read, auth.Read}, {"admin"}} {
		if _, err := h.IssueToken(ctx, "app", scopes, time.Hour, privateTokenPath(t)); err == nil {
			t.Fatal("invalid scope accepted")
		}
	}
	if _, err := h.IssueToken(ctx, "missing", []auth.Scope{auth.Read}, time.Hour, privateTokenPath(t)); err == nil {
		t.Fatal("missing workspace token")
	}
	if _, err := h.EnableWorkspace(ctx, "missing", true); err == nil {
		t.Fatal("enable created workspace")
	}
}

func privateTokenPath(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "secrets")
	if _, err := statefs.PrivateDir(path, true); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(path, "token")
}
