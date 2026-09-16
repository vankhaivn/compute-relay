package runtimehost

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/statefs"
)

type WorkspaceView struct {
	ID              domain.WorkspaceID `json:"id"`
	Enabled         bool               `json:"enabled"`
	AllowedProfiles []string           `json:"allowed_profiles"`
}

// CreateWorkspace starts with no allowed profiles. General profile configuration
// belongs to the next CLI/configuration slice; no pretend executable profile is seeded.
func (h *Host) CreateWorkspace(ctx context.Context, id domain.WorkspaceID) (WorkspaceView, error) {
	if !id.Valid() {
		return WorkspaceView{}, ErrRequest
	}
	if _, err := h.store.LookupWorkspace(ctx, id); !errors.Is(err, auth.ErrNotFound) {
		return WorkspaceView{}, ErrState // Never overwrite authority in a create operation.
	}
	if err := h.store.PutWorkspace(ctx, auth.Workspace{ID: id, Enabled: true}); err != nil {
		return WorkspaceView{}, ErrState
	}
	return h.Workspace(ctx, id)
}
func (h *Host) Workspace(ctx context.Context, id domain.WorkspaceID) (WorkspaceView, error) {
	if !id.Valid() {
		return WorkspaceView{}, ErrRequest
	}
	w, err := h.store.LookupWorkspace(ctx, id)
	if err != nil || w.ID != id {
		return WorkspaceView{}, ErrState
	}
	profiles := append([]string{}, w.AllowedProfiles...)
	sort.Strings(profiles)
	return WorkspaceView{w.ID, w.Enabled, profiles}, nil
}
func (h *Host) EnableWorkspace(ctx context.Context, id domain.WorkspaceID, enabled bool) (WorkspaceView, error) {
	if !id.Valid() {
		return WorkspaceView{}, ErrRequest
	}
	w, err := h.store.LookupWorkspace(ctx, id)
	if err != nil || w.ID != id {
		return WorkspaceView{}, ErrState
	}
	w.Enabled = enabled
	if err := h.store.PutWorkspace(ctx, w); err != nil {
		return WorkspaceView{}, ErrState
	}
	return h.Workspace(ctx, id)
}

type TokenReceipt struct {
	ID          string             `json:"id"`
	WorkspaceID domain.WorkspaceID `json:"workspace_id"`
	Scopes      []auth.Scope        `json:"scopes"`
	ExpiresAt   time.Time           `json:"expires_at"`
	Delivery    string              `json:"delivery"`
}

var ErrTokenDelivery = errors.New("token file delivery failed; inspect the non-secret receipt and preserve any partial file")

// IssueToken persists only the digest and writes the secret once to an explicit
// NEW file in a private operator directory. It never returns the secret for logging.
// Failure attempts independent-context revocation; a crash between two resources
// cannot be atomic, so bounded expiry and the retained token ID remain important.
func (h *Host) IssueToken(ctx context.Context, id domain.WorkspaceID, scopes []auth.Scope, ttl time.Duration, output string) (TokenReceipt, error) {
	return h.issueToken(ctx, id, scopes, ttl, output, func(path string, raw []byte) error {
		if err := statefs.WriteNew(path, raw); err != nil {
			return err
		}
		return statefs.SyncDir(filepath.Dir(path))
	})
}
func (h *Host) issueToken(ctx context.Context, id domain.WorkspaceID, scopes []auth.Scope, ttl time.Duration, output string, publish func(string, []byte) error) (TokenReceipt, error) {
	var receipt TokenReceipt
	if !id.Valid() || ttl < time.Minute || ttl > 30*24*time.Hour || output == "" || len(scopes) == 0 || len(scopes) > 3 {
		return receipt, ErrRequest
	}
	seen := map[auth.Scope]bool{}
	for _, scope := range scopes {
		if seen[scope] || (scope != auth.Read && scope != auth.Write && scope != auth.Operate) {
			return receipt, ErrRequest
		}
		seen[scope] = true
	}
	output, err := filepath.Abs(output)
	if err != nil || statefs.CheckDir(filepath.Dir(output)) != nil {
		return receipt, ErrRequest
	}
	relative, err := filepath.Rel(h.root.Path, output)
	if err != nil {
		return receipt, ErrRequest
	}
	first := strings.Split(filepath.ToSlash(relative), "/")[0]
	if first == "state" || first == "inputs" || first == "results" || relative == "." {
		return receipt, ErrRequest
	}
	if _, err := os.Lstat(output); !errors.Is(err, os.ErrNotExist) {
		return receipt, ErrRequest
	}
	secret, record, err := h.access.Issue(ctx, id, scopes, time.Now().UTC().Add(ttl))
	if err != nil {
		return receipt, ErrState
	}
	receipt = TokenReceipt{record.ID, record.WorkspaceID, append([]auth.Scope{}, record.Scopes...), record.ExpiresAt, "written-once"}
	raw := []byte(secret.Reveal() + "\n")
	defer clear(raw)
	if err := publishSecret(output, raw, publish); err != nil {
		revoke, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		receipt.Delivery = "revoked-after-delivery-error"
		if h.access.Revoke(revoke, record.ID) != nil {
			receipt.Delivery = "revocation-unconfirmed"
		}
		return receipt, ErrTokenDelivery
	}
	return receipt, nil
}
func (h *Host) RevokeToken(ctx context.Context, id string) error {
	if len(id) != 36 || !strings.HasPrefix(id, "tok_") {
		return ErrRequest
	}
	for _, c := range id[4:] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return ErrRequest
		}
	}
	if err := h.access.Revoke(ctx, id); err != nil {
		return ErrState
	}
	return nil
}

func publishSecret(path string, raw []byte, publish func(string, []byte) error) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrTokenDelivery
		}
	}()
	return publish(path, raw)
}
