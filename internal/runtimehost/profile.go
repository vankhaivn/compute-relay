package runtimehost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"sort"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/jsonwire"
)

var profileName = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,127}$`)

// ProfileDocument configures local admission only. It does not configure a provider
// implementation, probe an account, enable workers or replace M5-02 runtime TOML.
type ProfileDocument struct {
	Version       int    `json:"schema_version"`
	Name          string `json:"name"`
	InstanceID    string `json:"instance_id"`
	Revision      string `json:"revision"`
	AccountScope  string `json:"account_scope"`
	CredentialRef string `json:"credential_ref,omitempty"`
	Enabled       bool   `json:"enabled"`
	CostClass     string `json:"cost_class"`
	AllowInternet bool   `json:"allow_remote_internet"`
	MaxWall       int64  `json:"max_remote_wall_seconds"`
	MaxBundle     int64  `json:"max_bundle_bytes"`
	MaxInputs     int64  `json:"max_input_bytes"`
}

func (d ProfileDocument) profile() admission.Profile {
	return admission.Profile{
		Binding:      domain.ProviderBinding{Profile: d.Name, ProviderInstanceID: domain.ProviderInstanceID(d.InstanceID), ConfigurationRevision: d.Revision},
		AccountScope: d.AccountScope, CredentialRef: d.CredentialRef, CostClass: d.CostClass,
		AllowRemoteInternet: d.AllowInternet, MaxRemoteWallSeconds: d.MaxWall, MaxBundleBytes: d.MaxBundle, MaxInputBytes: d.MaxInputs,
	}
}
func ParseProfile(raw []byte) (ProfileDocument, error) {
	var d ProfileDocument
	m, err := jsonwire.Object(raw, 8192)
	if err != nil || jsonwire.Fields(m, []string{"schema_version", "name", "instance_id", "revision", "account_scope", "enabled", "cost_class", "allow_remote_internet", "max_remote_wall_seconds", "max_bundle_bytes", "max_input_bytes"}, []string{"credential_ref"}) != nil || json.Unmarshal(raw, &d) != nil || d.Version != 1 || !profileName.MatchString(d.Name) || d.profile().Validate() != nil {
		return ProfileDocument{}, ErrRequest
	}
	return d, nil
}

// ProfileView deliberately omits account scope and credential references. Snapshot
// identity covers the entire private admission revision, not just these public fields.
type ProfileView struct {
	Name                 string `json:"name"`
	InstanceID           string `json:"instance_id"`
	Revision             string `json:"revision"`
	Enabled              bool   `json:"enabled"`
	SnapshotSHA256       string `json:"snapshot_sha256"`
	CredentialConfigured bool   `json:"credential_configured"`
	MaxWall              int64  `json:"max_remote_wall_seconds"`
	MaxBundle            int64  `json:"max_bundle_bytes"`
	MaxInputs            int64  `json:"max_input_bytes"`
	AllowInternet        bool   `json:"allow_remote_internet"`
	ProviderChecked      bool   `json:"provider_checked"`
	DispatchEnabled      bool   `json:"dispatch_enabled"`
}

func profileView(p admission.Profile, enabled bool) ProfileView {
	raw, _ := json.Marshal(p)
	sum := sha256.Sum256(raw)
	return ProfileView{Name: p.Binding.Profile, InstanceID: string(p.Binding.ProviderInstanceID), Revision: p.Binding.ConfigurationRevision, Enabled: enabled,
		SnapshotSHA256: hex.EncodeToString(sum[:]), CredentialConfigured: p.CredentialRef != "", MaxWall: p.MaxRemoteWallSeconds, MaxBundle: p.MaxBundleBytes, MaxInputs: p.MaxInputBytes, AllowInternet: p.AllowRemoteInternet}
}
func (h *Host) Profile(ctx context.Context, name string) (ProfileView, error) {
	if !profileName.MatchString(name) {
		return ProfileView{}, ErrRequest
	}
	p, enabled, err := h.store.ReadProfile(ctx, name)
	if err != nil {
		return ProfileView{}, ErrState
	}
	return profileView(p, enabled), nil
}
func (h *Host) ApplyProfile(ctx context.Context, d ProfileDocument) (ProfileView, error) {
	p := d.profile()
	if d.Version != 1 || !profileName.MatchString(d.Name) || p.Validate() != nil {
		return ProfileView{}, ErrRequest
	}
	if err := h.store.PutProfile(ctx, p, d.Enabled); err != nil {
		return ProfileView{}, err
	}
	return profileView(p, d.Enabled), nil
}

// GrantProfile changes only the named workspace's allowlist. CLI administration
// owns the exclusive installation lock and is serialized; no HTTP admin is added.
// Revocation need not resolve an obsolete profile, and never erases its revisions.
func (h *Host) GrantProfile(ctx context.Context, workspace domain.WorkspaceID, name string, grant bool) (WorkspaceView, error) {
	if !workspace.Valid() || !profileName.MatchString(name) {
		return WorkspaceView{}, ErrRequest
	}
	if grant {
		if _, _, err := h.store.ReadProfile(ctx, name); err != nil {
			return WorkspaceView{}, ErrRequest
		}
	}
	w, err := h.store.LookupWorkspace(ctx, workspace)
	if err != nil || w.ID != workspace {
		return WorkspaceView{}, ErrState
	}
	result := make([]string, 0, len(w.AllowedProfiles)+1)
	for _, p := range w.AllowedProfiles {
		if p != name {
			result = append(result, p)
		}
	}
	if grant {
		result = append(result, name)
	}
	sort.Strings(result)
	w.AllowedProfiles = result
	if err := h.store.PutWorkspace(ctx, w); err != nil {
		return WorkspaceView{}, ErrState
	}
	return h.Workspace(ctx, workspace)
}
