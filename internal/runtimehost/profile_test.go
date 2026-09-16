package runtimehost

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

func profileDocument() ProfileDocument {
	return ProfileDocument{Version: 1, Name: "local-test", InstanceID: "operator_instance", Revision: "rev1", AccountScope: "private_account_alias", CredentialRef: "env:SYNTHETIC_UNRESOLVED_REFERENCE", Enabled: true, CostClass: "free_allowance", MaxWall: 60, MaxBundle: 100 << 20, MaxInputs: 4 << 30}
}
func TestProfileDocumentRejectsAmbiguousPolicy(t *testing.T) {
	d := profileDocument()
	raw, _ := json.Marshal(d)
	parsed, err := ParseProfile(raw)
	if err != nil || parsed != d {
		t.Fatal(parsed, err)
	}
	for _, fault := range []string{
		strings.Replace(string(raw), `"schema_version":1`, `"schema_version":1,"schema_version":1`, 1),
		strings.Replace(string(raw), `"enabled":true`, `"enabled":null`, 1),
		strings.Replace(string(raw), `"allow_remote_internet":false,`, "", 1),
		strings.Replace(string(raw), `"name":`, `"Name":`, 1),
		strings.Replace(string(raw), `"free_allowance"`, `"paid"`, 1),
		strings.Replace(string(raw), `"max_remote_wall_seconds":60`, `"max_remote_wall_seconds":86401`, 1),
		strings.Replace(string(raw), `"schema_version":1`, `"schema_version":1,"token":"SYNTHETIC_SECRET"`, 1),
		string(raw) + `{}`, `null`, strings.Repeat(" ", 8193),
	} {
		if _, err := ParseProfile([]byte(fault)); err == nil {
			t.Fatal("accepted invalid profile policy")
		}
	}
}
func TestProfileRevisionsAndWorkspaceGrantsPersistWithoutEnablingDispatch(t *testing.T) {
	ctx := context.Background()
	root := initialized(t)
	h, err := Open(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	for _, w := range []string{"app", "other"} {
		if _, err := h.CreateWorkspace(ctx, workspaceID(w)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := h.GrantProfile(ctx, "app", "missing", true); err == nil {
		t.Fatal("granted missing profile")
	}
	d := profileDocument()
	first, err := h.ApplyProfile(ctx, d)
	if err != nil || first.ProviderChecked || first.DispatchEnabled || !first.CredentialConfigured {
		t.Fatal(first, err)
	}
	encoded, _ := json.Marshal(first)
	if strings.Contains(string(encoded), d.AccountScope) || strings.Contains(string(encoded), d.CredentialRef) {
		t.Fatal("private binding appeared in receipt")
	}
	w, _ := h.Workspace(ctx, "app")
	if len(w.AllowedProfiles) != 0 {
		t.Fatal("apply implicitly granted access")
	}
	changed := d
	changed.AccountScope = "changed_account"
	if _, err := h.ApplyProfile(ctx, changed); !errors.Is(err, sqlite.ErrConflict) {
		t.Fatal("same revision changed its meaning", err)
	}
	changed.Revision = "rev2"
	second, err := h.ApplyProfile(ctx, changed)
	if err != nil || first.SnapshotSHA256 == second.SnapshotSHA256 {
		t.Fatal(second, err)
	}
	changed.Enabled = false
	disabled, err := h.ApplyProfile(ctx, changed)
	if err != nil || disabled.Enabled || disabled.SnapshotSHA256 != second.SnapshotSHA256 {
		t.Fatal("enabled flag rewrote revision", disabled, err)
	}
	for i := 0; i < 2; i++ {
		if _, err := h.GrantProfile(ctx, "app", d.Name, true); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := h.EnableWorkspace(ctx, "app", false); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	h, err = Open(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	got, err := h.Profile(ctx, d.Name)
	if err != nil || !reflect.DeepEqual(got, disabled) {
		t.Fatal("profile did not survive reopen", got, err)
	}
	w, err = h.Workspace(ctx, "app")
	if err != nil || w.Enabled || !reflect.DeepEqual(w.AllowedProfiles, []string{d.Name}) {
		t.Fatal("grant lost or workspace was implicitly enabled", w, err)
	}
	other, _ := h.Workspace(ctx, "other")
	if len(other.AllowedProfiles) != 0 {
		t.Fatal("grant leaked to another workspace")
	}
	if w, err := h.GrantProfile(ctx, "app", d.Name, false); err != nil || w.Enabled || len(w.AllowedProfiles) != 0 {
		t.Fatal(w, err)
	}
	if _, err := h.Profile(ctx, d.Name); err != nil {
		t.Fatal("revocation removed recovery revision", err)
	}
}
