package sqlite

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/hosthealth"
)

func TestHostHealthAttemptTargetKeepsFrozenPolicyAndMachine(t *testing.T) {
	f, _ := healthStateFixture(t)
	c := createHealthConnection(t, f, "a", "guard-frozen-target", "SYNTHETIC_LAN")
	_, record := f.admit(t, "a", c, "guard-original-attempt")
	original, err := f.s.HostHealthTargetForAttempt(t.Context(), record)
	if err != nil || original == nil || original.Policy == nil || *original.Policy != hosthealth.DefaultPolicy() {
		t.Fatal("original frozen policy unavailable", err)
	}
	c = configureHealthPolicy(t, f, c, "guard-current-policy", `{"low_battery_percent":0,"critical_duration_seconds":900}`)
	current, err := f.s.ResolveHostHealthTarget(t.Context(), "a", f.principals["a"].TokenID(), c.ID)
	requireNoError(t, err)
	frozen, err := f.s.HostHealthTargetForAttempt(t.Context(), record)
	if err != nil || frozen == nil || current.Policy == nil || current.Policy.LowBatteryPercent != 0 || *frozen.Policy != *original.Policy || frozen.ResourceID != original.ResourceID || string(frozen.RuntimeConfig) != string(original.RuntimeConfig) {
		t.Fatal("current configuration replaced exact-attempt policy", err)
	}
	changed := record
	changed.Profile.Binding.ConfigurationRevision += "_different"
	if _, err := f.s.HostHealthTargetForAttempt(t.Context(), changed); !errors.Is(err, ErrCorrupt) {
		t.Fatal("a different binding supplied target evidence", err)
	}
	_, err = f.s.db.Exec(`UPDATE managed_resource_accounts SET resource_scope=? WHERE account_scope=?`, "resource_"+strings.Repeat("f", 64), record.Profile.AccountScope)
	if err == nil {
		t.Fatal("verified machine association was mutable")
	}
	changed = record
	changed.Profile.ResourceScope = "resource_" + strings.Repeat("f", 64)
	if _, err := f.s.HostHealthTargetForAttempt(t.Context(), changed); !errors.Is(err, ErrCorrupt) {
		t.Fatal("another machine was accepted as the historical execution location", err)
	}
}

func TestHostHealthAdmissionPreviewAuthorizesAndReplaysBeforeCurrentSelection(t *testing.T) {
	f, _ := healthStateFixture(t)
	c := createHealthConnection(t, f, "a", "guard-preview", "SYNTHETIC_LAN")
	first, record := f.admit(t, "a", c, "guard-preview-original")
	key, err := admission.KeyDigest("guard-preview-original")
	requireNoError(t, err)
	if _, err := f.s.PreviewHostHealthAdmission(context.Background(), "a", f.principals["b"].TokenID(), key, record.Request); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("foreign actor obtained admission replay", err)
	}
	f.action(t, "a", c, "guard-preview-disable", "disable", "")
	f.run(t)
	preview, err := f.s.PreviewHostHealthAdmission(t.Context(), "a", f.principals["a"].TokenID(), key, record.Request)
	first.Replay = true
	if err != nil || preview.Replay == nil || *preview.Replay != first || preview.Target != nil {
		t.Fatal("replay resolved disabled current selection", preview.Replay, err)
	}
	newKey, err := admission.KeyDigest("guard-preview-new")
	requireNoError(t, err)
	if _, err := f.s.PreviewHostHealthAdmission(t.Context(), "a", f.principals["a"].TokenID(), newKey, record.Request); err == nil {
		t.Fatal("new admission used disabled selection")
	}
}
