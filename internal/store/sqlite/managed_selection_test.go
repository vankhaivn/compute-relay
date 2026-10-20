package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"strings"
	"testing"
)

func TestManagedAdmissionSurvivesRoutineCheck(t *testing.T) {
	f := newManagedStateFixture(t)
	original := f.create(t, "a", "selection-create", managedStateSecret)
	f.action(t, "a", original, "selection-check", "check", "")
	f.run(t)
	current := f.get(t, "a", original.ID)
	if current.Selection == nil || current.Selection.Profile == original.Selection.Profile {
		t.Fatal("check did not create a fresh selection")
	}
	_, job := f.admit(t, "a", original, "selection-old-request")
	if job.Profile.Binding.Profile != original.Selection.Profile {
		t.Fatal("admission changed the requested frozen binding")
	}
}

type selectionAdapter struct {
	*stateAdapter
	change   func(*admission.Profile)
	runtime  []byte
	resource *connections.ResourceIdentity
}

func (a selectionAdapter) Profile(binding domain.ProviderBinding, account string) admission.Profile {
	p := a.stateAdapter.Profile(binding, account)
	if a.change != nil {
		a.change(&p)
	}
	return p
}
func (a selectionAdapter) Verify(ctx context.Context, secret []byte) (connections.Verification, error) {
	v, err := a.stateAdapter.Verify(ctx, secret)
	v.Resource = a.resource
	return v, err
}
func (a selectionAdapter) RuntimeConfig() []byte { return a.runtime }

func selectionRequest(t *testing.T, f *managedStateFixture, c connections.Connection) (*admission.Service, []byte) {
	t.Helper()
	s, err := admission.New(f.access, f.s, admission.DefaultLimits())
	requireNoError(t, err)
	raw := strings.Replace(dispatchJSON, `"profile":"fixture"`, `"profile":"`+c.Selection.Profile+`"`, 1)
	return s, []byte(raw)
}

func TestManagedStaleSelectionAdmissionBoundary(t *testing.T) {
	for _, name := range []string{"compatible_limits", "wall_too_small", "credential_replaced", "resource_class", "cost_class", "runtime_identity", "pending_check", "pending_credential", "disabled", "other_workspace", "internet_removed", "bundle_limit", "input_limit", "account_changed", "resource_identity"} {
		t.Run(name, func(t *testing.T) {
			f := newManagedStateFixture(t)
			var err error
			base := selectionAdapter{stateAdapter: f.discovery, change: func(p *admission.Profile) { p.AllowRemoteInternet = true }}
			f.service, err = connections.New(f.access, f.s, f.vault, []connections.Adapter{base})
			requireNoError(t, err)
			original := f.create(t, "a", "selection-create", managedStateSecret)
			svc, raw := selectionRequest(t, f, original)
			if name == "internet_removed" {
				raw = []byte(strings.Replace(string(raw), `"remote_internet":"disabled"`, `"remote_internet":"required"`, 1))
			}
			want := admission.ErrSelection
			switch name {
			case "compatible_limits":
				configureConnection(t, f, original, "selection-configure", 7200)
				want = nil
			case "wall_too_small":
				configureConnection(t, f, original, "selection-configure", 1)
			case "account_changed":
				f.action(t, "a", original, "selection-account", "replace_credential", managedStateOther)
				f.run(t)
			case "credential_replaced":
				f.action(t, "a", original, "selection-rotate", "replace_credential", managedStateRotated)
				f.run(t)
			case "disabled":
				f.action(t, "a", original, "selection-disable", "disable", "")
				f.run(t)
			case "pending_credential":
				f.action(t, "a", original, "selection-rotate", "replace_credential", managedStateRotated)
				want = admission.ErrSelectionPending
			case "pending_check":
				f.action(t, "a", original, "selection-check", "check", "")
				want = admission.ErrSelectionPending
			case "other_workspace":
				want = auth.ErrForbidden
			default:
				changed := base
				changed.change = func(p *admission.Profile) {
					base.change(p)
					switch name {
					case "resource_class":
						p.Accelerator = "gpu"
					case "cost_class":
						p.CostClass = admission.CostOperatorManaged
					case "internet_removed":
						p.AllowRemoteInternet = false
					case "bundle_limit":
						p.MaxBundleBytes = 1
					case "input_limit":
						p.MaxInputBytes = 1
					}
				}
				if name == "resource_identity" {
					changed.resource = &connections.ResourceIdentity{Kind: "machine", ID: "resource_" + strings.Repeat("a", 64)}
				}
				if name == "runtime_identity" {
					changed.runtime = []byte(`{"machine_shape":"different"}`)
				}
				f.service, err = connections.New(f.access, f.s, f.vault, []connections.Adapter{changed})
				requireNoError(t, err)
				f.action(t, "a", original, "selection-check", "check", "")
				f.run(t)
			}
			w := domain.WorkspaceID("a")
			if name == "other_workspace" {
				w = "b"
			}
			_, err = svc.Submit(context.Background(), f.principals[w], w, "selection-request", raw)
			if !errors.Is(err, want) {
				t.Fatalf("admission error = %v, want %v", err, want)
			}
		})
	}
}

func TestManagedCompatibleAdmissionReplayRetainsExactRequestAcrossRestartAndRotation(t *testing.T) {
	f := newManagedStateFixture(t)
	original := f.create(t, "a", "selection-create", managedStateSecret)
	svc, raw := selectionRequest(t, f, original)
	f.action(t, "a", original, "selection-check", "check", "")
	f.run(t)
	first, err := svc.Submit(context.Background(), f.principals["a"], "a", "selection-request", raw)
	requireNoError(t, err)
	current := f.get(t, "a", original.ID)
	f.action(t, "a", current, "selection-rotate", "replace_credential", managedStateRotated)
	f.run(t)
	f.restartManaged(t)
	svc, err = admission.New(f.access, f.s, admission.DefaultLimits())
	requireNoError(t, err)
	replay, err := svc.Submit(context.Background(), f.principals["a"], "a", "selection-request", raw)
	requireNoError(t, err)
	first.Replay = true
	if replay != first {
		t.Fatal("replay changed receipt")
	}
	changed := strings.Replace(string(raw), original.Selection.Profile, f.get(t, "a", original.ID).Selection.Profile, 1)
	_, err = svc.Submit(context.Background(), f.principals["a"], "a", "selection-request", []byte(changed))
	if !errors.Is(err, admission.ErrConflict) {
		t.Fatal("changed request replayed", err)
	}
	job, err := svc.Get(context.Background(), f.principals["a"], "a", first.JobID)
	requireNoError(t, err)
	if job.Profile.Binding.Profile != original.Selection.Profile {
		t.Fatal("admitted binding changed")
	}
}

func TestAdmissionRuntimePolicyChangesPreserveHostIdentity(t *testing.T) {
	original := []byte(`{"machine_id":"same","image":"same","cpus":"4","host_health":{"old":true},"storage_budget":{"old":true}}`)
	policy := []byte(`{"machine_id":"same","image":"same","cpus":"4","host_health":{"new":true},"storage_budget":{"new":true}}`)
	ok, err := sameAdmissionRuntime("ssh", original, policy)
	if err != nil || !ok {
		t.Fatal("policy-only change refused", err)
	}
	for _, field := range []string{"machine_id", "image", "cpus"} {
		var changed map[string]any
		requireNoError(t, json.Unmarshal(policy, &changed))
		changed[field] = "other"
		raw, _ := json.Marshal(changed)
		ok, err = sameAdmissionRuntime("ssh", original, raw)
		if err != nil || ok {
			t.Fatal("host identity change accepted", field, err)
		}
	}
}
