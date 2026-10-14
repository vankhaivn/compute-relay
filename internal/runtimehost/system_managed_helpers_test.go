package runtimehost

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

const systemManagedSecret = "synthetic-state-secret-canary-initial"
const systemManagedRotated = "synthetic-state-secret-canary-rotated"
const systemManagedOther = "synthetic-state-secret-canary-other-account"

func newManagedLifecycleFixture(t *testing.T) *managedTestFixture {
	t.Helper()
	f := newManagedTestFixtureAt(t, true, &fixedClock{now: time.Now().UTC().Truncate(time.Second)})
	f.config.MaxRemoteWallSeconds = 1800
	f.adapter.config = f.config
	f.adapter.verifyHook = func(_ context.Context, value []byte) (connections.Verification, error) {
		account := "fixture_account_alpha"
		if string(value) == systemManagedOther {
			account = "fixture_account_beta"
		}
		limit, used, left := 60.0, 0.0, 60.0
		return connections.Verification{CanonicalAccount: account, Quota: provider.QuotaObservation{Status: provider.QuotaKnown, Resource: "gpu", Unit: "seconds", Limit: &limit, Used: &used, Remaining: &left, ObservedAt: f.clock.Now(), Source: "synthetic", Precision: "lower_bound"}}, nil
	}
	scenario := fake.DefaultScenario()
	scenario.GPU = domain.CapabilitySupportSupported
	f.useFake(t, scenario)
	return f
}
func (f *managedTestFixture) managedPrincipal(t *testing.T, w domain.WorkspaceID) auth.Principal {
	t.Helper()
	if w == "app" {
		return f.actor
	}
	p, _ := f.principal(t, w, auth.Read, auth.Write, auth.Operate, auth.Manage, auth.Execute)
	return p
}
func systemManagedCreateBody(secret string) []byte {
	raw, _ := json.Marshal(map[string]any{"provider_type": "kaggle", "label": "Fixture", "credentials": map[string]string{"api_token": secret}})
	return raw
}
func systemManagedActionBody(action string, revision int64, secret string) []byte {
	body := map[string]any{"action": action, "expected_revision": revision}
	if secret != "" {
		body["credentials"] = map[string]string{"api_token": secret}
	}
	raw, _ := json.Marshal(body)
	return raw
}
func (f *managedTestFixture) managedRun(t *testing.T) {
	t.Helper()
	worked, err := f.services.connections.RunOnce(t.Context())
	if !worked || err != nil {
		t.Fatal("expected managed operation", worked, err)
	}
}
func (f *managedTestFixture) managedGet(t *testing.T, w domain.WorkspaceID, id string) connections.Connection {
	t.Helper()
	c, err := f.services.connections.Get(t.Context(), f.managedPrincipal(t, w), w, id)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func (f *managedTestFixture) managedCreate(t *testing.T, w domain.WorkspaceID, key, secret string) connections.Connection {
	t.Helper()
	return f.connectionFor(t, f.managedPrincipal(t, w), w, key, secret)
}
func (f *managedTestFixture) managedAction(t *testing.T, w domain.WorkspaceID, c connections.Connection, key, action, secret string) connections.Operation {
	t.Helper()
	op, err := f.services.connections.Submit(t.Context(), f.managedPrincipal(t, w), w, c.ID, key, systemManagedActionBody(action, c.Revision, secret))
	if err != nil {
		t.Fatal(err)
	}
	return op
}
func (f *managedTestFixture) managedAdmit(t *testing.T, w domain.WorkspaceID, c connections.Connection, key string) (admission.Receipt, admission.Record) {
	t.Helper()
	actor := f.managedPrincipal(t, w)
	jobs, err := admission.New(f.host.access, f.host.store, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	spec := f.jobSpecFor(t, actor, w, c)
	receipt, err := jobs.Submit(t.Context(), actor, w, key, systemJSON(t, spec))
	if err != nil {
		t.Fatal(err)
	}
	record, err := jobs.Get(t.Context(), actor, w, receipt.JobID)
	if err != nil {
		t.Fatal(err)
	}
	return receipt, record
}
func (f *managedTestFixture) managedResolvedSecret(t *testing.T, ref string) string {
	t.Helper()
	var value string
	if err := f.services.connections.WithCredential(t.Context(), ports.CredentialRef(ref), func(raw []byte) error { value = string(raw); return nil }); err != nil {
		t.Fatal(err)
	}
	return value
}
func systemConfigureBody(revision, wall int64) []byte {
	return []byte(fmt.Sprintf(`{"action":"configure","expected_revision":%d,"configuration":{"max_remote_wall_seconds":%d}}`, revision, wall))
}
func systemConfigure(t *testing.T, f *managedTestFixture, c connections.Connection, key string, wall int64) connections.Operation {
	t.Helper()
	op, err := f.services.connections.Submit(t.Context(), f.actor, "app", c.ID, key, systemConfigureBody(c.Revision, wall))
	if err != nil || op.Status != "succeeded" || op.Action != "configure" {
		t.Fatal("configuration did not commit locally", op, err)
	}
	return op
}
func systemAuthorizationRequest(attempt domain.AttemptID, wall int64) []byte {
	return []byte(fmt.Sprintf(`{"attempt_id":%q,"max_remote_wall_seconds":%d,"authorize_private_staging":true,"authorize_compute":true}`, attempt, wall))
}

func (f *managedTestFixture) vaultEntries() int {
	f.vault.mu.Lock()
	defer f.vault.mu.Unlock()
	return len(f.vault.entries)
}
