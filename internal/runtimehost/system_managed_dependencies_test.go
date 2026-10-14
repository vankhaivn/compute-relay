package runtimehost

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

func TestManagedRemovalRejectsQueuedAmbiguousAndCollectionDependencies(t *testing.T) {
	for _, phase := range []string{"queued", "ambiguous", "collection"} {
		t.Run(phase, func(t *testing.T) {
			f := newManagedLifecycleFixture(t)
			scenario := fake.DefaultScenario()
			scenario.GPU = domain.CapabilitySupportSupported
			if phase == "ambiguous" {
				scenario.Mode = fake.Unresolved
			}
			f.useFake(t, scenario)
			connection := f.managedCreate(t, "app", "dependent-create-key", systemManagedSecret)
			receipt, _ := f.managedAdmit(t, "app", connection, "dependent-job-key")
			if phase != "queued" {
				f.authorizeJob(t, receipt)
				f.dispatchOnce(t)
				f.dispatchOnce(t)
				if phase == "ambiguous" {
					target, err := f.host.store.ReadLogAttempt(t.Context(), "app", f.actor.TokenID(), receipt.JobID, receipt.AttemptID)
					if err != nil || target.Remote != nil || f.backend.Stats().SubmitCalls != 1 {
						t.Fatal("fixture did not preserve submission uncertainty", err)
					}
				} else {
					f.runToCollection(t, receipt)
					record, err := f.host.store.ReadJob(t.Context(), "app", f.actor.TokenID(), receipt.JobID)
					if err != nil || record.Attempt.State.Orchestration != domain.OrchestrationCollecting {
						t.Fatal("fixture lacks retained collection dependency", record.Attempt.State, err)
					}
				}
			}
			before := f.vault.deletes
			_, err := f.services.connections.Submit(t.Context(), f.actor, "app", connection.ID, "blocked-remove-key", systemManagedActionBody("remove", connection.Revision, ""))
			if !errors.Is(err, connections.ErrActiveWork) {
				t.Fatal("dependent removal accepted", phase, err)
			}
			current := f.managedGet(t, "app", connection.ID)
			if current.Revision != connection.Revision || current.NewWork != "enabled" || f.vault.deletes != before {
				t.Fatal("rejected removal changed connection")
			}
		})
	}
}
func TestManagedResourceRecheckCannotReassignAcceptedWork(t *testing.T) {
	f := newManagedLifecycleFixture(t)
	base := f.adapter.verifyHook
	f.adapter.descriptorHook = func(d connections.Descriptor) connections.Descriptor {
		d.Capabilities.Concurrency = &connections.ConcurrencyCapability{Support: "supported", Scope: "resource", Minimum: 1, Maximum: 64, Default: 1}
		return d
	}
	changed := false
	f.adapter.verifyHook = func(ctx context.Context, value []byte) (connections.Verification, error) {
		result, err := base(ctx, value)
		native := strings.Repeat("a", 64)
		if changed {
			native = strings.Repeat("c", 64)
		}
		result.Resource = &connections.ResourceIdentity{Kind: "machine", ID: "resource_" + native}
		return result, err
	}
	connection := f.managedCreate(t, "app", "bound-lan", systemManagedSecret)
	receipt, before := f.managedAdmit(t, "app", connection, "bound-job")
	f.authorizeJob(t, receipt)
	changed = true
	op := f.managedAction(t, "app", connection, "changed-native-identity", "check", "")
	f.managedRun(t)
	result, err := f.services.connections.Operation(t.Context(), f.actor, "app", op.ID)
	if err != nil || result.Status != "failed" || result.Problem == nil || *result.Problem != "resource_changed" {
		t.Fatal("recheck silently moved the connection", result, err)
	}
	after, err := f.host.store.ReadJob(t.Context(), "app", f.actor.TokenID(), receipt.JobID)
	if err != nil || before.Profile != after.Profile || before.Job != after.Job {
		t.Fatal("identity mismatch rewrote accepted work", err)
	}
	encoded, _ := json.Marshal(result)
	if strings.Contains(string(encoded), "synthetic-state-secret") {
		t.Fatal("credential reached resource failure projection")
	}
}
