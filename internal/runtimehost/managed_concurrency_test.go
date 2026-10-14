package runtimehost

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/objects"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func TestManagedKaggleConcurrencyConfiguresTwoAuthorizedJobsAndQueuesThird(t *testing.T) {
	for _, shape := range []string{"cpu", "NvidiaTeslaT4"} {
		t.Run(shape, func(t *testing.T) {
			ctx := context.Background()
			f := newManagedTestFixture(t, true)
			f.adapter.config.MachineShape = shape
			original := f.connection(t, "concurrency-create", "SYNTHETIC_FIRST")
			frozen := f.binding(t, original)
			if original.Selection.MaxActiveAttempts != 1 {
				t.Fatal("new connection lost serial default")
			}
			op, err := f.services.connections.Submit(ctx, f.actor, "app", original.ID, "concurrency-configure", []byte(fmt.Sprintf(`{"action":"configure","expected_revision":%d,"configuration":{"max_remote_wall_seconds":300,"max_active_attempts":2}}`, original.Revision)))
			if err != nil || op.Status != "succeeded" || f.adapter.checks != 1 {
				t.Fatal("local concurrency configuration failed or contacted provider", op, err)
			}
			updated, err := f.services.connections.Get(ctx, f.actor, "app", original.ID)
			if err != nil || updated.Selection == nil || updated.Selection.MaxActiveAttempts != 2 {
				t.Fatal("configured limit unavailable", updated, err)
			}
			saved, err := f.host.store.ReadManagedBinding(ctx, frozen)
			if err != nil || saved.Profile.MaxActiveAttempts != 1 {
				t.Fatal("configuration rewrote frozen profile", err)
			}
			settings := scheduler.DefaultSettings()
			settings.MaxWorkers, settings.MaxActivePerAccount = 4, 64
			if err := f.host.store.ConfigureScheduler(ctx, settings); err != nil {
				t.Fatal(err)
			}
			for i := range 3 {
				admitConcurrencyJob(t, f, updated, fmt.Sprintf("concurrent-%d", i))
			}
			for i := range 2 {
				result, err := f.host.store.ClaimNext(ctx, fmt.Sprintf("worker-%d", i))
				if err != nil || result.Claim == nil {
					t.Fatal("authorized account slot unavailable", result, err)
				}
			}
			result, err := f.host.store.ClaimNext(ctx, "third-worker")
			if err != nil || result.Claim != nil || result.View.AccountReservations[*updated.AccountID] != 2 || len(result.View.Heads) != 1 || result.View.Heads[0].Reason != scheduler.AccountLimit {
				t.Fatal("third job bypassed account capacity", result, err)
			}
		})
	}
}

func TestManagedKaggleConcurrencyDoesNotEnableProcessLimits(t *testing.T) {
	a := &managedKaggleAdapter{}
	for _, c := range []connections.Configuration{
		{MaxRemoteWallSeconds: 300, MaxActiveAttempts: 65},
		{MaxRemoteWallSeconds: 300, MaxActiveAttempts: -1},
		{MaxRemoteWallSeconds: 300, MaxActiveAttempts: 2, MaxProcesses: 128},
	} {
		if err := a.ValidateConfiguration(c); err != connections.ErrRequest {
			t.Fatal("unsupported Kaggle policy accepted", c, err)
		}
	}
}

func admitConcurrencyJob(t *testing.T, f *managedTestFixture, c connections.Connection, key string) admission.Receipt {
	t.Helper()
	ctx := context.Background()
	objectService, err := objects.New(f.host.access, f.host.inputs, f.host.store)
	if err != nil {
		t.Fatal(err)
	}
	payload := "synthetic bundle never executed"
	bundle, err := objectService.Upload(ctx, f.actor, "app", int64(len(payload)), provider.Digest([]byte(payload)), strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := admission.New(f.host.access, f.host.store, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	resources := `"accelerator":"cpu"`
	if c.Selection.Accelerator == "gpu" {
		resources = `"accelerator":"gpu","minimum_gpu_count":1`
	}
	raw := fmt.Sprintf(`{"api_version":"compute-connector/v1alpha1","name":"concurrency fixture","profile":%q,"bundle":{"object_id":%q},"execution":{"kind":"python","command":["python","MUST_NOT_EXECUTE.py"]},"inputs":[],"outputs":[{"path":"answer.json","required":true}],"resources":{%s},"network":{"remote_internet":"disabled"},"timeouts":{"remote_wall_seconds":10,"setup_seconds":5,"finalization_grace_seconds":2}}`, c.Selection.Profile, bundle.ID, resources)
	job, err := jobs.Submit(ctx, f.actor, "app", key+"-admit", []byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	grant := fmt.Sprintf(`{"attempt_id":%q,"max_remote_wall_seconds":10,"authorize_private_staging":true,"authorize_compute":true}`, job.AttemptID)
	if _, err := f.services.authorizations.Authorize(ctx, f.actor, "app", job.JobID, key+"-grant", []byte(grant)); err != nil {
		t.Fatal(err)
	}
	return job
}
