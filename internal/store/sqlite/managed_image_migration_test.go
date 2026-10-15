package sqlite

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/sshhost"
)

type legacySSHStateAdapter struct{ *stateAdapter }

func (a legacySSHStateAdapter) Descriptor() connections.Descriptor {
	d := a.stateAdapter.Descriptor()
	d.Type = "ssh"
	return d
}
func (a legacySSHStateAdapter) Profile(binding domain.ProviderBinding, account string) admission.Profile {
	p := a.stateAdapter.Profile(binding, account)
	p.CostClass, p.Accelerator = admission.CostOperatorManaged, "cpu"
	return p
}
func (legacySSHStateAdapter) RuntimeConfig() []byte {
	return []byte(fmt.Sprintf(`{"schema_version":1,"image":%q}`, sshhost.StandardImage))
}

func TestImageMigrationRetiresLegacyAdmissionButPreservesFrozenJobs(t *testing.T) {
	f := newManagedStateFixture(t)
	ctx := context.Background()
	var err error
	f.service, err = connections.New(f.access, f.s, f.vault, []connections.Adapter{legacySSHStateAdapter{f.discovery}})
	requireNoError(t, err)
	body := strings.Replace(string(managedCreateBody(managedStateSecret)), `"fixture"`, `"ssh"`, 1)
	op, err := f.service.Submit(ctx, f.principals["a"], "a", "", "legacy-ssh-connection", []byte(body))
	requireNoError(t, err)
	f.run(t)
	connection := f.get(t, "a", op.ConnectionID)
	if connection.Selection == nil {
		t.Fatal("legacy selection not created")
	}
	jobs, err := admission.New(f.access, f.s, admission.DefaultLimits())
	requireNoError(t, err)
	spec := strings.Replace(dispatchJSON, `"profile":"fixture"`, `"profile":"`+connection.Selection.Profile+`"`, 1)
	receipt, err := jobs.Submit(ctx, f.principals["a"], "a", "legacy-frozen-job", []byte(spec))
	requireNoError(t, err)
	original, err := jobs.Get(ctx, f.principals["a"], "a", receipt.JobID)
	requireNoError(t, err)
	var originalRuntime []byte
	requireNoError(t, f.s.db.QueryRow(`SELECT config FROM managed_profile_runtime WHERE profile=? AND revision=?`, connection.Selection.Profile, connection.Selection.ConfigurationRevision).Scan(&originalRuntime))
	oldPath := copyFixtureAtSchema(t, f.s, 17)
	upgraded, err := Open(ctx, oldPath, DefaultOptions())
	if err != nil {
		t.Fatal("upgrade", err)
	}
	defer upgraded.Close()
	current, err := upgraded.ReadConnection(ctx, "a", f.principals["a"].TokenID(), connection.ID)
	if err != nil || current.Selection != nil || current.Authentication != "verified" || !current.CredentialPresent {
		t.Fatal("upgrade lost credentials or kept a legacy admission selection", current, err)
	}
	retained, err := upgraded.ReadJob(ctx, "a", f.principals["a"].TokenID(), receipt.JobID)
	if err != nil || retained.Profile != original.Profile || retained.Job != original.Job {
		t.Fatal("upgrade rewrote admitted work", err)
	}
	binding := provider.BindingSnapshot{Binding: retained.Profile.Binding, AccountScope: retained.Profile.AccountScope, CredentialRef: retained.Profile.CredentialRef}
	frozen, err := upgraded.ReadManagedBinding(ctx, binding)
	if err != nil || string(frozen.RuntimeConfig) != string(originalRuntime) {
		t.Fatal("upgrade lost exact image recovery policy", err)
	}
	var enabled bool
	if err := upgraded.db.QueryRow(`SELECT enabled FROM profiles WHERE profile=?`, connection.Selection.Profile).Scan(&enabled); err != nil || enabled {
		t.Fatal("old alias still admits new jobs", err)
	}
}
