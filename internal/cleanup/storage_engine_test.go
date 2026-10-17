package cleanup

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

type storageStubRepository struct {
	work      *StorageWork
	beginErr  error
	begins    int
	completed []hoststorage.DeleteOutcome
}

func (r *storageStubRepository) ClaimStorageCleanup(context.Context, time.Duration) (*StorageWork, error) {
	w := r.work
	r.work = nil
	return w, nil
}
func (r *storageStubRepository) RenewStorageCleanup(context.Context, StorageWork, time.Duration) (time.Time, error) {
	return time.Now().Add(time.Minute), nil
}
func (r *storageStubRepository) BeginStorageCleanup(context.Context, StorageWork) error {
	r.begins++
	return r.beginErr
}
func (r *storageStubRepository) CompleteStorageCleanup(_ context.Context, _ StorageWork, outcome hoststorage.DeleteOutcome) error {
	r.completed = append(r.completed, outcome)
	return nil
}

type storageStubProvider struct {
	provider.Provider
	verifyErr   error
	observeErr  error
	deleteErr   error
	panicDelete bool
	deletes     int
}

func (p *storageStubProvider) Verify(context.Context, provider.BindingSnapshot) error {
	return p.verifyErr
}
func (p *storageStubProvider) ObserveStorageTarget(context.Context, hoststorage.Target) (hoststorage.TargetObservation, error) {
	return hoststorage.TargetObservation{Present: true, Eligible: true}, p.observeErr
}
func (p *storageStubProvider) CleanupStorageTarget(context.Context, hoststorage.Target, bool) (hoststorage.DeleteOutcome, error) {
	p.deletes++
	if p.panicDelete {
		panic("private-provider-detail")
	}
	return hoststorage.DeleteConfirmed, p.deleteErr
}

func storageStubWork(t *testing.T) *StorageWork {
	t.Helper()
	w := &StorageWork{
		Lease:   Lease{WorkspaceID: "a", OperationID: "op_1", Generation: 1, Fence: strings.Repeat("f", 64), Until: time.Now().Add(time.Minute)},
		Binding: provider.BindingSnapshot{Binding: domain.ProviderBinding{Profile: "fixture", ProviderInstanceID: "fake_1", ConfigurationRevision: "one"}, AccountScope: "account", CredentialRef: "env:FIXTURE"},
		Target: hoststorage.Target{Scope: hoststorage.Scope{InstallationID: "rt_1", MachineID: domain.SHA256Digest(strings.Repeat("a", 64)), EngineID: domain.SHA256Digest(strings.Repeat("b", 64))},
			Class: hoststorage.Volume, ID: "relay-cache-example", Labels: map[string]string{"io.compute-relay.installation": "rt_1"}, Action: hoststorage.OrphanVolume},
		State: "pending",
	}
	if !w.Valid() {
		t.Fatal("invalid storage fixture")
	}
	return w
}

func TestStorageEngineKeepsLocalFailuresAndUnknownDeleteSeparate(t *testing.T) {
	local := provider.LocalStateError{Err: errors.New("local state failed")}
	for _, tc := range []struct {
		name        string
		provider    storageStubProvider
		beginErr    error
		wantErr     error
		wantBegin   int
		wantDelete  int
		wantOutcome hoststorage.DeleteOutcome
	}{
		{name: "local_observation", provider: storageStubProvider{observeErr: local}, wantErr: local},
		{name: "local_delete", provider: storageStubProvider{deleteErr: local}, wantErr: local, wantBegin: 1, wantDelete: 1},
		{name: "changed_eligibility", beginErr: hoststorage.ErrIneligible, wantBegin: 1, wantOutcome: hoststorage.DeleteRetained},
		{name: "local_begin", beginErr: local, wantErr: local, wantBegin: 1},
		{name: "provider_panic_after_intent", provider: storageStubProvider{panicDelete: true}, wantBegin: 1, wantDelete: 1, wantOutcome: hoststorage.DeleteUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &storageStubRepository{work: storageStubWork(t), beginErr: tc.beginErr}
			adapter := &tc.provider
			engine, err := New(&stubRepo{}, stubResolver{p: adapter}, DefaultConfig())
			if err != nil {
				t.Fatal(err)
			}
			engine.WithStorage(repo)
			worked, err := engine.RunOnce(context.Background())
			if !worked || (tc.wantErr == nil && err != nil) || (tc.wantErr != nil && !errors.Is(err, tc.wantErr)) {
				t.Fatalf("work=%v err=%v, want %v", worked, err, tc.wantErr)
			}
			if repo.begins != tc.wantBegin || adapter.deletes != tc.wantDelete {
				t.Fatalf("begin/delete calls=%d/%d, want %d/%d", repo.begins, adapter.deletes, tc.wantBegin, tc.wantDelete)
			}
			if tc.wantOutcome == "" && len(repo.completed) != 0 || tc.wantOutcome != "" && (len(repo.completed) != 1 || repo.completed[0] != tc.wantOutcome) {
				t.Fatalf("stored outcomes=%v, want %q", repo.completed, tc.wantOutcome)
			}
		})
	}
}

func TestStorageEngineDefersInterruptedDeletionWhileProviderIsUnreachable(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		wantErr error
	}{
		{name: "provider_outage", err: errors.New("SYNTHETIC_SSH_CONNECTION_RESET"), wantErr: ErrDeferred},
		{name: "local_state", err: provider.LocalStateError{Err: errors.New("local state failed")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			work := storageStubWork(t)
			work.State = "running"
			repo := &storageStubRepository{work: work}
			adapter := &storageStubProvider{verifyErr: tc.err}
			engine, err := New(&stubRepo{}, stubResolver{p: adapter}, DefaultConfig())
			if err != nil {
				t.Fatal(err)
			}
			engine.WithStorage(repo)
			worked, err := engine.RunOnce(context.Background())
			want := tc.wantErr
			if want == nil {
				want = tc.err
			}
			if !worked || !errors.Is(err, want) || adapter.deletes != 0 || len(repo.completed) != 0 {
				t.Fatalf("work=%v err=%v deletes=%d outcomes=%v", worked, err, adapter.deletes, repo.completed)
			}
			if strings.Contains(err.Error(), "SYNTHETIC") {
				t.Fatal("raw provider text escaped", err)
			}
		})
	}
}
