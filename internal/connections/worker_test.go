package connections

import (
	"context"
	"errors"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/worker"
)

type connectionWriteFailure struct{}

func (connectionWriteFailure) Error() string   { return "connection store temporarily unavailable" }
func (connectionWriteFailure) Temporary() bool { return true }

type connectionWriteRepository struct {
	Repository
	record                         Record
	failure                        error
	committed                      bool
	begin, ready, deferred, failed int
}

func (r *connectionWriteRepository) NextConnectionOperation(context.Context) (Record, bool, error) {
	return r.record, true, nil
}
func (r *connectionWriteRepository) ReadyConnection(context.Context, domain.WorkspaceID, string, domain.OperationID) error {
	r.ready++
	return r.failure
}
func (r *connectionWriteRepository) DeferConnectionSecret(context.Context, domain.OperationID) error {
	r.deferred++
	return nil
}
func (r *connectionWriteRepository) BeginConnectionInstall(context.Context, domain.OperationID) (bool, error) {
	r.begin++
	if r.committed || r.failure == nil {
		r.record.Stage = "installing"
	}
	return r.failure == nil, r.failure
}
func (r *connectionWriteRepository) FailConnection(context.Context, domain.OperationID, string, string) error {
	r.failed++
	return nil
}

type connectionWriteVault struct {
	credentials.Vault
	failure error
}

func (v connectionWriteVault) WithSecret(_ context.Context, _ string, use func([]byte) error) error {
	if v.failure != nil {
		return v.failure
	}
	return use([]byte("SYNTHETIC_CREDENTIAL"))
}

type connectionWriteInstaller struct {
	Adapter
	ImageInstaller
	calls       int
	recoverOnly bool
}

func (a *connectionWriteInstaller) InstallImage(_ context.Context, _ []byte, _ Record, recoverOnly bool) (Verification, error) {
	a.calls++
	a.recoverOnly = recoverOnly
	return Verification{}, ErrCredentialRejected
}

func TestConnectionWriteFailuresPreserveWorkAndInstallationIntent(t *testing.T) {
	for _, stage := range []string{"waiting_secret", "ready", "commit_ack_lost"} {
		for _, cause := range []error{connectionWriteFailure{}, context.DeadlineExceeded, errors.New("local state invariant failed")} {
			t.Run(stage+"/"+cause.Error(), func(t *testing.T) {
				r := &connectionWriteRepository{failure: cause, committed: stage == "commit_ack_lost",
					record: Record{Stage: "ready", ProviderType: "ssh", Installation: &ImageInstallation{},
						Operation: Operation{Action: "install_image"}}}
				if stage == "waiting_secret" {
					r.record.Stage = stage
				}
				a := &connectionWriteInstaller{}
				s := Service{repo: r, vault: connectionWriteVault{}, adapters: map[string]Adapter{"ssh": a}}
				worked, err := s.RunOnce(t.Context())
				if !worked || !errors.Is(err, cause) || worker.Classify(err) != worker.Classify(cause) || r.failed != 0 || r.deferred != 0 || a.calls != 0 {
					t.Fatal("store error became a vault/provider outcome", worked, err, r.failed, r.deferred, a.calls)
				}
				r.failure = nil
				if worked, err := s.RunOnce(t.Context()); !worked || err != nil {
					t.Fatal("original operation did not resume", worked, err)
				}
				if stage == "waiting_secret" {
					if r.ready != 2 || r.begin != 0 || a.calls != 0 || r.failed != 0 {
						t.Fatal("credential readiness recovery changed work")
					}
				} else {
					wantBegin := 2
					if r.committed {
						wantBegin = 1
					}
					if a.calls != 1 || a.recoverOnly != r.committed || r.begin != wantBegin || r.failed != 1 {
						t.Fatal("installation recovery rearmed a committed intent or lost a real refusal", a.calls, a.recoverOnly, r.begin, r.failed)
					}
				}
			})
		}
	}
}

func TestConnectionVaultRefusalsKeepTheirExistingOutcome(t *testing.T) {
	for _, stage := range []string{"waiting_secret", "ready"} {
		t.Run(stage, func(t *testing.T) {
			r := &connectionWriteRepository{record: Record{Stage: stage, ProviderType: "ssh", Installation: &ImageInstallation{}, Operation: Operation{Action: "install_image"}}}
			a := &connectionWriteInstaller{}
			s := Service{repo: r, vault: connectionWriteVault{failure: credentials.ErrUnavailable}, adapters: map[string]Adapter{"ssh": a}}
			worked, err := s.RunOnce(t.Context())
			if !worked || err != nil || a.calls != 0 || r.begin != 0 || r.ready != 0 {
				t.Fatal("vault refusal reached local intent or provider", worked, err)
			}
			if stage == "waiting_secret" && (r.deferred != 1 || r.failed != 0) || stage == "ready" && (r.deferred != 0 || r.failed != 1) {
				t.Fatal("vault refusal changed disposition", r.deferred, r.failed)
			}
		})
	}
}
