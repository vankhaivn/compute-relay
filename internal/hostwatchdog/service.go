// Package hostwatchdog owns explicit one-shot host watchdog installation receipts.
package hostwatchdog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hosthealth"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider/sshhost/watchdog"
	"github.com/vankhaivn/compute-relay/internal/worker"
)

var ErrUnsupported = errors.New("host watchdog requires native macOS Intel")
var ErrIntegrity = errors.New("host watchdog integrity check failed")

const Timeout = 45 * time.Second

type Target struct {
	Health       hosthealth.Target
	Revision     int64
	Installation string
	Engine       string
}
type Plan struct {
	Config watchdog.Config
	UID    string
}

func (p Plan) Validate() error {
	uid, err := strconv.ParseUint(p.UID, 10, 32)
	if err != nil || uid == 0 || strconv.FormatUint(uid, 10) != p.UID || p.Config.Validate() != nil {
		return connections.ErrRequest
	}
	return nil
}

type Operation struct {
	Problem      *string            `json:"problem"`
	ID           domain.OperationID `json:"operation_id"`
	ConnectionID string             `json:"connection_id"`
	Action       string             `json:"action"`
	Status       string             `json:"status"`
	Revision     int64              `json:"connection_revision"`
	CreatedAt    time.Time          `json:"created_at"`
	Replay       bool               `json:"replay"`
}
type Accept struct {
	Workspace                                   domain.WorkspaceID
	Token, Key, Fingerprint, Connection, Action string
	Revision                                    int64
	Target                                      Target
	Plan                                        Plan
}
type Work struct {
	Operation Operation
	Target    Target
	Plan      Plan
	Stage     string
}
type Repository interface {
	ReleaseWatchdog(context.Context, domain.OperationID) error
	PendingWatchdogs(context.Context) ([]Work, error)
	ClaimWatchdog(context.Context, domain.OperationID) (bool, error)
	ResolveWatchdogTarget(context.Context, domain.WorkspaceID, string, string) (Target, error)
	WatchdogReplay(context.Context, Accept) (Operation, bool, error)
	AcceptWatchdog(context.Context, Accept) (Operation, bool, error)
	FinishWatchdog(context.Context, domain.OperationID, string) error
	ReadWatchdogOperation(context.Context, domain.WorkspaceID, string, domain.OperationID) (Operation, error)
	WatchdogPlan(context.Context, string) (Plan, error)
}
type Remote interface {
	Plan(context.Context, Target) (Plan, error)
	Apply(context.Context, Target, Plan, string) error
	Status(context.Context, Target, Plan) (*watchdog.Status, error)
	Observe(context.Context, Target, Plan, string) (bool, error)
}
type Service struct {
	access *auth.Service
	repo   Repository
	remote Remote
	mu     chan struct{}
}

func New(access *auth.Service, repo Repository, remote Remote) (*Service, error) {
	if access == nil || repo == nil || remote == nil {
		return nil, connections.ErrUnavailable
	}
	return &Service{access: access, repo: repo, remote: remote, mu: make(chan struct{}, 1)}, nil
}
func (s *Service) authorize(ctx context.Context, p auth.Principal, w domain.WorkspaceID, scope auth.Scope) (auth.Principal, error) {
	p, err := s.access.Revalidate(ctx, p)
	if err == nil {
		err = auth.Require(p, w, scope)
	}
	return p, err
}
func (s *Service) Submit(ctx context.Context, p auth.Principal, w domain.WorkspaceID, id, key, action string, revision int64) (Operation, error) {
	p, err := s.authorize(ctx, p, w, auth.Manage)
	if err != nil {
		return Operation{}, err
	}
	if action != "install_host_watchdog" && action != "remove_host_watchdog" || revision < 1 || !domain.ObjectID(id).Valid() {
		return Operation{}, connections.ErrRequest
	}
	kh, err := admission.KeyDigest(key)
	if err != nil {
		return Operation{}, err
	}
	raw, _ := json.Marshal([]any{id, action, revision})
	hash := sha256.Sum256(raw)
	in := Accept{Workspace: w, Token: p.TokenID(), Key: string(kh), Fingerprint: hex.EncodeToString(hash[:]), Connection: id, Action: action, Revision: revision}
	if err := s.lock(ctx); err != nil {
		return Operation{}, err
	}
	defer s.unlock()
	op, found, err := s.repo.WatchdogReplay(ctx, in)
	if err != nil || found {
		return op, err
	}
	target, err := s.repo.ResolveWatchdogTarget(ctx, w, p.TokenID(), id)
	if err != nil {
		return Operation{}, err
	}
	in.Target = target
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()
	if action == "install_host_watchdog" {
		in.Plan, err = s.remote.Plan(ctx, target)
	} else {
		in.Plan, err = s.repo.WatchdogPlan(ctx, target.Health.ResourceID)
	}
	if err != nil {
		return Operation{}, err
	}
	op, found, err = s.repo.AcceptWatchdog(ctx, in)
	if err != nil || found {
		return op, err
	}
	return op, nil
}

// The runtime state root has one exclusive process owner; mu serializes its
// submission and worker turns. A bounded lease also fences explicit removal after
// an interrupted install. Removal may supersede uncertainty, never replay it.
// RunOnce commits the applying barrier before any mutation. Interrupted barriers
// and ambiguous outcomes are recovered exclusively through remote observation.
func (s *Service) RunOnce(ctx context.Context) error {
	if err := s.lock(ctx); err != nil {
		return err
	}
	defer s.unlock()
	items, err := s.repo.PendingWatchdogs(ctx)
	if err != nil {
		return err
	}
	for _, item := range items {
		step, cancel := context.WithTimeout(ctx, Timeout)
		if item.Stage == "queued" {
			claimed, claimErr := s.repo.ClaimWatchdog(step, item.Operation.ID)
			if claimErr != nil {
				cancel()
				return claimErr
			}
			if !claimed {
				cancel()
				continue
			}
			applyErr := s.remote.Apply(step, item.Target, item.Plan, item.Operation.Action)
			releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(step), 5*time.Second)
			releaseErr := s.repo.ReleaseWatchdog(releaseCtx, item.Operation.ID)
			releaseCancel()
			if releaseErr != nil {
				cancel()
				return releaseErr
			}
			if applyErr == nil {
				err = s.repo.FinishWatchdog(step, item.Operation.ID, "succeeded")
			} else {
				err = nil
			}
		} else {
			confirmed, observeErr := s.remote.Observe(step, item.Target, item.Plan, item.Operation.Action)
			if releaseErr := s.repo.ReleaseWatchdog(step, item.Operation.ID); releaseErr != nil {
				cancel()
				return releaseErr
			}
			if observeErr == nil && confirmed {
				err = s.repo.FinishWatchdog(step, item.Operation.ID, "succeeded")
			} else {
				err = nil
			}
		}
		cancel()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Local persistence failures must retain their retry/fatal classification.
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *Service) Run(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		if err := s.RunOnce(ctx); err != nil && worker.Classify(err) == worker.Stop {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Service) Operation(ctx context.Context, p auth.Principal, w domain.WorkspaceID, id domain.OperationID) (Operation, error) {
	p, err := s.authorize(ctx, p, w, auth.Manage)
	if err != nil {
		return Operation{}, err
	}
	if !id.Valid() {
		return Operation{}, connections.ErrRequest
	}
	return s.repo.ReadWatchdogOperation(ctx, w, p.TokenID(), id)
}
func (s *Service) Status(ctx context.Context, p auth.Principal, w domain.WorkspaceID, id string) (*watchdog.Status, error) {
	p, err := s.authorize(ctx, p, w, auth.Read)
	if err != nil {
		return nil, err
	}
	target, err := s.repo.ResolveWatchdogTarget(ctx, w, p.TokenID(), id)
	if err != nil {
		return nil, err
	}
	plan, err := s.repo.WatchdogPlan(ctx, target.Health.ResourceID)
	if errors.Is(err, connections.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()
	status, err := s.remote.Status(ctx, target, plan)
	if err != nil {
		return nil, err
	}
	_, err = s.authorize(ctx, p, w, auth.Read)
	return status, err
}

// The target is private durable recovery material, never a response projection.
func (t Target) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Account, Credential, Resource, Connection, Installation, Engine string
		Workspace                                                       domain.WorkspaceID
		Revision                                                        int64
		Policy                                                          *hosthealth.Policy
	}{t.Health.CanonicalAccount, string(t.Health.CredentialRef), t.Health.ResourceID, t.Health.ConnectionID, t.Installation, t.Engine, t.Health.Workspace, t.Revision, t.Health.Policy})
}
func (t *Target) UnmarshalJSON(raw []byte) error {
	var v struct {
		Account, Credential, Resource, Connection, Installation, Engine string
		Workspace                                                       domain.WorkspaceID
		Revision                                                        int64
		Policy                                                          *hosthealth.Policy
	}
	if json.Unmarshal(raw, &v) != nil {
		return connections.ErrRequest
	}
	t.Installation, t.Engine, t.Revision = v.Installation, v.Engine, v.Revision
	t.Health = hosthealth.Target{Workspace: v.Workspace, ConnectionID: v.Connection, ProviderType: "ssh", CanonicalAccount: v.Account, CredentialRef: ports.CredentialRef(v.Credential), ResourceID: v.Resource, Policy: v.Policy}
	if !t.Health.Valid() {
		return connections.ErrRequest
	}
	return nil
}

func (s *Service) lock(ctx context.Context) error {
	select {
	case s.mu <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s *Service) unlock() { <-s.mu }
