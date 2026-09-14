package admission

import (
	"context"
	"net/url"
	"strings"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
)

// Profile is a non-secret, immutable local resolution revision. Provider-specific
// capabilities, credential values and live preflight remain adapter responsibilities.
// Changing a revision's contents is forbidden; publish a new revision instead.
type Profile struct {
	Binding              domain.ProviderBinding `json:"binding"`
	AccountScope         string                 `json:"account_scope"`
	CredentialRef        string                 `json:"credential_ref,omitempty"`
	CostClass            string                 `json:"cost_class"`
	AllowRemoteInternet  bool                   `json:"allow_remote_internet"`
	MaxRemoteWallSeconds int64                  `json:"max_remote_wall_seconds"`
	MaxBundleBytes       int64                  `json:"max_bundle_bytes"`
	MaxInputBytes        int64                  `json:"max_input_bytes"`
}

func DefaultProfile(binding domain.ProviderBinding, accountScope string) Profile {
	return Profile{Binding: binding, AccountScope: accountScope, CostClass: "free_allowance", MaxRemoteWallSeconds: 1800, MaxBundleBytes: 100 << 20, MaxInputBytes: 4 << 30}
}
func (p Profile) Validate() error {
	if p.Binding.Validate() != nil || len(p.Binding.Profile) > 128 || len(p.Binding.ConfigurationRevision) > 128 || !domain.ObjectID(p.AccountScope).Valid() ||
		p.CostClass != "free_allowance" || p.MaxRemoteWallSeconds < 1 || p.MaxRemoteWallSeconds > 86400 || p.MaxBundleBytes < 1 || p.MaxBundleBytes > 100<<20 || p.MaxInputBytes < 1 || p.MaxInputBytes > 4<<30 {
		return ErrRequirements
	}
	for i, c := range p.Binding.Profile {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || i > 0 && (c == '_' || c == '.' || c == '-')) {
			return ErrRequirements
		}
	}
	if !domain.ObjectID(p.Binding.ConfigurationRevision).Valid() {
		return ErrRequirements
	}
	if p.CredentialRef != "" {
		kind, value, ok := strings.Cut(p.CredentialRef, ":")
		if !ok || len(value) == 0 || len(value) > 1024 || strings.ContainsAny(value, "\x00\r\n") {
			return ErrRequirements
		}
		switch kind {
		case "env":
			for i, c := range value {
				if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c == '_' || i > 0 && c >= '0' && c <= '9') {
					return ErrRequirements
				}
			}
		case "file": // Explicit operator reference, never opened during admission.
		default:
			return ErrRequirements
		}
	}
	return nil
}
func (p Profile) Check(s Spec) error {
	if p.Validate() != nil || p.Binding.Profile != s.Profile || s.Timeouts.RemoteWallSeconds > p.MaxRemoteWallSeconds || s.Network.RemoteInternet == "required" && !p.AllowRemoteInternet {
		return ErrRequirements
	}
	return nil
}

type Limits struct {
	MaxOutstandingTotal     int
	MaxOutstandingWorkspace int
}

func DefaultLimits() Limits { return Limits{1000, 100} }
func (l Limits) Valid() bool {
	return l.MaxOutstandingTotal > 0 && l.MaxOutstandingTotal <= 100000 && l.MaxOutstandingWorkspace > 0 && l.MaxOutstandingWorkspace <= l.MaxOutstandingTotal
}

type Links struct {
	Self      string `json:"self"`
	Events    string `json:"events"`
	Attempts  string `json:"attempts"`
	Artifacts string `json:"artifacts"`
}

func JobLinks(w domain.WorkspaceID, j domain.JobID) Links {
	base := "/v1/workspaces/" + url.PathEscape(string(w)) + "/jobs/" + url.PathEscape(string(j))
	return Links{base, base + "/events", base + "/attempts", base + "/artifacts"}
}

// Receipt is the ORIGINAL admission result. A replay retains queued/created_at even
// when the active attempt has progressed; callers use GET for current status.
type Receipt struct {
	JobID     domain.JobID     `json:"job_id"`
	AttemptID domain.AttemptID `json:"attempt_id"`
	Status    string           `json:"status"`
	CreatedAt time.Time        `json:"created_at"`
	Replay    bool             `json:"idempotency_replay"`
	Links     Links            `json:"links"`
}

type FrozenObject struct {
	Role   string                `json:"role"`
	Object domain.ObjectMetadata `json:"object"`
}

// Record is INTERNAL: canonical request and resolution may include public URLs or
// non-secret environment data. API status must not serialize this record wholesale.
type Record struct {
	Job          domain.Job
	Attempt      domain.Attempt
	AttemptNonce string
	Request      Request
	Profile      Profile
	Objects      []FrozenObject
	PendingHTTPS int
}

type Requirement struct {
	Name         string `json:"name"`
	Verification string `json:"verification"`
	Reason       string `json:"reason,omitempty"`
}
type Validation struct {
	Valid        bool          `json:"valid"`
	Warnings     []string      `json:"warnings"`
	Requirements []Requirement `json:"requirements"`
}

// Repository owns the admission transaction: current token/workspace authority,
// idempotency lookup, immutable profile resolution, object ownership/pins, job/attempt,
// original receipt and job.accepted event must commit together. No network/file I/O.
type Repository interface {
	AdmitJob(context.Context, domain.WorkspaceID, string, string, Request, Limits) (Receipt, error)
	ValidateJob(context.Context, domain.WorkspaceID, string, Request) (Validation, error)
	ReadJob(context.Context, domain.WorkspaceID, string, domain.JobID) (Record, error)
}

type Service struct {
	access *auth.Service
	repo   Repository
	limits Limits
}

func New(access *auth.Service, repo Repository, limits Limits) (*Service, error) {
	if access == nil || repo == nil || !limits.Valid() {
		return nil, ErrUnavailable
	}
	return &Service{access, repo, limits}, nil
}
func (s *Service) Submit(ctx context.Context, p auth.Principal, w domain.WorkspaceID, key string, raw []byte) (Receipt, error) {
	p, err := s.authorize(ctx, p, w, auth.Write)
	if err != nil {
		return Receipt{}, err
	}
	k, err := KeyDigest(key)
	if err != nil {
		return Receipt{}, err
	}
	req, err := Parse(raw)
	if err != nil {
		return Receipt{}, err
	}
	return s.repo.AdmitJob(ctx, w, p.TokenID(), k, req, s.limits)
}
func (s *Service) Validate(ctx context.Context, p auth.Principal, w domain.WorkspaceID, raw []byte) (Validation, error) {
	p, err := s.authorize(ctx, p, w, auth.Read)
	if err != nil {
		return Validation{}, err
	}
	req, err := Parse(raw)
	if err != nil {
		return Validation{}, err
	}
	return s.repo.ValidateJob(ctx, w, p.TokenID(), req)
}
func (s *Service) Get(ctx context.Context, p auth.Principal, w domain.WorkspaceID, id domain.JobID) (Record, error) {
	p, err := s.authorize(ctx, p, w, auth.Read)
	if err != nil {
		return Record{}, err
	}
	if !id.Valid() {
		return Record{}, ErrNotFound
	}
	return s.repo.ReadJob(ctx, w, p.TokenID(), id)
}
func (s *Service) authorize(ctx context.Context, p auth.Principal, w domain.WorkspaceID, scope auth.Scope) (auth.Principal, error) {
	fresh, err := s.access.Revalidate(ctx, p)
	if err != nil {
		return auth.Principal{}, err
	}
	if err = auth.Require(fresh, w, scope); err != nil {
		return auth.Principal{}, err
	}
	return fresh, nil
}
