// Package auth implements local application-token and workspace authorization policy.
// Provider credentials are not accepted here. Persistence is supplied by the operator
// runtime's composition root; no in-memory store is silently enabled in production.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

var (
	ErrUnauthenticated = errors.New("missing or invalid runtime token")
	ErrForbidden       = errors.New("workspace action not permitted")
	ErrNotFound        = errors.New("resource not found")
	ErrUnavailable     = errors.New("authorization repository unavailable")
)

type Scope string

const (
	Read    Scope = "read"
	Write   Scope = "write"
	Operate Scope = "operate"
)

// Secret requires explicit Reveal at the local token-issuance boundary. Formatting and
// JSON serialization deliberately redact it. This is log hygiene, not secure memory.
type Secret struct{ value string }

func (s Secret) Reveal() string             { return s.value }
func (Secret) String() string               { return "[REDACTED]" }
func (Secret) GoString() string             { return "[REDACTED]" }
func (Secret) MarshalJSON() ([]byte, error) { return []byte(`"[REDACTED]"`), nil }
func (Secret) MarshalText() ([]byte, error) { return []byte("[REDACTED]"), nil }

type TokenRecord struct {
	ID          string
	WorkspaceID domain.WorkspaceID
	Digest      [sha256.Size]byte
	Scopes      []Scope
	CreatedAt   time.Time
	ExpiresAt   time.Time // Zero means no expiry. Revocation is checked on every request.
	Revoked     bool
}

type Workspace struct {
	ID              domain.WorkspaceID
	Enabled         bool
	AllowedProfiles []string
}

// TokenRepository must persist digests only, enforce unique IDs/digests, and acknowledge
// CreateToken/RevokeToken only after commit. ErrNotFound is the lookup miss sentinel.
// LookupToken returns an independent snapshot; it must not return aliased mutable slices.
type TokenRepository interface {
	CreateToken(context.Context, TokenRecord) error
	LookupToken(context.Context, [sha256.Size]byte) (TokenRecord, error)
	RevokeToken(context.Context, string) error
}

type WorkspaceRepository interface {
	LookupWorkspace(context.Context, domain.WorkspaceID) (Workspace, error)
}

// Principal cannot be constructed from untrusted workspace headers or request bodies.
type Principal struct {
	tokenID   string
	workspace domain.WorkspaceID
	digest    [sha256.Size]byte
	scopes    []Scope
}

func (p Principal) WorkspaceID() domain.WorkspaceID { return p.workspace }
func (p Principal) TokenID() string                 { return p.tokenID }

type Service struct {
	tokens TokenRepository
	spaces WorkspaceRepository
	now    func() time.Time
}

func New(tokens TokenRepository, spaces WorkspaceRepository, now func() time.Time) (*Service, error) {
	if tokens == nil || spaces == nil {
		return nil, ErrUnavailable
	}
	if now == nil {
		now = time.Now
	}
	return &Service{tokens: tokens, spaces: spaces, now: now}, nil
}

// Issue is a LOCAL administrative operation, never an application HTTP route. The raw
// secret is returned only after the digest and scopes have been committed successfully.
func (s *Service) Issue(ctx context.Context, workspace domain.WorkspaceID, scopes []Scope, expires time.Time) (Secret, TokenRecord, error) {
	if !workspace.Valid() || !validScopes(scopes) {
		return Secret{}, TokenRecord{}, ErrForbidden
	}
	if err := s.enabled(ctx, workspace); err != nil {
		return Secret{}, TokenRecord{}, err
	}
	now := s.now().UTC()
	if !expires.IsZero() && !expires.After(now) {
		return Secret{}, TokenRecord{}, ErrForbidden
	}
	var secretBytes [32]byte
	var idBytes [16]byte
	if _, err := rand.Read(secretBytes[:]); err != nil {
		return Secret{}, TokenRecord{}, ErrUnavailable
	}
	if _, err := rand.Read(idBytes[:]); err != nil {
		return Secret{}, TokenRecord{}, ErrUnavailable
	}
	secret := Secret{value: "cr1_" + base64.RawURLEncoding.EncodeToString(secretBytes[:])}
	record := TokenRecord{
		ID: "tok_" + hex.EncodeToString(idBytes[:]), WorkspaceID: workspace,
		Digest: sha256.Sum256([]byte(secret.value)), Scopes: append([]Scope(nil), scopes...),
		CreatedAt: now, ExpiresAt: expires.UTC(),
	}
	// Do not allow a repository implementation to mutate the returned scope snapshot.
	stored := record
	stored.Scopes = append([]Scope(nil), record.Scopes...)
	if err := s.tokens.CreateToken(ctx, stored); err != nil {
		return Secret{}, TokenRecord{}, ErrUnavailable
	}
	return secret, record, nil
}

func (s *Service) Revoke(ctx context.Context, tokenID string) error {
	if _, err := domain.ParseObjectID(tokenID); err != nil {
		return ErrNotFound
	}
	if err := s.tokens.RevokeToken(ctx, tokenID); err != nil {
		if errors.Is(err, ErrNotFound) {
			return ErrNotFound
		}
		return ErrUnavailable
	}
	return nil
}

func (s *Service) Authenticate(ctx context.Context, raw string) (Principal, error) {
	if len(raw) != 47 || !strings.HasPrefix(raw, "cr1_") {
		return Principal{}, ErrUnauthenticated
	}
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(raw[4:])
	if err != nil || len(decoded) != 32 {
		return Principal{}, ErrUnauthenticated
	}
	return s.lookup(ctx, sha256.Sum256([]byte(raw)))
}

// Revalidate refreshes revocation, workspace availability, expiry and scopes after a
// long transfer. A previously authenticated Principal is not a permanent grant.
func (s *Service) Revalidate(ctx context.Context, p Principal) (Principal, error) {
	if p.tokenID == "" || !p.workspace.Valid() {
		return Principal{}, ErrUnauthenticated
	}
	fresh, err := s.lookup(ctx, p.digest)
	if err != nil {
		return Principal{}, err
	}
	if fresh.tokenID != p.tokenID || fresh.workspace != p.workspace {
		return Principal{}, ErrUnauthenticated
	}
	return fresh, nil
}

func (s *Service) lookup(ctx context.Context, digest [sha256.Size]byte) (Principal, error) {
	if err := ctx.Err(); err != nil {
		return Principal{}, err
	}
	record, err := s.tokens.LookupToken(ctx, digest)
	if errors.Is(err, ErrNotFound) {
		return Principal{}, ErrUnauthenticated
	}
	if err != nil {
		return Principal{}, ErrUnavailable
	}
	if subtle.ConstantTimeCompare(record.Digest[:], digest[:]) != 1 || record.Revoked ||
		!record.WorkspaceID.Valid() || !validScopes(record.Scopes) || record.ID == "" ||
		(!record.ExpiresAt.IsZero() && !s.now().Before(record.ExpiresAt)) {
		return Principal{}, ErrUnauthenticated
	}
	if err := s.enabled(ctx, record.WorkspaceID); err != nil {
		return Principal{}, err
	}
	return Principal{tokenID: record.ID, workspace: record.WorkspaceID, digest: digest,
		scopes: append([]Scope(nil), record.Scopes...)}, nil
}

func (s *Service) enabled(ctx context.Context, id domain.WorkspaceID) error {
	workspace, err := s.spaces.LookupWorkspace(ctx, id)
	if errors.Is(err, ErrNotFound) {
		return ErrForbidden
	}
	if err != nil {
		return ErrUnavailable
	}
	if workspace.ID != id || !workspace.Enabled {
		return ErrForbidden
	}
	return nil
}

func Require(p Principal, workspace domain.WorkspaceID, scope Scope) error {
	if p.tokenID == "" {
		return ErrUnauthenticated
	}
	if !workspace.Valid() || p.workspace != workspace {
		return ErrForbidden
	}
	for _, granted := range p.scopes {
		if granted == scope {
			return nil
		}
	}
	return ErrForbidden
}

func (s *Service) RequireProfile(ctx context.Context, p Principal, workspace domain.WorkspaceID, profile string) error {
	if err := Require(p, workspace, Read); err != nil {
		return err
	}
	w, err := s.spaces.LookupWorkspace(ctx, workspace)
	if err != nil {
		return ErrUnavailable
	}
	if w.ID != workspace || !w.Enabled {
		return ErrForbidden
	}
	for _, allowed := range w.AllowedProfiles {
		if profile != "" && profile == allowed {
			return nil
		}
	}
	return ErrForbidden
}

type ResourceKind string

const (
	Job       ResourceKind = "job"
	Attempt   ResourceKind = "attempt"
	Object    ResourceKind = "object"
	Artifact  ResourceKind = "artifact"
	Event     ResourceKind = "event"
	Log       ResourceKind = "log"
	Operation ResourceKind = "operation"
)

// OwnershipResolver returns the STORED owner, never a caller-supplied owner. Listing
// queries must additionally filter by workspace at the repository boundary.
type OwnershipResolver interface {
	Owner(context.Context, ResourceKind, string) (domain.WorkspaceID, error)
}

// RequireResource checks every referenced resource, not just the parent route. All wrong
// owners and absent resources are hidden behind the same ErrNotFound. Parent/attempt
// associations must also be checked by the service when navigating nested resources.
func RequireResource(ctx context.Context, p Principal, workspace domain.WorkspaceID, scope Scope, kind ResourceKind, id string, owners OwnershipResolver) error {
	if err := Require(p, workspace, scope); err != nil {
		return err
	}
	switch kind {
	case Job, Attempt, Object, Artifact, Event, Log, Operation:
	default:
		return ErrNotFound
	}
	if _, err := domain.ParseObjectID(id); err != nil {
		return ErrNotFound
	}
	if owners == nil {
		return ErrUnavailable
	}
	owner, err := owners.Owner(ctx, kind, id)
	if errors.Is(err, ErrNotFound) {
		return ErrNotFound
	}
	if err != nil {
		return ErrUnavailable
	}
	if owner != workspace {
		return ErrNotFound
	}
	return nil
}

func validScopes(scopes []Scope) bool {
	if len(scopes) == 0 || len(scopes) > 3 {
		return false
	}
	seen := make(map[Scope]bool, len(scopes))
	for _, scope := range scopes {
		if (scope != Read && scope != Write && scope != Operate) || seen[scope] {
			return false
		}
		seen[scope] = true
	}
	return true
}
