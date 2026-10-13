// Package connections owns optional live provider administration. Provider policy and
// credential formats stay in adapters; jobs continue using immutable admission profiles.
package connections

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

var (
	ErrCredentialRejected       = errors.New("provider credential rejected")
	ErrRequest                  = errors.New("invalid connection request")
	ErrConflict                 = errors.New("connection request conflicts with current revision or original key")
	ErrNotFound                 = errors.New("connection or operation not found")
	ErrUnavailable              = errors.New("connection administration unavailable")
	ErrVault                    = errors.New("protected credential store unavailable")
	ErrUnsupported              = errors.New("provider type is not installed")
	ErrActiveWork               = errors.New("retained work requires this connection")
	ErrImageInstallationUnknown = errors.New("image installation outcome unknown")
	ErrLimit                    = errors.New("saved connection limit reached")
)

const MaxRequestBytes = 64 << 10
const MaxConnections = 100

type Field struct {
	Name      string `json:"name"`
	Label     string `json:"label"`
	Required  bool   `json:"required"`
	WriteOnly bool   `json:"write_only"`
	MaxBytes  int    `json:"max_bytes"`
}
type Capabilities struct {
	RemoteCleanup    string   `json:"remote_cleanup,omitempty"`
	EphemeralSecrets string   `json:"ephemeral_workload_secrets,omitempty"`
	Accelerators     []string `json:"accelerators"`
	RemoteCancel     string   `json:"remote_cancel"`
	Quota            string   `json:"quota"`
	SeparateStart    string   `json:"separate_prepare_start,omitempty"`
	AbsoluteDeadline string   `json:"absolute_execution_deadline,omitempty"`
}
type Descriptor struct {
	Type              string       `json:"type"`
	Label             string       `json:"label"`
	Fields            []Field      `json:"credential_fields"`
	Capabilities      Capabilities `json:"capabilities"`
	CredentialStorage string       `json:"credential_storage"`
}
type Selection struct {
	Profile               string `json:"profile"`
	ConfigurationRevision string `json:"configuration_revision"`
	MaxRemoteWallSeconds  int64  `json:"max_remote_wall_seconds"`
	MaxActiveAttempts     int    `json:"max_active_attempts,omitempty"`
	MaxProcesses          int    `json:"max_processes,omitempty"`
	AllowRemoteInternet   bool   `json:"allow_remote_internet"`
	Accelerator           string `json:"accelerator,omitempty"`
}

// Configuration is a persisted, non-secret policy override for future selections.
// Omission preserves the adapter's configured startup default.
type Configuration struct {
	MaxRemoteWallSeconds int64 `json:"max_remote_wall_seconds"`
	MaxActiveAttempts    int   `json:"max_active_attempts,omitempty"`
	MaxProcesses         int   `json:"max_processes,omitempty"`
}

func (c Configuration) Validate() error {
	if c.MaxRemoteWallSeconds < 1 || c.MaxRemoteWallSeconds > 86400 || c.MaxActiveAttempts < 0 || c.MaxActiveAttempts > 64 || c.MaxProcesses < 0 || c.MaxProcesses > 65536 {
		return ErrRequest
	}
	return nil
}

type Quota struct {
	Limit         *int64     `json:"limit,omitempty"`
	Used          *int64     `json:"used,omitempty"`
	LocalReserved *int64     `json:"local_reserved,omitempty"`
	ResetAt       *time.Time `json:"reset_at,omitempty"`
	Status        string     `json:"status"`
	Resource      string     `json:"resource"`
	Unit          string     `json:"unit"`
	Remaining     *int64     `json:"remaining"`
	ObservedAt    *time.Time `json:"observed_at"`
	Precision     string     `json:"precision"`
}
type Connection struct {
	Host              *HostEvidence      `json:"host,omitempty"`
	ID                string             `json:"connection_id"`
	Workspace         domain.WorkspaceID `json:"workspace_id"`
	Revision          int64              `json:"revision"`
	ProviderType      string             `json:"provider_type"`
	Label             string             `json:"label"`
	Authentication    string             `json:"authentication"`
	CredentialPresent bool               `json:"credential_present"`
	NewWork           string             `json:"new_work"`
	AccountID         *string            `json:"account_id"`
	Selection         *Selection         `json:"selection"`
	Configuration     *Configuration     `json:"configuration,omitempty"`
	Quotas            []Quota            `json:"quotas"`
	ActiveAttempts    int                `json:"active_attempts"`
	UpdatedAt         time.Time          `json:"updated_at"`
}
type Operation struct {
	ID                 domain.OperationID `json:"operation_id"`
	Workspace          domain.WorkspaceID `json:"workspace_id"`
	ConnectionID       string             `json:"connection_id"`
	Action             string             `json:"action"`
	Status             string             `json:"status"`
	ConnectionRevision int64              `json:"connection_revision"`
	CreatedAt          time.Time          `json:"created_at"`
	UpdatedAt          time.Time          `json:"updated_at"`
	Problem            *string            `json:"problem"`
	Replay             bool               `json:"replay"`
}

// Verification is private adapter evidence. The canonical provider account is never
// serialized in the public Connection projection. Quota uncertainty survives success.
type Verification struct {
	CanonicalAccount     string
	Quota                provider.QuotaObservation
	Host                 *HostEvidence
	RuntimeConfig        []byte
	SelectionUnavailable bool
}

// HostEvidence is the last successfully verified public host identity and platform.
// Endpoint names, login identities and credentials are never included.
type HostEvidence struct {
	HostKeyFingerprint string         `json:"host_key_fingerprint"`
	OS                 string         `json:"os"`
	Architecture       string         `json:"architecture"`
	Image              *ImageEvidence `json:"image,omitempty"`
}

var hostFingerprint = regexp.MustCompile(`^SHA256:[A-Za-z0-9+/]{43}$`)

func (h HostEvidence) Validate() error {
	if !hostFingerprint.MatchString(h.HostKeyFingerprint) || h.OS != "linux" || (h.Architecture != "amd64" && h.Architecture != "arm64") {
		return ErrRequest
	}
	if h.Image != nil && h.Image.Validate() != nil {
		return ErrRequest
	}
	return nil
}

// ImageEvidence identifies the repository recipe and an installed immutable image.
type ImageEvidence struct {
	Status       string `json:"status"`
	RecipeSHA256 string `json:"recipe_sha256"`
	ID           string `json:"id,omitempty"`
}

func (i ImageEvidence) Validate() error {
	if !domain.SHA256Digest(i.RecipeSHA256).Valid() || (i.Status != "missing" && i.Status != "ready") ||
		i.Status == "missing" && i.ID != "" || i.Status == "ready" && (!strings.HasPrefix(i.ID, "sha256:") || !domain.SHA256Digest(strings.TrimPrefix(i.ID, "sha256:")).Valid()) {
		return ErrRequest
	}
	return nil
}

type ImageInstallation struct {
	RecipeSHA256 string `json:"recipe_sha256"`
	Architecture string `json:"architecture"`
}

func (i ImageInstallation) Validate() error {
	if !domain.SHA256Digest(i.RecipeSHA256).Valid() || (i.Architecture != "amd64" && i.Architecture != "arm64") {
		return ErrRequest
	}
	return nil
}

// ImageInstaller is a bounded explicit management action, separate from job preparation.
// The durable record fixes the recipe, verified account and operation identity.
type ImageInstaller interface {
	PlanImageInstallation(Connection) (ImageInstallation, error)
	InstallImage(context.Context, []byte, Record, bool) (Verification, error)
}

type Adapter interface {
	Descriptor() Descriptor
	// Encode validates declared provider fields and returns owned opaque credential bytes.
	Encode(map[string]string) ([]byte, error)
	Verify(context.Context, []byte) (Verification, error)
	Profile(domain.ProviderBinding, string) admission.Profile
}

// BoundVerifier pins prior account identity before offering a credential on recheck.
type BoundVerifier interface {
	VerifyBound(context.Context, []byte, string) (Verification, error)
}

// ConfigurationValidator opts an adapter into process and concurrency overrides.
type ConfigurationValidator interface{ ValidateConfiguration(Configuration) error }

// RuntimeConfigurator freezes adapter-owned non-secret runtime policy, never host paths or credentials.
type RuntimeConfigurator interface{ RuntimeConfig() []byte }

// Record is private worker state, not a public serializer.
type Record struct {
	Operation           Operation
	Stage               string
	ProviderType        string
	CanonicalAccount    string
	CredentialKey       string
	ActiveCredentialKey string
	ActorTokenID        string
	Configuration       *Configuration
	Installation        *ImageInstallation
}
type Accept struct {
	Workspace        domain.WorkspaceID
	TokenID          string
	KeyHash          string
	Fingerprint      string
	ConnectionID     string
	ExpectedRevision int64
	ProviderType     string
	Label            string
	Action           string
	Secret           bool
	Configuration    *Configuration
	Installation     *ImageInstallation
	Now              time.Time
}
type Completion struct {
	RuntimeConfig []byte
	Verification  Verification
	Profile       admission.Profile
	Problem       string
	Now           time.Time
}
type Repository interface {
	FingerprintInitialized(context.Context) (bool, error)
	MarkFingerprintInitialized(context.Context) error
	ReplayConnection(context.Context, Accept) (Record, bool, error)
	AcceptConnection(context.Context, Accept) (Record, error)
	ReadyConnection(context.Context, domain.WorkspaceID, string, domain.OperationID) error
	DeferConnectionSecret(context.Context, domain.OperationID, time.Time) error
	FailConnection(context.Context, domain.OperationID, string, time.Time) error
	NextConnectionOperation(context.Context, time.Time) (Record, bool, error)
	BeginConnectionInstall(context.Context, domain.OperationID, time.Time) (bool, error)
	FinishConnection(context.Context, Record, Completion) error
	ReadConnection(context.Context, domain.WorkspaceID, string, string, time.Time) (Connection, error)
	ListConnections(context.Context, domain.WorkspaceID, string, time.Time) ([]Connection, error)
	ReadConnectionOperation(context.Context, domain.WorkspaceID, string, domain.OperationID) (Operation, error)
	ResolveConnectionCredential(context.Context, string) (string, error)
	ConnectionSecrets(context.Context, domain.OperationID) ([]string, error)
	NextSecretDeletion(context.Context) (string, bool, error)
	CompleteSecretDeletion(context.Context, string) error
}

// RuntimeBinding is private reconstruction evidence for an exact frozen profile.
// The canonical account and credential reference never enter public projections.
type RuntimeBinding struct {
	ProviderType     string            `json:"-"`
	CanonicalAccount string            `json:"-"`
	Profile          admission.Profile `json:"-"`
	RuntimeConfig    []byte            `json:"-"`
}
