package operations

import (
	"regexp"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

// The ledger currently permits one staging and one execution row per attempt.
// Keep the bound explicit at every read and wire boundary.
const MaxCleanupResources = 2

var resourceReason = regexp.MustCompile(`^[a-z][a-z0-9_]{0,95}$`)

type ResourceState string

const (
	ResourcePending  ResourceState = "pending"
	ResourceRunning  ResourceState = "running"
	ResourceDeleted  ResourceState = "deleted"
	ResourceAbsent   ResourceState = "absent"
	ResourceRetained ResourceState = "retained"
	ResourceFailed   ResourceState = "failed"
	ResourceUnknown  ResourceState = "unknown"
)

func (s ResourceState) Valid() bool {
	switch s {
	case ResourcePending, ResourceRunning, ResourceDeleted, ResourceAbsent, ResourceRetained, ResourceFailed, ResourceUnknown:
		return true
	}
	return false
}

func (s ResourceState) Confirmed() bool { return s == ResourceDeleted || s == ResourceAbsent }

type ResourceResult struct {
	ResourceID  domain.ProviderResourceID `json:"resource_id"`
	Purpose     string                    `json:"purpose"`
	Fingerprint domain.SHA256Digest       `json:"target_sha256"`
	State       ResourceState             `json:"state"`
	Reason      string                    `json:"reason"`
}

func (r ResourceResult) Valid() bool {
	return r.ResourceID.Valid() && (r.Purpose == "staging" || r.Purpose == "execution") && r.Fingerprint.Valid() &&
		r.State.Valid() && resourceReason.MatchString(r.Reason)
}
