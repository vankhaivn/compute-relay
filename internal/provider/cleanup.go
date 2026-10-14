package provider

import (
	"regexp"
	"unicode"
	"unicode/utf8"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

// CleanupDescription is a deliberately small, non-secret projection of one recorded
// resource. It never includes credentials, URLs, filesystem paths or opaque receipts.
// Description does not verify current remote state and is never permission to delete.
type CleanupDescription struct {
	Kind            string                   `json:"kind"`
	ID              string                   `json:"id"`
	Name            string                   `json:"name"`
	Version         string                   `json:"version"`
	Scope           string                   `json:"scope"`
	Support         domain.CapabilitySupport `json:"support"`
	Reason          string                   `json:"reason"`
	Bytes           *int64                   `json:"bytes"`
	NameBasedDelete bool                     `json:"name_based_delete,omitempty"`
	Warning         string                   `json:"warning,omitempty"`
}

var cleanupLabel = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$`)
var cleanupReason = regexp.MustCompile(`^[a-z][a-z0-9_]{0,95}$`)

func (d CleanupDescription) Valid() bool {
	if !utf8.ValidString(d.Warning) || len(d.Warning) > 512 || d.NameBasedDelete != (d.Warning != "") {
		return false
	}
	for _, c := range d.Warning {
		if unicode.IsControl(c) {
			return false
		}
	}
	return cleanupLabel.MatchString(d.Kind) && cleanupLabel.MatchString(d.ID) && cleanupLabel.MatchString(d.Name) &&
		cleanupLabel.MatchString(d.Version) && cleanupReason.MatchString(d.Scope) && d.Support.Valid() &&
		cleanupReason.MatchString(d.Reason) && (d.Bytes == nil || *d.Bytes >= 0)
}

type CleanupResourceRequest struct {
	CleanupRequest
	Purpose              string
	OwnershipOperationID domain.OperationID
	PlanSHA256           domain.SHA256Digest
	// Runtime options are not part of the immutable ledger target/fingerprint.
	AcceptNameBasedDelete bool `json:",omitempty"`
	RecoverOnly           bool `json:",omitempty"`
}

func (r CleanupResourceRequest) Valid() bool {
	owned := r.Purpose == "staging" && r.OwnershipOperationID == r.CreationOperationID ||
		r.Purpose == "execution" && string(r.OwnershipOperationID) == string(r.Remote.Identity.IntentID)
	return owned && r.Remote.Validate() == nil && r.LedgerID.Valid() && r.CreationOperationID.Valid() &&
		r.OwnershipOperationID.Valid() && r.PlanSHA256.Valid() && (r.Purpose == "staging" || r.Purpose == "execution") &&
		(r.Mode == CleanupApply || r.Mode == CleanupDryRun)
}
