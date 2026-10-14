package sqlite

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/jsonwire"
	"github.com/vankhaivn/compute-relay/internal/operations"
)

// cleanupRequestV1 retains only the data needed to verify historical request
// hashes. Its retired option never becomes worker input or mutation authority.
type cleanupRequestV1 struct {
	AttemptID            domain.AttemptID `json:"attempt_id"`
	DiscardUncollected   bool             `json:"discard_uncollected"`
	AcceptedNameDeletion bool             `json:"accept_name_based_delete,omitempty"`
	Reason               string           `json:"reason,omitempty"`
}

func decodeStoredOperationRequest(job domain.JobID, kind domain.OperationKind, raw []byte) (operations.Request, string, error) {
	if kind != domain.OperationCleanup {
		request, err := operations.Parse(kind, raw)
		if err != nil {
			return operations.Request{}, "", ErrCorrupt
		}
		digest, err := request.Digest(job, kind)
		if err != nil {
			return operations.Request{}, "", ErrCorrupt
		}
		return request, digest, nil
	}
	fields, err := jsonwire.Object(raw, operations.MaxRequestBytes)
	var saved cleanupRequestV1
	if err != nil || jsonwire.Fields(fields, []string{"attempt_id"}, []string{"discard_uncollected", "accept_name_based_delete", "reason"}) != nil || json.Unmarshal(raw, &saved) != nil {
		return operations.Request{}, "", ErrCorrupt
	}
	request := operations.Request{AttemptID: saved.AttemptID, DiscardUncollected: saved.DiscardUncollected, Reason: saved.Reason}
	if !job.Valid() || request.Validate(kind) != nil {
		return operations.Request{}, "", ErrCorrupt
	}
	canonical, err := json.Marshal(struct {
		Version string               `json:"version"`
		Job     domain.JobID         `json:"job_id"`
		Kind    domain.OperationKind `json:"kind"`
		Request cleanupRequestV1     `json:"request"`
	}{"cleanup/v1", job, kind, saved})
	if err != nil {
		return operations.Request{}, "", ErrCorrupt
	}
	digest := sha256.Sum256(canonical)
	return request, hex.EncodeToString(digest[:]), nil
}
