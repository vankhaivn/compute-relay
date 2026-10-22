package provider

import (
	"context"
	"errors"
	"strconv"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

// RemoteResourceCountLimit bounds retained objects and uncertain creation intents,
// independently of their byte size. It is local operator policy, not provider quota.
const RemoteResourceCountLimit = 128

// RunnerImageCountLimit bounds declared standard runner image generations per host.
const RunnerImageCountLimit = 16

var ErrResourceCountLimit = errors.New("remote resource count limit reached")

// SharedResourceClaim links one content identity to its original preparation.
// Account scope is supplied by the trusted composition root, never by a workload.
type SharedResourceClaim struct {
	Identity      Identity
	PreparationID domain.OperationID
	PlanSHA256    domain.SHA256Digest
	ContentSHA256 domain.SHA256Digest
	Bytes         int64
}

func (c SharedResourceClaim) Valid() bool {
	return c.Identity.Validate() == nil && c.PreparationID.Valid() && c.PlanSHA256.Valid() && SharedResourceKey(c.ContentSHA256, c.Bytes) != ""
}

// SharedResourceKey is internal durable identity, never a remote name or public diagnostic.
func SharedResourceKey(sha domain.SHA256Digest, bytes int64) string {
	if !sha.Valid() || bytes < 0 {
		return ""
	}
	return string(sha) + "_" + strconv.FormatInt(bytes, 10)
}

// SharedResourceClaims reserves before observation and grants a creation invocation
// once. False never permits another upload or create, including after restart.
type SharedResourceClaims interface {
	TrackSharedUse(context.Context, SharedResourceClaim) error
	ClaimSharedCreation(context.Context, SharedResourceClaim) (bool, error)
}

// Managed staging uses these defaults; custom adapter policy may select a different
// threshold without changing the original frozen provider plan.
const DefaultSharedInputMinBytes int64 = 256 << 20
const DefaultSharedInputLimit = 7

// SelectSharedInputs keeps first occurrence order and counts unique content,
// leaving further distinct inputs inline when the provider mount bound is full.
func SelectSharedInputs(inputs []FrozenInput, minBytes int64, limit int) []domain.ObjectMetadata {
	if minBytes <= 0 || limit <= 0 {
		return nil
	}
	seen := map[string]bool{}
	var selected []domain.ObjectMetadata
	for _, input := range inputs {
		object := input.Object
		if object.Bytes < minBytes {
			continue
		}
		key := SharedResourceKey(object.SHA256, object.Bytes)
		if key == "" || seen[key] {
			continue
		}
		if len(selected) >= limit {
			break
		}
		seen[key] = true
		selected = append(selected, object)
	}
	return selected
}
