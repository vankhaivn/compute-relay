package kaggle

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

const maxStagingBytes int64 = (4 << 30) + (100 << 20) + (64 << 10)

var (
	ErrStaging         = errors.New("staging unresolved; inspect or reconcile the recorded identity, never repeat creation")
	ErrStagingInput    = errors.New("frozen staging input is unavailable or changed")
	ErrStagingIdentity = errors.New("staging identity, version, privacy or bytes do not match")
)

// StagingPolicy is operator configuration, not workload metadata. License labels
// preserve existing rights; they do not grant rights to upload arbitrary data.
type StagingPolicy struct {
	License  string
	MaxBytes int64
	Timeout  time.Duration
}

func DefaultStagingPolicy() StagingPolicy {
	return StagingPolicy{License: "other", MaxBytes: maxStagingBytes, Timeout: 5 * time.Minute}
}
func (p StagingPolicy) valid() bool {
	return (p.License == "other" || p.License == "unknown") &&
		p.MaxBytes > 0 && p.MaxBytes <= maxStagingBytes && p.Timeout >= time.Second && p.Timeout <= time.Hour
}

type stagingFile struct {
	Name   string              `json:"name"`
	Bytes  int64               `json:"bytes"`
	SHA256 domain.SHA256Digest `json:"sha256"`
}

type stagingRequest struct {
	Protocol     int                 `json:"protocol"`
	Owner        string              `json:"owner"`
	Slug         string              `json:"slug"`
	License      string              `json:"license"`
	MarkerSHA256 domain.SHA256Digest `json:"marker_sha256"`
	Files        []stagingFile       `json:"files"`
}

type stagingPlan struct {
	request stagingRequest
	marker  []byte
	objects []domain.ObjectMetadata // Same order as Files[1:]; never sent to SDK.
}

// The opaque name is derived entirely from the prewritten attempt identity and
// preparation operation. Changed payload/configuration cannot choose a new name
// underneath an existing intent. No human job name becomes a provider resource.
func buildStagingPlan(c Config, policy StagingPolicy, plan provider.Plan, operation domain.OperationID) (stagingPlan, error) {
	var result stagingPlan
	job := plan.Job
	if c.Validate() != nil || !policy.valid() || job.Validate() != nil || job.Inputs == nil || !operation.Valid() ||
		string(job.Identity.InstanceID) != c.InstanceID || string(job.Binding.ProviderInstanceID) != c.InstanceID ||
		job.Binding.ConfigurationRevision != c.Revision {
		return result, ErrConfig
	}
	id := job.Identity
	nameSeed, err := json.Marshal([]string{string(id.InstallationID), string(id.WorkspaceID), string(id.InstanceID), string(id.JobID), string(id.AttemptID), string(operation)})
	if err != nil {
		return result, ErrConfig
	}
	slug := "crs-" + string(provider.Digest(nameSeed))[:40]
	objects := []domain.ObjectMetadata{job.Inputs.Bundle}
	files := []stagingFile{{Name: "code.bin", Bytes: job.Inputs.Bundle.Bytes, SHA256: job.Inputs.Bundle.SHA256}}
	for i, input := range job.Inputs.Inputs {
		objects = append(objects, input.Object)
		files = append(files, stagingFile{Name: fmt.Sprintf("input-%03d.bin", i), Bytes: input.Object.Bytes, SHA256: input.Object.SHA256})
	}
	// Record identity and logical input mapping, never the job command, environment,
	// credential reference, source URL or local filesystem path.
	marker := struct {
		Format        string              `json:"format"`
		Identity      provider.Identity   `json:"identity"`
		PreparationID domain.OperationID  `json:"preparation_id"`
		PlanSHA256    domain.SHA256Digest `json:"plan_sha256"`
		License       string              `json:"license"`
		Files         []stagingFile       `json:"files"`
		Inputs        []struct {
			Name   string `json:"name"`
			Target string `json:"target"`
			File   string `json:"file"`
		} `json:"inputs"`
	}{Format: "compute-relay/staging/v1", Identity: id, PreparationID: operation, PlanSHA256: plan.Digest(), License: policy.License, Files: files}
	for i, input := range job.Inputs.Inputs {
		marker.Inputs = append(marker.Inputs, struct {
			Name   string `json:"name"`
			Target string `json:"target"`
			File   string `json:"file"`
		}{input.Name, input.Target, files[i+1].Name})
	}
	raw, err := json.Marshal(marker)
	if err != nil || len(raw) > 64<<10 {
		return result, ErrConfig
	}
	var total int64 = int64(len(raw))
	for _, f := range files {
		if f.Bytes > policy.MaxBytes-total {
			return result, ErrStagingInput
		}
		total += f.Bytes
	}
	if total > policy.MaxBytes {
		return result, ErrStagingInput
	}
	digest := provider.Digest(raw)
	all := append([]stagingFile{{Name: "relay-stage.bin", Bytes: int64(len(raw)), SHA256: digest}}, files...)
	return stagingPlan{request: stagingRequest{1, c.AccountName, slug, policy.License, digest, all}, marker: raw, objects: objects}, nil
}
