package kaggle

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

// The marker bound fits 256 inputs with 64-byte names and 1024-byte targets even when every
// target character is JSON-escaped (about 1.56 MiB).
const maxMarkerBytes = 2 << 20
const maxStagingBytes int64 = (4 << 30) + (100 << 20) + maxMarkerBytes

var (
	ErrStaging         = errors.New("staging unresolved; inspect or reconcile the recorded identity, never repeat creation")
	ErrStagingInput    = errors.New("frozen staging input is unavailable or changed")
	ErrStagingIdentity = errors.New("staging identity, version, privacy or bytes do not match")
)

// StagingPolicy is operator configuration, not workload metadata. License labels
// preserve existing rights; they do not grant rights to upload arbitrary data.
// Inputs of at least SharedMinBytes are staged once per account as shared datasets;
// zero stages every input with its attempt.
type StagingPolicy struct {
	License        string
	MaxBytes       int64
	Timeout        time.Duration
	SharedMinBytes int64
}

// The default budget fits the staging limit on a 10 Mbit/s uplink, so the one-shot upload is not
// cut before creation merely because the inputs are large. The shared threshold keeps a kernel at a
// few mounts while catching large unchanged inputs such as an application runtime.
func DefaultStagingPolicy() StagingPolicy {
	return StagingPolicy{License: "other", MaxBytes: maxStagingBytes, Timeout: time.Hour, SharedMinBytes: 256 << 20}
}
func (p StagingPolicy) valid() bool {
	return (p.License == "other" || p.License == "unknown") &&
		p.MaxBytes > 0 && p.MaxBytes <= maxStagingBytes && p.Timeout >= time.Second && p.Timeout <= time.Hour && p.SharedMinBytes >= 0
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
	objects []domain.ObjectMetadata // Same order as Files[1:]; never sent to the provider.
	shared  []sharedInput
	inputs  []stagedInput // One per frozen input, in plan order.
}

// stagedInput is where the remote runner finds one frozen input: a file in the
// attempt dataset or in a shared dataset.
type stagedInput struct {
	Slug string
	File string
}

const (
	sharedMarkerFile = "relay-shared.bin"
	sharedBlobFile   = "blob.bin"
	// maxSharedInputs leaves one of Kaggle's kernel mounts for the attempt dataset.
	maxSharedInputs = 7
)

// sharedInput is one content-addressed private dataset holding a single large input.
// Its name and marker derive only from the installation, account and content, so
// every attempt with the same bytes finds the same dataset.
type sharedInput struct {
	request stagingRequest
	marker  []byte
	object  domain.ObjectMetadata
}

func buildSharedInput(c Config, policy StagingPolicy, installation domain.RuntimeInstallationID, object domain.ObjectMetadata) (sharedInput, error) {
	seed, err := json.Marshal([]string{string(installation), c.AccountName, string(object.SHA256), fmt.Sprint(object.Bytes)})
	if err != nil {
		return sharedInput{}, ErrConfig
	}
	marker, err := json.Marshal(struct {
		Format string              `json:"format"`
		SHA256 domain.SHA256Digest `json:"sha256"`
		Bytes  int64               `json:"bytes"`
	}{"compute-relay/shared-input/v1", object.SHA256, object.Bytes})
	if err != nil {
		return sharedInput{}, ErrConfig
	}
	digest := provider.Digest(marker)
	files := []stagingFile{{Name: sharedMarkerFile, Bytes: int64(len(marker)), SHA256: digest},
		{Name: sharedBlobFile, Bytes: object.Bytes, SHA256: object.SHA256}}
	request := stagingRequest{1, c.AccountName, "crx-" + string(provider.Digest(seed))[:40], policy.License, digest, files}
	return sharedInput{request: request, marker: marker, object: object}, nil
}

// buildStagingPlans returns the plan for new preparation first. When it shares inputs,
// the layout used before shared inputs follows, so attempts prepared by an earlier
// release still match their pinned marker.
func buildStagingPlans(c Config, policy StagingPolicy, plan provider.Plan, operation domain.OperationID) ([]stagingPlan, error) {
	current, err := buildStagingPlan(c, policy, plan, operation)
	if err != nil || len(current.shared) == 0 {
		return []stagingPlan{current}, err
	}
	policy.SharedMinBytes = 0
	legacy, err := buildStagingPlan(c, policy, plan, operation)
	if err != nil {
		return nil, err
	}
	return []stagingPlan{current, legacy}, nil
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
	type markerInput struct {
		Name    string `json:"name"`
		Target  string `json:"target"`
		File    string `json:"file"`
		Dataset string `json:"dataset,omitempty"`
	}
	type markerShared struct {
		Dataset string              `json:"dataset"`
		SHA256  domain.SHA256Digest `json:"sha256"`
		Bytes   int64               `json:"bytes"`
	}
	inputs := make([]markerInput, 0, len(job.Inputs.Inputs))
	var shared []sharedInput
	var sharedRefs []markerShared
	bySlug := map[string]bool{}
	for i, input := range job.Inputs.Inputs {
		if policy.SharedMinBytes > 0 && input.Object.Bytes >= policy.SharedMinBytes {
			entry, err := buildSharedInput(c, policy, id.InstallationID, input.Object)
			if err != nil {
				return result, err
			}
			// Beyond the kernel's mount bound, further large inputs stay with the attempt.
			if bySlug[entry.request.Slug] || len(shared) < maxSharedInputs {
				if !bySlug[entry.request.Slug] {
					bySlug[entry.request.Slug] = true
					shared = append(shared, entry)
					sharedRefs = append(sharedRefs, markerShared{entry.request.Slug, input.Object.SHA256, input.Object.Bytes})
				}
				inputs = append(inputs, markerInput{input.Name, input.Target, sharedBlobFile, entry.request.Slug})
				result.inputs = append(result.inputs, stagedInput{entry.request.Slug, sharedBlobFile})
				continue
			}
		}
		name := fmt.Sprintf("input-%03d.bin", i)
		objects = append(objects, input.Object)
		files = append(files, stagingFile{Name: name, Bytes: input.Object.Bytes, SHA256: input.Object.SHA256})
		inputs = append(inputs, markerInput{input.Name, input.Target, name, ""})
		result.inputs = append(result.inputs, stagedInput{slug, name})
	}
	// Record identity and logical input mapping, never the job command, environment,
	// credential reference, source URL or local filesystem path. Without shared inputs
	// the marker keeps its original v1 bytes.
	format := "compute-relay/staging/v1"
	if len(shared) > 0 {
		format = "compute-relay/staging/v2"
	}
	marker := struct {
		Format        string              `json:"format"`
		Identity      provider.Identity   `json:"identity"`
		PreparationID domain.OperationID  `json:"preparation_id"`
		PlanSHA256    domain.SHA256Digest `json:"plan_sha256"`
		License       string              `json:"license"`
		Files         []stagingFile       `json:"files"`
		Inputs        []markerInput       `json:"inputs"`
		Shared        []markerShared      `json:"shared,omitempty"`
	}{Format: format, Identity: id, PreparationID: operation, PlanSHA256: plan.Digest(), License: policy.License, Files: files, Inputs: inputs, Shared: sharedRefs}
	if len(inputs) == 0 {
		marker.Inputs = nil
	}
	raw, err := json.Marshal(marker)
	if err != nil || len(raw) > maxMarkerBytes {
		return result, ErrConfig
	}
	var total int64 = int64(len(raw))
	for _, f := range files {
		if f.Bytes > policy.MaxBytes-total {
			return result, ErrStagingInput
		}
		total += f.Bytes
	}
	for _, entry := range shared {
		if entry.object.Bytes > policy.MaxBytes-total {
			return result, ErrStagingInput
		}
		total += entry.object.Bytes
	}
	digest := provider.Digest(raw)
	all := append([]stagingFile{{Name: "relay-stage.bin", Bytes: int64(len(raw)), SHA256: digest}}, files...)
	result.request, result.marker, result.objects, result.shared = stagingRequest{1, c.AccountName, slug, policy.License, digest, all}, raw, objects, shared
	return result, nil
}
