package kaggle

import (
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	runnerassets "github.com/vankhaivn/compute-relay/runner"
)

const maxKernelSource = 2 << 20

var ErrExecutionIdentity = errors.New("kernel identity, source, version or privacy does not match the recorded attempt")

// ExecutionPolicy is immutable operator policy, not a field accepted from a job.
// GPU shape must be selected explicitly; empty means CPU-only. No paid-capacity,
// priority, custom-image, interactive-session or fallback option exists here.
type ExecutionPolicy struct {
	MachineShape   string
	AllowInternet  bool
	MaxWallSeconds int64
	Timeout        time.Duration
}

func DefaultExecutionPolicy() ExecutionPolicy {
	return ExecutionPolicy{MaxWallSeconds: 1800, Timeout: time.Minute}
}
func (p ExecutionPolicy) valid() bool {
	return (p.MachineShape == "" || p.MachineShape == "NvidiaTeslaT4" || p.MachineShape == "NvidiaTeslaP100") &&
		p.MaxWallSeconds > 0 && p.MaxWallSeconds <= 86400 && p.Timeout >= time.Second && p.Timeout <= 5*time.Minute
}

type executionRequest struct {
	Protocol     int                 `json:"protocol"`
	Owner        string              `json:"owner"`
	Slug         string              `json:"slug"`
	Source       string              `json:"source"`
	SourceSHA256 domain.SHA256Digest `json:"source_sha256"`
	Dataset      string              `json:"dataset"`
	WallSeconds  int64               `json:"wall_seconds"`
	Internet     bool                `json:"internet"`
	GPU          bool                `json:"gpu"`
	MachineShape string              `json:"machine_shape"`
	KernelID     string              `json:"kernel_id"`
}

type stagingReference struct {
	DatasetID string              `json:"dataset_id"`
	Reference string              `json:"reference"`
	Version   int                 `json:"version"`
	Marker    domain.SHA256Digest `json:"marker_sha256"`
}

//go:embed execution_bootstrap.py
var executionBootstrap string

// buildExecutionRequest only constructs inert source. It does not import Python,
// resolve credentials, inspect mutable URLs or execute the runner on this host.
func buildExecutionRequest(c Config, stagingPolicy StagingPolicy, policy ExecutionPolicy, plan provider.Plan, prepared provider.Prepared) (executionRequest, error) {
	var none executionRequest
	if !policy.valid() || prepared.Validate(plan, prepared.PreparationID) != nil || !prepared.Ready || !prepared.Private {
		return none, ErrConfig
	}
	stage, err := buildStagingPlan(c, stagingPolicy, plan, prepared.PreparationID)
	if err != nil {
		return none, err
	}
	var ref stagingReference
	if closedObject([]byte(prepared.Resource), &ref) != nil || !positiveDecimal(ref.DatasetID) || ref.Version != 1 || ref.Reference != c.AccountName+"/"+stage.request.Slug || ref.Marker != stage.request.MarkerSHA256 {
		return none, ErrExecutionIdentity
	}
	req, err := admission.Parse(plan.Job.Specification)
	if err != nil {
		return none, ErrConfig
	}
	spec := req.Spec()
	if spec.Profile != plan.Job.Binding.Profile || spec.Bundle.ObjectID != plan.Job.Inputs.Bundle.ID || spec.Timeouts.RemoteWallSeconds != plan.Job.WallSeconds || plan.Job.WallSeconds > policy.MaxWallSeconds || len(spec.Inputs) != len(plan.Job.Inputs.Inputs) {
		return none, ErrConfig
	}
	gpu := spec.Resources.Accelerator == "gpu"
	if (gpu && policy.MachineShape == "") || (!gpu && policy.MachineShape != "") || (spec.Network.RemoteInternet == "required" && !policy.AllowInternet) {
		return none, ErrConfig
	}
	// Copy only remote execution fields from the already strictly validated JSON.
	// Labels, source URLs, profile names and credential references are not embedded.
	var fields map[string]json.RawMessage
	if json.Unmarshal(req.Canonical(), &fields) != nil {
		return none, ErrConfig
	}
	manifest := map[string]any{
		"manifest_version": "compute-relay/runner/v1", "job_id": plan.Job.Identity.JobID,
		"attempt_id": plan.Job.Identity.AttemptID, "attempt_nonce": plan.Job.Identity.Nonce,
		"input_manifest_sha256": plan.Job.Identity.InputManifestSHA256,
		"bundle":                map[string]any{"path": "code.bin", "bytes": plan.Job.Inputs.Bundle.Bytes, "sha256": plan.Job.Inputs.Bundle.SHA256},
	}
	for _, key := range []string{"execution", "outputs", "resources", "network", "timeouts"} {
		manifest[key] = fields[key]
	}
	inputs := make([]map[string]any, 0, len(spec.Inputs))
	for i, in := range spec.Inputs {
		frozen := plan.Job.Inputs.Inputs[i]
		if in.Source.Kind != "object" || in.Source.ObjectID != frozen.Object.ID || in.Name != frozen.Name || in.Target != frozen.Target {
			return none, ErrConfig
		}
		inputs = append(inputs, map[string]any{"name": in.Name, "target": in.Target, "path": fmt.Sprintf("input-%03d.bin", i), "bytes": frozen.Object.Bytes, "sha256": frozen.Object.SHA256})
	}
	manifest["inputs"] = inputs
	modules, err := runnerassets.Sources()
	if err != nil {
		return none, ErrConfig
	}
	payload, err := json.Marshal(map[string]any{
		"identity": plan.Job.Identity, "plan_sha256": plan.Digest(),
		"preparation_id": prepared.PreparationID, "dataset_id": ref.DatasetID,
		"dataset_slug": stage.request.Slug, "marker_sha256": ref.Marker,
		"manifest": manifest, "modules": modules, "remote_policy": map[string]any{"gpu": gpu, "machine_shape": policy.MachineShape},
	})
	if err != nil || len(payload) > 1<<20 {
		return none, ErrConfig
	}
	source := executionBootstrap + "\nrun_remote(json.loads(base64.b64decode(\"" + base64.StdEncoding.EncodeToString(payload) + "\", validate=True)))\n"
	if len(source) > maxKernelSource {
		return none, ErrConfig
	}
	id := plan.Job.Identity
	seed, err := json.Marshal([]string{string(id.InstallationID), string(id.WorkspaceID), string(id.InstanceID), string(id.JobID), string(id.AttemptID), string(id.IntentID), id.ResourceKey, id.Nonce})
	if err != nil {
		return none, ErrConfig
	}
	return executionRequest{Protocol: 1, Owner: c.AccountName, Slug: "cre-" + string(provider.Digest(seed))[:40], Source: source, SourceSHA256: provider.Digest([]byte(source)), Dataset: ref.Reference, WallSeconds: plan.Job.WallSeconds, Internet: spec.Network.RemoteInternet == "required", GPU: gpu, MachineShape: policy.MachineShape}, nil
}

func positiveDecimal(value string) bool {
	n, err := strconv.ParseInt(value, 10, 64)
	return err == nil && n > 0 && strconv.FormatInt(n, 10) == value
}
