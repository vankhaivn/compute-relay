package kaggle

import (
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/runnerplan"
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
	manifest, spec, err := runnerplan.Build(plan)
	if err != nil || plan.Job.WallSeconds > policy.MaxWallSeconds {
		return none, ErrConfig
	}
	gpu := spec.Resources.Accelerator == "gpu"
	if (gpu && policy.MachineShape == "") || (!gpu && policy.MachineShape != "") || (spec.Network.RemoteInternet == "required" && !policy.AllowInternet) {
		return none, ErrConfig
	}
	modules, err := runnerassets.Sources()
	if err != nil {
		return none, ErrConfig
	}
	payload, err := json.Marshal(map[string]any{
		"identity": plan.Job.Identity, "plan_sha256": plan.Digest(),
		"preparation_id": prepared.PreparationID, "dataset_id": ref.DatasetID,
		"dataset_owner": c.AccountName, "dataset_slug": stage.request.Slug, "marker_sha256": ref.Marker,
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
