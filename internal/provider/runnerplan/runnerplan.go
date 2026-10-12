// Package runnerplan builds the provider-neutral compute-relay/runner/v1 manifest for one
// frozen plan. Adapters stage the bundle and inputs under the returned relative paths and
// enforce their own resource and network policy.
package runnerplan

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

// BundlePath is the staged bundle file name relative to the staging root.
const BundlePath = "code.bin"

var ErrPlan = errors.New("plan cannot form a runner manifest")

// InputPath is the staged file name of the plan's i-th frozen input.
func InputPath(i int) string { return fmt.Sprintf("input-%03d.bin", i) }

// Build checks the plan against its own frozen specification and returns the manifest
// with the parsed specification. Labels, source URLs, profile names and credential
// references are never copied into the manifest.
func Build(plan provider.Plan) (map[string]any, admission.Spec, error) {
	var none admission.Spec
	if plan.Job.Inputs == nil {
		return nil, none, ErrPlan
	}
	req, err := admission.Parse(plan.Job.Specification)
	if err != nil {
		return nil, none, ErrPlan
	}
	spec := req.Spec()
	if spec.Profile != plan.Job.Binding.Profile || spec.Bundle.ObjectID != plan.Job.Inputs.Bundle.ID ||
		spec.Timeouts.RemoteWallSeconds != plan.Job.WallSeconds || len(spec.Inputs) != len(plan.Job.Inputs.Inputs) {
		return nil, none, ErrPlan
	}
	// Copy only remote execution fields from the already strictly validated JSON.
	var fields map[string]json.RawMessage
	if json.Unmarshal(req.Canonical(), &fields) != nil {
		return nil, none, ErrPlan
	}
	manifest := map[string]any{
		"manifest_version": "compute-relay/runner/v1", "job_id": plan.Job.Identity.JobID,
		"attempt_id": plan.Job.Identity.AttemptID, "attempt_nonce": plan.Job.Identity.Nonce,
		"input_manifest_sha256": plan.Job.Identity.InputManifestSHA256,
		"bundle":                map[string]any{"path": BundlePath, "bytes": plan.Job.Inputs.Bundle.Bytes, "sha256": plan.Job.Inputs.Bundle.SHA256},
	}
	for _, key := range []string{"execution", "outputs", "resources", "network", "timeouts"} {
		manifest[key] = fields[key]
	}
	if workloadSecrets, exists := fields["workload_secrets"]; exists {
		manifest["workload_secrets"] = workloadSecrets
	}
	if spec.SeparateStart() {
		manifest["execution_control"] = "prepare_then_start"
	}
	inputs := make([]map[string]any, 0, len(spec.Inputs))
	for i, in := range spec.Inputs {
		frozen := plan.Job.Inputs.Inputs[i]
		if in.Source.Kind != "object" || in.Source.ObjectID != frozen.Object.ID || in.Name != frozen.Name || in.Target != frozen.Target {
			return nil, none, ErrPlan
		}
		inputs = append(inputs, map[string]any{"name": in.Name, "target": in.Target, "path": InputPath(i), "bytes": frozen.Object.Bytes, "sha256": frozen.Object.SHA256})
	}
	manifest["inputs"] = inputs
	return manifest, spec, nil
}
