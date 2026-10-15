package sshhost

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/runnerplan"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
	runnerassets "github.com/vankhaivn/compute-relay/runner"
)

const startControlPath = "/relay/start.json"

var errStartClaimed = errors.New("original start control already exists")

func startExpired(s provider.StartAuthorization) provider.SubmissionOutcome {
	before, _ := time.Parse(time.RFC3339, s.StartBefore)
	deadline, _ := time.Parse(time.RFC3339, s.ExecutionDeadline)
	if before.Before(deadline) {
		p := provider.Problem(domain.CodeRemoteTimeout, domain.FailureStageExecution, "Preparation hold expired before execution.").WithDetail(domain.ReasonDetail, "preparation_expired")
		return provider.SubmissionOutcome{Status: provider.SubmissionRejected, Problem: &p}
	}
	return deadlineRejected()
}

type startControl struct {
	Version        string              `json:"version"`
	JobID          domain.JobID        `json:"job_id"`
	AttemptID      domain.AttemptID    `json:"attempt_id"`
	AttemptNonce   string              `json:"attempt_nonce"`
	ManifestSHA256 domain.SHA256Digest `json:"manifest_sha256"`
	provider.StartAuthorization
}

type stagedFile struct {
	Bytes  int64
	SHA256 domain.SHA256Digest
}

func expectedStaging(plan provider.Plan, modules map[string]string, manifest []byte, cached bool) map[string]stagedFile {
	files := map[string]stagedFile{}
	add := func(name string, raw []byte) { files[name] = stagedFile{int64(len(raw)), provider.Digest(raw)} }
	add("manifest.json", manifest)
	add("runner/run.py", []byte(entryPoint))
	for name, source := range modules {
		add("runner/relay_runner/"+name, []byte(source))
	}
	if !cached {
		files["staging/"+runnerplan.BundlePath] = stagedFile{plan.Job.Inputs.Bundle.Bytes, plan.Job.Inputs.Bundle.SHA256}
		for i, input := range plan.Job.Inputs.Inputs {
			files["staging/"+runnerplan.InputPath(i)] = stagedFile{input.Object.Bytes, input.Object.SHA256}
		}
	}
	return files
}

// observedPreparationHash binds the actual stopped container configuration, image,
// frozen job and complete runtime/input manifest. It contains no mutable file paths
// outside this exact owned container and cannot be reused for another attempt.
func observedPreparationHash(info containerInfo, planDigest domain.SHA256Digest, files map[string]stagedFile) domain.SHA256Digest {
	// Preserve the original v1 byte shape for stopped preparations made before
	// cache mounts existed. Adding a field to containerInfo's HostConfig alone
	// would otherwise change every legacy readiness revision on restart.
	if info.Config.Labels[labelPrefix+"cache"] == "" && len(info.HostConfig.Mounts) == 0 && len(info.Mounts) == 0 {
		host := struct {
			Tmpfs         map[string]string `json:"Tmpfs"`
			NetworkMode   string            `json:"NetworkMode"`
			PidMode       string            `json:"PidMode"`
			Privileged    bool              `json:"Privileged"`
			CapDrop       []string          `json:"CapDrop"`
			CapAdd        []string          `json:"CapAdd"`
			SecurityOpt   []string          `json:"SecurityOpt"`
			Binds         []string          `json:"Binds"`
			Memory        int64             `json:"Memory"`
			MemorySwap    int64             `json:"MemorySwap"`
			NanoCPUs      int64             `json:"NanoCpus"`
			PidsLimit     int64             `json:"PidsLimit"`
			RestartPolicy struct {
				Name string `json:"Name"`
			} `json:"RestartPolicy"`
		}{info.HostConfig.Tmpfs, info.HostConfig.NetworkMode, info.HostConfig.PidMode, info.HostConfig.Privileged,
			info.HostConfig.CapDrop, info.HostConfig.CapAdd, info.HostConfig.SecurityOpt, info.HostConfig.Binds,
			info.HostConfig.Memory, info.HostConfig.MemorySwap, info.HostConfig.NanoCPUs, info.HostConfig.PidsLimit, info.HostConfig.RestartPolicy}
		raw, _ := json.Marshal(struct {
			Version    string
			Container  string
			Image      string
			Config     any
			HostConfig any
			Plan       domain.SHA256Digest
			Files      map[string]stagedFile
		}{"ssh-readiness/v1", info.ID, info.Image, info.Config, host, planDigest, files})
		return provider.Digest(raw)
	}
	raw, _ := json.Marshal(struct {
		Version    string
		Container  string
		Image      string
		Config     any
		HostConfig any
		Mounts     any
		Plan       domain.SHA256Digest
		Files      map[string]stagedFile
	}{"ssh-readiness/v2", info.ID, info.Image, info.Config, info.HostConfig, info.Mounts, planDigest, files})
	return provider.Digest(raw)
}
func validStoppedPolicy(info containerInfo, internet, secrets bool) bool {
	if secrets {
		if len(info.HostConfig.Tmpfs) != 1 || info.HostConfig.Tmpfs[secretTmpfsPath] != secretTmpfsOptions {
			return false
		}
	} else if len(info.HostConfig.Tmpfs) != 0 {
		return false
	}
	network := "none"
	if internet {
		network = "bridge"
	}
	return !info.Started() && !info.State.Running && info.Config.User == "65534:65534" && info.Config.WorkingDir == "/relay" && len(info.Config.Entrypoint) == 0 && info.HostConfig.NetworkMode == network && info.HostConfig.PidMode == "" && !info.HostConfig.Privileged && len(info.HostConfig.Binds) == 0 && len(info.HostConfig.CapAdd) == 0 && slices.Contains(info.HostConfig.CapDrop, "ALL") && slices.Contains(info.HostConfig.SecurityOpt, "no-new-privileges") && info.HostConfig.RestartPolicy.Name == "no"
}

// A separate, fixed verifier hashes the stopped container's archive on the
// host. Only digests and sizes return over SSH; the target runs no code.
func (a *Adapter) verifyPreparedBytes(ctx context.Context, d docker, info containerInfo, plan provider.Plan, modules map[string]string, manifest []byte, bindings []cacheBinding) (domain.SHA256Digest, error) {
	req, err := admission.Parse(plan.Job.Specification)
	if err != nil || !validStoppedPolicy(info, req.Spec().Network.RemoteInternet == "required", len(req.Spec().WorkloadSecrets) > 0) {
		return "", ErrIdentity
	}
	if len(bindings) > 0 {
		if err := a.verifyCacheMounts(ctx, d, info, plan.Job.Identity, bindings); err != nil {
			return "", err
		}
	} else if len(info.HostConfig.Mounts) != 0 || len(info.Mounts) != 0 {
		return "", ErrIdentity
	}
	expected := expectedStaging(plan, modules, manifest, len(bindings) > 0)
	if err := a.verifyStagedOnHost(ctx, d, info, plan.Job.Identity, expected); err != nil {
		return "", err
	}
	return observedPreparationHash(info, plan.Digest(), expected), nil
}

func (a *Adapter) reconcileControlledPreparation(ctx context.Context, plan provider.Plan, operation domain.OperationID) (provider.PreparationObservation, error) {
	manifest, _, err := runnerplan.Build(plan)
	if err != nil {
		return provider.PreparationObservation{}, err
	}
	modules, err := runnerassets.Sources()
	if err != nil {
		return provider.PreparationObservation{}, err
	}
	seen := provider.PreparationObservation{Status: provider.ReconciliationNotFound}
	err = a.withDocker(ctx, func(d docker, _ Endpoint) error {
		name := containerName(plan.Job.Identity)
		info, found, err := d.inspect(ctx, name)
		if err != nil || !found {
			return err
		}
		if !owned(info, plan.Job.Identity) || info.Started() || info.Config.Labels[labelPrefix+"preparation"] != string(operation) || info.Config.Labels[labelPrefix+"plan"] != string(plan.Digest()) {
			return ErrIdentity
		}
		engine := info.Config.Labels[labelPrefix+"engine"]
		if err := d.verifyEngine(ctx, engine); err != nil {
			return err
		}
		p := provider.Prepared{Identity: plan.Job.Identity, PreparationID: operation, PlanSHA256: plan.Digest(), Resource: encodeRef(name, info.ID, engine), Private: true}
		raw, found, err := readFile(ctx, d, info.ID, stagedMarker, maxCacheMarkerBytes)
		if err != nil {
			return err
		}
		if !found {
			// Exact partial staging may be cancelled and cleaned up, but is never ready.
			seen = provider.PreparationObservation{Status: provider.ReconciliationFound, Prepared: &p}
			return nil
		}
		var staged marker
		var bindings []cacheBinding
		if json.Unmarshal(raw, &staged) != nil || staged.Preparation != operation || staged.Plan != plan.Digest() || staged.Container != info.ID || staged.EngineID != engine || !staged.Verification.Valid() || staged.Cache != (info.Config.Labels[labelPrefix+"cache"] == "volumes/v1") {
			return ErrIdentity
		}
		if staged.Cache {
			bindings, err = a.activeCachePlan(ctx, plan)
			if err != nil || len(bindings) == 0 {
				return ErrIdentity
			}
			if err := cacheManifest(manifest, plan, bindings); err != nil {
				return err
			}
		}
		rawManifest, _ := json.Marshal(manifest)
		if staged.Manifest != provider.Digest(rawManifest) {
			return ErrIdentity
		}
		image, found, err := d.inspectImage(ctx, a.config.Image)
		if err != nil {
			return err
		}
		if !found || info.Image != image.ID {
			return ErrIdentity
		}
		verified, err := a.verifyPreparedBytes(ctx, d, info, plan, modules, rawManifest, bindings)
		if err != nil {
			return err
		}
		if verified != staged.Verification {
			return ErrIdentity
		}
		p.Ready, p.VerificationSHA256, p.ManifestSHA256 = true, verified, staged.Manifest
		seen = provider.PreparationObservation{Status: provider.ReconciliationFound, Prepared: &p}
		return nil
	})
	if err == ErrIdentity {
		problem := provider.Problem(domain.CodeRemoteIdentityMismatch, domain.FailureStageObservation, "prepared container identity or verified bytes changed; no restaging is permitted")
		return provider.PreparationObservation{}, problem
	}
	return seen, err
}

// Start sends the prepared container's one-shot start command only after authority checks.
func (a *Adapter) Start(ctx context.Context, prepared provider.Prepared, authorization *provider.StartAuthorization, material *workloadsecret.Material) provider.SubmissionOutcome {
	if authorization == nil {
		if material != nil {
			return rejected("start_authorization_required")
		}
	} else if authorization.Secrets == nil {
		if material != nil || authorization.Validate(prepared) != nil {
			return rejected("start_authorization_required")
		}
	} else if material == nil || authorization.ValidateMaterial(prepared, *material) != nil {
		return rejected("workload_secret_unavailable")
	}
	return a.submit(ctx, prepared, authorization, material)
}

// Recheck small control files, container policy and all staged bytes before
// writing start authority. The on-host verifier does not start the target.
func (a *Adapter) checkControlledStart(ctx context.Context, d docker, info containerInfo, p provider.Prepared, authorization provider.StartAuthorization) error {
	if authorization.Validate(p) != nil || info.Config.Labels[labelPrefix+"plan"] != string(p.PlanSHA256) {
		return ErrIdentity
	}
	raw, found, err := readFile(ctx, d, info.ID, stagedMarker, maxCacheMarkerBytes)
	var m marker
	ref, refErr := decodeRef(p.Resource, p.Identity)
	cached := info.Config.Labels[labelPrefix+"cache"] == "volumes/v1"
	if refErr != nil || err != nil || !found || json.Unmarshal(raw, &m) != nil || m != (marker{Preparation: p.PreparationID, Plan: p.PlanSHA256, Container: info.ID, EngineID: ref.EngineID, Verification: p.VerificationSHA256, Manifest: p.ManifestSHA256, Cache: cached}) {
		return ErrIdentity
	}
	raw, found, err = readFile(ctx, d, info.ID, "/relay/manifest.json", maxManifestBytes)
	if err != nil || !found || provider.Digest(raw) != p.ManifestSHA256 {
		return ErrIdentity
	}
	var manifest struct {
		WorkloadSecrets  []workloadsecret.Slot `json:"workload_secrets"`
		ExecutionControl string                `json:"execution_control"`
		Bundle           struct {
			Path   string              `json:"path"`
			Bytes  int64               `json:"bytes"`
			SHA256 domain.SHA256Digest `json:"sha256"`
		} `json:"bundle"`
		Inputs []struct {
			Path   string              `json:"path"`
			Bytes  int64               `json:"bytes"`
			SHA256 domain.SHA256Digest `json:"sha256"`
		} `json:"inputs"`
		Network struct {
			RemoteInternet string `json:"remote_internet"`
		} `json:"network"`
		Timeouts struct {
			RemoteWallSeconds        int64  `json:"remote_wall_seconds"`
			SetupSeconds             int64  `json:"setup_seconds"`
			FinalizationGraceSeconds int64  `json:"finalization_grace_seconds"`
			ExecutionDeadline        string `json:"execution_deadline"`
		} `json:"timeouts"`
	}
	if json.Unmarshal(raw, &manifest) != nil || manifest.ExecutionControl != "prepare_then_start" || !validStoppedPolicy(info, manifest.Network.RemoteInternet == "required", len(manifest.WorkloadSecrets) > 0) || authorization.MaxRemoteWallSeconds > manifest.Timeouts.RemoteWallSeconds || authorization.MaxRemoteWallSeconds <= manifest.Timeouts.SetupSeconds+manifest.Timeouts.FinalizationGraceSeconds {
		return ErrIdentity
	}
	if len(manifest.WorkloadSecrets) > 0 {
		if authorization.Secrets == nil || !slices.Equal(manifest.WorkloadSecrets, authorization.Secrets.Slots) {
			return ErrIdentity
		}
	} else if authorization.Secrets != nil {
		return ErrIdentity
	}
	if bound := manifest.Timeouts.ExecutionDeadline; bound != "" {
		original, err := time.Parse(time.RFC3339, bound)
		deadline, deadlineErr := time.Parse(time.RFC3339, authorization.ExecutionDeadline)
		if err != nil || deadlineErr != nil || deadline.After(original) {
			return ErrIdentity
		}
	}
	modules, err := runnerassets.Sources()
	if err != nil {
		return err
	}
	files := map[string]stagedFile{"manifest.json": {int64(len(raw)), provider.Digest(raw)}, "runner/run.py": {int64(len(entryPoint)), provider.Digest([]byte(entryPoint))}}
	for name, source := range modules {
		files["runner/relay_runner/"+name] = stagedFile{int64(len(source)), provider.Digest([]byte(source))}
	}
	if !cached {
		files["staging/"+manifest.Bundle.Path] = stagedFile{manifest.Bundle.Bytes, manifest.Bundle.SHA256}
		for _, input := range manifest.Inputs {
			files["staging/"+input.Path] = stagedFile{input.Bytes, input.SHA256}
		}
	}
	if observedPreparationHash(info, p.PlanSHA256, files) != p.VerificationSHA256 {
		return ErrIdentity
	}
	// A stale/mismatched control file cannot be overwritten after a possible write.
	if _, exists, err := readFile(ctx, d, info.ID, startControlPath, 4096); err != nil {
		return err
	} else if exists {
		return errStartClaimed
	}
	if cached {
		return a.verifyCachedReadiness(ctx, d, info, p.Identity, p.PlanSHA256, raw, p.ManifestSHA256, p.VerificationSHA256)
	}
	if err := a.verifyStagedOnHost(ctx, d, info, p.Identity, files); err != nil {
		return err
	}
	return nil
}
