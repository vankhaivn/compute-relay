package sshhost

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

//go:embed image/Dockerfile
var standardDockerfile []byte

const imageRecipeLabel = "io.compute-relay.image-recipe"
const imageOperationLabel = "io.compute-relay.image-operation"

// RunnerImageCountLimit bounds fresh standard-runner builds independently of bytes.
// Historical installation intents are separately bounded by the control plane.
const RunnerImageCountLimit = provider.RunnerImageCountLimit

var ErrImageInstallationUnknown = errors.New("image installation outcome unknown")

// ImageRecipeDigest covers the exact embedded Dockerfile, including its base digest.
func ImageRecipeDigest() string {
	sum := sha256.Sum256(standardDockerfile)
	return hex.EncodeToString(sum[:])
}

func standardImageTag() string { return "compute-relay/runner:recipe-" + ImageRecipeDigest() }
func operationImageTag(operation domain.OperationID) string {
	return "compute-relay/runner:install-" + string(operation)
}
func ValidImageID(id string) bool {
	return strings.HasPrefix(id, "sha256:") && domain.SHA256Digest(strings.TrimPrefix(id, "sha256:")).Valid()
}

func (d docker) runnerImageCapacity(ctx context.Context) error {
	var out bytes.Buffer
	result, err := d.run(ctx, nil, &limitedWriter{w: &out, limit: 64 << 10}, "image", "ls", "--all", "--no-trunc",
		"--filter", "label=io.compute-relay.image-schema=1", "--format", "{{.ID}}")
	if err != nil || result.ExitCode != 0 {
		return failure(errors.Join(ErrImageInstallationUnknown, err), "runner_image_inventory_unknown")
	}
	ids := map[string]bool{}
	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	if out.Len() == 0 {
		lines = nil
	}
	for _, id := range lines {
		if !ValidImageID(id) {
			return failure(ErrImageInstallationUnknown, "runner_image_inventory_unknown")
		}
		ids[id] = true
	}
	if len(ids) >= RunnerImageCountLimit {
		return failure(ErrImageInstallationUnknown, "runner_image_count_limit")
	}
	return nil
}

func (d docker) installedImage(ctx context.Context, ref, architecture, operation string) (imageInfo, bool, error) {
	image, found, err := d.inspectImage(ctx, ref)
	if err != nil || !found {
		return imageInfo{}, found, err
	}
	if !ValidImageID(image.ID) || image.OS != "linux" || image.Architecture != architecture ||
		image.Config.Labels[imageRecipeLabel] != ImageRecipeDigest() || image.Config.Labels["io.compute-relay.image-schema"] != "1" ||
		operation != "" && image.Config.Labels[imageOperationLabel] != operation {
		return imageInfo{}, false, failure(ErrIdentity, "installed_image_identity_unverified")
	}
	return image, true, nil
}

// VerifyManagedHost also inspects the current recipe image, without pulling or building.
func VerifyManagedHost(ctx context.Context, raw []byte) (HostVerification, error) {
	return VerifyManagedHostBound(ctx, raw, "")
}

// VerifyManagedHostBound refuses a changed pinned identity before authentication.
func VerifyManagedHostBound(ctx context.Context, raw []byte, account string) (HostVerification, error) {
	return withImageHost(ctx, raw, account, func(ctx context.Context, d docker, verified HostVerification) (HostVerification, error) {
		image, found, err := d.installedImage(ctx, standardImageTag(), verified.Architecture, "")
		if err == nil && found {
			verified.ImageID = image.ID
		}
		return verified, err
	})
}

// InstallImage executes one fixed metadata-only build. The caller must persist its
// one-shot barrier first. Recovery inspects the exact operation tag and never builds.
func InstallImage(ctx context.Context, raw []byte, account, machineID, architecture string, operation domain.OperationID, recoverOnly bool) (HostVerification, error) {
	if !operation.Valid() || !strings.HasPrefix(string(operation), "op_") || account == "" ||
		machineID == "" && !recoverOnly || machineID != "" && !domain.SHA256Digest(machineID).Valid() || !SupportedArchitecture(architecture) {
		return HostVerification{}, failure(ErrRequest, "install_image_request_invalid")
	}
	return withImageHost(ctx, raw, account, func(ctx context.Context, d docker, verified HostVerification) (HostVerification, error) {
		if machineID != "" && verified.MachineID != machineID {
			return HostVerification{}, failure(ErrResourceIdentity, "install_image_identity_unverified")
		}
		if verified.Architecture != architecture {
			return HostVerification{}, failure(ErrIdentity, "install_image_identity_unverified")
		}
		tag := operationImageTag(operation)
		image, found, err := d.installedImage(ctx, tag, verified.Architecture, string(operation))
		if err != nil {
			return HostVerification{}, err
		}
		if found {
			verified.ImageID = image.ID
			return verified, nil
		}
		if recoverOnly {
			return HostVerification{}, failure(ErrImageInstallationUnknown, "install_image_outcome_unknown")
		}
		if err := d.runnerImageCapacity(ctx); err != nil {
			return HostVerification{}, err
		}
		// The tar contains only the repository recipe. No job, credential, local context,
		// provider Dockerfile, build argument or remote URL enters this build.
		buildResult, err := d.run(ctx, singleFile("Dockerfile", standardDockerfile), nil, "build", "--pull=false", "--network=none",
			"--platform", "linux/"+verified.Architecture, "--label", imageRecipeLabel+"="+ImageRecipeDigest(),
			"--label", imageOperationLabel+"="+string(operation), "--tag", tag, "--tag", standardImageTag(), "-")
		if err != nil {
			return HostVerification{}, failure(errors.Join(ErrImageInstallationUnknown, err), "install_image_outcome_unknown")
		}
		image, found, err = d.installedImage(ctx, tag, verified.Architecture, string(operation))
		if err != nil || !found {
			reason := "install_image_outcome_unknown"
			if buildResult.ExitCode != 0 {
				reason = "docker_image_build_failed"
			}
			return HostVerification{}, failure(errors.Join(ErrImageInstallationUnknown, err), reason)
		}
		verified.ImageID = image.ID
		return verified, nil
	})
}

func withImageHost(ctx context.Context, raw []byte, account string, use func(context.Context, docker, HostVerification) (HostVerification, error)) (HostVerification, error) {
	endpoint, err := ParseEndpoint(raw)
	if err != nil {
		return HostVerification{}, failure(err, "ssh_endpoint_invalid")
	}
	if account != "" {
		endpoint, err = endpoint.Pin(account)
		if err != nil {
			return HostVerification{}, failure(err, "host_key_mismatch")
		}
	}
	client, err := Dial(ctx, endpoint.Target, endpoint.Credential)
	if err != nil {
		return HostVerification{}, failure(err, failureReasonForTransport(err))
	}
	defer client.Close()
	verified, err := verifyDocker(ctx, client, endpoint, client.HostKey)
	if err != nil {
		return HostVerification{}, err
	}
	binary, err := findDocker(ctx, client, endpoint.DockerPath)
	if err != nil {
		return HostVerification{}, err
	}
	d, err := (docker{exec: client, binary: binary}).localResource(ctx)
	if err != nil {
		return HostVerification{}, err
	}
	os, arch, err := d.platform(ctx)
	if err != nil || os != verified.OS || arch != verified.Architecture {
		return HostVerification{}, failure(errors.Join(ErrResourceIdentity, err), "managed_host_identity_unverified")
	}
	verified.MachineID, err = machineIdentity(ctx, client)
	if err != nil {
		return HostVerification{}, err
	}
	verified.EngineID, err = d.engineIdentity(ctx)
	if err != nil {
		return HostVerification{}, err
	}
	return use(ctx, d, verified)
}
