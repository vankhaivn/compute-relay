package sshhost

import (
	"context"
	"io"
	"path"
	"strings"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hosthealth"
)

// CollectHealth uses the selected vault entry and the already verified account
// and machine. Engine pins, when present, come from frozen runtime policy.
func CollectHealth(ctx context.Context, raw []byte, account, resourceID, engineID string) (hosthealth.Snapshot, error) {
	if (strings.TrimPrefix(resourceID, "resource_") == resourceID) ||
		!domain.SHA256Digest(strings.TrimPrefix(resourceID, "resource_")).Valid() ||
		engineID != "" && !domain.SHA256Digest(engineID).Valid() {
		return hosthealth.Snapshot{}, ErrResourceIdentity
	}
	endpoint, err := ParseEndpoint(raw)
	if err != nil {
		return hosthealth.Snapshot{}, ErrRequest
	}
	defer clear(endpoint.Credential.PrivateKey)
	defer clear(endpoint.Credential.Passphrase)
	defer clear(endpoint.Credential.Password)
	endpoint, err = endpoint.Pin(account)
	if err != nil {
		return hosthealth.Snapshot{}, ErrHostKeyMismatch
	}
	ctx, cancel := context.WithTimeout(ctx, hosthealth.RefreshTimeout)
	defer cancel()
	client, err := Dial(ctx, endpoint.Target, endpoint.Credential)
	if err != nil {
		return hosthealth.Snapshot{}, err
	}
	defer client.Close()
	return collectHealth(ctx, healthBoundedExecutor{client}, endpoint, resourceID, engineID)
}

// Identity and Docker discovery use the same per-command bound as source probes.
type healthBoundedExecutor struct{ executor }

func (e healthBoundedExecutor) Run(ctx context.Context, env map[string]string, args []string, in io.Reader, out io.Writer) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, hosthealth.CommandTimeout)
	defer cancel()
	if closer, ok := e.executor.(io.Closer); ok {
		// Session channel creation and Start can wait before Run reaches its
		// cancellation select. Closing this probe's client bounds those phases too.
		stop := context.AfterFunc(ctx, func() { _ = closer.Close() })
		defer stop()
	}
	result, err := e.executor.Run(ctx, env, args, in, out)
	clear(result.Stderr)
	result.Stderr = nil
	return result, err
}

type healthExecutor struct {
	exec   executor
	binary string
}

func (e healthExecutor) Run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	env := map[string]string{"PATH": "/usr/bin:/bin:/usr/sbin:/sbin"}
	if e.binary != "" {
		env["PATH"] = path.Dir(e.binary) + ":" + env["PATH"]
	}
	// Collect supplies an explicit --host for Docker. Clear both ambient overrides
	// so a login context cannot redirect that fixed local endpoint.
	args = append([]string{"/usr/bin/env", "-u", "DOCKER_CONTEXT", "-u", "DOCKER_HOST"}, args...)
	result, err := e.exec.Run(ctx, env, args, nil, stdout)
	if len(result.Stderr) != 0 {
		_, _ = stderr.Write(result.Stderr)
		clear(result.Stderr)
	}
	if err != nil || result.ExitCode != 0 {
		return ErrUnavailable
	}
	return nil
}

func collectHealth(ctx context.Context, exec executor, endpoint Endpoint, resourceID, engineID string) (hosthealth.Snapshot, error) {
	machine, err := machineIdentity(ctx, exec)
	if err != nil || "resource_"+machine != resourceID {
		return hosthealth.Snapshot{}, ErrResourceIdentity
	}
	var config hosthealth.Config
	binary, err := findDocker(ctx, exec, endpoint.DockerPath)
	if err == nil {
		d, localErr := (docker{exec: exec, binary: binary}).localResource(ctx)
		if localErr != nil {
			return hosthealth.Snapshot{}, ErrResourceIdentity
		}
		actualEngine, identityErr := d.engineIdentity(ctx)
		if identityErr == nil && (engineID == "" || actualEngine == engineID) {
			config = hosthealth.Config{DockerPath: binary, DockerHost: d.host}
		} else if engineID != "" {
			return hosthealth.Snapshot{}, ErrResourceIdentity
		}
	} else if engineID != "" {
		return hosthealth.Snapshot{}, ErrResourceIdentity
	}
	// A machine verified before image installation can still expose host metrics.
	// If its local Docker budget cannot be observed, those metrics remain unavailable.
	return hosthealth.Collect(ctx, healthExecutor{exec: exec, binary: config.DockerPath}, resourceID, config), nil
}
