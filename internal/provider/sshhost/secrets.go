package sshhost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

// Only this transient packet contains values. Docker metadata, start control and
// the fixed SSH argv contain non-secret identities and paths. No receiver output
// is forwarded and a lost acknowledgement is never retried.
func deliverSecrets(ctx context.Context, d docker, container string, prepared provider.Prepared, material workloadsecret.Material) error {
	packet := struct {
		Version         string              `json:"version"`
		JobID           domain.JobID        `json:"job_id"`
		AttemptID       domain.AttemptID    `json:"attempt_id"`
		AttemptNonce    string              `json:"attempt_nonce"`
		ManifestSHA256  domain.SHA256Digest `json:"manifest_sha256"`
		AuthorizationID domain.OperationID  `json:"authorization_id"`
		ExpiresAt       string              `json:"expires_at"`
		Values          map[string][]byte   `json:"values"`
	}{"compute-relay/secret-delivery/v1", prepared.Identity.JobID, prepared.Identity.AttemptID, prepared.Identity.Nonce, prepared.ManifestSHA256, material.AuthorizationID, material.ExpiresAt, material.Values}
	raw, err := json.Marshal(packet)
	if err != nil || len(raw) > workloadsecret.MaxRequestBytes {
		clear(raw)
		return failure(ErrRequest, "deliver_secrets_request_invalid")
	}
	defer clear(raw)
	bounded, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	result, err := d.run(bounded, bytes.NewReader(raw), io.Discard, "container", "exec", "-i", "--user", "65534:65534", container,
		"python3", "-E", "-s", "/relay/runner/run.py", "--receive-secrets", "--manifest", "/relay/manifest.json", "--start-control", startControlPath)
	clear(result.Stderr)
	if err != nil || result.ExitCode != 0 {
		return failure(errors.Join(ErrRemoteDocker, err), "workload_secret_delivery_failed")
	}
	return nil
}
