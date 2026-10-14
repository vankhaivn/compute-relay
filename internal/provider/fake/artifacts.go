package fake

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func (p *Basic) makeFiles(r *record) []File {
	files := append([]File(nil), p.backend.scenario.Files...)
	artifacts := make([]map[string]any, 0, len(files))
	for _, f := range files {
		artifacts = append(artifacts, map[string]any{"path": f.Path, "bytes": len(f.Content), "sha256": provider.Digest(f.Content)})
	}
	id := r.prepared.Identity
	state := p.state(r)
	phase := "completed"
	var exitCode any = 0
	var failure any
	timedOut := false
	gpuRequired := requiresGPU(r.plan.Job)
	gpuVerified := gpuRequired && p.backend.scenario.GPU == domain.CapabilitySupportSupported
	if state != domain.ExecutionSucceeded {
		phase = string(state)
		exitCode = nil
		code := domain.CodeCommandFailed
		if state == domain.ExecutionTimedOut {
			timedOut = true
			code = domain.CodeRemoteTimeout
		}
		if gpuRequired && !gpuVerified {
			phase = "resource_check_failed"
			code = domain.CodeResourceRequirementUnsatisfied
		}
		stage, _ := code.Category()
		failure = map[string]any{"code": code, "stage": string(stage), "message": "synthetic fixture failure"}
	}

	// Synthetic manifest for contract tests only. It does not attest that a command or
	// GPU ran. Metadata identifies the fixture runner explicitly.
	manifest, _ := json.Marshal(map[string]any{
		"manifest_version": "1", "runner_version": "fake-fixture-1", "job_id": id.JobID, "attempt_id": id.AttemptID,
		"attempt_nonce": id.Nonce, "bundle_sha256": id.BundleSHA256, "input_manifest_sha256": id.InputManifestSHA256,
		"started_at": p.clock.Now(), "finished_at": p.clock.Now(), "phase": phase, "exit_code": exitCode, "timed_out": timedOut,
		"resource_check": map[string]bool{"gpu_required": gpuRequired, "gpu_verified": gpuVerified}, "artifacts": artifacts, "error": failure,
	})
	files = append(files, File{Path: "execution-result.json", Content: append(manifest, '\n')})
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files
}
func (p *Basic) ListArtifacts(ctx context.Context, remote provider.RemoteReference) ([]provider.Artifact, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.backend.mu.Lock()
	defer p.backend.mu.Unlock()
	r, err := p.lookup(remote)
	if err != nil {
		return nil, err
	}
	if !p.state(r).Terminal() {
		return nil, provider.Problem(domain.CodeRemoteExecutionUnresolved, domain.FailureStageOperation, "artifacts are not collectible before terminal evidence")
	}
	if r.files == nil {
		r.files = p.makeFiles(r)
	}
	artifacts := make([]provider.Artifact, 0, len(r.files))
	var total int64
	for _, f := range r.files {
		if len(artifacts) >= provider.MaxArtifactFiles || int64(len(f.Content)) > provider.MaxArtifactBytes-total {
			return nil, errors.New("artifact catalog exceeds bounds")
		}
		total += int64(len(f.Content))
		artifacts = append(artifacts, provider.Artifact{Remote: remote, Path: f.Path, Bytes: int64(len(f.Content)), SHA256: provider.Digest(f.Content)})
	}
	return artifacts, nil
}
func (p *Basic) Fetch(ctx context.Context, remote provider.RemoteReference, artifact provider.Artifact, dst io.Writer, maxBytes int64, progress func(int64) error) (provider.TransferResult, error) {
	if err := ctx.Err(); err != nil {
		return provider.TransferResult{}, err
	}
	if err := artifact.Validate(remote); err != nil {
		return provider.TransferResult{}, err
	}
	p.backend.mu.Lock()
	r, err := p.lookup(remote)
	if err != nil {
		p.backend.mu.Unlock()
		return provider.TransferResult{}, err
	}
	if !p.state(r).Terminal() {
		p.backend.mu.Unlock()
		return provider.TransferResult{}, errors.New("execution is not terminal")
	}
	if r.files == nil {
		r.files = p.makeFiles(r)
	}
	var content []byte
	found := false
	for _, f := range r.files {
		if f.Path == artifact.Path {
			content = append([]byte(nil), f.Content...)
			found = true
			break
		}
	}
	p.backend.stats.FetchCalls++
	p.backend.mu.Unlock()
	if !found {
		return provider.TransferResult{}, provider.Problem(domain.CodeArtifactMissing, domain.FailureStageResults, "fixture artifact is missing")
	}
	return provider.CopyVerified(ctx, bytes.NewReader(content), provider.WithTransferProgress(dst, progress), artifact, maxBytes)
}
func cursor(remote provider.RemoteReference, offset int) string {
	data, _ := json.Marshal(remote)
	return fmt.Sprintf("%s.%d", provider.Digest(data), offset)
}
func pageOffset(remote provider.RemoteReference, token string, length int) (int, error) {
	if token == "" {
		return 0, nil
	}
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return 0, errors.New("invalid fixture cursor")
	}
	i, err := strconv.Atoi(parts[1])
	if err != nil || i < 0 || i >= length || cursor(remote, i) != token {
		return 0, errors.New("cursor does not match execution or range")
	}
	return i, nil
}
