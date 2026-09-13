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
func (p *Basic) ListArtifacts(ctx context.Context, remote provider.RemoteReference, page provider.PageRequest) (provider.ArtifactPage, error) {
	if err := ctx.Err(); err != nil {
		return provider.ArtifactPage{}, err
	}
	if err := page.Validate(); err != nil {
		return provider.ArtifactPage{}, err
	}
	p.backend.mu.Lock()
	defer p.backend.mu.Unlock()
	r, err := p.lookup(remote)
	if err != nil {
		return provider.ArtifactPage{}, err
	}
	if !p.state(r).Terminal() {
		return provider.ArtifactPage{}, provider.Problem(domain.CodeRemoteExecutionUnresolved, domain.FailureStageOperation, "artifacts are not collectible before terminal evidence")
	}
	if r.files == nil {
		r.files = p.makeFiles(r)
	}
	start, err := pageOffset(remote, page.Cursor, len(r.files))
	if err != nil {
		return provider.ArtifactPage{}, err
	}
	end := start + page.Limit
	if end > len(r.files) {
		end = len(r.files)
	}
	result := provider.ArtifactPage{Artifacts: make([]provider.Artifact, 0, end-start)}
	for _, f := range r.files[start:end] {
		result.Artifacts = append(result.Artifacts, provider.Artifact{Remote: remote, Path: f.Path, Bytes: int64(len(f.Content)), SHA256: provider.Digest(f.Content)})
	}
	if end < len(r.files) {
		result.NextCursor = cursor(remote, end)
	}
	return result, nil
}
func (p *Basic) FetchArtifact(ctx context.Context, remote provider.RemoteReference, artifact provider.Artifact, dst io.Writer, maxBytes int64) (provider.TransferResult, error) {
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
	return provider.CopyVerified(ctx, bytes.NewReader(content), dst, artifact, maxBytes)
}
func (p *Basic) Cleanup(ctx context.Context, request provider.CleanupRequest) (provider.CleanupOutcome, error) {
	if err := ctx.Err(); err != nil {
		return provider.CleanupOutcome{}, err
	}
	if err := request.Remote.Validate(); err != nil {
		return provider.CleanupOutcome{}, err
	}
	if !request.LedgerID.Valid() || !request.CreationOperationID.Valid() || !request.ResultsCollected {
		return provider.CleanupOutcome{}, errors.New("cleanup requires recorded ownership and collected results")
	}
	if request.Mode != provider.CleanupDryRun && request.Mode != provider.CleanupApply {
		return provider.CleanupOutcome{}, errors.New("cleanup mode must be explicit")
	}
	p.backend.mu.Lock()
	defer p.backend.mu.Unlock()
	r, ok := p.backend.records[attemptKey(request.Remote.Identity)]
	if !ok || r.remote != request.Remote || request.Remote.Identity.InstanceID != p.instance || request.CreationOperationID != r.prepared.PreparationID {
		return provider.CleanupOutcome{}, identityError()
	}
	if !r.submitted || !p.state(r).Terminal() {
		return provider.CleanupOutcome{}, provider.Problem(domain.CodeRemoteExecutionUnresolved, domain.FailureStageOperation, "cleanup cannot cancel active or unresolved compute")
	}
	p.backend.stats.CleanupCalls++
	if r.deleted {
		return provider.CleanupOutcome{AlreadyAbsent: true}, nil
	}
	if request.Mode == provider.CleanupDryRun {
		return provider.CleanupOutcome{WouldDelete: true}, nil
	}
	r.deleted = true
	return provider.CleanupOutcome{Deleted: true}, nil
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
