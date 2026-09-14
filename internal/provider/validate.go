package provider

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

func Digest(data []byte) domain.SHA256Digest {
	sum := sha256.Sum256(data)
	return domain.SHA256Digest(hex.EncodeToString(sum[:]))
}

func (id Identity) Validate() error {
	if !id.InstallationID.Valid() || !id.WorkspaceID.Valid() || !id.JobID.Valid() ||
		!id.AttemptID.Valid() || !id.InstanceID.Valid() || !id.IntentID.Valid() {
		return errors.New("invalid submission identity")
	}
	if !domain.AttemptID(id.ResourceKey).Valid() || !domain.AttemptID(id.Nonce).Valid() || len(id.Nonce) < 16 {
		return errors.New("resource key and nonce must be bounded opaque values")
	}
	if !id.BundleSHA256.Valid() || !id.InputManifestSHA256.Valid() {
		return errors.New("submission requires frozen bundle and input digests")
	}
	return nil
}

func (job ResolvedJob) Validate() error {
	if err := job.Identity.Validate(); err != nil {
		return err
	}
	if err := job.Binding.Validate(); err != nil {
		return err
	}
	if job.Binding.ProviderInstanceID != job.Identity.InstanceID {
		return errors.New("job binding does not match submission instance")
	}
	if job.WallSeconds <= 0 || job.WallSeconds > 86400 {
		return errors.New("resolved wall budget must be in 1..86400 seconds")
	}
	if len(job.Specification) == 0 || len(job.Specification) > 1<<20 ||
		!json.Valid(job.Specification) || strings.TrimSpace(string(job.Specification))[0] != '{' {
		return errors.New("specification must be a bounded JSON object validated by admission")
	}
	if Digest(job.Specification) != job.SpecificationSHA256 {
		return errors.New("frozen specification digest mismatch")
	}
	if job.Inputs != nil && (job.Inputs.Validate(job.Identity.WorkspaceID) != nil || job.Inputs.Bundle.SHA256 != job.Identity.BundleSHA256 || job.Inputs.InputDigest() != job.Identity.InputManifestSHA256) {
		return errors.New("frozen input snapshot digest mismatch")
	}
	if len(job.Required) == 0 || len(job.Required) > 64 {
		return errors.New("resolved job requires a bounded capability list")
	}
	seen := make(map[domain.CapabilityName]bool)
	for _, name := range job.Required {
		if strings.TrimSpace(string(name)) == "" || len(name) > 128 || seen[name] {
			return errors.New("empty, duplicate, or oversized required capability")
		}
		seen[name] = true
	}
	if !seen[domain.CapabilityBatchExecution] {
		return errors.New("finite batch capability is required")
	}
	return nil
}

// Clone removes aliases to caller-owned mutable request memory.
func (job ResolvedJob) Clone() ResolvedJob {
	job.Specification = append(json.RawMessage(nil), job.Specification...)
	job.Required = append([]domain.CapabilityName(nil), job.Required...)
	if job.Inputs != nil {
		snapshot := job.Inputs.Clone()
		job.Inputs = &snapshot
	}
	return job
}
func (plan Plan) Clone() Plan {
	plan.Job = plan.Job.Clone()
	plan.VerifyAfterStart = append([]domain.CapabilityName(nil), plan.VerifyAfterStart...)
	return plan
}
func (plan Plan) Digest() domain.SHA256Digest {
	// Plan contains only deterministic JSON-encodable data; no maps with non-string keys,
	// functions, or floating point values. Call Job.Validate before accepting the plan.
	data, err := json.Marshal(plan)
	if err != nil {
		return ""
	}
	return Digest(data)
}
func (remote RemoteReference) Validate() error {
	if err := remote.Identity.Validate(); err != nil {
		return err
	}
	if !safeText(remote.Resource, 1024) || !safeText(remote.Version, 256) {
		return errors.New("remote reference requires bounded resource and version")
	}
	return nil
}
func (outcome SubmissionOutcome) Validate(expected Identity) error {
	if err := expected.Validate(); err != nil {
		return err
	}
	if outcome.Remote != nil {
		if outcome.Remote.Identity != expected {
			return Problem(domain.CodeRemoteIdentityMismatch, domain.FailureStageObservation, "submission identity mismatch")
		}
		if err := outcome.Remote.Validate(); err != nil {
			return err
		}
	}
	if outcome.Problem != nil {
		if err := outcome.Problem.Validate(); err != nil {
			return err
		}
	}
	switch outcome.Status {
	case SubmissionAccepted:
		if outcome.Remote == nil || outcome.Problem != nil {
			return errors.New("acceptance requires identity and no failure")
		}
	case SubmissionRejected:
		if outcome.Remote != nil || outcome.Problem == nil || outcome.Problem.ComputeMayHaveStarted {
			return errors.New("rejection requires proof of non-acceptance")
		}
	case SubmissionUnknown:
		if outcome.Problem == nil || !outcome.Problem.ComputeMayHaveStarted || outcome.Problem.SafeOperationRetry ||
			outcome.Problem.RecommendedAction != domain.RecommendedActionReconcile {
			return errors.New("unknown submission requires ambiguity and reconciliation without retry")
		}
	default:
		return errors.New("invalid submission outcome")
	}
	return nil
}
func (obs Observation) Validate(expected RemoteReference) error {
	if err := expected.Validate(); err != nil {
		return err
	}
	if obs.Remote != expected {
		return Problem(domain.CodeRemoteIdentityMismatch, domain.FailureStageObservation, "observation belongs to another execution")
	}
	if obs.ObservedAt.IsZero() || !obs.Execution.Valid() || !obs.RemoteActivity.Valid() || !obs.ReleaseEvidence.Valid() || !safeText(obs.RawState, 1024) {
		return errors.New("invalid provider observation")
	}
	if obs.Execution.Terminal() && obs.RemoteActivity != domain.RemoteActivityInactive {
		return errors.New("terminal observation cannot leave remote activity unresolved")
	}
	if obs.RemoteActivity == domain.RemoteActivityNotStarted {
		return errors.New("identified remote execution cannot be not-started")
	}
	if obs.Execution == domain.ExecutionNotSubmitted {
		return errors.New("identified remote run cannot be not-submitted")
	}
	if !obs.Execution.Terminal() && obs.RemoteActivity == domain.RemoteActivityInactive {
		return errors.New("inactive observation requires terminal evidence")
	}
	if obs.ReleaseEvidence == domain.ReleaseEvidenceNotApplicable ||
		((obs.ReleaseEvidence == domain.ReleaseEvidenceConfirmed || obs.ReleaseEvidence == domain.ReleaseEvidenceProviderReported) && !obs.Execution.Terminal()) {
		return errors.New("release claim contradicts execution evidence")
	}
	return nil
}
func (page PageRequest) Validate() error {
	if page.Limit < 1 || page.Limit > 100 || len(page.Cursor) > 512 {
		return errors.New("invalid page limit or cursor")
	}
	return nil
}
func (artifact Artifact) Validate(expected RemoteReference) error {
	if err := expected.Validate(); err != nil {
		return err
	}
	if artifact.Remote != expected {
		return Problem(domain.CodeRemoteIdentityMismatch, domain.FailureStageObservation, "artifact belongs to another execution")
	}
	if !SafeArtifactPath(artifact.Path) || artifact.Bytes < 0 || !artifact.SHA256.Valid() {
		return errors.New("invalid artifact metadata")
	}
	return nil
}
func SafeArtifactPath(path string) bool {
	if path == "" || len(path) > 1024 || strings.ContainsAny(path, "\\:") || strings.HasPrefix(path, "/") {
		return false
	}
	for _, c := range path {
		if c < 32 || c == 127 {
			return false
		}
	}
	for _, part := range strings.Split(path, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}
func safeText(s string, max int) bool {
	if strings.TrimSpace(s) == "" || len(s) > max {
		return false
	}
	for _, c := range s {
		if c < 32 || c == 127 {
			return false
		}
	}
	return true
}

// Problem constructs a conservative provider error; callers add retry semantics explicitly.
func Problem(code domain.ErrorCode, stage domain.FailureStage, message string) domain.Problem {
	p, err := domain.NewProblem(code, message, stage)
	if err != nil {
		panic(fmt.Sprintf("invalid internal problem definition: %v", err))
	}
	return p
}

// ValidateManifestIdentity is the identity gate before associating downloaded outputs with
// an attempt. M3 must additionally validate the complete M2-03 manifest schema, execution
// outcome, required output set and each checksum before publishing success.
func ValidateManifestIdentity(expected Identity, data []byte) error {
	if err := expected.Validate(); err != nil {
		return err
	}
	if len(data) > 1<<20 {
		return errors.New("manifest exceeds byte limit")
	}
	var manifest struct {
		Version   string              `json:"manifest_version"`
		JobID     domain.JobID        `json:"job_id"`
		AttemptID domain.AttemptID    `json:"attempt_id"`
		Nonce     string              `json:"attempt_nonce"`
		Bundle    domain.SHA256Digest `json:"bundle_sha256"`
		Inputs    domain.SHA256Digest `json:"input_manifest_sha256"`
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		return errors.New("invalid manifest JSON")
	}
	if manifest.Version != "1" || manifest.JobID != expected.JobID || manifest.AttemptID != expected.AttemptID ||
		manifest.Nonce != expected.Nonce || manifest.Bundle != expected.BundleSHA256 || manifest.Inputs != expected.InputManifestSHA256 {
		return Problem(domain.CodeRemoteIdentityMismatch, domain.FailureStageObservation, "result manifest identity mismatch")
	}
	return nil
}

func (result Reconciliation) Validate(expected Identity) error {
	if err := expected.Validate(); err != nil {
		return err
	}
	switch result.Status {
	case ReconciliationFound:
		if result.Remote == nil || result.Remote.Identity != expected {
			return errors.New("reconciliation identity mismatch")
		}
		return result.Remote.Validate()
	case ReconciliationNotFound, ReconciliationUnknown:
		if result.Remote != nil {
			return errors.New("unresolved lookup cannot invent a remote reference")
		}
		return nil
	default:
		return errors.New("invalid reconciliation outcome")
	}
}
