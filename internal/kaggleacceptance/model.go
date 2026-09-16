// Package kaggleacceptance is a finite, explicitly authorized operator experiment.
// It is not serve, a multi-job runtime or an automatic live capability declaration.
package kaggleacceptance

import (
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle"
)

const Workspace domain.WorkspaceID = "acceptance"
const ProfileName = "acceptance"

var ErrState = errors.New("acceptance state is missing, inconsistent or incompatible; preserve the original directory and binary")
var ErrPermission = errors.New("explicit mode-specific acceptance authorization is required")
var ErrEvidence = errors.New("GPU, publication or restart evidence did not qualify")

type Options struct {
	Mode string
	Root string
	Config kaggle.Config
	MachineShape string
	ProgramSHA256 domain.SHA256Digest
	AllowPrivateStaging bool
	AllowGPU bool
	AllowReadOnly bool
	CollectionKey string
	MaxWait time.Duration
}

func (o Options) valid() bool {
	if o.Root == "" || !o.ProgramSHA256.Valid() || o.MaxWait < time.Second || o.MaxWait > 30*time.Minute {
		return false
	}
	switch o.Mode {
	case "prepare":
		return o.Config.Validate() == nil && (o.MachineShape == "NvidiaTeslaT4" || o.MachineShape == "NvidiaTeslaP100") && !o.AllowPrivateStaging && !o.AllowGPU && !o.AllowReadOnly && o.CollectionKey == ""
	case "submit":
		return o.AllowPrivateStaging && o.AllowGPU && !o.AllowReadOnly && o.CollectionKey == ""
	case "resume":
		return o.AllowReadOnly && !o.AllowPrivateStaging && !o.AllowGPU && o.CollectionKey == ""
	case "collect":
		_, err := admission.KeyDigest(o.CollectionKey)
		return o.AllowReadOnly && !o.AllowPrivateStaging && !o.AllowGPU && err == nil
	case "status":
		return !o.AllowPrivateStaging && !o.AllowGPU && !o.AllowReadOnly && o.CollectionKey == ""
	}
	return false
}

type record struct {
	Protocol int `json:"protocol"`
	Config kaggle.Config `json:"config"`
	MachineShape string `json:"machine_shape"`
	ProgramSHA256 domain.SHA256Digest `json:"program_sha256"`
	Challenge string `json:"challenge"`
	BundleSHA256 domain.SHA256Digest `json:"bundle_sha256"`
	InputSHA256 domain.SHA256Digest `json:"input_sha256"`
	SpecificationSHA256 domain.SHA256Digest `json:"specification_sha256"`
	Receipt admission.Receipt `json:"receipt"`
	Fixture bool `json:"fixture"`
}

type submissionMark struct {
	Protocol int `json:"protocol"`
	ProcessNonce string `json:"process_nonce"`
	PlanSHA256 domain.SHA256Digest `json:"plan_sha256"`
	IntentID domain.SubmissionIntentID `json:"intent_id"`
	RecordedAt time.Time `json:"recorded_at"`
}

type ArtifactEvidence struct {
	Path string `json:"path"`
	Bytes int64 `json:"bytes"`
	SHA256 domain.SHA256Digest `json:"sha256"`
}

type Report struct {
	Protocol int `json:"protocol"`
	Scope string `json:"scope"`
	Status string `json:"status"`
	Evidence string `json:"evidence"`
	CheckedAt time.Time `json:"checked_at"`
	JobID domain.JobID `json:"job_id"`
	AttemptID domain.AttemptID `json:"attempt_id"`
	AttemptNumber uint64 `json:"attempt_number"`
	Execution domain.ExecutionState `json:"execution"`
	Result domain.ResultState `json:"result"`
	Orchestration domain.OrchestrationState `json:"orchestration"`
	ReleaseEvidence domain.ReleaseEvidence `json:"release_evidence"`
	GPUVerified bool `json:"gpu_verified"`
	RestartVerified bool `json:"restart_verified"`
	RemoteExecutionCount string `json:"remote_execution_count"`
	ProviderTimeoutEnforcement string `json:"provider_timeout_enforcement"`
	Cancellation string `json:"cancellation"`
	FullM1Acceptance bool `json:"full_m1_acceptance"`
	Artifacts []ArtifactEvidence `json:"artifacts"`
	ProblemCode domain.ErrorCode `json:"problem_code"`
}

func baseReport(r record) Report {
	return Report{Protocol:1, Scope:"m4-06-fixed-gpu-smoke", Status:"prepared-local", Evidence:"not-tested", CheckedAt:time.Now().UTC(), JobID:r.Receipt.JobID, AttemptID:r.Receipt.AttemptID, RemoteExecutionCount:"not-observable", ProviderTimeoutEnforcement:"unverified", Cancellation:"manual-required", Artifacts:[]ArtifactEvidence{}}
}
func (r record) binding() provider.BindingSnapshot {
	return provider.BindingSnapshot{Binding:domain.ProviderBinding{Profile:ProfileName, ProviderInstanceID:domain.ProviderInstanceID(r.Config.InstanceID), ConfigurationRevision:r.Config.Revision}, AccountScope:r.Config.AccountName, CredentialRef:string(r.Config.CredentialRef)}
}
