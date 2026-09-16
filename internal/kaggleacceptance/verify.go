package kaggleacceptance

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/domain"
)

type calculationEvidence struct {
	Schema          int                 `json:"schema"`
	JobID           domain.JobID        `json:"job_id"`
	AttemptID       domain.AttemptID    `json:"attempt_id"`
	Challenge       string              `json:"challenge"`
	InputSHA256     domain.SHA256Digest `json:"input_sha256"`
	Device          string              `json:"device"`
	MatrixSize      int                 `json:"matrix_size"`
	Scale           int                 `json:"scale"`
	Elements        int                 `json:"elements"`
	ResultSum       float64             `json:"result_sum"`
	ExpectedSum     int64               `json:"expected_sum"`
	AllCorrect      bool                `json:"all_correct"`
	CUDAComputation bool                `json:"cuda_computation"`
}

type hardwareEvidence struct {
	Schema        int    `json:"schema"`
	TorchVersion  string `json:"torch_version"`
	CUDAVersion   string `json:"cuda_version"`
	PythonVersion string `json:"python_version"`
	DeviceName    string `json:"device_name"`
	Device        string `json:"device"`
}

func verifiedGPU(r record, result, hardware []byte) bool {
	var calculation calculationEvidence
	var device hardwareEvidence
	if len(result) > 4096 || len(hardware) > 4096 || decodeRecord(result, &calculation) != nil || decodeRecord(hardware, &device) != nil || !domain.SHA256Digest(r.Challenge).Valid() {
		return false
	}
	prefix, err := strconv.ParseInt(r.Challenge[:2], 16, 64)
	scale := int(prefix%7) + 1
	expected := int64(64 * 64 * 64 * scale)
	if err != nil || calculation.Schema != 1 || calculation.JobID != r.Receipt.JobID || calculation.AttemptID != r.Receipt.AttemptID || calculation.Challenge != r.Challenge || calculation.InputSHA256 != r.InputSHA256 || calculation.Device != "cuda:0" || calculation.MatrixSize != 64 || calculation.Scale != scale || calculation.Elements != 4096 || calculation.ResultSum != float64(expected) || calculation.ExpectedSum != expected || !calculation.AllCorrect || !calculation.CUDAComputation || device.Schema != 1 || device.Device != "cuda:0" {
		return false
	}
	for _, value := range []string{device.TorchVersion, device.CUDAVersion, device.PythonVersion, device.DeviceName} {
		if value == "" || value == "None" || len(value) > 128 {
			return false
		}
		for _, char := range value {
			if char < 32 || char > 126 {
				return false
			}
		}
	}
	model := "T4"
	if r.MachineShape == "NvidiaTeslaP100" {
		model = "P100"
	}
	return strings.Contains(strings.ToUpper(device.DeviceName), model)
}

func stateReport(ctx context.Context, s *session) (Report, error) {
	r := baseReport(s.record)
	attempt, err := s.store.LoadAttempt(ctx, Workspace, s.record.Receipt.JobID, s.record.Receipt.AttemptID)
	if err != nil || attempt.Number != 1 {
		return r, ErrState
	}
	r.AttemptNumber = uint64(attempt.Number)
	r.Execution, r.Result, r.Orchestration, r.ReleaseEvidence = attempt.State.Execution, attempt.State.Result, attempt.State.Orchestration, attempt.State.ReleaseEvidence
	j, err := s.journal(ctx)
	if err != nil {
		return r, ErrState
	}
	if j.Problem != nil {
		r.ProblemCode = j.Problem.Code
	}
	if j.SubmitStarted {
		r.Status = "resume-required"
	}
	if attempt.State.Orchestration == domain.OrchestrationNeedsAttention {
		r.Status = "needs-attention"
	}
	if attempt.State.Result == domain.ResultIncomplete || attempt.State.Result == domain.ResultInvalid {
		r.Status = "collection-retry-required"
	}
	if attempt.State.Orchestration.Terminal() && attempt.State.Result != domain.ResultAvailable {
		r.Status = "experiment-failed"
	}
	return r, nil
}

func verifyPublication(ctx context.Context, s *session) (Report, error) {
	r, err := stateReport(ctx, s)
	if err != nil || r.Result != domain.ResultAvailable {
		return r, err
	}
	reader, err := collection.NewReader(s.access, s.store, s.results)
	if err != nil {
		return r, ErrEvidence
	}
	result, err := reader.Read(ctx, s.actor, Workspace, s.record.Receipt.JobID, s.record.Receipt.AttemptID)
	if err != nil || result.Phase != "completed" || r.Execution != domain.ExecutionSucceeded || r.Orchestration != domain.OrchestrationSucceeded || len(result.Files) != 6 {
		return r, ErrEvidence
	}
	caps := map[string]int64{"outputs/result.json": 4096, "outputs/hardware.json": 4096, "control/execution-result.json": 1 << 20, "control/environment.json": 1 << 20, "control/stdout.log": 20 << 20, "control/stderr.log": 20 << 20}
	payloads := map[string][]byte{}
	for _, file := range result.Files {
		cap, ok := caps[file.Path]
		if !ok || file.Object.Bytes > cap {
			return r, ErrEvidence
		}
		delete(caps, file.Path)
		meta, stream, err := reader.Open(ctx, s.actor, Workspace, s.record.Receipt.JobID, s.record.Receipt.AttemptID, file.ID)
		if err != nil || meta.Object != file.Object {
			if stream != nil {
				_ = stream.Close()
			}
			return r, ErrEvidence
		}
		hash := sha256.New()
		var captured bytes.Buffer
		var sink io.Writer = hash
		if strings.HasPrefix(file.Path, "outputs/") {
			sink = io.MultiWriter(hash, &captured)
		}
		n, readErr := io.Copy(sink, io.LimitReader(stream, cap+1))
		closeErr := stream.Close()
		if readErr != nil || closeErr != nil || n != file.Object.Bytes || hex.EncodeToString(hash.Sum(nil)) != string(file.Object.SHA256) {
			return r, ErrEvidence
		}
		if strings.HasPrefix(file.Path, "outputs/") {
			payloads[file.Path] = captured.Bytes()
		}
		r.Artifacts = append(r.Artifacts, ArtifactEvidence{file.Path, n, file.Object.SHA256})
	}
	if len(caps) != 0 || !verifiedGPU(s.record, payloads["outputs/result.json"], payloads["outputs/hardware.json"]) {
		return r, ErrEvidence
	}
	r.GPUVerified = true
	r.Status = "restart-unverified"
	var start, resume submissionMark
	j, err := s.journal(ctx)
	if err != nil || j.Plan == nil || !j.SubmitStarted {
		return r, ErrEvidence
	}
	if readRecord(filepath.Join(s.root.Path, "submission-process.json"), &start) != nil || readRecord(filepath.Join(s.root.Path, "resume-process.json"), &resume) != nil {
		return r, nil
	}
	if !validMark(start, j.Plan.Digest(), j.Plan.Job.Identity.IntentID) || !validMark(resume, j.Plan.Digest(), j.Plan.Job.Identity.IntentID) || start.ProcessNonce == resume.ProcessNonce || resume.RecordedAt.Before(start.RecordedAt) {
		return r, ErrEvidence
	}
	r.RestartVerified = true
	r.Status, r.Evidence = "passed-live", "passed-live"
	if s.record.Fixture {
		r.Status, r.Evidence = "passed-offline", "passed-offline"
	}
	return r, nil
}
func validMark(m submissionMark, digest domain.SHA256Digest, intent domain.SubmissionIntentID) bool {
	return m.Protocol == 1 && domain.SHA256Digest(m.ProcessNonce).Valid() && m.PlanSHA256 == digest && m.IntentID == intent && !m.RecordedAt.IsZero()
}
