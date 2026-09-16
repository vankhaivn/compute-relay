package kaggle

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

type executionUnreadBlobs struct{}

func (executionUnreadBlobs) Open(context.Context, domain.WorkspaceID, domain.ObjectID) (io.ReadCloser, error) {
	return nil, errors.New("execution must not refresh original input bytes")
}

func newExecutionFixture(t *testing.T) *Executor {
	t.Helper()
	c, plan, prepared := executionFixture(t)
	resolver, err := credentials.NewEnvironment([]ports.CredentialRef{c.CredentialRef}, func(string) (string, bool) { return "SYNTHETIC_TOKEN", true })
	if err != nil {
		t.Fatal(err)
	}
	stager, err := NewStager(c, DefaultStagingPolicy(), resolver, executionUnreadBlobs{}, false)
	if err != nil {
		t.Fatal(err)
	}
	stager.local = func(context.Context, Config, Mode, []byte) (Report, error) { return baseline(Local), nil }
	stager.run = func(_ context.Context, _ Config, mode string, _ []byte, p stagingPlan, _ StagingBlobs) (stagingResponse, error) {
		if mode != "observe" {
			t.Fatal("execution recreated staging")
		}
		return stageResponse(p, "ready"), nil
	}
	e, err := NewExecutor(stager, DefaultExecutionPolicy(), plan, prepared, true)
	if err != nil {
		t.Fatal(err)
	}
	return e
}
func executionFound(r executionRequest, raw string) executionResponse {
	return executionResponse{Protocol: 1, Status: "found", KernelID: "42", Reference: r.Owner + "/" + r.Slug, Version: 1, SourceSHA256: r.SourceSHA256, RawState: raw}
}

func TestExecutorConcurrentSubmitHasOneInvocationAndNoAutomaticRetry(t *testing.T) {
	e := newExecutionFixture(t)
	var calls, accepted atomic.Int32
	e.run = func(_ context.Context, _ Config, mode string, secret []byte, r executionRequest) (executionResponse, error) {
		if mode != "submit" || string(secret) != "SYNTHETIC_TOKEN" || r.Source == "" {
			t.Error("incorrect execution invocation")
		}
		calls.Add(1)
		return executionFound(r, "QUEUED"), nil
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out := e.Submit(context.Background(), e.prepared)
			if err := out.Validate(e.plan.Job.Identity); err != nil {
				t.Error(err)
			}
			if out.Status == provider.SubmissionAccepted {
				accepted.Add(1)
			} else if out.Status != provider.SubmissionUnknown {
				t.Error("duplicate invocation invented rejection")
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 || accepted.Load() != 1 {
		t.Fatal("multiplied kernel submission", calls.Load(), accepted.Load())
	}
}

func TestExecutorAmbiguityReconcilesWithoutAnotherSubmit(t *testing.T) {
	for _, fault := range []string{"error", "panic", "unknown", "bad-response"} {
		t.Run(fault, func(t *testing.T) {
			e := newExecutionFixture(t)
			calls := 0
			e.run = func(_ context.Context, _ Config, mode string, _ []byte, r executionRequest) (executionResponse, error) {
				calls++
				if mode != "submit" {
					if r.Source != "" {
						t.Fatal("read retransmitted private source")
					}
					return executionFound(r, "RUNNING"), nil
				}
				switch fault {
				case "error":
					return executionResponse{}, errors.New("SYNTHETIC_TOKEN")
				case "panic":
					panic("SYNTHETIC_TOKEN")
				case "bad-response":
					return executionResponse{Protocol: 1, Status: "unknown", KernelID: "42"}, nil
				default:
					return executionResponse{Protocol: 1, Status: "unknown"}, nil
				}
			}
			out := e.Submit(context.Background(), e.prepared)
			if out.Validate(e.plan.Job.Identity) != nil || out.Status != provider.SubmissionUnknown || strings.Contains(out.Problem.Message, "SYNTHETIC_TOKEN") {
				t.Fatal("ambiguous result escaped", out)
			}
			if e.Submit(context.Background(), e.prepared).Status != provider.SubmissionUnknown || calls != 1 {
				t.Fatal("blind resubmission")
			}
			recovered, err := e.ReconcileSubmission(context.Background(), e.plan.Job.Identity)
			if err != nil || recovered.Status != provider.ReconciliationFound || calls != 2 {
				t.Fatal("failed same-identity recovery", err)
			}
		})
	}
}

func TestExecutorUnreadyOrUnboundInputStopsBeforeKernelInvocation(t *testing.T) {
	for _, fault := range []string{"disabled", "changed-prepared", "staging-unknown", "staging-replaced", "local-mismatch", "cancelled"} {
		t.Run(fault, func(t *testing.T) {
			e := newExecutionFixture(t)
			e.run = func(context.Context, Config, string, []byte, executionRequest) (executionResponse, error) {
				t.Fatal("unsafe kernel invocation")
				return executionResponse{}, nil
			}
			prepared := e.prepared
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch fault {
			case "disabled":
				e.allowSubmit = false
			case "changed-prepared":
				prepared.Resource = "other"
			case "staging-unknown", "staging-replaced":
				e.stager.run = func(_ context.Context, _ Config, _ string, _ []byte, p stagingPlan, _ StagingBlobs) (stagingResponse, error) {
					if fault == "staging-unknown" {
						return stageResponse(p, "unknown"), nil
					}
					r := stageResponse(p, "ready")
					r.DatasetID = "999"
					return r, nil
				}
			case "local-mismatch":
				e.stager.local = func(context.Context, Config, Mode, []byte) (Report, error) { return Report{}, ErrConfig }
			case "cancelled":
				cancel()
			}
			out := e.Submit(ctx, prepared)
			if out.Validate(e.plan.Job.Identity) != nil || out.Status != provider.SubmissionRejected || out.Problem.ComputeMayHaveStarted {
				t.Fatal("unattempted request was not rejected safely")
			}
		})
	}
}

func TestExecutorStateMappingKeepsCancellationAndReleaseEvidenceHonest(t *testing.T) {
	e := newExecutionFixture(t)
	remote, err := e.remote(executionFound(e.request, "QUEUED"))
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"QUEUED", "RUNNING", "COMPLETE", "ERROR", "CANCEL_REQUESTED", "CANCEL_ACKNOWLEDGED", "NEW_SCRIPT", "UNKNOWN"} {
		e.run = func(_ context.Context, _ Config, mode string, _ []byte, r executionRequest) (executionResponse, error) {
			if mode != "observe" || r.KernelID != "42" || r.Source != "" {
				t.Fatal("unbound observation")
			}
			return executionFound(r, raw), nil
		}
		obs, err := e.Observe(context.Background(), remote)
		if err != nil || obs.Validate(remote) != nil || obs.ReleaseEvidence != domain.ReleaseEvidenceNotObservable {
			t.Fatal("invalid observation", raw, err)
		}
		terminal := raw == "COMPLETE" || raw == "ERROR"
		if obs.Execution.Terminal() != terminal || (obs.RemoteActivity == domain.RemoteActivityInactive) != terminal {
			t.Fatal("unproven terminal evidence", raw, obs)
		}
		if !terminal && raw != "QUEUED" && raw != "RUNNING" && obs.Execution != domain.ExecutionUnknown {
			t.Fatal("invented execution state", raw)
		}
	}
	foreign := remote
	foreign.Identity.AttemptID = "foreign"
	if _, err := e.Observe(context.Background(), foreign); !errors.Is(err, ErrExecutionIdentity) {
		t.Fatal("foreign identity accepted", err)
	}
	e.run = func(_ context.Context, _ Config, _ string, _ []byte, r executionRequest) (executionResponse, error) {
		response := executionFound(r, "COMPLETE")
		response.KernelID = "43"
		return response, nil
	}
	if _, err := e.Observe(context.Background(), remote); !errors.Is(err, ErrExecutionIdentity) {
		t.Fatal("replacement ID accepted", err)
	}
}

func TestExecutionSourceContainsValidatedOriginalRunnerManifest(t *testing.T) {
	e := newExecutionFixture(t)
	prefix := executionBootstrap + "\nrun_remote(json.loads(base64.b64decode(\""
	encoded := strings.TrimSuffix(strings.TrimPrefix(e.request.Source, prefix), "\", validate=True)))\n")
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Manifest json.RawMessage   `json:"manifest"`
		Modules  map[string]string `json:"modules"`
	}
	if json.Unmarshal(raw, &payload) != nil || len(payload.Modules) != 5 {
		t.Fatal("invalid runner package")
	}
	for _, secret := range []string{e.stager.config.PythonExecutable, string(e.stager.config.CredentialRef), "SYNTHETIC_TOKEN", "private name"} {
		if bytes.Contains(raw, []byte(secret)) {
			t.Fatal("private host metadata in decoded source")
		}
	}
	root, err := filepath.Abs("../../../runner/python")
	if err != nil {
		t.Fatal(err)
	}
	// Validate the actual generated manifest with the original Python contract.
	// No runner execute/main/payload entry point is called by this test.
	cmd := exec.Command(pythonForTest(t), "-I", "-c", "import sys;sys.path.insert(0,sys.argv[1]);from relay_runner.contract import load;load(sys.stdin.buffer.read());print('validated-not-executed')", root)
	cmd.Stdin = bytes.NewReader(payload.Manifest)
	output, err := cmd.CombinedOutput()
	if err != nil || strings.TrimSpace(string(output)) != "validated-not-executed" {
		t.Fatal("generated manifest differs from runner contract", err, string(output))
	}
}
