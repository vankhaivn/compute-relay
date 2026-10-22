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
	"time"

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
	resolver, err := fixtureCredentials([]ports.CredentialRef{c.CredentialRef}, func(string) (string, bool) { return "SYNTHETIC_TOKEN", true })
	if err != nil {
		t.Fatal(err)
	}
	stager, err := NewStager(c, DefaultStagingPolicy(), resolver, executionUnreadBlobs{}, false)
	if err != nil {
		t.Fatal(err)
	}
	stager.run = func(_ context.Context, _ Config, mode string, _ []byte, p stagingPlan, _ StagingBlobs) (stagingResponse, error) {
		if mode != "recheck" {
			t.Fatal("execution did not use the metadata recheck")
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
			out := e.Start(context.Background(), e.prepared, nil, nil)
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
			recovery := "found"
			e.run = func(_ context.Context, _ Config, mode string, _ []byte, r executionRequest) (executionResponse, error) {
				calls++
				if mode != "submit" {
					if r.Source != "" {
						t.Fatal("read retransmitted private source")
					}
					if recovery != "found" {
						return executionResponse{Protocol: 1, Status: recovery}, nil
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
			out := e.Start(context.Background(), e.prepared, nil, nil)
			if out.Validate(e.plan.Job.Identity) != nil || out.Status != provider.SubmissionUnknown || strings.Contains(out.Problem.Message, "SYNTHETIC_TOKEN") {
				t.Fatal("ambiguous result escaped", out)
			}
			if e.Start(context.Background(), e.prepared, nil, nil).Status != provider.SubmissionUnknown || calls != 1 {
				t.Fatal("blind resubmission")
			}
			for i, status := range []provider.ReconciliationStatus{provider.ReconciliationUnknown, provider.ReconciliationNotFound, provider.ReconciliationFound} {
				recovery = string(status)
				recovered, err := e.Reconcile(context.Background(), e.plan.Job.Identity)
				if err != nil || recovered.Status != status || calls != i+2 {
					t.Fatal("failed same-identity recovery", status, err)
				}
			}
		})
	}
}

func TestExecutorUnreadyOrUnboundInputStopsBeforeKernelInvocation(t *testing.T) {
	for fault, want := range map[string]string{
		"disabled": reasonSubmissionDisabled, "changed-prepared": reasonPreparationMismatch,
		"staging-error": reasonStagingRecheck, "cancelled": reasonStagingRecheck,
		"staging-unknown": reasonStagingUnconfirmed, "staging-replaced": reasonStagingUnconfirmed,
		"credential": reasonCredential,
		"timeout":    reasonCallTimeout, "stopping": reasonRuntimeStopped,
	} {
		t.Run(fault, func(t *testing.T) {
			e := newExecutionFixture(t)
			e.run = func(context.Context, Config, string, []byte, executionRequest) (executionResponse, error) {
				t.Fatal("unsafe kernel invocation")
				return executionResponse{}, nil
			}
			prepared := e.prepared
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			// afterStaging arms a fault for the submission call once staging is rechecked.
			staged := e.stager.run
			afterStaging := func(arm func()) {
				e.stager.run = func(ctx context.Context, c Config, mode string, secret []byte, p stagingPlan, b StagingBlobs) (stagingResponse, error) {
					arm()
					return staged(ctx, c, mode, secret, p, b)
				}
			}
			switch fault {
			case "disabled":
				e.allowSubmit = false
			case "changed-prepared":
				prepared.Resource = "other"
			case "staging-error":
				e.stager.run = func(context.Context, Config, string, []byte, stagingPlan, StagingBlobs) (stagingResponse, error) {
					return stagingResponse{}, ErrUnavailable
				}
			case "credential":
				missing, err := fixtureCredentials([]ports.CredentialRef{e.stager.config.CredentialRef}, func(string) (string, bool) { return "", false })
				if err != nil {
					t.Fatal(err)
				}
				afterStaging(func() { e.stager.credentials = missing })
			case "timeout":
				e.policy.Timeout = 10 * time.Millisecond
				afterStaging(func() {
					e.stager.credentials = stagingCredentialFunc(func(ctx context.Context, _ ports.CredentialRef, _ func([]byte) error) error {
						<-ctx.Done()
						return ctx.Err()
					})
				})
			case "stopping":
				afterStaging(func() {
					e.stager.credentials = stagingCredentialFunc(func(ctx context.Context, _ ports.CredentialRef, _ func([]byte) error) error {
						cancel()
						return ctx.Err()
					})
				})
			case "staging-unknown", "staging-replaced":
				e.stager.run = func(_ context.Context, _ Config, _ string, _ []byte, p stagingPlan, _ StagingBlobs) (stagingResponse, error) {
					if fault == "staging-unknown" {
						return stageResponse(p, "unknown"), nil
					}
					r := stageResponse(p, "ready")
					r.DatasetID = "999"
					return r, nil
				}
			case "cancelled":
				cancel()
			}
			out := e.Start(ctx, prepared, nil, nil)
			if out.Validate(e.plan.Job.Identity) != nil || out.Status != provider.SubmissionRejected || out.Problem.ComputeMayHaveStarted || out.Problem.Details[domain.ReasonDetail] != want {
				t.Fatal("unattempted request was not rejected safely with its reason", out.Problem)
			}
		})
	}
}

func TestExecutorRechecksUnresolvedStagingBeforeOneSubmission(t *testing.T) {
	limited := func(p stagingPlan) stagingResponse {
		r := stageResponse(p, "unknown")
		r.Reason = "staging_lookup_failed_http_429"
		return r
	}
	// Every read that does not confirm the prepared staging is read again until the window
	// ends; persistent replies end it with their cause.
	for name, tc := range map[string]struct {
		replies    []string
		want       provider.SubmissionStatus
		reads      int
		reason     string
		cause      string
		persistent bool
	}{
		"rate-limited-then-ready": {replies: []string{"limited", "not_found", "ready"}, want: provider.SubmissionAccepted, reads: 3},
		"mismatch-then-ready":     {replies: []string{"mismatch", "ready"}, want: provider.SubmissionAccepted, reads: 2},
		"unready-then-ready":      {replies: []string{"pending", "public", "ready"}, want: provider.SubmissionAccepted, reads: 3},
		"stays-unanswered":        {replies: []string{"limited"}, want: provider.SubmissionRejected, reason: reasonStagingUnconfirmed, cause: "staging_lookup_failed_http_429", persistent: true},
		"identity-changed":        {replies: []string{"replaced"}, want: provider.SubmissionRejected, reason: reasonStagingUnconfirmed, cause: "staging_identity_changed", persistent: true},
		"stays-public":            {replies: []string{"public"}, want: provider.SubmissionRejected, reason: reasonStagingUnconfirmed, cause: "staging_not_private", persistent: true},
		"stays-mismatched":        {replies: []string{"mismatch"}, want: provider.SubmissionRejected, reason: reasonStagingRecheck, cause: "shared_files_mismatch", persistent: true},
		"invalid-without-reason":  {replies: []string{"invalid"}, want: provider.SubmissionRejected, reason: reasonStagingRecheck, cause: "staging_response_invalid", persistent: true},
	} {
		t.Run(name, func(t *testing.T) {
			e := newExecutionFixture(t)
			reads := 0
			e.stager.run = func(_ context.Context, _ Config, mode string, _ []byte, p stagingPlan, _ StagingBlobs) (stagingResponse, error) {
				if mode != "recheck" {
					t.Fatal("recheck used a mutating mode", mode)
				}
				reply := tc.replies[min(reads, len(tc.replies)-1)]
				reads++
				switch reply {
				case "limited":
					return limited(p), nil
				case "replaced":
					r := stageResponse(p, "ready")
					r.DatasetID = "999"
					return r, nil
				case "mismatch":
					r := stageResponse(p, "invalid")
					r.Reason = "shared_files_mismatch"
					return r, nil
				case "public":
					r := stageResponse(p, "pending")
					r.Private = false
					return r, nil
				}
				return stageResponse(p, reply), nil
			}
			submits := 0
			e.run = func(_ context.Context, _ Config, mode string, _ []byte, r executionRequest) (executionResponse, error) {
				if mode != "submit" {
					t.Fatal("unexpected execution mode", mode)
				}
				submits++
				return executionFound(r, "QUEUED"), nil
			}
			out := e.Start(context.Background(), e.prepared, nil, nil)
			if out.Validate(e.plan.Job.Identity) != nil || out.Status != tc.want {
				t.Fatal("unexpected recheck outcome", out.Status, out.Problem)
			}
			if tc.persistent && reads < 2 || !tc.persistent && reads != tc.reads {
				t.Fatal("recheck read count", reads)
			}
			if tc.want == provider.SubmissionAccepted {
				if submits != 1 {
					t.Fatal("verified staging did not reach exactly one submission", submits)
				}
				return
			}
			details := out.Problem.PublicDetails()
			if submits != 0 || out.Problem.ComputeMayHaveStarted || details[domain.ReasonDetail] != tc.reason || details[domain.CauseDetail] != tc.cause {
				t.Fatal("unsubmitted request lost its reason or cause", submits, details)
			}
		})
	}
}

func TestExecutorReadsAgainAfterPreSaveReadRefusals(t *testing.T) {
	for name, tc := range map[string]struct {
		replies []executionResponse
		want    provider.SubmissionStatus
		calls   int
		reason  string
		cause   string
	}{
		"auth-then-found":   {replies: []executionResponse{{Status: "rejected", Reason: "auth_read_failed", Cause: "lookup_rate_limited"}, {Status: "found"}}, want: provider.SubmissionAccepted, calls: 2},
		"quota-then-kernel": {replies: []executionResponse{{Status: "rejected", Reason: "quota_read_failed"}, {Status: "rejected", Reason: "kernel_read_failed", Cause: "lookup_access_denied"}, {Status: "found"}}, want: provider.SubmissionAccepted, calls: 3},
		"stays-refused":     {replies: []executionResponse{{Status: "rejected", Reason: "auth_read_failed", Cause: "lookup_rate_limited"}}, want: provider.SubmissionRejected, reason: "auth_read_failed", cause: "lookup_rate_limited"},
		"token-inactive":    {replies: []executionResponse{{Status: "rejected", Reason: "token_inactive"}, {Status: "found"}}, want: provider.SubmissionRejected, calls: 1, reason: "token_inactive"},
	} {
		t.Run(name, func(t *testing.T) {
			e := newExecutionFixture(t)
			calls := 0
			e.run = func(_ context.Context, _ Config, mode string, _ []byte, r executionRequest) (executionResponse, error) {
				if mode != "submit" {
					t.Fatal("unexpected execution mode", mode)
				}
				reply := tc.replies[min(calls, len(tc.replies)-1)]
				calls++
				if reply.Status == "found" {
					return executionFound(r, "QUEUED"), nil
				}
				reply.Protocol = 1
				return reply, nil
			}
			out := e.Start(context.Background(), e.prepared, nil, nil)
			if out.Validate(e.plan.Job.Identity) != nil || out.Status != tc.want || tc.calls > 0 && calls != tc.calls || tc.calls == 0 && calls < 2 {
				t.Fatal("pre-save read refusal was not read again within the window", out.Status, calls)
			}
			if tc.want == provider.SubmissionRejected {
				details := out.Problem.PublicDetails()
				if out.Problem.ComputeMayHaveStarted || details[domain.ReasonDetail] != tc.reason || details[domain.CauseDetail] != tc.cause {
					t.Fatal("refusal lost its reason or cause", details)
				}
			}
		})
	}
	invalid := executionResponse{Protocol: 1, Status: "rejected", Reason: "auth_read_failed", Cause: "403 Forbidden"}
	if invalid.valid() {
		t.Fatal("raw provider text escaped as a refusal cause")
	}
}

func TestExecutorCarriesOnlyClosedHelperRefusals(t *testing.T) {
	// After invocation, a refusal without a closed reason is a protocol fault and stays ambiguous.
	for reason, want := range map[string]provider.SubmissionStatus{"kernel_read_failed": provider.SubmissionRejected, "": provider.SubmissionUnknown, "403 Forbidden": provider.SubmissionUnknown} {
		e := newExecutionFixture(t)
		e.run = func(context.Context, Config, string, []byte, executionRequest) (executionResponse, error) {
			return executionResponse{Protocol: 1, Status: "rejected", Reason: reason}, nil
		}
		out := e.Start(context.Background(), e.prepared, nil, nil)
		if out.Validate(e.plan.Job.Identity) != nil || out.Status != want || want == provider.SubmissionRejected && out.Problem.Details[domain.ReasonDetail] != reason {
			t.Fatal("helper refusal was misclassified", reason, out)
		}
	}
	e := newExecutionFixture(t)
	found := executionFound(e.request, "QUEUED")
	found.Reason = "kernel_read_failed"
	if found.valid() || (executionResponse{Protocol: 1, Status: "unknown", Reason: "403 Forbidden"}).valid() {
		t.Fatal("a reason accompanied a found status or raw provider text escaped")
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
	if json.Unmarshal(raw, &payload) != nil || len(payload.Modules) != 7 {
		t.Fatal("invalid runner package")
	}
	for _, secret := range []string{string(e.stager.config.CredentialRef), "SYNTHETIC_TOKEN", "private name"} {
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
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("Python unavailable for remote runner contract check")
	}
	cmd := exec.Command(python, "-I", "-c", "import sys;sys.path.insert(0,sys.argv[1]);from relay_runner.contract import load;load(sys.stdin.buffer.read());print('validated-not-executed')", root)
	cmd.Stdin = bytes.NewReader(payload.Manifest)
	output, err := cmd.CombinedOutput()
	if err != nil || strings.TrimSpace(string(output)) != "validated-not-executed" {
		t.Fatal("generated manifest differs from runner contract", err, string(output))
	}
}

func TestExecutorStagesUnderPreparationBudgetBeforeBoundedSubmissionCall(t *testing.T) {
	e := newExecutionFixture(t)
	e.policy.Timeout = time.Second
	e.stager.policy.Timeout = 5 * time.Second
	e.stager.run = func(ctx context.Context, _ Config, mode string, _ []byte, p stagingPlan, _ StagingBlobs) (stagingResponse, error) {
		deadline, ok := ctx.Deadline()
		if mode != "recheck" || !ok || time.Until(deadline) < 3*time.Second || time.Until(deadline) > 5*time.Second {
			t.Fatal("staging verification inherited the shorter SDK-call budget")
		}
		return stageResponse(p, "ready"), nil
	}
	calls := 0
	e.run = func(ctx context.Context, _ Config, mode string, _ []byte, r executionRequest) (executionResponse, error) {
		deadline, ok := ctx.Deadline()
		if mode != "submit" || !ok || time.Until(deadline) <= 0 || time.Until(deadline) > time.Second {
			t.Fatal("submission call lost its own finite SDK budget")
		}
		calls++
		return executionFound(r, "QUEUED"), nil
	}
	if result := e.Start(context.Background(), e.prepared, nil, nil); result.Status != provider.SubmissionAccepted || calls != 1 {
		t.Fatal("verified staging did not reach exactly one submission")
	}
}
