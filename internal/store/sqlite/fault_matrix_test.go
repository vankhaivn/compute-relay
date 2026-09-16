package sqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"reflect"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

// The backend represents synthetic remote state. This wrapper injects transport,
// account and observation faults; it never invokes Kaggle or an admitted command.
type matrixProvider struct {
	*probeProvider
	mode         string
	wrongAccount bool
	childExit    int
}

func (p *matrixProvider) VerifyBinding(ctx context.Context, binding provider.BindingSnapshot) error {
	if p.wrongAccount {
		return errors.New("private-account-canary")
	}
	return p.probeProvider.VerifyBinding(ctx, binding)
}
func (p *matrixProvider) Observe(ctx context.Context, remote provider.RemoteReference) (provider.Observation, error) {
	obs, err := p.probeProvider.Observe(ctx, remote)
	if err != nil {
		return obs, err
	}
	switch p.mode {
	case "unknown-format":
		obs.Execution = domain.ExecutionUnknown
		obs.RemoteActivity = domain.RemoteActivityUnknown
		obs.ReleaseEvidence = domain.ReleaseEvidenceUnknown
		obs.RawState = "new-provider-format-private-canary"
	case "invalid-state":
		obs.Execution = "new-unmapped-state"
	case "modified-resource":
		obs.Remote.Version = "manually-replaced-version"
	}
	return obs, nil
}
func (p *matrixProvider) ReconcileSubmission(ctx context.Context, id provider.Identity) (provider.Reconciliation, error) {
	if p.mode == "deadline" {
		<-ctx.Done()
		return provider.Reconciliation{}, ctx.Err()
	}
	return p.probeProvider.ReconcileSubmission(ctx, id)
}
func (p *matrixProvider) Submit(ctx context.Context, prepared provider.Prepared) provider.SubmissionOutcome {
	outcome := p.probeProvider.Submit(ctx, prepared)
	if p.mode != "cli-exit" {
		return outcome
	}
	// Acceptance happens only in the in-memory fixture. A real helper-process
	// nonzero exit then loses its usable response. This is not an official-CLI
	// live test or a claim that local process failure proves non-acceptance.
	executable, err := os.Executable()
	if err != nil {
		return provider.SubmissionOutcome{}
	}
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestFaultMatrixCLIChild$", "--", "relay-cli-fault")
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	cmd.WaitDelay = time.Second
	err = cmd.Run()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		p.childExit = exit.ExitCode()
	}
	return provider.SubmissionOutcome{}
}

func installMatrixProvider(t *testing.T, f *dispatchFixture, mode string) *matrixProvider {
	t.Helper()
	p := &matrixProvider{probeProvider: f.adapter, mode: mode}
	registry := provider.NewSnapshotRegistry()
	binding := provider.BindingSnapshot{Binding: f.profile.Binding, AccountScope: f.profile.AccountScope, CredentialRef: f.profile.CredentialRef}
	if err := registry.Register(binding, p); err != nil {
		t.Fatal(err)
	}
	cfg := dispatch.DefaultConfig()
	if mode == "deadline" {
		cfg.ControlTimeout = 30 * time.Millisecond
	}
	engine, err := dispatch.New(f.s, registry, f.blobs, nil, f.clock, cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.engine = engine
	return p
}

func TestFaultMatrixUnknownAndModifiedObservations(t *testing.T) {
	for _, mode := range []string{"unknown-format", "invalid-state", "modified-resource"} {
		t.Run(mode, func(t *testing.T) {
			f := newDispatchFixture(t, fake.DefaultScenario())
			id := f.seed(t, "a", 1, false)
			submittedControl(t, f, id)
			remote := *f.journal(t, id).Remote
			if err := f.adapter.Advance(remote); err != nil {
				t.Fatal(err)
			}
			if err := f.step(t); err != nil {
				t.Fatal(err)
			}
			before := f.state(t, id)
			installMatrixProvider(t, f, mode)
			err := f.step(t)
			if mode != "unknown-format" && err == nil {
				t.Fatal("invalid identity/state was not rejected")
			}
			j, state := f.journal(t, id), f.state(t, id)
			if j.Problem == nil || !j.Problem.ComputeMayHaveStarted || j.Problem.SafeOperationRetry || *j.Remote != remote ||
				state.Execution != before.Execution || state.RemoteActivity != before.RemoteActivity || state.ReleaseEvidence != before.ReleaseEvidence || state.Orchestration.Terminal() {
				t.Fatal("uncertain observation rewrote confirmed evidence")
			}
			raw, err := json.Marshal(j)
			if err != nil || bytes.Contains(raw, []byte("private-canary")) {
				t.Fatal("raw provider diagnostics escaped into journal")
			}
			f.restart(t)
			f.seed(t, "b", 2, false)
			claim, err := f.s.ClaimNext(context.Background(), "other", f.clock.Now())
			if err != nil || claim.Claim != nil {
				t.Fatal("uncertain remote activity released account capacity", err)
			}
			if f.backend.Stats().SubmitCalls != 1 || f.backend.Stats().Executions != 1 {
				t.Fatal("observation fault repeated compute")
			}
		})
	}
}

func TestFaultMatrixRestartKeepsActiveAttempt(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	submittedControl(t, f, id)
	remote := *f.journal(t, id).Remote
	if err := f.adapter.Advance(remote); err != nil {
		t.Fatal(err)
	}
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	before := f.state(t, id)
	if before.Execution != domain.ExecutionRunning {
		t.Fatal("fixture did not reach active remote work")
	}
	stats := f.backend.Stats()
	f.restart(t)
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	if f.state(t, id) != before || *f.journal(t, id).Remote != remote || f.backend.Stats() != stats ||
		controlCount(t, f.s, "SELECT count(*) FROM submission_intents") != 1 || controlCount(t, f.s, "SELECT count(*) FROM attempts") != 1 {
		t.Fatal("restart lost or recreated the active attempt")
	}
}

func TestFaultMatrixStalePollCannotRewriteTerminal(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	submittedControl(t, f, id)
	remote := *f.journal(t, id).Remote
	if err := f.adapter.Advance(remote); err != nil {
		t.Fatal(err)
	}
	f.clock.advance(time.Minute)
	claim, err := f.s.ClaimRecovery(context.Background(), "observer", f.clock.Now())
	if err != nil || claim == nil {
		t.Fatal("missing observation lease", err)
	}
	work, err := f.s.LoadDispatch(context.Background(), *claim, f.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	stale, err := f.adapter.Observe(context.Background(), remote)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.adapter.Advance(remote); err != nil {
		t.Fatal(err)
	}
	f.clock.advance(time.Millisecond)
	terminal, err := f.adapter.Observe(context.Background(), remote)
	if err != nil {
		t.Fatal(err)
	}
	current, err := f.s.CommitDispatch(context.Background(), work.Handle, dispatch.Action{Kind: dispatch.ObservationSeen, Observation: &terminal}, f.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	events := controlCount(t, f.s, "SELECT count(*) FROM events")
	// Both the old owner version and a current handle must reject the late poll.
	for _, handle := range []dispatch.Handle{work.Handle, current.Handle} {
		if _, err := f.s.CommitDispatch(context.Background(), handle, dispatch.Action{Kind: dispatch.ObservationSeen, Observation: &stale}, f.clock.Now()); err == nil {
			t.Fatal("late poll regressed terminal state")
		}
	}
	if !reflect.DeepEqual(f.journal(t, id), current.Journal) || f.state(t, id) != current.Job.Attempt.State || controlCount(t, f.s, "SELECT count(*) FROM events") != events {
		t.Fatal("rejected poll changed durable evidence")
	}
	if f.state(t, id).Execution != domain.ExecutionSucceeded || f.state(t, id).Result != domain.ResultNotAvailable {
		t.Fatal("terminal observation fabricated artifact success")
	}
}

func TestFaultMatrixLocalDeadlineKeepsUnknownExecution(t *testing.T) {
	scenario := fake.DefaultScenario()
	scenario.Mode = fake.Unresolved
	f := newDispatchFixture(t, scenario)
	id := f.seed(t, "a", 1, false)
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	installMatrixProvider(t, f, "deadline")
	if err := f.step(t); err == nil {
		t.Fatal("expired observation invocation reported success")
	}
	f.restart(t)
	state := f.state(t, id)
	if state.Execution != domain.ExecutionUnknown || state.RemoteActivity != domain.RemoteActivityPossible || state.Orchestration.Terminal() ||
		state.Cancellation != domain.CancellationNotRequested || state.ReleaseEvidence != domain.ReleaseEvidenceUnknown {
		t.Fatal("local invocation deadline fabricated remote termination", state)
	}
	f.seed(t, "b", 2, false)
	claim, err := f.s.ClaimNext(context.Background(), "other", f.clock.Now())
	if err != nil || claim.Claim != nil || f.backend.Stats().SubmitCalls != 1 {
		t.Fatal("deadline/restart permitted replacement compute", err)
	}
}

func TestFaultMatrixCredentialRotationKeepsFrozenAccount(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	submittedControl(t, f, id)
	before := f.journal(t, id)
	p := installMatrixProvider(t, f, "")
	p.wrongAccount = true
	if err := f.step(t); err == nil {
		t.Fatal("mismatching effective account was accepted")
	}
	j := f.journal(t, id)
	if j.Problem == nil || j.Problem.Code != domain.CodeProviderAuthFailed || !j.Problem.ComputeMayHaveStarted || *j.Remote != *before.Remote {
		t.Fatal("account failure changed remote identity")
	}
	// This models credentials rotated back to the SAME verified account, without
	// loading any real secret. A different account is never a fallback.
	p.wrongAccount = false
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	j = f.journal(t, id)
	if j.Plan.Digest() != before.Plan.Digest() || *j.Remote != *before.Remote || f.backend.Stats().SubmitCalls != 1 || f.backend.Stats().PrepareCalls != 1 {
		t.Fatal("credential recovery remapped or resubmitted frozen work")
	}
}

func TestFaultMatrixNonzeroCLIAfterAcceptanceDoesNotResubmit(t *testing.T) {
	f := newDispatchFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	p := installMatrixProvider(t, f, "cli-exit")
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	j := f.journal(t, id)
	if p.childExit != 23 || j.Phase != dispatch.Submitting || j.Problem == nil || !j.Problem.ComputeMayHaveStarted || j.Problem.SafeOperationRetry || f.backend.Stats().Executions != 1 {
		t.Fatal("nonzero CLI exit was treated as proven non-acceptance")
	}
	identity := j.Plan.Job.Identity
	f.restart(t)
	if err := f.step(t); err != nil {
		t.Fatal(err)
	}
	j = f.journal(t, id)
	if j.Remote == nil || j.Remote.Identity != identity || j.Phase != dispatch.Submitted || f.backend.Stats().SubmitCalls != 1 || f.backend.Stats().Executions != 1 {
		t.Fatal("CLI failure repeated the accepted execution")
	}
}

func TestFaultMatrixCLIChild(t *testing.T) {
	if len(os.Args) == 0 || os.Args[len(os.Args)-1] != "relay-cli-fault" {
		t.Skip("helper process only; parent test verifies its actual exit status")
	}
	os.Exit(23)
}
