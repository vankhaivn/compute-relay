package runtimehost

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

type systemMatrixProbe struct {
	mode         string
	wrongAccount bool
	childExit    int
}

func installSystemMatrixProvider(t *testing.T, f *managedTestFixture, e *systemDispatchEffects, mode string) *systemMatrixProbe {
	t.Helper()
	p := &systemMatrixProbe{mode: mode}
	e.verify = func(context.Context, provider.BindingSnapshot) error {
		if p.wrongAccount {
			return errors.New("private-account-canary")
		}
		return nil
	}
	e.observe = func(_ context.Context, _ provider.RemoteReference, obs provider.Observation) (provider.Observation, error) {
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
	e.reconcile = func(ctx context.Context, _ provider.Identity) error {
		if p.mode == "deadline" {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}
	e.start = func(ctx context.Context, _ provider.Prepared, _ *provider.StartAuthorization, _ *workloadsecret.Material, out provider.SubmissionOutcome) provider.SubmissionOutcome {
		if p.mode != "cli-exit" {
			return out
		}
		// A real nonzero child exit loses the response after synthetic acceptance.
		executable, err := os.Executable()
		if err != nil {
			t.Error(err)
			return provider.SubmissionOutcome{}
		}
		cmd := exec.CommandContext(ctx, executable, "-test.run=^TestSystemFaultMatrixCLIChild$", "--", "relay-cli-fault")
		cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
		cmd.WaitDelay = time.Second
		var exit *exec.ExitError
		if err := cmd.Run(); errors.As(err, &exit) {
			p.childExit = exit.ExitCode()
		}
		return provider.SubmissionOutcome{}
	}
	if mode == "deadline" {
		cfg := dispatch.DefaultConfig()
		cfg.ControlTimeout = 30 * time.Millisecond
		var err error
		f.services.dispatcher, err = dispatch.New(f.wrapDispatch(f.host.store), f.services.resolver, f.host.inputs, f.fixtureClock(), cfg)
		if err != nil {
			t.Fatal(err)
		}
	}
	return p
}
