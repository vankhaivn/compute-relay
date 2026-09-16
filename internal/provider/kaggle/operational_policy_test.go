package kaggle

import (
	"context"
	"errors"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func TestOperationalCapabilitiesAreIndependentAndNotLiveEvidence(t *testing.T) {
	c := testConfig(t)
	first, err := OperationalDescriptor(c)
	if err != nil || len(first.Capabilities) != 14 {
		t.Fatal("missing capability inventory", err)
	}
	seen := map[domain.CapabilityName]bool{}
	for _, cap := range first.Capabilities {
		if cap.Validate() != nil || seen[cap.Name] || cap.AccountChecked || cap.Evidence == domain.EvidencePassedLive || !cap.CheckedAt.IsZero() {
			t.Fatal("invalid or inflated evidence", cap)
		}
		seen[cap.Name] = true
	}
	if first.Support(domain.CapabilityRemoteCancellation) != domain.CapabilitySupportUnsupported || first.Support(domain.CapabilityLogsWhileRunning) != domain.CapabilitySupportUnknown || first.Support(domain.CapabilityExecutionTimeout) != domain.CapabilitySupportUnknown || first.Support(domain.CapabilityBatchExecution) != domain.CapabilitySupportUnknown {
		t.Fatal("unverified capability became available")
	}
	for i := range first.Capabilities {
		first.Capabilities[i].Reason = "changed"
		if len(first.Capabilities[i].Conditions) > 0 {
			first.Capabilities[i].Conditions[0] = "changed"
		}
	}
	second, _ := OperationalDescriptor(c)
	for _, cap := range second.Capabilities {
		if cap.Reason == "changed" || len(cap.Conditions) > 0 && cap.Conditions[0] == "changed" {
			t.Fatal("caller mutated future descriptors")
		}
	}
	if _, err := OperationalDescriptor(Config{}); !errors.Is(err, ErrConfig) {
		t.Fatal("invalid config accepted")
	}
}

func TestOperationalCancellationIsManualWithoutAnyProviderIO(t *testing.T) {
	e := newExecutionFixture(t)
	ref, err := e.remote(executionFound(e.request, "RUNNING"))
	if err != nil {
		t.Fatal(err)
	}
	e.stager.local = func(context.Context, Config, Mode, []byte) (Report, error) {
		t.Fatal("manual cancel probed provider")
		return Report{}, nil
	}
	e.run = func(context.Context, Config, string, []byte, executionRequest) (executionResponse, error) {
		t.Fatal("manual cancel invoked execution")
		return executionResponse{}, nil
	}
	for i := 0; i < 3; i++ {
		out, err := e.Cancel(context.Background(), ref, "cancel_op")
		if err != nil || out.Validate() != nil || out.Status != domain.CancellationManual || out.TerminationConfirmed {
			t.Fatal("false remote termination", out, err)
		}
	}
	foreign := ref
	foreign.Identity.WorkspaceID = "foreign"
	if _, err := e.Cancel(context.Background(), foreign, "cancel_op"); !errors.Is(err, ErrExecutionIdentity) {
		t.Fatal("foreign reference accepted")
	}
	if _, err := e.Cancel(context.Background(), ref, ""); !errors.Is(err, ErrConfig) {
		t.Fatal("missing operation accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := e.Cancel(ctx, ref, "cancel_op"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if e.used.Load() {
		t.Fatal("cancel changed submission gate")
	}
}

func TestOperationalTimeoutsPreserveFrozenBudgetsAndUncertainty(t *testing.T) {
	e := newExecutionFixture(t)
	budgets, err := e.TimeoutBudgets()
	if err != nil || budgets.RemoteWallSeconds != 10 || budgets.SetupSeconds != 5 || budgets.FinalizationGraceSeconds != 2 || budgets.ControlCall != e.policy.Timeout || budgets.ProviderEnforcement != domain.CapabilitySupportUnknown {
		t.Fatal(budgets, err)
	}
	for _, raw := range []string{"ERROR", "CANCEL_ACKNOWLEDGED", "REMOTE_TIMEOUT", "UNKNOWN"} {
		execution, _ := executionState(raw)
		if execution == domain.ExecutionTimedOut || execution == domain.ExecutionCancelled {
			t.Fatal("unproven timeout or cancel", raw)
		}
	}
	var absent *Executor
	if _, err := absent.TimeoutBudgets(); !errors.Is(err, ErrConfig) {
		t.Fatal(err)
	}
}

var _ provider.Canceller = (*Executor)(nil)
