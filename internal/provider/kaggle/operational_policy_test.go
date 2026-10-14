package kaggle

import (
	"errors"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

func TestRuntimeCapabilitiesPreserveUnknownAndUnsupported(t *testing.T) {
	cpu := RuntimeCapabilities("cpu")
	gpu := RuntimeCapabilities("NvidiaTeslaT4")
	if !cpu.Valid() || !gpu.Valid() || cpu.GPU != domain.CapabilitySupportUnsupported ||
		gpu.GPU != domain.CapabilitySupportUnknown || gpu.GPUShape != "NvidiaTeslaT4" ||
		gpu.LogsWhileRunning != domain.CapabilitySupportUnknown ||
		gpu.Support(domain.CapabilityRemoteCancellation) != domain.CapabilitySupportUnsupported ||
		gpu.Support(domain.CapabilityRemoteCleanup) != domain.CapabilitySupportSupported {
		t.Fatal("runtime capabilities lost uncertainty or support")
	}
	gpu.Accelerators[0] = "changed"
	if RuntimeCapabilities("NvidiaTeslaT4").Accelerators[0] != "gpu" {
		t.Fatal("caller mutated future capability declaration")
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
