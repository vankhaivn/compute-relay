package admission

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestProfileResourcePolicyKeepsLegacyIdentityAndRejectsMismatch(t *testing.T) {
	const legacy = `{"binding":{"Profile":"default-gpu","ProviderInstanceID":"instance","ConfigurationRevision":"r1"},"account_scope":"account","cost_class":"free_allowance","allow_remote_internet":false,"max_remote_wall_seconds":1800,"max_bundle_bytes":104857600,"max_input_bytes":4294967296}`
	var p Profile
	if err := json.Unmarshal([]byte(legacy), &p); err != nil || p.Validate() != nil {
		t.Fatal("legacy profile rejected", err)
	}
	raw, err := json.Marshal(p)
	if err != nil || string(raw) != legacy {
		t.Fatal("legacy immutable profile/permit bytes changed", string(raw), err)
	}
	cpu, err := Parse([]byte(validJob))
	if err != nil {
		t.Fatal(err)
	}
	gpu, err := Parse([]byte(strings.Replace(validJob, `"accelerator":"cpu"`, `"accelerator":"gpu","minimum_gpu_count":1`, 1)))
	if err != nil {
		t.Fatal(err)
	}
	if p.Check(cpu.Spec()) != nil || p.Check(gpu.Spec()) != nil {
		t.Fatal("legacy provider-validated resource policy narrowed")
	}
	p.Accelerator = "cpu"
	if p.Check(cpu.Spec()) != nil || p.Check(gpu.Spec()) != ErrRequirements {
		t.Fatal("CPU profile allowed another resource")
	}
	p.Accelerator = "gpu"
	if p.Check(gpu.Spec()) != nil || p.Check(cpu.Spec()) != ErrRequirements {
		t.Fatal("GPU profile silently fell back to CPU")
	}
	p.Accelerator = "auto"
	if p.Validate() != ErrRequirements {
		t.Fatal("ambiguous resource policy accepted")
	}
}
