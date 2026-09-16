package kaggleacceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

func TestAcceptanceRecordDecoderRejectsMissingAliasedAndMalformedFields(t *testing.T) {
	type sample struct {
		Version int    `json:"version"`
		Name    string `json:"name"`
		Checked bool   `json:"checked"`
	}
	for _, raw := range []string{
		`{}`, `null`, `{"version":1,"name":"ok"}`, `{"version":1,"name":"ok","checked":null}`,
		`{"version":1,"name":"ok","checked":false,"extra":1}`,
		`{"Version":1,"name":"ok","checked":false}`,
		`{"version":1,"version":1,"name":"ok","checked":false}`,
		`{"version":1,"name":"ok","checked":false} {}`,
		`{"version":1,"name":"\ud800","checked":false}`,
		"{\"version\":1,\"name\":\"\xff\",\"checked\":false}",
		strings.Repeat("[", 18) + strings.Repeat("]", 18),
		strings.Repeat(" ", 16385),
	} {
		var decoded sample
		if decodeRecord([]byte(raw), &decoded) == nil {
			t.Fatal("invalid record accepted", raw)
		}
	}
	var decoded sample
	if err := decodeRecord([]byte(`{"version":1,"name":"unicode-\u00e9","checked":false}`), &decoded); err != nil || decoded.Name != "unicode-é" {
		t.Fatal("valid Unicode record rejected", err)
	}
}

func TestAcceptanceGPUVerificationRequiresExactOriginalCalculation(t *testing.T) {
	r := record{Challenge: strings.Repeat("0", 64), InputSHA256: provider.Digest([]byte("input")), MachineShape: "NvidiaTeslaT4", Receipt: admission.Receipt{JobID: "job", AttemptID: "attempt"}}
	base := calculationEvidence{1, "job", "attempt", r.Challenge, r.InputSHA256, "cuda:0", 64, 1, 4096, 262144, 262144, true, true}
	hardware := hardwareEvidence{1, "fixture-torch", "fixture-cuda", "fixture-python", "Tesla T4", "cuda:0"}
	for _, fault := range []string{"none", "cpu", "wrong-sum", "wrong-expected", "wrong-scale", "wrong-elements", "not-cuda", "not-checked", "challenge", "job", "attempt", "input", "hardware", "missing-version", "missing-field", "extra", "unpaired"} {
		t.Run(fault, func(t *testing.T) {
			calc, gpu := base, hardware
			switch fault {
			case "cpu":
				calc.Device = "cpu"
			case "wrong-sum":
				calc.ResultSum++
			case "wrong-expected":
				calc.ExpectedSum++
			case "wrong-scale":
				calc.Scale++
			case "wrong-elements":
				calc.Elements--
			case "not-cuda":
				calc.CUDAComputation = false
			case "not-checked":
				calc.AllCorrect = false
			case "challenge":
				calc.Challenge = strings.Repeat("f", 64)
			case "job":
				calc.JobID = "other"
			case "attempt":
				calc.AttemptID = "other"
			case "input":
				calc.InputSHA256 = provider.Digest(nil)
			case "hardware":
				gpu.DeviceName = "Tesla P100"
			case "missing-version":
				gpu.CUDAVersion = "None"
			}
			raw, device := mustJSON(t, calc), mustJSON(t, gpu)
			switch fault {
			case "missing-field":
				raw = bytes.Replace(raw, []byte(`,"cuda_computation":true`), nil, 1)
			case "extra":
				raw = bytes.Replace(raw, []byte(`"schema":1`), []byte(`"schema":1,"extra":true`), 1)
			case "unpaired":
				device = bytes.Replace(device, []byte(`"fixture-torch"`), []byte(`"\ud800"`), 1)
			case "none":
				// Python emits a JSON float; numerically equal finite values qualify.
				raw = bytes.Replace(raw, []byte(`"result_sum":262144`), []byte(`"result_sum":262144.0`), 1)
			}
			if verifiedGPU(r, raw, device) != (fault == "none") {
				t.Fatal("incorrect GPU evidence qualification", fault)
			}
		})
	}
}

func TestAcceptanceCreateOnlyRecordsAndIncompatibleStateFailClosed(t *testing.T) {
	f := newAcceptanceFixture(t, fake.Accept)
	f.must("prepare", processA, "prepared-local")
	path := filepath.Join(f.options.Root, "acceptance.json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if writeRecord(path, map[string]any{"replace": true}) == nil {
		t.Fatal("create-only record overwritten")
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("failed write damaged original record")
	}
	o := f.options
	o.Mode = "status"
	o.ProgramSHA256 = provider.Digest([]byte("different binary"))
	if _, err := runWith(context.Background(), o, f.deps(processB)); !errors.Is(err, ErrState) {
		t.Fatal("incompatible binary adopted old evidence", err)
	}
	o.ProgramSHA256 = f.options.ProgramSHA256
	// The public live path must reject a fixture record before constructing a provider.
	if _, err := Run(context.Background(), o); !errors.Is(err, ErrState) || f.constructed != 0 {
		t.Fatal("fixture state entered live composition", err)
	}
	if err := os.WriteFile(path, []byte(`{"protocol":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.run("status", processB); !errors.Is(err, ErrState) || f.constructed != 0 {
		t.Fatal("partial record was treated as fresh state", err)
	}
}

// Helper-only test: it runs in a fresh executable process, with no state/provider.
func TestAcceptanceProcessNonceChild(t *testing.T) {
	if os.Getenv("COMPUTE_RELAY_NONCE_CHILD") != "1" {
		t.Skip("subprocess helper only")
	}
	first, err := processNonce()
	second, nextErr := processNonce()
	if err != nil || nextErr != nil || first != second {
		t.Fatal("process identity is not stable")
	}
	if err := json.NewEncoder(os.Stdout).Encode(first); err != nil {
		t.Fatal(err)
	}
	os.Exit(0)
}
func TestAcceptanceProcessIdentityChangesAcrossActualExec(t *testing.T) {
	parent, err := processNonce()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{parent: true}
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAcceptanceProcessNonceChild$")
		cmd.Env = append(os.Environ(), "COMPUTE_RELAY_NONCE_CHILD=1")
		output, err := cmd.CombinedOutput()
		cancel()
		var identity string
		if err != nil || json.Unmarshal(output, &identity) != nil || len(identity) != 64 || seen[identity] {
			t.Fatal("exec did not produce independent process evidence", err, fmt.Sprintf("%q", output))
		}
		seen[identity] = true
	}
}
