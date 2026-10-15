package hoststorage

import (
	"encoding/json"
	"math"
	"testing"
)

func TestBudgetDefaultsAndFrozenRuntime(t *testing.T) {
	b := DefaultBudget()
	if !b.Valid() || b.SoftBytes != 68_719_476_736 || b.HardBytes != 85_899_345_920 {
		t.Fatal("incorrect qualified-host defaults", b)
	}
	if (Budget{SoftBytes: 4 << 30, HardBytes: 4 << 30}).Valid() || (Budget{SoftBytes: 1, HardBytes: MaximumHardBytes + 1}).Valid() {
		t.Fatal("invalid budget accepted")
	}
	if got := MinBudget(b, Budget{SoftBytes: 2 << 30, HardBytes: 3 << 30}); got != (Budget{2 << 30, 3 << 30}) {
		t.Fatal("aliases did not share conservative thresholds", got)
	}
	raw := []byte(`{"schema_version":3,"machine_id":"opaque"}`)
	frozen, err := FreezeRuntimeBudget(raw, b)
	if err != nil {
		t.Fatal(err)
	}
	var original, output map[string]any
	if json.Unmarshal(raw, &original) != nil || json.Unmarshal(frozen, &output) != nil || original["storage_budget"] != nil || output["storage_budget"] == nil {
		t.Fatal("freeze changed historical runtime or dropped policy")
	}
	if got, err := BudgetFromRuntime(frozen); err != nil || got != b {
		t.Fatal("frozen budget not readable", got, err)
	}
	if got, err := BudgetFromRuntime(raw); err != nil || got != b {
		t.Fatal("legacy revision did not retain default", got, err)
	}
	if _, err := BudgetFromRuntime([]byte(`{"storage_budget":{"soft_bytes":3,"hard_bytes":2}}`)); err == nil {
		t.Fatal("inverted budget accepted")
	}
}

func TestChargeArithmeticCannotWrapOrDoubleReserve(t *testing.T) {
	if n, err := Charge(300, 500); err != nil || n != 500 {
		t.Fatal(n, err)
	}
	if n, err := Charge(600, 500); err != nil || n != 600 {
		t.Fatal(n, err)
	}
	if _, err := AddChecked(math.MaxInt64, 1); err == nil {
		t.Fatal("overflow accepted")
	}
}
