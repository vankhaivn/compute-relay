package hoststorage

import (
	"encoding/json"

	"github.com/vankhaivn/compute-relay/internal/jsonwire"
)

// FreezeRuntimeBudget appends the effective non-secret policy to a new
// immutable managed SSH revision. Existing historical revisions are untouched.
func FreezeRuntimeBudget(raw []byte, budget Budget) ([]byte, error) {
	fields, err := jsonwire.Object(raw, 8192)
	if err != nil || !budget.Valid() {
		return nil, ErrInvalid
	}
	encoded, err := json.Marshal(budget)
	if err != nil {
		return nil, ErrInvalid
	}
	fields["storage_budget"] = encoded
	out, err := json.Marshal(fields)
	if err != nil || len(out) > 8192 {
		return nil, ErrInvalid
	}
	return out, nil
}

// BudgetFromRuntime preserves the default for old frozen revisions; it never
// looks at today's mutable connection configuration.
func BudgetFromRuntime(raw []byte) (Budget, error) {
	fields, err := jsonwire.Object(raw, 8192)
	if err != nil {
		return Budget{}, ErrInvalid
	}
	data, ok := fields["storage_budget"]
	if !ok {
		return DefaultBudget(), nil
	}
	object, err := jsonwire.Object(data, 256)
	if err != nil || jsonwire.Fields(object, []string{"soft_bytes", "hard_bytes"}, nil) != nil {
		return Budget{}, ErrInvalid
	}
	var budget Budget
	if json.Unmarshal(data, &budget) != nil || !budget.Valid() {
		return Budget{}, ErrInvalid
	}
	return budget, nil
}
