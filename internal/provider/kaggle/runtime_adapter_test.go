package kaggle

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func runtimeBudgetIdentity(attempt string) provider.Identity {
	digest := provider.Digest([]byte("fixture"))
	return provider.Identity{
		InstallationID: "runtime", WorkspaceID: "workspace", JobID: "job", AttemptID: domain.AttemptID(attempt),
		InstanceID: "instance", IntentID: domain.SubmissionIntentID("intent_" + attempt),
		ResourceKey: attempt, Nonce: strings.Repeat("n", 32),
		BundleSHA256: digest, InputManifestSHA256: digest,
	}
}

func TestRuntimeAdapterBudgetIsPerDistinctAttemptAndFinite(t *testing.T) {
	p := &RuntimeAdapter{
		allow: true, maxAttempts: 1, authorized: make(map[provider.Identity]bool),
	}
	first := runtimeBudgetIdentity("attempt-one")
	if err := p.authorize(first); err != nil || !p.BudgetExhausted() {
		t.Fatal("first attempt did not consume the finite budget", err)
	}
	if err := p.authorize(first); err != nil {
		t.Fatal("same attempt consumed budget twice", err)
	}
	if err := p.authorize(runtimeBudgetIdentity("attempt-two")); !errors.Is(err, ErrRuntimeBudget) {
		t.Fatal("second distinct attempt exceeded budget without blocking", err)
	}
	out := p.Submit(context.Background(), provider.Prepared{Identity: runtimeBudgetIdentity("attempt-two")})
	if out.Status != provider.SubmissionRejected || out.Problem.Details[domain.ReasonDetail] != reasonMutationBudget {
		t.Fatal("budget stop lost its reason", out)
	}
}

func TestRuntimeAdapterReadOnlyNeverAuthorizesMutation(t *testing.T) {
	p := &RuntimeAdapter{authorized: make(map[provider.Identity]bool)}
	if err := p.authorize(runtimeBudgetIdentity("attempt-one")); !errors.Is(err, ErrRuntimeBudget) || !p.BudgetExhausted() {
		t.Fatal("read-only adapter authorized provider mutation", err)
	}
}

func TestRuntimeCleanupIsExplicitlyUnsupported(t *testing.T) {
	_, err := (&RuntimeAdapter{}).Cleanup(context.Background(), provider.CleanupRequest{Mode: provider.CleanupApply})
	if !errors.Is(err, ErrRuntimeCleanup) || !errors.Is(err, provider.ErrCleanupUnsupported) {
		t.Fatal("runtime cleanup must report an unsupported adapter", err)
	}
}
