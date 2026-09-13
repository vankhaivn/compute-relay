// Package providertest provides reusable OFFLINE conformance tests for provider adapters.
// Factories must use fixtures/controlled transports, never operator accounts or compute.
package providertest

import (
	"bytes"
	"context"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

type Mode string

const (
	Normal       Mode = "normal"
	LostResponse Mode = "lost_response"
	Unresolved   Mode = "unresolved"
	Rejected     Mode = "rejected"
)

type Harness struct {
	Provider   provider.Provider
	Job        provider.ResolvedJob
	Advance    func(provider.RemoteReference) error
	Reconnect  func() provider.Provider
	Executions func() int
}
type Factory func(*testing.T, Mode) Harness

// Run tests the mandatory port without importing a concrete provider or its SDK.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("lifecycle_and_artifacts", func(t *testing.T) {
		h := factory(t, Normal)
		ctx := context.Background()
		check, err := h.Provider.Check(ctx)
		must(t, err)
		if !check.Ready || check.Evidence != domain.EvidenceImplementedOffline {
			t.Fatal("invalid offline diagnostic")
		}
		remote := submit(t, h)
		before, err := h.Provider.Observe(ctx, remote)
		must(t, err)
		again, err := h.Provider.Observe(ctx, remote)
		must(t, err)
		if before != again {
			t.Fatal("ordinary observation advanced fixture state")
		}
		if _, err := h.Provider.ListArtifacts(ctx, remote, provider.PageRequest{Limit: 1}); err == nil {
			t.Fatal("active outputs were accepted as stable")
		}
		finish(t, h, remote)
		seen := map[string]bool{}
		page := provider.PageRequest{Limit: 1}
		manifestFound := false
		for pages := 0; ; pages++ {
			if pages > 100 {
				t.Fatal("pagination did not terminate")
			}
			result, err := h.Provider.ListArtifacts(ctx, remote, page)
			must(t, err)
			if len(result.Artifacts) > page.Limit {
				t.Fatal("page bound exceeded")
			}
			for _, artifact := range result.Artifacts {
				must(t, artifact.Validate(remote))
				if seen[artifact.Path] {
					t.Fatal("duplicate output")
				}
				seen[artifact.Path] = true
				var dst bytes.Buffer
				receipt, err := h.Provider.FetchArtifact(ctx, remote, artifact, &dst, 1<<20)
				must(t, err)
				if receipt.Bytes != artifact.Bytes || receipt.SHA256 != artifact.SHA256 || provider.Digest(dst.Bytes()) != artifact.SHA256 {
					t.Fatal("unverified transfer")
				}
				if artifact.Path == "execution-result.json" {
					manifestFound = true
					must(t, provider.ValidateManifestIdentity(remote.Identity, dst.Bytes()))
				}
				bad := artifact
				bad.Remote.Identity.Nonce = "another-fixture-nonce"
				var rejected bytes.Buffer
				if _, err := h.Provider.FetchArtifact(ctx, remote, bad, &rejected, 1<<20); err == nil || rejected.Len() != 0 {
					t.Fatal("wrong-attempt artifact was written")
				}
			}
			if result.NextCursor == "" {
				break
			}
			page.Cursor = result.NextCursor
		}
		if !manifestFound || len(seen) < 2 {
			t.Fatal("manifest and multi-file result required")
		}
		request := provider.CleanupRequest{Remote: remote, LedgerID: "ledger_fixture", CreationOperationID: "prepare_fixture", ResultsCollected: true, Mode: provider.CleanupDryRun}
		dry, err := h.Provider.Cleanup(ctx, request)
		must(t, err)
		if !dry.WouldDelete || dry.Deleted {
			t.Fatal("dry-run mutated provider")
		}
		_, err = h.Provider.Observe(ctx, remote)
		must(t, err)
		request.Mode = provider.CleanupApply
		applied, err := h.Provider.Cleanup(ctx, request)
		must(t, err)
		if !applied.Deleted {
			t.Fatal("apply did not delete owned fixture")
		}
		repeat, err := h.Provider.Cleanup(ctx, request)
		must(t, err)
		if !repeat.AlreadyAbsent {
			t.Fatal("cleanup is not idempotent")
		}
		if h.Executions() != 1 {
			t.Fatal("collection/cleanup created compute")
		}
	})
	t.Run("lost_response_reconnect_without_resubmit", func(t *testing.T) {
		h := factory(t, LostResponse)
		prepared := prepare(t, h)
		out := h.Provider.Submit(context.Background(), prepared)
		must(t, out.Validate(h.Job.Identity))
		if out.Status != provider.SubmissionUnknown {
			t.Fatal("lost response was not ambiguous")
		}
		reconnected := h.Reconnect()
		reconciled, err := reconnected.ReconcileSubmission(context.Background(), h.Job.Identity)
		must(t, err)
		must(t, reconciled.Validate(h.Job.Identity))
		if reconciled.Status != provider.ReconciliationFound || reconciled.Remote == nil || reconciled.Remote.Identity != h.Job.Identity {
			t.Fatal("original execution not rediscovered")
		}
		for i := 0; i < 3; i++ {
			_, err := reconnected.Observe(context.Background(), *reconciled.Remote)
			must(t, err)
		}
		if h.Executions() != 1 {
			t.Fatal("reconnect or observation duplicated execution")
		}
	})
	t.Run("unresolved_stays_unknown", func(t *testing.T) {
		h := factory(t, Unresolved)
		out := h.Provider.Submit(context.Background(), prepare(t, h))
		must(t, out.Validate(h.Job.Identity))
		if out.Status != provider.SubmissionUnknown {
			t.Fatal("uncertainty hidden")
		}
		for i := 0; i < 3; i++ {
			r, err := h.Provider.ReconcileSubmission(context.Background(), h.Job.Identity)
			must(t, err)
			if r.Status != provider.ReconciliationUnknown || r.Remote != nil {
				t.Fatal("invented remote identity")
			}
		}
		if h.Executions() != 0 {
			t.Fatal("reconciliation created execution")
		}
	})
	t.Run("proven_rejection", func(t *testing.T) {
		h := factory(t, Rejected)
		out := h.Provider.Submit(context.Background(), prepare(t, h))
		must(t, out.Validate(h.Job.Identity))
		if out.Status != provider.SubmissionRejected || h.Executions() != 0 {
			t.Fatal("invalid rejection")
		}
	})
	t.Run("missing_optional_capabilities", func(t *testing.T) {
		h := factory(t, Normal)
		ctx := context.Background()
		remote := submit(t, h)
		if _, ok := h.Provider.(provider.Canceller); ok {
			t.Fatal("minimal fixture implements cancellation")
		}
		if _, ok := h.Provider.(provider.LogReader); ok {
			t.Fatal("minimal fixture implements logs")
		}
		if _, ok := h.Provider.(provider.QuotaReader); ok {
			t.Fatal("minimal fixture implements quota")
		}
		cancel, err := provider.RequestCancellation(ctx, h.Provider, remote, "cancel_fixture")
		must(t, err)
		if cancel.Status != domain.CancellationManual || cancel.TerminationConfirmed {
			t.Fatal("unsupported cancellation reported success")
		}
		logs, err := provider.ReadAvailableLogs(ctx, h.Provider, remote, provider.PageRequest{Limit: 1})
		must(t, err)
		if logs.Availability != "unavailable" || len(logs.Lines) != 0 {
			t.Fatal("invented live logs")
		}
		quota, err := provider.ReadAvailableQuota(ctx, h.Provider)
		must(t, err)
		if quota.Status != provider.QuotaUnknown || quota.Remaining != nil || quota.Limit != nil || quota.ResetAt != nil {
			t.Fatal("unknown quota became a fabricated number")
		}
		request := provider.CleanupRequest{Remote: remote, LedgerID: "ledger_fixture", CreationOperationID: "prepare_fixture", ResultsCollected: true, Mode: provider.CleanupApply}
		if _, err := h.Provider.Cleanup(ctx, request); err == nil {
			t.Fatal("cleanup cancelled active execution")
		}
		if h.Executions() != 1 {
			t.Fatal("optional capability lookup created execution")
		}
	})
	t.Run("cancelled_context_has_no_mutations", func(t *testing.T) {
		h := factory(t, Normal)
		prepared := prepare(t, h)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		out := h.Provider.Submit(ctx, prepared)
		must(t, out.Validate(h.Job.Identity))
		if out.Status != provider.SubmissionRejected || h.Executions() != 0 {
			t.Fatal("cancelled preflight created execution")
		}
	})
}
func prepare(t *testing.T, h Harness) provider.Prepared {
	t.Helper()
	plan, err := h.Provider.Validate(context.Background(), h.Job)
	must(t, err)
	prepared, err := h.Provider.Prepare(context.Background(), plan, "prepare_fixture")
	must(t, err)
	if !prepared.Ready {
		t.Fatal("fixture not ready")
	}
	return prepared
}
func submit(t *testing.T, h Harness) provider.RemoteReference {
	t.Helper()
	out := h.Provider.Submit(context.Background(), prepare(t, h))
	must(t, out.Validate(h.Job.Identity))
	if out.Status != provider.SubmissionAccepted || out.Remote == nil {
		t.Fatal("expected accepted fixture")
	}
	return *out.Remote
}
func finish(t *testing.T, h Harness, remote provider.RemoteReference) {
	t.Helper()
	for i := 0; i < 100; i++ {
		obs, err := h.Provider.Observe(context.Background(), remote)
		must(t, err)
		must(t, obs.Validate(remote))
		if obs.Execution.Terminal() {
			return
		}
		must(t, h.Advance(remote))
	}
	t.Fatal("fixture did not terminate")
}
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
