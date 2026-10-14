// Package providertest provides reusable OFFLINE conformance tests for provider adapters.
// Factories must use fixtures/controlled transports, never operator accounts or compute.
package providertest

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

type Mode string

const (
	Normal       Mode = "normal"
	LostResponse Mode = "lost_response"
	Unresolved   Mode = "unresolved"
	Rejected     Mode = "rejected"
)

type Harness struct {
	Provider     provider.Provider
	Binding      provider.BindingSnapshot
	Job          provider.ResolvedJob
	Advance      func(provider.RemoteReference) error
	Reconnect    func() provider.Provider
	BeforeStart  func()
	Executions   func() int
	Preparations func() int
}
type Factory func(*testing.T, Mode) Harness

// Run checks the mandatory port through controlled provider transports.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("lifecycle_and_artifacts", func(t *testing.T) {
		h := factory(t, Normal)
		ctx := context.Background()
		must(t, h.Provider.Verify(ctx, h.Binding))
		remote := submit(t, h)
		before, err := h.Provider.Observe(ctx, remote)
		must(t, err)
		again, err := h.Provider.Observe(ctx, remote)
		must(t, err)
		if before != again {
			t.Fatal("ordinary observation advanced fixture state")
		}
		if _, err := h.Provider.ListArtifacts(ctx, remote); err == nil {
			t.Fatal("active outputs were accepted as stable")
		}
		finish(t, h, remote)
		catalog, err := h.Provider.ListArtifacts(ctx, remote)
		must(t, err)
		if len(catalog) > provider.MaxArtifactFiles {
			t.Fatal("catalog exceeds the full-list bound")
		}
		seen := map[string]bool{}
		manifestFound := false
		var total int64
		for _, artifact := range catalog {
			must(t, artifact.Validate(remote))
			if seen[artifact.Path] {
				t.Fatal("duplicate output")
			}
			seen[artifact.Path] = true
			total += artifact.Bytes
			if total > provider.MaxArtifactBytes {
				t.Fatal("catalog exceeds byte bound")
			}
			var dst bytes.Buffer
			var received int64
			receipt, err := h.Provider.Fetch(ctx, remote, artifact, &dst, artifact.Bytes, func(n int64) error {
				if n < received || n > artifact.Bytes {
					t.Fatal("invalid transfer progress")
				}
				received = n
				return nil
			})
			must(t, err)
			if receipt.Bytes != artifact.Bytes || receipt.SHA256 != artifact.SHA256 || provider.Digest(dst.Bytes()) != artifact.SHA256 || received != artifact.Bytes {
				t.Fatal("unverified transfer or incomplete progress")
			}
			if artifact.Path == "execution-result.json" || artifact.Path == "control/execution-result.json" {
				manifestFound = true
				must(t, provider.ValidateManifestIdentity(remote.Identity, dst.Bytes()))
			}
			if _, err := h.Provider.Fetch(ctx, remote, artifact, nil, artifact.Bytes, func(int64) error { t.Fatal("nil destination reported transferred bytes"); return nil }); err == nil {
				t.Fatal("nil destination accepted")
			}
			bad := artifact
			bad.Remote.Identity.Nonce = "another-fixture-nonce"
			var rejected bytes.Buffer
			if _, err := h.Provider.Fetch(ctx, remote, bad, &rejected, artifact.Bytes, nil); err == nil || rejected.Len() != 0 {
				t.Fatal("wrong-attempt artifact was written")
			}
			if strings.Contains(artifact.Path, "result.json") || artifact.Bytes == 0 {
				continue
			}
			stop := errors.New("stop transfer fixture")
			if _, err := h.Provider.Fetch(ctx, remote, artifact, &bytes.Buffer{}, artifact.Bytes, func(int64) error { return stop }); err == nil {
				t.Fatal("callback error did not stop transfer")
			}
		}
		if !manifestFound || len(seen) < 2 {
			t.Fatal("manifest and multi-file result required")
		}
		request := cleanupRequest(t, h, remote, provider.CleanupDryRun)
		description, err := h.Provider.DescribeCleanup(request)
		must(t, err)
		if !description.Valid() || description.Version != remote.Version {
			t.Fatal("cleanup descriptor lost the original target")
		}
		same, err := h.Reconnect().DescribeCleanup(request)
		must(t, err)
		if same.ID != description.ID || same.Name != description.Name || same.Version != description.Version || same.Scope != description.Scope {
			t.Fatal("cleanup descriptor changed after reconnect")
		}
		dry, err := h.Provider.CleanupResource(ctx, request)
		must(t, err)
		if !dry.WouldDelete || dry.Deleted {
			t.Fatal("dry-run mutated provider")
		}
		_, err = h.Provider.Observe(ctx, remote)
		must(t, err)
		request.Mode = provider.CleanupApply
		request.RecoverOnly = true
		recovered, err := h.Provider.CleanupResource(ctx, request)
		if err == nil || recovered.Deleted {
			t.Fatal("observation-only cleanup replayed deletion")
		}
		request.RecoverOnly = false
		applied, err := h.Provider.CleanupResource(ctx, request)
		must(t, err)
		if !applied.Deleted {
			t.Fatal("apply did not delete owned fixture")
		}
		request.RecoverOnly = true
		repeat, err := h.Provider.CleanupResource(ctx, request)
		must(t, err)
		if !repeat.AlreadyAbsent {
			t.Fatal("cleanup absence was not confirmed")
		}
		if h.Executions() != 1 {
			t.Fatal("collection/cleanup created compute")
		}
	})
	t.Run("preparation_recovery_is_read_only", func(t *testing.T) {
		h := factory(t, Normal)
		ctx := context.Background()
		prepared := prepare(t, h)
		plan, err := h.Provider.Validate(ctx, h.Job)
		must(t, err)
		before := h.Preparations()
		fresh := h.Reconnect()
		must(t, fresh.Verify(ctx, h.Binding))
		for i := 0; i < 2; i++ {
			seen, err := fresh.ReconcilePrepare(ctx, plan, "prepare_fixture")
			must(t, err)
			must(t, seen.Validate(plan, "prepare_fixture"))
			if seen.Status != provider.ReconciliationFound || seen.Prepared == nil || *seen.Prepared != prepared {
				t.Fatal("original preparation was not rediscovered")
			}
		}
		changed := plan.Clone()
		changed.Job.Identity.Nonce = "another-fixture-nonce"
		for _, query := range []struct {
			plan      provider.Plan
			operation domain.OperationID
		}{{plan, "another_preparation"}, {changed, "prepare_fixture"}} {
			seen, err := fresh.ReconcilePrepare(ctx, query.plan, query.operation)
			if seen.Prepared != nil || err == nil && seen.Status == provider.ReconciliationFound {
				t.Fatal("changed preparation identity was accepted")
			}
		}
		if h.Preparations() != before || h.Executions() != 0 {
			t.Fatal("preparation recovery repeated mutation")
		}
	})
	t.Run("lost_response_reconnect_without_resubmit", func(t *testing.T) {
		h := factory(t, LostResponse)
		out := h.Provider.Start(context.Background(), prepare(t, h), nil, nil)
		must(t, out.Validate(h.Job.Identity))
		if out.Status != provider.SubmissionUnknown {
			t.Fatal("lost response was not ambiguous")
		}
		reconnected := h.Reconnect()
		must(t, reconnected.Verify(context.Background(), h.Binding))
		seen, err := reconnected.Reconcile(context.Background(), h.Job.Identity)
		must(t, err)
		must(t, seen.Validate(h.Job.Identity))
		if seen.Status != provider.ReconciliationFound || seen.Remote == nil {
			t.Fatal("original execution not rediscovered")
		}
		for i := 0; i < 3; i++ {
			_, err := reconnected.Observe(context.Background(), *seen.Remote)
			must(t, err)
		}
		if h.Executions() != 1 {
			t.Fatal("reconnect or observation duplicated execution")
		}
	})
	t.Run("unresolved_does_not_invent_an_execution", func(t *testing.T) {
		h := factory(t, Unresolved)
		out := h.Provider.Start(context.Background(), prepare(t, h), nil, nil)
		must(t, out.Validate(h.Job.Identity))
		if out.Status != provider.SubmissionUnknown {
			t.Fatal("uncertainty hidden")
		}
		for i := 0; i < 3; i++ {
			seen, err := h.Provider.Reconcile(context.Background(), h.Job.Identity)
			if seen.Remote != nil {
				t.Fatal("invented remote identity")
			}
			if err == nil {
				must(t, seen.Validate(h.Job.Identity))
				if seen.Status == provider.ReconciliationFound {
					t.Fatal("invented acceptance")
				}
			}
		}
		if h.Executions() != 0 {
			t.Fatal("reconciliation created execution")
		}
	})
	t.Run("proven_rejection", func(t *testing.T) {
		h := factory(t, Rejected)
		out := h.Provider.Start(context.Background(), prepare(t, h), nil, nil)
		must(t, out.Validate(h.Job.Identity))
		if out.Status != provider.SubmissionRejected || h.Executions() != 0 {
			t.Fatal("invalid rejection")
		}
	})
	t.Run("cancellation_logs_and_active_cleanup_are_truthful", func(t *testing.T) {
		h := factory(t, Normal)
		ctx := context.Background()
		remote := submit(t, h)
		if _, err := h.Provider.CleanupResource(ctx, cleanupRequest(t, h, remote, provider.CleanupApply)); err == nil {
			t.Fatal("cleanup cancelled active execution")
		}
		logs, err := h.Provider.Logs(ctx, remote, provider.PageRequest{Limit: 1})
		must(t, err)
		must(t, logs.Validate(provider.PageRequest{Limit: 1}))
		if h.Provider.Describe().Support(domain.CapabilityLogsWhileRunning) == domain.CapabilitySupportUnsupported && (logs.Availability != "unavailable" || len(logs.Lines) != 0) {
			t.Fatal("unsupported logs invented evidence")
		}
		if _, err := h.Provider.Cancel(ctx, remote, ""); err == nil {
			t.Fatal("invalid cancellation operation accepted")
		}
		outcome, err := h.Provider.Cancel(ctx, remote, "cancel_fixture")
		must(t, err)
		must(t, outcome.Validate())
		if h.Provider.Describe().Support(domain.CapabilityRemoteCancellation) != domain.CapabilitySupportSupported && (outcome.Status != domain.CancellationManual || outcome.TerminationConfirmed) {
			t.Fatal("unsupported cancellation reported termination")
		}
		if outcome.TerminationConfirmed {
			observed, err := h.Provider.Observe(ctx, remote)
			must(t, err)
			if !observed.Execution.Terminal() {
				t.Fatal("cancellation confirmation lacks terminal evidence")
			}
		}
		if h.Executions() != 1 {
			t.Fatal("logs/cancellation created execution")
		}
	})
	t.Run("authority_and_identity_fail_before_start", func(t *testing.T) {
		h := factory(t, Normal)
		prepared := prepare(t, h)
		material := &workloadsecret.Material{}
		out := h.Provider.Start(context.Background(), prepared, nil, material)
		must(t, out.Validate(h.Job.Identity))
		if out.Status != provider.SubmissionRejected || h.Executions() != 0 {
			t.Fatal("material without authority started compute")
		}
		changed := prepared
		changed.Identity.Nonce = "another-fixture-nonce"
		out = h.Provider.Start(context.Background(), changed, nil, nil)
		if out.Status != provider.SubmissionRejected || h.Executions() != 0 {
			t.Fatal("changed preparation started compute")
		}
		binding := h.Binding
		binding.AccountScope = "another_account"
		if err := h.Provider.Verify(context.Background(), binding); err == nil {
			t.Fatal("changed account binding accepted")
		}
	})
	t.Run("cancelled_context_has_no_mutations", func(t *testing.T) {
		h := factory(t, Normal)
		prepared := prepare(t, h)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		out := h.Provider.Start(ctx, prepared, nil, nil)
		must(t, out.Validate(h.Job.Identity))
		if out.Status != provider.SubmissionRejected || h.Executions() != 0 {
			t.Fatal("cancelled start created execution")
		}
	})
}
func cleanupRequest(t *testing.T, h Harness, remote provider.RemoteReference, mode provider.CleanupMode) provider.CleanupResourceRequest {
	t.Helper()
	plan, err := h.Provider.Validate(context.Background(), h.Job)
	must(t, err)
	return provider.CleanupResourceRequest{CleanupRequest: provider.CleanupRequest{Remote: remote, LedgerID: "ledger_fixture", CreationOperationID: "prepare_fixture", ResultsCollected: true, Mode: mode}, Purpose: "execution", OwnershipOperationID: domain.OperationID(remote.Identity.IntentID), PlanSHA256: plan.Digest()}
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
	if h.BeforeStart != nil {
		h.BeforeStart()
	}
	return prepared
}
func submit(t *testing.T, h Harness) provider.RemoteReference {
	t.Helper()
	out := h.Provider.Start(context.Background(), prepare(t, h), nil, nil)
	must(t, out.Validate(h.Job.Identity))
	if out.Status != provider.SubmissionAccepted || out.Remote == nil {
		t.Fatal("expected accepted fixture")
	}
	return *out.Remote
}
func finish(t *testing.T, h Harness, remote provider.RemoteReference) {
	t.Helper()
	for i := 0; i < 100; i++ {
		observed, err := h.Provider.Observe(context.Background(), remote)
		must(t, err)
		must(t, observed.Validate(remote))
		if observed.Execution.Terminal() {
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
