package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

type stagingRecoveryProbe struct {
	*probeProvider
	observeCalls  int
	unavailable   bool
	observe       func(context.Context, provider.Plan, domain.OperationID) (provider.PreparationObservation, error)
	planDigest    domain.SHA256Digest
	preparationID domain.OperationID
}

func (p *stagingRecoveryProbe) ReconcilePrepare(ctx context.Context, plan provider.Plan, id domain.OperationID) (provider.PreparationObservation, error) {
	p.observeCalls++
	if plan.Digest() != p.planDigest || id != p.preparationID {
		return provider.PreparationObservation{}, errors.New("original staging identity changed")
	}
	if p.observe != nil {
		return p.observe(ctx, plan, id)
	}
	if p.unavailable {
		return provider.PreparationObservation{}, errors.New("synthetic staging observation failure")
	}
	return p.probeProvider.ReconcilePrepare(ctx, plan, id)
}

func TestStagingReconcilePreservesGrantAndAttemptRows(t *testing.T) {
	f := managedFixture(t, fake.DefaultScenario())
	id := f.seed(t, "a", 1, false)
	grant, authService, p := grantAuthorization(t, f, id)
	f.adapter.losePrepare = true
	if err := f.step(t); err == nil {
		t.Fatal("lost ACK hidden")
	}
	original := f.journal(t, id)
	probe := &stagingRecoveryProbe{probeProvider: f.adapter, unavailable: true, planDigest: original.Plan.Digest(), preparationID: original.PreparationID}
	registry := provider.NewSnapshotRegistry()
	requireNoError(t, registry.Register(provider.BindingSnapshot{Binding: f.profile.Binding, AccountScope: f.profile.AccountScope, CredentialRef: f.profile.CredentialRef}, probe))
	var err error
	f.engine, err = dispatch.New(f.s, registry, f.blobs, f.clock, dispatch.DefaultConfig())
	requireNoError(t, err)
	// Persist the historical attention state through its original transitions.
	for i := 1; i < dispatch.MaxFailures; i++ {
		f.clock.advance(time.Minute)
		claim, err := f.s.ClaimRecovery(t.Context(), "legacy")
		requireNoError(t, err)
		work, err := f.s.LoadDispatch(t.Context(), *claim)
		requireNoError(t, err)
		work, err = f.s.CommitDispatch(t.Context(), work.Handle, dispatch.Action{Kind: dispatch.Fault, Code: domain.CodeStagingFailed})
		requireNoError(t, err)
		requireNoError(t, f.s.YieldDispatch(t.Context(), work.Handle, 0))
	}
	service, actor, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	before, err := f.s.ReadJob(t.Context(), "a", actor.TokenID(), id.JobID)
	requireNoError(t, err)
	permit, err := authService.Get(t.Context(), p, "a", id.JobID, grant.AuthorizationID)
	if err != nil || permit.Status != executionauth.Consumed {
		t.Fatal(err)
	}
	if _, err = service.Submit(t.Context(), actor, "a", id.JobID, domain.OperationReconcile, "staging-storage", controlRequest(id.AttemptID)); err != nil {
		t.Fatal(err)
	}
	record, err := f.s.ReadJob(t.Context(), "a", actor.TokenID(), id.JobID)
	if err != nil || record.Attempt.ID != id.AttemptID || record.Job.ActiveAttemptID != before.Job.ActiveAttemptID ||
		!reflect.DeepEqual(record.Objects, before.Objects) || record.Attempt.State.Orchestration != domain.OrchestrationReconciling ||
		record.Attempt.State.Execution != domain.ExecutionNotSubmitted {
		t.Fatal("reconcile changed original pre-submit attempt or frozen inputs", err)
	}
	queued := f.journal(t, id)
	if queued.Phase != dispatch.Staging || queued.SubmitStarted || queued.PreparationID != original.PreparationID || !reflect.DeepEqual(queued.Plan, original.Plan) {
		t.Fatal("reconcile changed original journal identity")
	}
	probe.unavailable = false
	err = f.step(t)
	requireNoError(t, err)
	after, err := authService.Get(t.Context(), p, "a", id.JobID, grant.AuthorizationID)
	if err != nil || !reflect.DeepEqual(after, permit) || controlCount(t, f.s, "SELECT count(*) FROM attempt_grants WHERE kind='execution'") != 1 || controlCount(t, f.s, "SELECT count(*) FROM attempts") != 1 || controlCount(t, f.s, "SELECT count(*) FROM submission_intents") != 0 {
		t.Fatal("reconcile changed durable history", err)
	}
}

func TestUnconfirmedStagingEndsAfterObservationGrace(t *testing.T) {
	for _, mode := range []string{"unknown", "not_found", "failure", "timeout", "historical_attention", "confirmed_pending", "local_state_error", "shutdown", "verify_failure"} {
		t.Run(mode, func(t *testing.T) {
			f := newDispatchFixture(t, fake.DefaultScenario())
			id := f.seed(t, "a", 1, false)
			f.adapter.losePrepare = true
			if err := f.step(t); err == nil {
				t.Fatal("lost preparation acknowledgement hidden")
			}
			original := f.journal(t, id)
			probe := &stagingRecoveryProbe{probeProvider: f.adapter, planDigest: original.Plan.Digest(), preparationID: original.PreparationID}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			probe.observe = func(ctx context.Context, plan provider.Plan, op domain.OperationID) (provider.PreparationObservation, error) {
				switch mode {
				case "failure", "historical_attention":
					return provider.PreparationObservation{}, errors.New("synthetic observation failure")
				case "timeout":
					<-ctx.Done()
					return provider.PreparationObservation{}, ctx.Err()
				case "shutdown":
					cancel()
					<-ctx.Done()
					return provider.PreparationObservation{}, ctx.Err()
				case "local_state_error":
					return provider.PreparationObservation{}, provider.LocalStateError{Err: errors.New("synthetic local state failure")}
				case "confirmed_pending":
					seen, err := probe.probeProvider.ReconcilePrepare(ctx, plan, op)
					if seen.Prepared != nil {
						seen.Prepared.Ready = false
					}
					return seen, err
				case "not_found":
					return provider.PreparationObservation{Status: provider.ReconciliationNotFound}, nil
				default:
					return provider.PreparationObservation{Status: provider.ReconciliationUnknown}, nil
				}
			}
			if mode == "historical_attention" {
				for i := 1; i < dispatch.MaxFailures; i++ {
					f.clock.advance(time.Minute)
					claim, err := f.s.ClaimRecovery(t.Context(), "legacy")
					requireNoError(t, err)
					work, err := f.s.LoadDispatch(t.Context(), *claim)
					requireNoError(t, err)
					work, err = f.s.CommitDispatch(t.Context(), work.Handle, dispatch.Action{Kind: dispatch.Fault, Code: domain.CodeStagingNotReady})
					requireNoError(t, err)
					requireNoError(t, f.s.YieldDispatch(t.Context(), work.Handle, 0))
				}
				controls, actor, _ := controlService(t, f, "a", auth.Read, auth.Operate)
				_, err := controls.Submit(t.Context(), actor, "a", id.JobID, domain.OperationReconcile, "historical-observation", controlRequest(id.AttemptID))
				requireNoError(t, err)
			}
			if mode == "verify_failure" {
				probe.verifyError = errors.New("synthetic auth observation failure")
			}
			registry := provider.NewSnapshotRegistry()
			requireNoError(t, registry.Register(provider.BindingSnapshot{Binding: f.profile.Binding, AccountScope: f.profile.AccountScope, CredentialRef: f.profile.CredentialRef}, probe))
			cfg := dispatch.DefaultConfig()
			cfg.ControlTimeout = 20 * time.Millisecond
			var err error
			f.engine, err = dispatch.New(f.s, registry, f.blobs, f.clock, cfg)
			requireNoError(t, err)
			f.clock.advance(10 * time.Minute)
			started := time.Now()
			_, _ = f.engine.RunOnce(ctx, "observer")
			if mode == "timeout" && time.Since(started) > time.Second {
				t.Fatal("observation ignored control budget")
			}
			state, j := f.state(t, id), f.journal(t, id)
			terminal := mode != "confirmed_pending" && mode != "local_state_error" && mode != "shutdown"
			if terminal {
				if state.Orchestration != domain.OrchestrationFailed || state.Execution != domain.ExecutionNotSubmitted || state.RemoteActivity != domain.RemoteActivityNotStarted || state.ReleaseEvidence != domain.ReleaseEvidenceNotApplicable || j.Problem == nil || j.Problem.PublicDetails()[domain.ReasonDetail] != "staging_unconfirmed" || j.Problem.ComputeMayHaveStarted || j.Problem.SafeOperationRetry || scheduler.HoldsAccount(state, true) {
					t.Fatalf("unconfirmed staging did not safely terminate: %s %+v", j.Phase, state)
				}
				if _, _, _, err := dispatch.Apply(j, state, dispatch.Action{Kind: dispatch.RequestReconciliation}, f.clock.Now()); err == nil {
					t.Fatal("terminal preparation rearmed")
				}
			} else if state.Orchestration.Terminal() || !scheduler.HoldsAccount(state, true) {
				t.Fatal("unresolved or confirmed staging was incorrectly released")
			}
			if !reflect.DeepEqual(j.Plan, original.Plan) || j.PreparationID != original.PreparationID || j.SubmitStarted || f.backend.Stats().PrepareCalls != 1 || f.backend.Stats().SubmitCalls != 0 || f.backend.Stats().CleanupCalls != 0 {
				t.Fatal("observation changed intent or repeated a mutation")
			}
			if mode != "verify_failure" && probe.observeCalls != 1 {
				t.Fatal("observation was repeated", probe.observeCalls)
			}
			result, err := f.s.ClaimNext(t.Context(), "capacity-probe")
			requireNoError(t, err)
			wantReservations := 1
			if terminal {
				wantReservations = 0
			}
			if result.Claim != nil || result.View.AccountReservations[f.profile.AccountScope] != wantReservations {
				t.Fatal("incorrect account reservation after observation", result.View.AccountReservations)
			}
			if mode == "confirmed_pending" {
				if j.Prepared == nil || j.Prepared.Ready {
					t.Fatal("confirmed pending resource lost")
				}
			} else {
				var ref, readiness string
				requireNoError(t, f.s.db.QueryRow(`SELECT resource_ref,readiness FROM provider_resources WHERE purpose='staging'`).Scan(&ref, &readiness))
				if ref != "" || readiness != "unknown" {
					t.Fatal("unknown staging became absence or identity evidence")
				}
				requireNoError(t, f.s.write(t.Context(), func(ctx context.Context, tx *sql.Tx, now time.Time) error {
					record, err := controlTarget(ctx, tx, id.WorkspaceID, id.JobID, id.AttemptID)
					if err != nil {
						return err
					}
					resources, err := cleanupResources(ctx, tx, record)
					if err != nil {
						return err
					}
					if len(resources) != 1 || resources[0].Reason != "creation_unresolved" {
						t.Fatal("unconfirmed staging became a cleanup target")
					}
					return nil
				}))
			}
		})
	}
}

func TestUnconfirmedStagingGraceSurvivesRestartWithoutRepeatingPreparation(t *testing.T) {
	for _, appears := range []bool{true, false} {
		t.Run(fmt.Sprint(appears), func(t *testing.T) {
			f := newDispatchFixture(t, fake.DefaultScenario())
			id := f.seed(t, "a", 1, false)
			f.adapter.losePrepare = true
			if err := f.step(t); err == nil {
				t.Fatal("lost acknowledgement hidden")
			}
			original := f.journal(t, id)
			if original.PreparationObservationStarted == nil {
				t.Fatal("observation budget was not durable")
			}
			attach := func() *stagingRecoveryProbe {
				probe := &stagingRecoveryProbe{probeProvider: f.adapter, planDigest: original.Plan.Digest(), preparationID: original.PreparationID}
				probe.observe = func(ctx context.Context, plan provider.Plan, op domain.OperationID) (provider.PreparationObservation, error) {
					return provider.PreparationObservation{Status: provider.ReconciliationNotFound}, nil
				}
				registry := provider.NewSnapshotRegistry()
				requireNoError(t, registry.Register(provider.BindingSnapshot{Binding: f.profile.Binding, AccountScope: f.profile.AccountScope, CredentialRef: f.profile.CredentialRef}, probe))
				var err error
				cfg := dispatch.DefaultConfig()
				cfg.PollDelay = time.Millisecond
				f.engine, err = dispatch.New(f.s, registry, f.blobs, f.clock, cfg)
				requireNoError(t, err)
				return probe
			}
			attach()
			// Several transient misses may exceed the old five-fault limit while still
			// fitting the grace. They cannot release capacity or rearm preparation.
			for i := 0; i < 5; i++ {
				f.clock.advance(20 * time.Second)
				_, _ = f.engine.RunOnce(t.Context(), "observer")
				if f.state(t, id).Orchestration.Terminal() || f.journal(t, id).Phase != dispatch.Staging {
					t.Fatal("staging ended before its observation grace")
				}
			}
			requireNoError(t, f.s.Close())
			options := DefaultOptions()
			options.Clock = f.clock.Now
			reopened, err := Open(t.Context(), f.root, options)
			requireNoError(t, err)
			f.attach(t, reopened)
			probe := attach()
			if appears {
				probe.observe = func(ctx context.Context, plan provider.Plan, op domain.OperationID) (provider.PreparationObservation, error) {
					return probe.probeProvider.ReconcilePrepare(ctx, plan, op)
				}
			}
			f.clock.advance(dispatch.PreparationObservationGrace - 100*time.Second + time.Second)
			_, _ = f.engine.RunOnce(t.Context(), "restarted")
			j := f.journal(t, id)
			if appears {
				if j.Phase != dispatch.Ready || j.Prepared == nil {
					t.Fatal("restart did not observe original staging", j.Phase)
				}
			} else if j.Phase != dispatch.Prevented || j.Problem == nil || j.Problem.Code != domain.CodeStagingNotReady {
				t.Fatal("staging that never appeared did not end after grace", j.Phase)
			}
			if j.PreparationObservationStarted == nil || !j.PreparationObservationStarted.Equal(*original.PreparationObservationStarted) || !reflect.DeepEqual(j.Plan, original.Plan) || j.PreparationID != original.PreparationID || f.backend.Stats().PrepareCalls != 1 || f.backend.Stats().SubmitCalls != 0 {
				t.Fatal("restart changed observation budget or repeated a mutation")
			}
		})
	}
}

type lateCleanupProbe struct {
	*probeProvider
	mode string
}

func (p *lateCleanupProbe) ObserveStagingCleanup(ctx context.Context, plan provider.Plan, op domain.OperationID) (provider.PreparationObservation, error) {
	if p.mode == "absent" {
		return provider.PreparationObservation{Status: provider.ReconciliationNotFound}, nil
	}
	if p.mode == "unknown" {
		return provider.PreparationObservation{Status: provider.ReconciliationUnknown}, nil
	}
	seen, err := p.ReconcilePrepare(ctx, plan, op)
	if seen.Prepared != nil {
		seen.Prepared.Ready = false
	}
	return seen, err
}

func TestUnconfirmedStagingCleanupObservesWithoutReopeningCompute(t *testing.T) {
	for _, mode := range []string{"present", "absent", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			f := newDispatchFixture(t, fake.DefaultScenario())
			id := f.seed(t, "a", 1, false)
			f.adapter.losePrepare = true
			if err := f.step(t); err == nil {
				t.Fatal("lost acknowledgement hidden")
			}
			original := f.journal(t, id)
			probe := &stagingRecoveryProbe{probeProvider: f.adapter, planDigest: original.Plan.Digest(), preparationID: original.PreparationID, unavailable: true}
			registry := provider.NewSnapshotRegistry()
			requireNoError(t, registry.Register(provider.BindingSnapshot{Binding: f.profile.Binding, AccountScope: f.profile.AccountScope, CredentialRef: f.profile.CredentialRef}, probe))
			var err error
			f.engine, err = dispatch.New(f.s, registry, f.blobs, f.clock, dispatch.DefaultConfig())
			requireNoError(t, err)
			if err := f.step(t); err == nil {
				t.Fatal("unconfirmed preparation did not end")
			}
			before, state := f.journal(t, id), f.state(t, id)
			_, actor := cleanupService(t, f, "a", auth.Read, auth.Operate)
			plan, err := f.s.PlanCleanup(t.Context(), "a", actor.TokenID(), id.JobID, id.AttemptID, false)
			requireNoError(t, err)
			if plan.Reason != "creation_unresolved" {
				t.Fatal(plan.Reason)
			}
			adapter := &lateCleanupProbe{probeProvider: f.adapter, mode: mode}
			service := cleanupPreviewService(t, f).WithPreviewResolver(cleanupResolver{adapter})
			preview, err := service.Preview(t.Context(), actor, "a", id.JobID, id.AttemptID, false)
			requireNoError(t, err)
			after := f.journal(t, id)
			if after.Phase != before.Phase || !reflect.DeepEqual(after.Problem, before.Problem) || f.state(t, id) != state || after.SubmitStarted || f.backend.Stats().PrepareCalls != 1 || f.backend.Stats().SubmitCalls != 0 {
				t.Fatal("late cleanup discovery reopened compute or changed terminal history")
			}
			f.clock.advance(8 * 24 * time.Hour)
			scanRetention(t, f)
			if controlCount(t, f.s, "SELECT count(*) FROM retention_inventory WHERE workspace_id='a' AND expired_at IS NOT NULL") != 0 {
				t.Fatal("discovery without confirmed cleanup released input pins", mode)
			}
			switch mode {
			case "present":
				if !preview.Eligible || after.Prepared == nil || preview.Resources[0].Observation != "present" {
					t.Fatal("late exact identity stayed uncleanable", preview)
				}
				// Durable reconstruction and cleanup admission now consume the discovered
				// reference through the ordinary guards.
				if _, _, err := f.s.LoadProviderAttempt(t.Context(), after.Plan.Job.Identity); err != nil {
					t.Fatal(err)
				}
				admitCleanup(t, f, id, "late-cleanup", false)
				worker, _ := cleanupWorker(t, f)
				runCleanup(t, worker)
				if controlCount(t, f.s, "SELECT count(*) FROM cleanup_resources WHERE state='deleted'") != 1 {
					t.Fatal("late exact staging could not be cleaned")
				}
			case "absent":
				if preview.Eligible || preview.Reason != "absence_observed" || preview.Resources[0].Observation != "absent" || after.Prepared != nil {
					t.Fatal("strict absence invented a deletion target", preview)
				}
			case "unknown":
				if preview.Eligible || preview.Reason != "creation_unresolved" || after.Prepared != nil {
					t.Fatal("unknown existence allowed cleanup", preview)
				}
			}
			scanRetention(t, f)
			if controlCount(t, f.s, "SELECT count(*) FROM retention_inventory WHERE workspace_id='a' AND expired_at IS NOT NULL") != 0 {
				t.Fatal("cleanup skipped the input retention window")
			}
			f.clock.advance(8 * 24 * time.Hour)
			scanRetention(t, f)
			want := 0
			if mode == "present" {
				want = 2
			}
			if controlCount(t, f.s, "SELECT count(*) FROM retention_inventory WHERE workspace_id='a' AND expired_at IS NOT NULL") != want {
				t.Fatal("confirmed cleanup did not release inputs or unresolved staging lost its pins", mode)
			}
			if mode == "present" && (f.journal(t, id).Prepared.Ready || f.state(t, id).Orchestration != domain.OrchestrationFailed) {
				t.Fatal("input retention invented readiness or changed the failed history")
			}
		})
	}
}
