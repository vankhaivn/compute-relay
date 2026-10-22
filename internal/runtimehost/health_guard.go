package runtimehost

import (
	"context"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hosthealth"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
	"github.com/vankhaivn/compute-relay/internal/worker"
)

type managedHostHealthGuard struct {
	store  *sqlite.Store
	health *hosthealth.Service
	clock  ports.Clock
}

func (h *Host) newHostHealthGuard(health *hosthealth.Service, clock ports.Clock) (*managedHostHealthGuard, error) {
	if h == nil || h.store == nil || health == nil || clock == nil {
		return nil, ErrState
	}
	return &managedHostHealthGuard{h.store, health, clock}, nil
}

func (g *managedHostHealthGuard) PreflightAdmission(ctx context.Context, w domain.WorkspaceID, token, key string, req admission.Request) (context.Context, *admission.Receipt, error) {
	check, err := g.store.PreviewHostHealthAdmission(ctx, w, token, key, req)
	if errors.Is(err, hosthealth.ErrUnverified) {
		return ctx, nil, provider.WithReason(admission.ErrHostUnhealthy, "health_binding_unverified")
	}
	if err != nil {
		return ctx, nil, err
	}
	if check.Replay != nil || check.Target == nil {
		return ctx, check.Replay, nil
	}
	view, err := g.health.Refresh(ctx, *check.Target)
	if err == nil && !hosthealth.Evaluate(view, *check.Target.Policy, g.clock.Now()).RefuseNew {
		return sqlite.WithHostHealthEvidence(ctx, check.Profile, g.health.Evidence(check.Target.ResourceID)), nil, nil
	}
	// A concurrent exact caller may have committed while this read failed. Replay
	// always wins over later health, expiry or a changed current selection.
	prior, replayErr := g.store.ReplayHostHealthAdmission(ctx, w, token, key, req)
	if replayErr != nil {
		return ctx, nil, replayErr
	}
	if prior != nil {
		return ctx, prior, nil
	}
	if err != nil {
		return ctx, nil, err
	}
	return ctx, nil, healthRefusal(view, *check.Target.Policy, g.clock.Now())
}

func (g *managedHostHealthGuard) BeforeNewWork(ctx context.Context, record admission.Record) (context.Context, error) {
	target, err := g.store.HostHealthTargetForAttempt(ctx, record)
	if errors.Is(err, hosthealth.ErrUnverified) {
		return sqlite.WithHostHealthEvidence(ctx, record.Profile, hosthealth.Evidence{}), provider.WithReason(admission.ErrHostUnhealthy, "health_binding_unverified")
	}
	if err != nil {
		return ctx, provider.LocalStateError{Err: err}
	}
	if target == nil {
		return ctx, nil
	}
	view, err := g.health.Refresh(ctx, *target)
	guarded := sqlite.WithHostHealthEvidence(ctx, record.Profile, g.health.Evidence(target.ResourceID))
	if err != nil {
		return guarded, provider.LocalStateError{Err: err}
	}
	if hosthealth.Evaluate(view, *target.Policy, g.clock.Now()).RefuseNew {
		return guarded, healthRefusal(view, *target.Policy, g.clock.Now())
	}
	return guarded, nil
}

func (g *managedHostHealthGuard) RunOnce(ctx context.Context) error {
	failures := g.health.RunOnce(ctx)
	if failures != nil && worker.Classify(failures) == worker.Stop {
		return failures
	}
	attempts, err := g.store.HostHealthAttempts(ctx)
	if err != nil {
		return errors.Join(failures, err)
	}
	for _, attempt := range attempts {
		_, err := g.store.RequestHostHealthStop(ctx, attempt, g.health.Evidence(attempt.Target.ResourceID))
		failures = errors.Join(failures, err)
	}
	return failures
}

func (g *managedHostHealthGuard) Run(ctx context.Context) error {
	ticker := time.NewTicker(hosthealth.SampleCadence)
	defer ticker.Stop()
	for {
		if err := g.RunOnce(ctx); err != nil && worker.Classify(err) == worker.Stop {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func healthRefusal(view hosthealth.View, policy hosthealth.Policy, now time.Time) error {
	cause := "health_observation_failed"
	decision := hosthealth.Evaluate(view, policy, now)
	if len(decision.Reasons) > 0 {
		cause = string(decision.Reasons[0])
	}
	if view.Latest != nil && domain.ValidReason(view.Latest.Cause) {
		cause = view.Latest.Cause
	}
	return provider.WithReason(admission.ErrHostUnhealthy, cause)
}
