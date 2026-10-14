package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

// One transaction shares one complete scheduler snapshot across a connection list.
type managedScheduling struct {
	loaded   bool
	settings scheduler.Settings
	view     scheduler.View
	err      error
}

func (s *managedScheduling) read(ctx context.Context, tx *sql.Tx, now time.Time) error {
	if s.loaded {
		return s.err
	}
	s.loaded = true
	var last string
	s.settings, last, s.err = schedulerControl(ctx, tx)
	if s.err != nil {
		return s.err
	}
	rows, err := schedulerSnapshot(ctx, tx, s.settings, now)
	if err != nil {
		s.err = err
		return err
	}
	_, s.view, s.err = scheduler.Select(rows, domain.WorkspaceID(last), s.settings, now)
	return s.err
}

func managedConcurrency(ctx context.Context, tx *sql.Tx, r managedRecord, selection *connections.Selection, snapshot *managedScheduling, now time.Time) (*connections.ConcurrencyStatus, error) {
	configured := 1
	if selection != nil && selection.MaxActiveAttempts > 0 {
		configured = selection.MaxActiveAttempts
	} else if r.view.Configuration != nil && r.view.Configuration.MaxActiveAttempts > 0 {
		configured = r.view.Configuration.MaxActiveAttempts
	}
	result := &connections.ConcurrencyStatus{Scope: "account", ConfiguredLimit: configured}
	err := snapshot.read(ctx, tx, now)
	if err != nil {
		return nil, err
	}
	settings, view := snapshot.settings, snapshot.view
	c := scheduler.Candidate{AccountScope: r.accountScope, Policy: scheduler.AccountPolicy{MaxActive: configured}}
	var raw string
	err = tx.QueryRowContext(ctx, `SELECT policy FROM scheduler_accounts WHERE account_scope=?`, r.accountScope).Scan(&raw)
	if err == nil {
		if len(raw) > 2048 || json.Unmarshal([]byte(raw), &c.Policy) != nil || !c.Policy.Valid() {
			return nil, ErrCorrupt
		}
		c.Policy.MaxActive = min(c.Policy.MaxActive, configured)
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, dbError(err)
	}
	resource, err := managedResource(ctx, tx, r.accountScope)
	if err != nil {
		return nil, err
	}
	wait := func(reason scheduler.Reason) {
		value := string(reason)
		result.WaitReason = &value
	}
	if resource.Kind != "" {
		result.Scope = "resource"
		c.ResourceKind, c.ResourceScope = resource.Kind, resource.ID
		if resource.ID == "" {
			wait(scheduler.ResourceUnverified)
			return result, nil
		}
		result.Resource = &resource
		if view.UnresolvedResources[resource.Kind] {
			wait(scheduler.ResourceUnresolved)
			return result, nil
		}
		c.ResourceMaxActive, err = managedResourceLimit(ctx, tx, resource.ID)
		if err != nil {
			return nil, err
		}
		c.ResourceMaxActive = min(c.ResourceMaxActive, configured)
	}
	accountLimit, resourceLimit := scheduler.EffectiveLimits(c, settings, view)
	effective, active := accountLimit, view.AccountReservations[r.accountScope]
	if resource.ID != "" {
		effective, active = min(effective, resourceLimit), view.ResourceReservations[resource.ID]
	}
	result.EffectiveLimit, result.ActiveAttempts = &effective, &active
	switch {
	case settings.Paused:
		wait(scheduler.Paused)
	case c.Policy.Disabled:
		wait(scheduler.AccountDisabled)
	case view.LiveWorkers >= settings.MaxWorkers:
		wait(scheduler.WorkerLimit)
	case view.AccountReservations[r.accountScope] >= accountLimit:
		wait(scheduler.AccountLimit)
	case resource.ID != "" && active >= resourceLimit:
		wait(scheduler.ResourceLimit)
	}
	return result, nil
}
