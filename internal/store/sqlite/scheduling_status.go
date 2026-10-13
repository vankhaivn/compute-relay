package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func readSchedulingStatus(ctx context.Context, tx *sql.Tx, record admission.Record, now time.Time) (*admission.SchedulingStatus, error) {
	if !executionauth.Managed(record.Profile) || !scheduler.LocalOnly(record.Attempt.State) ||
		record.Attempt.State.Orchestration != domain.OrchestrationQueued && record.Attempt.State.Orchestration != domain.OrchestrationBlocked {
		return nil, nil
	}
	settings, last, err := schedulerControl(ctx, tx, now)
	if errors.Is(err, scheduler.ErrClock) {
		return &admission.SchedulingStatus{Reason: "scheduler_clock_unavailable"}, nil
	}
	if err != nil {
		return nil, err
	}
	rows, err := schedulerSnapshot(ctx, tx, settings, now)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		rows[i].DispatchModeExcluded = !rows[i].Managed
	}
	_, view, err := scheduler.Select(rows, domain.WorkspaceID(last), settings, now)
	if err != nil {
		return nil, err
	}
	for _, head := range view.Heads {
		if head.WorkspaceID == record.Job.WorkspaceID && head.JobID == record.Job.ID && head.AttemptID == record.Attempt.ID {
			return &admission.SchedulingStatus{Reason: string(head.Reason)}, nil
		}
	}
	return &admission.SchedulingStatus{Reason: "workspace_fifo"}, nil
}
