package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

type workKind string

const (
	dispatchWork       workKind = "dispatch"
	collectionWorkKind workKind = "collection"
	cleanupWork        workKind = "cleanup"
)

// Context is immutable while a generation is held. Until is renewable metadata;
// ownership always uses the persisted expiry and the exact generation and fence.
type workLease struct {
	kind              workKind
	key, owner, fence string
	generation        int64
	until             time.Time
	operationID       sql.NullString
	attemptRevision   sql.NullInt64
}

func workKey(parts ...any) string {
	raw, _ := json.Marshal(parts) // Only validated opaque ASCII IDs and positive sequence numbers.
	return string(raw)
}

func (l workLease) lost() error {
	switch l.kind {
	case collectionWorkKind:
		return collection.ErrLeaseLost
	case cleanupWork:
		return cleanup.ErrLeaseLost
	default:
		return scheduler.ErrLeaseLost
	}
}

func loadWorkLease(ctx context.Context, tx *sql.Tx, kind workKind, key string) (workLease, bool, error) {
	l := workLease{kind: kind, key: key}
	var until int64
	var held bool
	err := tx.QueryRowContext(ctx, `SELECT generation,owner,fence,until_ms,held,operation_id,attempt_revision
 FROM work_leases WHERE kind=? AND key=?`, kind, key).Scan(&l.generation, &l.owner, &l.fence, &until, &held, &l.operationID, &l.attemptRevision)
	if errors.Is(err, sql.ErrNoRows) {
		return l, false, sql.ErrNoRows
	}
	if err != nil {
		return l, false, dbError(err)
	}
	l.until = time.UnixMilli(until).UTC()
	return l, held, nil
}

func claimWorkLease(ctx context.Context, tx *sql.Tx, want workLease, now time.Time, ttl time.Duration) (workLease, error) {
	old, held, err := loadWorkLease(ctx, tx, want.kind, want.key)
	exists := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return workLease{}, err
	}
	if exists && held && old.until.After(now) {
		return workLease{}, want.lost()
	}
	if old.generation == math.MaxInt64 {
		return workLease{}, ErrConflict
	}
	want.generation = old.generation + 1
	want.fence, err = randomID("", 32)
	if err != nil {
		return workLease{}, err
	}
	want.until = time.UnixMilli(now.Add(ttl).UnixMilli()).UTC()
	if !scheduler.ValidTime(want.until) || !want.until.After(now) {
		return workLease{}, ErrInvalid
	}
	var res sql.Result
	if !exists {
		res, err = tx.ExecContext(ctx, `INSERT INTO work_leases(kind,key,generation,owner,fence,until_ms,held,operation_id,attempt_revision)
 VALUES(?,?,?,?,?,?,1,?,?)`, want.kind, want.key, want.generation, want.owner, want.fence, want.until.UnixMilli(), want.operationID, want.attemptRevision)
	} else {
		res, err = tx.ExecContext(ctx, `UPDATE work_leases SET generation=?,owner=?,fence=?,until_ms=?,held=1,operation_id=?,attempt_revision=?
 WHERE kind=? AND key=? AND generation=? AND (held=0 OR until_ms<=?)`, want.generation, want.owner, want.fence, want.until.UnixMilli(), want.operationID, want.attemptRevision, want.kind, want.key, old.generation, now.UnixMilli())
	}
	if err = leaseUpdate(res, err, want); err != nil {
		return workLease{}, err
	}
	return want, nil
}

func checkWorkLease(ctx context.Context, tx *sql.Tx, want workLease, now time.Time) (workLease, error) {
	current, held, err := loadWorkLease(ctx, tx, want.kind, want.key)
	if errors.Is(err, sql.ErrNoRows) {
		return workLease{}, want.lost()
	}
	if err != nil {
		return workLease{}, err
	}
	if !held || !current.until.After(now) || current.generation != want.generation || current.fence != want.fence || current.owner != want.owner ||
		current.operationID != want.operationID || current.attemptRevision != want.attemptRevision {
		return workLease{}, want.lost()
	}
	return current, nil
}

func renewWorkLease(ctx context.Context, tx *sql.Tx, want workLease, now time.Time, ttl time.Duration) (time.Time, error) {
	current, err := checkWorkLease(ctx, tx, want, now)
	if err != nil {
		return time.Time{}, err
	}
	until := time.UnixMilli(now.Add(ttl).UnixMilli()).UTC()
	if !scheduler.ValidTime(until) {
		return time.Time{}, ErrInvalid
	}
	if until.Before(current.until) {
		until = current.until
	}
	res, err := tx.ExecContext(ctx, `UPDATE work_leases SET until_ms=? WHERE kind=? AND key=? AND generation=? AND fence=? AND held=1 AND until_ms>?`, until.UnixMilli(), want.kind, want.key, want.generation, want.fence, now.UnixMilli())
	if err = leaseUpdate(res, err, want); err != nil {
		return time.Time{}, err
	}
	return until, nil
}

func releaseWorkLease(ctx context.Context, tx *sql.Tx, want workLease, now time.Time) error {
	if _, err := checkWorkLease(ctx, tx, want, now); err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `UPDATE work_leases SET held=0 WHERE kind=? AND key=? AND generation=? AND fence=? AND held=1 AND until_ms>?`, want.kind, want.key, want.generation, want.fence, now.UnixMilli())
	return leaseUpdate(res, err, want)
}

func leaseUpdate(res sql.Result, err error, l workLease) error {
	if err != nil {
		return dbError(err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return dbError(err)
	}
	if n != 1 {
		return l.lost()
	}
	return nil
}

func dispatchLease(c scheduler.Claim) workLease {
	return workLease{kind: dispatchWork, key: workKey(c.Sequence), owner: c.Owner, generation: c.Generation, fence: c.Fence, until: c.ExpiresAt}
}
func collectionLease(l collection.Lease) workLease {
	return workLease{kind: collectionWorkKind, key: workKey(l.WorkspaceID, l.JobID, l.AttemptID), generation: l.Generation, fence: l.Fence, until: l.Until,
		operationID: sql.NullString{String: string(l.OperationID), Valid: true}, attemptRevision: sql.NullInt64{Int64: int64(l.AttemptRevision), Valid: true}}
}
func cleanupLease(l cleanup.Lease) workLease {
	return workLease{kind: cleanupWork, key: workKey(l.WorkspaceID, l.OperationID), generation: l.Generation, fence: l.Fence, until: l.Until}
}
