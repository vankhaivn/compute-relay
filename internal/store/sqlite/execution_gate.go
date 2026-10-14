package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

func gateTable(kind string) string {
	if kind == "prepare" {
		return "preparation_authorizations"
	}
	if kind == "start" {
		return "start_authorizations"
	}
	panic("invalid internal authorization kind")
}

func gateReceipt(raw, kind string, consumed sql.NullString) (executionauth.GateReceipt, error) {
	var r executionauth.GateReceipt
	if len(raw) > executionauth.MaxRequestBytes || json.Unmarshal([]byte(raw), &r) != nil || r.Validate() != nil || r.Kind != kind || r.Status != executionauth.Granted || r.Replay {
		return r, ErrCorrupt
	}
	if consumed.Valid {
		at, err := time.Parse(time.RFC3339Nano, consumed.String)
		if err != nil {
			return r, ErrCorrupt
		}
		r.Status, r.ConsumedAt = executionauth.Consumed, &at
	}
	if r.Validate() != nil {
		return r, ErrCorrupt
	}
	return r, nil
}

// Replay precedes temporal/state validation. It never recreates or rearms consent.
func replayGate(ctx context.Context, tx *sql.Tx, c executionauth.GateCommand, kind string, request any) (executionauth.GateReceipt, bool, error) {
	var hash, raw string
	err := tx.QueryRowContext(ctx, "SELECT request_sha256,original_receipt FROM "+gateTable(kind)+" WHERE workspace_id=? AND key_sha256=?", string(c.WorkspaceID), c.KeyDigest).Scan(&hash, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return executionauth.GateReceipt{}, false, nil
	}
	if err != nil {
		return executionauth.GateReceipt{}, false, dbError(err)
	}
	if hash != string(executionauth.GateDigest(kind, c.JobID, request)) {
		return executionauth.GateReceipt{}, true, executionauth.ErrConflict
	}
	r, err := gateReceipt(raw, kind, sql.NullString{})
	if err != nil || r.WorkspaceID != c.WorkspaceID || r.JobID != c.JobID {
		return executionauth.GateReceipt{}, true, ErrCorrupt
	}
	r.Replay = true
	return r, true, nil
}

func newGateReceipt(now time.Time, c executionauth.GateCommand, attempt domain.AttemptID, kind string) (executionauth.GateReceipt, error) {
	id, err := randomID("grant_", 16)
	if err != nil {
		return executionauth.GateReceipt{}, err
	}
	return executionauth.GateReceipt{AuthorizationID: domain.OperationID(id), WorkspaceID: c.WorkspaceID, JobID: c.JobID, AttemptID: attempt, Kind: kind, Status: executionauth.Granted, CreatedAt: now.UTC()}, nil
}

func gateRecord(ctx context.Context, tx *sql.Tx, now time.Time, c executionauth.GateCommand, attempt domain.AttemptID) (admission.Record, error) {
	r, err := readJobRecord(ctx, tx, c.WorkspaceID, c.JobID)
	if errors.Is(err, admission.ErrNotFound) {
		return r, executionauth.ErrNotFound
	}
	if err != nil {
		return r, err
	}
	if r.Attempt.ID != attempt {
		return r, executionauth.ErrConflict
	}
	if !r.Request.Spec().SeparateStart() || !scheduler.LocalOnly(r.Attempt.State) {
		return r, executionauth.ErrState
	}
	if now.Before(r.Attempt.UpdatedAt) {
		return r, ErrCorrupt
	}
	if r.Request.Spec().Expired(now) {
		return r, admission.ErrExpired
	}
	if err = managedProfileOwned(ctx, tx, c.WorkspaceID, r.Profile); err != nil {
		return r, err
	}
	if err = r.Profile.Check(r.Request.Spec()); err != nil {
		return r, err
	}
	var cancelled bool
	err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM operations WHERE workspace_id=? AND job_id=? AND attempt_id=? AND kind='cancel' AND status IN ('accepted','running'))`, string(c.WorkspaceID), string(c.JobID), string(attempt)).Scan(&cancelled)
	if err != nil {
		return r, dbError(err)
	}
	if cancelled {
		return r, executionauth.ErrState
	}
	return r, nil
}

func (s *Store) PrepareExecution(ctx context.Context, c executionauth.GateCommand, req executionauth.PrepareRequest) (executionauth.GateReceipt, error) {
	if c.Validate() != nil || req.Validate() != nil {
		return executionauth.GateReceipt{}, executionauth.ErrRequest
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return executionauth.GateReceipt{}, err
	}
	defer done()
	var receipt executionauth.GateReceipt
	err = s.clockedTx(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if _, err := actorInTx(ctx, tx, c.WorkspaceID, c.TokenID, auth.Execute); err != nil {
			return err
		}
		prior, found, err := replayGate(ctx, tx, c, "prepare", req)
		if err != nil || found {
			receipt = prior
			return err
		}
		r, err := gateRecord(ctx, tx, now, c, req.AttemptID)
		if err != nil {
			return err
		}
		until, _ := time.Parse(time.RFC3339, req.ReadyUntil)
		if !now.Before(until) {
			return admission.ErrExpired
		}
		if until.After(now.Add(executionauth.MaxReadyHold)) || !r.Request.Spec().Deadline().IsZero() && until.After(r.Request.Spec().Deadline()) {
			return executionauth.ErrRequest
		}
		work, err := controlWork(ctx, tx, r)
		if err != nil {
			return err
		}
		if work.Journal.Phase != dispatch.Local || work.Journal.Version != 0 {
			return executionauth.ErrState
		}
		var existing bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM preparation_authorizations WHERE workspace_id=? AND job_id=? AND attempt_id=?)`, string(c.WorkspaceID), string(c.JobID), string(req.AttemptID)).Scan(&existing); err != nil {
			return dbError(err)
		}
		if existing {
			return executionauth.ErrConflict
		}
		inputs, err := authorizationInputs(r)
		if err != nil {
			return err
		}
		var total int64
		for _, object := range r.Objects {
			total += object.Object.Bytes
		}
		if total < 1 || total > req.MaxStagedBytes {
			return executionauth.ErrRequest
		}
		var accountCount, globalCount int
		var accountBytes, globalBytes int64
		if err = tx.QueryRowContext(ctx, `SELECT count(*),COALESCE(sum(reserved_bytes),0),COALESCE(sum(account_scope=?),0),COALESCE(sum(CASE WHEN account_scope=? THEN reserved_bytes ELSE 0 END),0) FROM preparation_authorizations WHERE storage_released=0`, r.Profile.AccountScope, r.Profile.AccountScope).Scan(&globalCount, &globalBytes, &accountCount, &accountBytes); err != nil {
			return dbError(err)
		}
		if accountCount >= executionauth.MaxHeldPerAccount || globalCount >= executionauth.MaxHeldGlobal || accountBytes+total > executionauth.MaxHeldPerAccount*executionauth.MaxStagedBytes || globalBytes+total > executionauth.MaxHeldGlobal*executionauth.MaxStagedBytes {
			return admission.ErrLimit
		}
		receipt, err = newGateReceipt(now, c, req.AttemptID, "prepare")
		if err != nil {
			return err
		}
		raw, _ := json.Marshal(req)
		encoded, _ := json.Marshal(receipt)
		binding, _ := json.Marshal(r.Profile)
		_, err = tx.ExecContext(ctx, `INSERT INTO preparation_authorizations(workspace_id,authorization_id,job_id,attempt_id,key_sha256,request_sha256,request,original_receipt,binding,account_scope,inputs_sha256,attempt_nonce,reserved_bytes,ready_until_ms) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, string(c.WorkspaceID), string(receipt.AuthorizationID), string(c.JobID), string(req.AttemptID), c.KeyDigest, string(executionauth.GateDigest("prepare", c.JobID, req)), string(raw), string(encoded), string(binding), r.Profile.AccountScope, inputs, r.AttemptNonce, total, until.UnixMilli())
		return dbError(err)
	})
	if err != nil {
		return executionauth.GateReceipt{}, err
	}
	return receipt, nil
}

func loadExecutionGate(ctx context.Context, tx *sql.Tx, record admission.Record, now time.Time) (*executionauth.Gate, error) {
	if !record.Request.Spec().SeparateStart() {
		return nil, nil
	}
	var request, raw, binding, account, inputs, nonce string
	var consumed, revision, observed sql.NullString
	var until, total int64
	err := tx.QueryRowContext(ctx, `SELECT request,original_receipt,binding,account_scope,inputs_sha256,attempt_nonce,reserved_bytes,ready_until_ms,consumed_at,ready_revision,ready_at FROM preparation_authorizations WHERE workspace_id=? AND job_id=? AND attempt_id=?`, string(record.Job.WorkspaceID), string(record.Job.ID), string(record.Attempt.ID)).Scan(&request, &raw, &binding, &account, &inputs, &nonce, &total, &until, &consumed, &revision, &observed)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, dbError(err)
	}
	g := &executionauth.Gate{StagedBytes: total}
	g.Request, err = executionauth.ParsePrepare([]byte(request))
	if err != nil {
		return nil, ErrCorrupt
	}
	g.Preparation, err = gateReceipt(raw, "prepare", consumed)
	if err != nil || g.Preparation.WorkspaceID != record.Job.WorkspaceID || g.Preparation.JobID != record.Job.ID || g.Preparation.AttemptID != record.Attempt.ID || g.Request.AttemptID != record.Attempt.ID {
		return nil, ErrCorrupt
	}
	expectedBinding, _ := json.Marshal(record.Profile)
	expectedInputs, inputErr := authorizationInputs(record)
	deadline, _ := time.Parse(time.RFC3339, g.Request.ReadyUntil)
	var expectedBytes int64
	for _, object := range record.Objects {
		expectedBytes += object.Object.Bytes
	}
	if inputErr != nil || binding != string(expectedBinding) || account != record.Profile.AccountScope || inputs != expectedInputs || nonce != record.AttemptNonce || total != expectedBytes || total > g.Request.MaxStagedBytes || until != deadline.UnixMilli() || !deadline.After(g.Preparation.CreatedAt) || deadline.After(g.Preparation.CreatedAt.Add(executionauth.MaxReadyHold)) || revision.Valid != observed.Valid {
		return nil, ErrCorrupt
	}
	if now.Before(g.Preparation.CreatedAt) || g.Preparation.ConsumedAt != nil && now.Before(*g.Preparation.ConsumedAt) {
		return nil, ErrCorrupt
	}
	if revision.Valid {
		at, err := time.Parse(time.RFC3339Nano, observed.String)
		g.ReadyRevision = domain.SHA256Digest(revision.String)
		if err != nil || !g.ReadyRevision.Valid() || g.Preparation.ConsumedAt == nil || at.Before(*g.Preparation.ConsumedAt) {
			return nil, ErrCorrupt
		}
		if now.Before(at) {
			return nil, ErrCorrupt
		}
		g.ReadyAt = &at
	}
	err = tx.QueryRowContext(ctx, `SELECT request,original_receipt,consumed_at FROM start_authorizations WHERE workspace_id=? AND job_id=? AND attempt_id=?`, string(record.Job.WorkspaceID), string(record.Job.ID), string(record.Attempt.ID)).Scan(&request, &raw, &consumed)
	if errors.Is(err, sql.ErrNoRows) {
		return g, nil
	}
	if err != nil {
		return nil, dbError(err)
	}
	req, err := executionauth.ParseStart([]byte(request))
	if err != nil {
		return nil, ErrCorrupt
	}
	receipt, err := gateReceipt(raw, "start", consumed)
	if err != nil || receipt.WorkspaceID != record.Job.WorkspaceID || receipt.JobID != record.Job.ID || receipt.AttemptID != record.Attempt.ID || req.AttemptID != record.Attempt.ID || req.ReadinessRevision != g.ReadyRevision || g.ReadyAt == nil || receipt.CreatedAt.Before(*g.ReadyAt) || req.MaxRemoteWallSeconds > record.Request.Spec().Timeouts.RemoteWallSeconds {
		return nil, ErrCorrupt
	}
	if now.Before(receipt.CreatedAt) || receipt.ConsumedAt != nil && now.Before(*receipt.ConsumedAt) {
		return nil, ErrCorrupt
	}
	stop, _ := time.Parse(time.RFC3339, req.ExecutionDeadline)
	if !stop.After(receipt.CreatedAt) || stop.After(receipt.CreatedAt.Add(time.Duration(req.MaxRemoteWallSeconds)*time.Second)) || !record.Request.Spec().Deadline().IsZero() && stop.After(record.Request.Spec().Deadline()) || !receipt.CreatedAt.Before(deadline) {
		return nil, ErrCorrupt
	}
	g.StartReceipt = &receipt
	if stop.Before(deadline) {
		deadline = stop
	}
	g.Start = &provider.StartAuthorization{AuthorizationID: receipt.AuthorizationID, ReadinessRevision: req.ReadinessRevision, MaxRemoteWallSeconds: req.MaxRemoteWallSeconds, ExecutionDeadline: req.ExecutionDeadline, StartBefore: deadline.UTC().Format(time.RFC3339)}
	if len(record.Request.Spec().WorkloadSecrets) > 0 {
		key := workloadsecret.Key{WorkspaceID: record.Job.WorkspaceID, JobID: record.Job.ID, AttemptID: record.Attempt.ID}
		secret, err := loadSecret(ctx, tx, key, now)
		if err != nil {
			return nil, err
		}
		expires, _ := workloadsecret.CanonicalTime(secret.original.ExpiresAt)
		if req.SecretAuthorizationID != secret.original.AuthorizationID || secret.original.ReadinessRevision != g.ReadyRevision || !matchSecretSlots(record.Request.Spec().WorkloadSecrets, secret.original.Metadata) || expires.Before(stop) || secret.original.CreatedAt.After(receipt.CreatedAt) {
			return nil, ErrCorrupt
		}
		if receipt.Status == executionauth.Consumed {
			if secret.current.Status != "consumed" || !secret.current.ConsumedAt.Equal(*receipt.ConsumedAt) {
				return nil, ErrCorrupt
			}
		} else if secret.current.Status == "consumed" {
			return nil, ErrCorrupt
		}
		g.Start.Secrets = &provider.SecretAuthorization{AuthorizationID: secret.original.AuthorizationID, ExpiresAt: secret.original.ExpiresAt, Slots: record.Request.Spec().WorkloadSecrets}
	} else if req.SecretAuthorizationID != "" {
		return nil, ErrCorrupt
	}
	return g, nil
}

func (s *Store) StartExecution(ctx context.Context, c executionauth.GateCommand, req executionauth.StartRequest) (executionauth.GateReceipt, error) {
	if c.Validate() != nil || req.Validate() != nil {
		return executionauth.GateReceipt{}, executionauth.ErrRequest
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return executionauth.GateReceipt{}, err
	}
	defer done()
	var receipt executionauth.GateReceipt
	err = s.clockedTx(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if _, err := actorInTx(ctx, tx, c.WorkspaceID, c.TokenID, auth.Execute); err != nil {
			return err
		}
		prior, found, err := replayGate(ctx, tx, c, "start", req)
		if err != nil || found {
			receipt = prior
			return err
		}
		r, err := gateRecord(ctx, tx, now, c, req.AttemptID)
		if err != nil {
			return err
		}
		g, err := loadExecutionGate(ctx, tx, r, now)
		if err != nil {
			return err
		}
		if g == nil || g.Preparation.Status != executionauth.Consumed || g.ReadyAt == nil {
			return executionauth.ErrState
		}
		if g.Start != nil {
			return executionauth.ErrConflict
		}
		if g.Expired(now) {
			return admission.ErrExpired
		}
		if g.ReadyRevision != req.ReadinessRevision {
			return executionauth.ErrConflict
		}
		work, err := controlWork(ctx, tx, r)
		if err != nil {
			return err
		}
		if work.Journal.Phase != dispatch.Ready || work.Journal.SubmitStarted || work.Journal.Prepared == nil || work.Journal.Prepared.ReadinessRevision() != g.ReadyRevision {
			return executionauth.ErrState
		}
		stop, _ := time.Parse(time.RFC3339, req.ExecutionDeadline)
		if !now.Before(stop) {
			return admission.ErrExpired
		}
		if req.MaxRemoteWallSeconds <= r.Request.Spec().Timeouts.SetupSeconds+r.Request.Spec().Timeouts.FinalizationGraceSeconds || req.MaxRemoteWallSeconds > r.Request.Spec().Timeouts.RemoteWallSeconds || stop.After(now.Add(time.Duration(req.MaxRemoteWallSeconds)*time.Second)) || !r.Request.Spec().Deadline().IsZero() && stop.After(r.Request.Spec().Deadline()) {
			return executionauth.ErrRequest
		}
		secret, err := secretForStart(ctx, tx, r, req.SecretAuthorizationID, req.ReadinessRevision, req.ExecutionDeadline, now)
		if err != nil {
			return err
		}
		if secret != nil && !c.SecretAvailable {
			return workloadsecret.ErrUnavailable
		}
		if err = authorizationQuota(ctx, tx, r, now); err != nil {
			return err
		}
		receipt, err = newGateReceipt(now, c, req.AttemptID, "start")
		if err != nil {
			return err
		}
		raw, _ := json.Marshal(req)
		encoded, _ := json.Marshal(receipt)
		_, err = tx.ExecContext(ctx, `INSERT INTO start_authorizations(workspace_id,authorization_id,job_id,attempt_id,key_sha256,request_sha256,request,original_receipt) VALUES(?,?,?,?,?,?,?,?)`, string(c.WorkspaceID), string(receipt.AuthorizationID), string(c.JobID), string(req.AttemptID), c.KeyDigest, string(executionauth.GateDigest("start", c.JobID, req)), string(raw), string(encoded))
		return dbError(err)
	})
	if err != nil {
		return executionauth.GateReceipt{}, err
	}
	return receipt, nil
}

func (s *Store) ReadGateAuthorization(ctx context.Context, w domain.WorkspaceID, token string, job domain.JobID, id domain.OperationID) (executionauth.GateReceipt, error) {
	if !w.Valid() || !job.Valid() || !id.Valid() {
		return executionauth.GateReceipt{}, executionauth.ErrNotFound
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return executionauth.GateReceipt{}, err
	}
	defer done()
	var receipt executionauth.GateReceipt
	err = s.clockedTx(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if _, err := actorInTx(ctx, tx, w, token, auth.Execute); err != nil {
			return err
		}
		for _, kind := range []string{"prepare", "start"} {
			var raw string
			var used sql.NullString
			err := tx.QueryRowContext(ctx, "SELECT original_receipt,consumed_at FROM "+gateTable(kind)+" WHERE workspace_id=? AND job_id=? AND authorization_id=?", string(w), string(job), string(id)).Scan(&raw, &used)
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			if err != nil {
				return dbError(err)
			}
			receipt, err = gateReceipt(raw, kind, used)
			if err != nil || receipt.WorkspaceID != w || receipt.JobID != job || receipt.AuthorizationID != id {
				return ErrCorrupt
			}
			return nil
		}
		return executionauth.ErrNotFound
	})
	if err != nil {
		return executionauth.GateReceipt{}, err
	}
	return receipt, nil
}

func (s *Store) ReadReadiness(ctx context.Context, w domain.WorkspaceID, token string, job domain.JobID) (executionauth.Readiness, error) {
	if !w.Valid() || !job.Valid() {
		return executionauth.Readiness{}, executionauth.ErrNotFound
	}
	ctx, done, err := s.operation(ctx, s.options.OperationTimeout)
	if err != nil {
		return executionauth.Readiness{}, err
	}
	defer done()
	var result executionauth.Readiness
	err = s.clockedTx(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if _, err := actorInTx(ctx, tx, w, token, auth.Read); err != nil {
			return err
		}
		r, err := readJobRecord(ctx, tx, w, job)
		if errors.Is(err, admission.ErrNotFound) {
			return executionauth.ErrNotFound
		}
		if err != nil {
			return err
		}
		if !r.Request.Spec().SeparateStart() {
			return executionauth.ErrState
		}
		work, err := controlWork(ctx, tx, r)
		if err != nil {
			return err
		}
		g, err := loadExecutionGate(ctx, tx, r, now)
		if err != nil {
			return err
		}
		result = executionauth.Readiness{AttemptID: r.Attempt.ID, State: "preparation_required", RequiredActions: []string{"prepare"}}
		if g != nil {
			result = executionauth.Readiness{AttemptID: r.Attempt.ID, State: "queued", ReadyUntil: g.Request.ReadyUntil, StagedBytes: 0, Revision: g.ReadyRevision, ObservedAt: g.ReadyAt, RequiredActions: []string{}}
			if g.ReadyAt != nil {
				result.StagedBytes = g.StagedBytes
			}
			switch work.Journal.Phase {
			case dispatch.Staging:
				result.State = "preparing"
			case dispatch.Ready:
				result.State, result.RequiredActions = "ready", []string{"start"}
				if g.Start != nil {
					result.State, result.RequiredActions = "start_authorized", []string{}
				}
			case dispatch.Attention, dispatch.Failed:
				result.State, result.RequiredActions = "uncertain", []string{"reconcile"}
				if work.Journal.Problem != nil && work.Journal.Problem.Code == domain.CodeRemoteIdentityMismatch {
					result.State, result.RequiredActions = "invalidated", []string{"cancel", "cleanup"}
				}
			}
		}
		switch {
		case work.Journal.SubmitStarted:
			result.State, result.RequiredActions = "starting", []string{}
			if work.Journal.Remote != nil {
				result.State = "started"
			}
			if work.Journal.Phase == dispatch.Rejected {
				result.State = "prevented"
			}
			if work.Journal.Phase == dispatch.Attention {
				result.State, result.RequiredActions = "uncertain", []string{"reconcile"}
			}
		case r.Attempt.State.Cancellation != domain.CancellationNotRequested:
			result.State, result.RequiredActions = "prevented", []string{"cleanup"}
		case r.Request.Spec().Expired(now) || g != nil && g.Expired(now):
			result.State, result.RequiredActions = "expired", []string{"cleanup"}
		}
		if slots := r.Request.Spec().WorkloadSecrets; len(slots) > 0 {
			result.Secrets = &workloadsecret.Readiness{RequiredSlots: slots, Status: "missing"}
			secret, err := loadSecret(ctx, tx, workloadsecret.Key{WorkspaceID: w, JobID: job, AttemptID: r.Attempt.ID}, now)
			if err != nil && !errors.Is(err, workloadsecret.ErrNotFound) {
				return err
			}
			if err == nil {
				result.Secrets.AuthorizationID, result.Secrets.ExpiresAt, result.Secrets.Status = secret.current.AuthorizationID, secret.current.ExpiresAt, secret.current.Status
			}
			if result.State == "ready" || result.State == "start_authorized" {
				switch result.Secrets.Status {
				case "missing":
					result.RequiredActions = append(result.RequiredActions, "supply_secrets")
				case "expired", "revoked":
					result.RequiredActions = []string{"cancel", "cleanup"}
				}
			}
		}
		return nil
	})
	if err != nil {
		return executionauth.Readiness{}, err
	}
	return result, nil
}
