package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hosthealth"
	"github.com/vankhaivn/compute-relay/internal/ports"
)

var _ hosthealth.Repository = (*Store)(nil)

func hostHealthTarget(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, id string) (hosthealth.Target, error) {
	r, err := loadManaged(ctx, tx, w, id)
	if err != nil {
		return hosthealth.Target{}, err
	}
	if r.view.ProviderType != "ssh" {
		return hosthealth.Target{}, hosthealth.ErrUnsupported
	}
	if r.view.Authentication != "verified" || r.view.NewWork == "removed" || r.credential == "" || r.account == "" {
		return hosthealth.Target{}, hosthealth.ErrUnverified
	}
	resource, err := managedResource(ctx, tx, r.accountScope)
	if err != nil {
		return hosthealth.Target{}, err
	}
	if resource.Kind != "machine" || resource.Validate() != nil {
		return hosthealth.Target{}, hosthealth.ErrUnverified
	}
	target := hosthealth.Target{Workspace: w, ConnectionID: id, ProviderType: r.view.ProviderType,
		CanonicalAccount: r.account, CredentialRef: ports.CredentialRef("vault:" + id), ResourceID: resource.ID}
	// A verified machine can be observed before its execution image is installed.
	// Existing frozen policy, when present, supplies an additional engine pin.
	if r.profile != "" {
		err = tx.QueryRowContext(ctx, `SELECT m.config FROM managed_profile_runtime m JOIN profiles p
 ON p.profile=m.profile AND p.revision=m.revision WHERE p.profile=?`, r.profile).Scan(&target.RuntimeConfig)
		if errors.Is(err, sql.ErrNoRows) {
			return hosthealth.Target{}, ErrCorrupt
		}
		if err != nil {
			return hosthealth.Target{}, dbError(err)
		}
	} else if r.view.NewWork == "disabled" {
		// Disabling admission removes the current alias, but retained work still
		// requires observation through its original immutable runtime binding.
		err = tx.QueryRowContext(ctx, `SELECT m.config FROM jobs j JOIN attempts a
 ON a.workspace_id=j.workspace_id AND a.job_id=j.job_id AND a.attempt_id=j.active_attempt_id
 JOIN profile_revisions p ON p.profile=j.profile AND p.revision=j.profile_revision
 JOIN managed_profile_runtime m ON m.profile=p.profile AND m.revision=p.revision
 WHERE json_extract(p.snapshot,'$.credential_ref')=? AND a.orchestration NOT IN ('succeeded','failed','cancelled','timed_out')
 ORDER BY j.job_id LIMIT 1`, target.CredentialRef).Scan(&target.RuntimeConfig)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return hosthealth.Target{}, dbError(err)
		}
	}
	if r.accountScope != connections.AccountScope(r.view.ProviderType, r.account) || !target.Valid() {
		return hosthealth.Target{}, ErrCorrupt
	}
	return target, nil
}

func (s *Store) ResolveHostHealthTarget(ctx context.Context, w domain.WorkspaceID, token, id string) (hosthealth.Target, error) {
	var target hosthealth.Target
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
		if err := managedReadActor(ctx, tx, w, token); err != nil {
			return err
		}
		var err error
		target, err = hostHealthTarget(ctx, tx, w, id)
		return err
	})
	return target, err
}

func (s *Store) ActiveHostHealthTargets(ctx context.Context) ([]hosthealth.Target, error) {
	targets := []hosthealth.Target{}
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
		rows, err := tx.QueryContext(ctx, `SELECT c.workspace_id,c.connection_id,r.resource_scope
 FROM managed_connections c JOIN managed_resource_accounts r ON r.account_scope=c.account_scope
 WHERE c.provider_type='ssh' AND c.authentication='verified' AND c.active_credential_key!=''
 AND c.new_work!='removed' AND r.resource_kind='machine' AND r.resource_scope!=''
 AND EXISTS(SELECT 1 FROM jobs j JOIN attempts a
 ON a.workspace_id=j.workspace_id AND a.job_id=j.job_id AND a.attempt_id=j.active_attempt_id
 JOIN profile_revisions p ON p.profile=j.profile AND p.revision=j.profile_revision
 JOIN managed_resource_accounts work_resource ON work_resource.account_scope=json_extract(p.snapshot,'$.account_scope')
 WHERE (a.orchestration IN ('queued','preparing','dispatching','submitted','running')
 OR json_extract(a.state,'$.RemoteActivity') IN ('unknown','possible','active'))
 AND work_resource.resource_scope=r.resource_scope)
 ORDER BY r.resource_scope,c.connection_id`)
		if err != nil {
			return dbError(err)
		}
		type ref struct {
			workspace    domain.WorkspaceID
			id, resource string
		}
		refs := []ref{}
		last := ""
		for rows.Next() {
			var item ref
			if err := rows.Scan(&item.workspace, &item.id, &item.resource); err != nil {
				rows.Close()
				return dbError(err)
			}
			if item.resource != last {
				refs, last = append(refs, item), item.resource
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return dbError(err)
		}
		for _, ref := range refs {
			target, err := hostHealthTarget(ctx, tx, ref.workspace, ref.id)
			if err != nil {
				return err
			}
			targets = append(targets, target)
		}
		return nil
	})
	return targets, err
}

func pruneHostHealth(ctx context.Context, tx *sql.Tx, resource string, now time.Time) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM host_health_history WHERE observed_ms<?`, now.Add(-hosthealth.HistoryMaxAge).UnixMilli()); err != nil {
		return dbError(err)
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM host_health_history WHERE resource_id=? AND sequence NOT IN
 (SELECT sequence FROM host_health_history WHERE resource_id=? ORDER BY observed_ms DESC,sequence DESC LIMIT ?)`, resource, resource, hosthealth.HistoryMaxRows)
	return dbError(err)
}

func (s *Store) RecordHostHealth(ctx context.Context, snapshot hosthealth.Snapshot) error {
	if snapshot.Validate() != nil {
		return hosthealth.ErrSnapshot
	}
	raw, err := json.Marshal(snapshot)
	if err != nil || len(raw) > hosthealth.MaxSnapshotBytes {
		return hosthealth.ErrSnapshot
	}
	return s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if snapshot.ObservedAt.After(now) {
			return hosthealth.ErrSnapshot
		}
		var verified bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM managed_resource_accounts WHERE resource_kind='machine' AND resource_scope=?)`, snapshot.ResourceID).Scan(&verified); err != nil {
			return dbError(err)
		}
		if !verified {
			return hosthealth.ErrUnverified
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO host_health_history(resource_id,observed_ms,snapshot) VALUES(?,?,?)`, snapshot.ResourceID, snapshot.ObservedAt.UnixMilli(), raw); err != nil {
			return dbError(err)
		}
		return pruneHostHealth(ctx, tx, snapshot.ResourceID, now)
	})
}

func (s *Store) ReadHostHealth(ctx context.Context, resourceID string) ([]hosthealth.Snapshot, error) {
	if (connections.ResourceIdentity{Kind: "machine", ID: resourceID}).Validate() != nil {
		return nil, hosthealth.ErrUnverified
	}
	history := []hosthealth.Snapshot{}
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if err := pruneHostHealth(ctx, tx, resourceID, now); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `SELECT observed_ms,snapshot FROM host_health_history
 WHERE resource_id=? ORDER BY observed_ms DESC,sequence DESC LIMIT ?`, resourceID, hosthealth.HistoryMaxRows)
		if err != nil {
			return dbError(err)
		}
		defer rows.Close()
		for rows.Next() {
			var millis int64
			var raw []byte
			if err := rows.Scan(&millis, &raw); err != nil {
				return dbError(err)
			}
			var snapshot hosthealth.Snapshot
			if len(raw) > hosthealth.MaxSnapshotBytes || json.Unmarshal(raw, &snapshot) != nil || snapshot.Validate() != nil ||
				snapshot.ResourceID != resourceID || snapshot.ObservedAt.UnixMilli() != millis {
				return ErrCorrupt
			}
			canonical, err := json.Marshal(snapshot)
			if err != nil || !bytes.Equal(raw, canonical) {
				return ErrCorrupt
			}
			history = append(history, snapshot)
		}
		return dbError(rows.Err())
	})
	return history, err
}
