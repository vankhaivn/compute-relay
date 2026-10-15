package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hosthealth"
	"github.com/vankhaivn/compute-relay/internal/ports"
)

type hostEvidenceKey struct{}
type hostEvidence struct {
	profile  admission.Profile
	evidence hosthealth.Evidence
}

// WithHostHealthEvidence binds process-confirmed health to one exact immutable
// profile. The final SQL gate validates the same latest row and policy locally.
func WithHostHealthEvidence(ctx context.Context, profile admission.Profile, evidence hosthealth.Evidence) context.Context {
	return context.WithValue(ctx, hostEvidenceKey{}, hostEvidence{profile, evidence})
}

func hostHealthRows(ctx context.Context, tx *sql.Tx, resource string, now time.Time) ([]hosthealth.Snapshot, error) {
	rows, err := tx.QueryContext(ctx, `SELECT observed_ms,snapshot FROM host_health_history
 WHERE resource_id=? AND observed_ms>=? ORDER BY observed_ms DESC,sequence DESC LIMIT ?`, resource, now.Add(-hosthealth.HistoryMaxAge).UnixMilli(), hosthealth.HistoryMaxRows)
	if err != nil {
		return nil, dbError(err)
	}
	defer rows.Close()
	history := []hosthealth.Snapshot{}
	for rows.Next() {
		var millis int64
		var raw []byte
		var snapshot hosthealth.Snapshot
		if err := rows.Scan(&millis, &raw); err != nil {
			return nil, dbError(err)
		}
		if len(raw) > hosthealth.MaxSnapshotBytes || json.Unmarshal(raw, &snapshot) != nil || snapshot.Validate() != nil || snapshot.ResourceID != resource || snapshot.ObservedAt.UnixMilli() != millis {
			return nil, ErrCorrupt
		}
		canonical, err := json.Marshal(snapshot)
		if err != nil || !bytes.Equal(raw, canonical) {
			return nil, ErrCorrupt
		}
		history = append(history, snapshot)
	}
	return history, dbError(rows.Err())
}

// The resource must be frozen in the profile or in the original scheduling
// reservation. A newly checked connection cannot locate historical execution.
func frozenHostHealthTarget(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, profile admission.Profile, reserved string) (*hosthealth.Target, error) {
	id, ok := credentials.VaultReference(ports.CredentialRef(profile.CredentialRef))
	if !ok || profile.Validate() != nil || profile.Binding.ProviderInstanceID != domain.ProviderInstanceID(id) {
		return nil, ErrCorrupt
	}
	r, err := loadManaged(ctx, tx, w, id)
	if err != nil {
		return nil, err
	}
	if r.view.ProviderType != "ssh" {
		return nil, nil
	}
	if r.account == "" || r.credential == "" || r.view.NewWork == "removed" {
		return nil, hosthealth.ErrUnverified
	}
	if r.accountScope != profile.AccountScope || profile.AccountScope != connections.AccountScope("ssh", r.account) {
		return nil, ErrCorrupt
	}
	resource := profile.ResourceScope
	if resource == "" {
		resource = reserved
	}
	verified, err := managedResource(ctx, tx, r.accountScope)
	if err != nil {
		return nil, err
	}
	if resource == "" || verified.Kind != "machine" || verified.Validate() != nil || verified.ID != resource {
		return nil, hosthealth.ErrUnverified
	}
	var raw []byte
	err = tx.QueryRowContext(ctx, `SELECT config FROM managed_profile_runtime WHERE profile=? AND revision=?`, profile.Binding.Profile, profile.Binding.ConfigurationRevision).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrCorrupt
	}
	if err != nil {
		return nil, dbError(err)
	}
	policy, err := hosthealth.PolicyFromRuntime(raw)
	if err != nil {
		return nil, ErrCorrupt
	}
	target := &hosthealth.Target{Workspace: w, ConnectionID: id, ProviderType: "ssh", CanonicalAccount: r.account,
		CredentialRef: ports.CredentialRef(profile.CredentialRef), ResourceID: resource, RuntimeConfig: raw, Policy: &policy}
	if !target.Valid() {
		return nil, ErrCorrupt
	}
	return target, nil
}

func reservedHostResource(ctx context.Context, tx *sql.Tx, record admission.Record) (string, error) {
	if record.Profile.ResourceScope != "" {
		return record.Profile.ResourceScope, nil
	}
	var resource string
	err := tx.QueryRowContext(ctx, `SELECT r.resource_scope FROM scheduler_resource_reservations r JOIN scheduler_queue q ON q.queue_seq=r.queue_seq
 WHERE q.workspace_id=? AND q.job_id=? AND q.attempt_id=? AND r.resource_kind='machine'`, record.Job.WorkspaceID, record.Job.ID, record.Attempt.ID).Scan(&resource)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return resource, dbError(err)
}

func (s *Store) HostHealthTargetForAttempt(ctx context.Context, record admission.Record) (*hosthealth.Target, error) {
	var target *hosthealth.Target
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
		current, err := controlTarget(ctx, tx, record.Job.WorkspaceID, record.Job.ID, record.Attempt.ID)
		if err != nil {
			return err
		}
		if current.Profile != record.Profile {
			return ErrCorrupt
		}
		resource, err := reservedHostResource(ctx, tx, current)
		if err != nil {
			return err
		}
		target, err = frozenHostHealthTarget(ctx, tx, record.Job.WorkspaceID, current.Profile, resource)
		return err
	})
	return target, err
}

func checkHostNewWork(ctx context.Context, tx *sql.Tx, record admission.Record, now time.Time) error {
	proof, enabled := ctx.Value(hostEvidenceKey{}).(hostEvidence)
	if !enabled {
		return nil
	}
	if proof.profile != record.Profile {
		return admission.ErrHostUnhealthy
	}
	resource, err := reservedHostResource(ctx, tx, record)
	if err != nil {
		return err
	}
	target, err := frozenHostHealthTarget(ctx, tx, record.Job.WorkspaceID, record.Profile, resource)
	if errors.Is(err, hosthealth.ErrUnverified) {
		return admission.ErrHostUnhealthy
	}
	if err != nil {
		return err
	}
	if target == nil {
		return admission.ErrHostUnhealthy
	}
	history, err := hostHealthRows(ctx, tx, target.ResourceID, now)
	if err != nil {
		return err
	}
	decision := hosthealth.Evaluate(proof.evidence.View(target.ResourceID, history, now), *target.Policy, now)
	if decision.RefuseNew {
		return admission.ErrHostUnhealthy
	}
	return nil
}
