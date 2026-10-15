package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hosthealth"
)

func replayAdmission(ctx context.Context, tx *sql.Tx, w domain.WorkspaceID, keyHash string, req admission.Request) (*admission.Receipt, error) {
	key := receiptKey{w, "admission", createJobOperation, keyHash, string(req.Hash()), admission.CanonicalVersion, admission.ErrConflict}
	prior, found, err := replayOrCreate(ctx, tx, key, nil)
	if err != nil || !found {
		return nil, err
	}
	var receipt admission.Receipt
	if json.Unmarshal([]byte(prior.raw), &receipt) != nil || receipt.JobID != prior.job || receipt.AttemptID != prior.attempt || !receipt.JobID.Valid() || !receipt.AttemptID.Valid() || receipt.Replay || receipt.Status != "queued" || receipt.CreatedAt.IsZero() || receipt.Links != admission.JobLinks(w, receipt.JobID) {
		return nil, ErrCorrupt
	}
	receipt.Replay = true
	return &receipt, nil
}

type HostHealthAdmission struct {
	Replay  *admission.Receipt
	Profile admission.Profile
	Target  *hosthealth.Target
}

// PreviewHostHealthAdmission checks authority, exact replay and every selected
// local binding before exposing a target to a bounded remote demand sampler.
func (s *Store) PreviewHostHealthAdmission(ctx context.Context, w domain.WorkspaceID, token, keyHash string, req admission.Request) (HostHealthAdmission, error) {
	var result HostHealthAdmission
	if !req.Valid() || !domain.SHA256Digest(keyHash).Valid() {
		return result, admission.ErrSpec
	}
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, now time.Time) error {
		if _, err := actorInTx(ctx, tx, w, token, auth.Write); err != nil {
			return err
		}
		var err error
		result.Replay, err = replayAdmission(ctx, tx, w, keyHash, req)
		if err != nil || result.Replay != nil {
			return err
		}
		result.Profile, _, err = resolveJob(ctx, tx, w, req)
		if err != nil {
			return err
		}
		if req.Spec().Expired(now) {
			return admission.ErrExpired
		}
		result.Target, err = frozenHostHealthTarget(ctx, tx, w, result.Profile, result.Profile.ResourceScope)
		return err
	})
	return result, err
}

// ReplayHostHealthAdmission is the read-failure race check: another exact caller
// may have committed the original receipt while this caller observed bad health.
func (s *Store) ReplayHostHealthAdmission(ctx context.Context, w domain.WorkspaceID, token, keyHash string, req admission.Request) (*admission.Receipt, error) {
	var receipt *admission.Receipt
	err := s.write(ctx, func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
		if _, err := actorInTx(ctx, tx, w, token, auth.Write); err != nil {
			return err
		}
		var err error
		receipt, err = replayAdmission(ctx, tx, w, keyHash, req)
		return err
	})
	return receipt, err
}
