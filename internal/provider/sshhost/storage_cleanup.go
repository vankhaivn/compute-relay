package sshhost

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/vankhaivn/compute-relay/internal/hoststorage"
)

func exactStorageLabels(have, expected map[string]string) bool {
	if len(have) != len(expected) {
		return false
	}
	for key, value := range expected {
		if have[key] != value {
			return false
		}
	}
	return true
}

func storageReferenced(ctx context.Context, d docker, budget *storageScanBudget, target hoststorage.Target) (bool, error) {
	var filter string
	switch target.Class {
	case hoststorage.Volume:
		filter = "volume=" + target.ID
	case hoststorage.Image:
		filter = "ancestor=" + target.ID
	default:
		return false, nil
	}
	raw, err := budget.command(ctx, d, "container", "ls", "-a", "--no-trunc", "--filter", filter, "--format", "reference")
	if err != nil {
		return false, err
	}
	lines, err := storageLines(raw, hoststorage.ScanMaxObjects)
	return len(lines) != 0, err
}

func observeStorageTarget(ctx context.Context, d docker, target hoststorage.Target) (hoststorage.TargetObservation, error) {
	var observation hoststorage.TargetObservation
	if !target.Valid() {
		return observation, hoststorage.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, hoststorage.ScanTimeout)
	defer cancel()
	budget := &storageScanBudget{remaining: hoststorage.ScanMaxBytes}
	nativeEngine, err := d.engineID(ctx)
	if err != nil {
		return observation, err
	}
	class := string(target.Class)
	ids, err := storageIDs(ctx, d, budget, class, storageLabel+"="+string(target.Scope.InstallationID))
	if err != nil {
		return observation, err
	}
	observation.ObservedAt = time.Now().UTC()
	if !slices.Contains(ids, target.ID) {
		observation.Eligible, observation.Reason = true, "observed_absent"
		return observation, nil
	}
	object, err := storageInspectObject(ctx, d, budget, target.Scope, nativeEngine, target.Class, target.ID)
	if err != nil {
		return observation, err
	}
	observation.Present, observation.SizeBytes, observation.SizeKnown = true, object.SizeBytes, object.SizeKnown
	if !exactStorageLabels(object.Labels, target.Labels) || !validStorageLabels(target.Class, object.Labels, target.Scope, nativeEngine) {
		observation.Reason = "identity_mismatch"
		return observation, nil
	}
	if target.Class == hoststorage.Container && object.Running {
		observation.Reason = "container_active"
		return observation, nil
	}
	if target.Class == hoststorage.Image && object.References != 0 {
		observation.Reason = "foreign_tag_or_digest"
		return observation, nil
	}
	referenced, err := storageReferenced(ctx, d, budget, target)
	if err != nil {
		return observation, err
	}
	if referenced {
		observation.Reason = "container_reference"
		return observation, nil
	}
	observation.Eligible, observation.Reason = true, "zero_references"
	return observation, nil
}

// ObserveStorageTarget is read-only and checks the exact immutable target on
// the frozen machine/engine. It does not inspect any foreign object.
func (a *Adapter) ObserveStorageTarget(ctx context.Context, target hoststorage.Target) (hoststorage.TargetObservation, error) {
	if a == nil || !target.Valid() || a.config.MachineID != string(target.Scope.MachineID) || a.config.EngineID != string(target.Scope.EngineID) {
		return hoststorage.TargetObservation{}, failure(hoststorage.ErrUnverified, "storage_binding_mismatch")
	}
	var observed hoststorage.TargetObservation
	err := a.withDocker(ctx, func(d docker, _ Endpoint) error {
		var err error
		observed, err = observeStorageTarget(ctx, d, target)
		return err
	})
	return observed, err
}

// CleanupStorageTarget sends one exact unforced removal only after a durable
// Begin intent. Recovery observes that same target and never sends a second rm.
func (a *Adapter) CleanupStorageTarget(ctx context.Context, target hoststorage.Target, recoverOnly bool) (hoststorage.DeleteOutcome, error) {
	if a == nil || !target.Valid() || a.config.MachineID != string(target.Scope.MachineID) || a.config.EngineID != string(target.Scope.EngineID) {
		return hoststorage.DeleteRetained, failure(hoststorage.ErrUnverified, "storage_binding_mismatch")
	}
	outcome := hoststorage.DeleteUnknown
	err := a.withDocker(ctx, func(d docker, _ Endpoint) error {
		observed, err := observeStorageTarget(ctx, d, target)
		if err != nil {
			return err
		}
		if !observed.Present {
			outcome = hoststorage.AbsentConfirmed
			return nil
		}
		if !observed.Eligible {
			outcome = hoststorage.DeleteRetained
			return nil
		}
		if recoverOnly {
			outcome = hoststorage.DeleteUnknown
			return nil
		}
		var args []string
		switch target.Class {
		case hoststorage.Container:
			args = []string{"container", "rm", target.ID}
		case hoststorage.Volume:
			args = []string{"volume", "rm", target.ID}
		case hoststorage.Image:
			args = []string{"image", "rm", target.ID}
		default:
			return hoststorage.ErrInvalid
		}
		result, err := d.run(ctx, nil, nil, args...)
		if err != nil {
			return err
		}
		if result.ExitCode == 0 {
			outcome = hoststorage.DeleteConfirmed
			return nil
		}
		if missing(result) {
			outcome = hoststorage.AbsentConfirmed
			return nil
		}
		return failure(errors.Join(ErrRemoteDocker, err), "storage_target_delete_failed")
	})
	return outcome, err
}
