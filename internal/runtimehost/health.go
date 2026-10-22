package runtimehost

import (
	"context"

	"github.com/vankhaivn/compute-relay/internal/hosthealth"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/sshhost"
)

type managedHealthSampler struct {
	credentials ports.CredentialResolver
	collect     func(context.Context, []byte, string, string, string) (hosthealth.Snapshot, error)
}

func (s managedHealthSampler) Sample(ctx context.Context, target hosthealth.Target) (hosthealth.Snapshot, error) {
	if !target.Valid() || target.ProviderType != sshhost.ProviderType || s.credentials == nil || s.collect == nil {
		return hosthealth.Snapshot{}, hosthealth.ErrUnverified
	}
	engineID := ""
	if len(target.RuntimeConfig) > 0 {
		frozen, err := parseManagedSSHRuntimeConfig(target.RuntimeConfig)
		if err != nil || frozen.MachineID != "" && "resource_"+frozen.MachineID != target.ResourceID {
			return hosthealth.Snapshot{}, hosthealth.ErrUnverified
		}
		engineID = frozen.EngineID
	}
	var snapshot hosthealth.Snapshot
	err := s.credentials.WithCredential(ctx, target.CredentialRef, func(raw []byte) error {
		var err error
		snapshot, err = s.collect(ctx, raw, target.CanonicalAccount, target.ResourceID, engineID)
		return err
	})
	if err != nil {
		return hosthealth.Snapshot{}, provider.WithReason(hosthealth.ErrUnavailable, provider.Reason(err, "health_credential_read_failed"))
	}
	if snapshot.Validate() != nil || snapshot.ResourceID != target.ResourceID {
		return hosthealth.Snapshot{}, provider.WithReason(hosthealth.ErrUnavailable, "health_snapshot_invalid")
	}
	return snapshot, nil
}
