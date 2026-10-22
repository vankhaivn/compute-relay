package sshhost

import (
	"context"
	"errors"

	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func failure(err error, reason string) error { return provider.WithReason(err, reason) }

func failureReason(err error, fallback string) string { return provider.Reason(err, fallback) }

func failureReasonForTransport(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "ssh_call_timeout"
	case errors.Is(err, context.Canceled):
		return "ssh_call_interrupted"
	case errors.Is(err, ErrHostKeyMismatch):
		return "host_key_mismatch"
	case errors.Is(err, ErrCredentialReject), errors.Is(err, credentials.ErrUnavailable), errors.Is(err, credentials.ErrUnconfigured), errors.Is(err, credentials.ErrUnsupported):
		return "credential_rejected"
	case errors.Is(err, ErrUnavailable):
		return "host_unavailable"
	case errors.Is(err, ErrBinding):
		return "ssh_binding_mismatch"
	case errors.Is(err, ErrResourceIdentity):
		return "ssh_resource_identity_unverified"
	case errors.Is(err, ErrRemoteDocker):
		return "remote_docker_failed"
	default:
		return "ssh_verification_failed"
	}
}
