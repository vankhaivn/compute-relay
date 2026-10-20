package sshhost

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func TestVerifyClassifiesFailuresWithoutExposingDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cause error
		code  domain.ErrorCode
	}{
		{"transport", ErrUnavailable, domain.CodeProviderUnreachable},
		{"authentication", ErrCredentialReject, domain.CodeProviderAuthFailed},
		{"host key", ErrHostKeyMismatch, domain.CodeRemoteIdentityMismatch},
		{"docker", ErrRemoteDocker, domain.CodeProviderStateUnknown},
		{"resource unreadable", ErrResourceIdentity, domain.CodeProviderStateUnknown},
		{"unexpected", errors.New("private diagnostic"), domain.CodeProviderStateUnknown},
		{"vault denial", credentials.ErrUnavailable, domain.CodeProviderAuthFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			wrapped := fmt.Errorf("private diagnostic: %w", tc.cause)
			if tc.name == "vault denial" {
				h.adapter.creds = localCredentialFailure{wrapped}
			} else {
				h.dialErr = wrapped
			}
			err := h.adapter.Verify(t.Context(), h.adapter.binding)
			var problem domain.Problem
			if !errors.As(err, &problem) || problem.Code != tc.code || problem.Validate() != nil {
				t.Fatalf("verification = %v; want %s", err, tc.code)
			}
			if !errors.Is(err, tc.cause) || strings.Contains(err.Error(), "private diagnostic") || len(problem.PublicDetails()) != 0 {
				t.Fatal("verification lost cause or exposed private diagnostics", err)
			}
			if h.host.created != 0 || h.host.starts != 0 || h.host.builds != 0 {
				t.Fatal("verification mutated the remote host")
			}
		})
	}
}

func TestVerifyPreservesLocalStateAndContextFailures(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, provider.LocalStateError{Err: errors.New("private diagnostic")}} {
		h := newHarness(t)
		h.adapter.creds = localCredentialFailure{cause}
		if err := h.adapter.Verify(t.Context(), h.adapter.binding); err != cause || h.dials != 0 {
			t.Fatal("verification replaced local control failure", err)
		}
	}
}

func TestVerifyDistinguishesChangedAndUnreadableResources(t *testing.T) {
	for _, kind := range []string{"machine", "engine", "unreadable"} {
		t.Run(kind, func(t *testing.T) {
			h := newHarness(t)
			h.adapter.config.MachineID = resourceDigest("linux-machine-id", h.host.machineID)
			h.adapter.config.EngineID = resourceDigest("docker-engine-id", h.host.engineID)
			want := domain.CodeRemoteIdentityMismatch
			switch kind {
			case "machine":
				h.host.machineID = strings.Repeat("b", 32)
			case "engine":
				h.host.engineID = "synthetic-engine-replaced"
			case "unreadable":
				h.host.machineID = ""
				want = domain.CodeProviderStateUnknown
			}
			err := h.adapter.Verify(t.Context(), h.adapter.binding)
			var problem domain.Problem
			if !errors.As(err, &problem) || problem.Code != want || !errors.Is(err, ErrResourceIdentity) {
				t.Fatalf("verification = %v; want %s", err, want)
			}
			if h.host.created != 0 || h.host.starts != 0 || h.host.builds != 0 {
				t.Fatal("changed or unreadable resource received mutations")
			}
		})
	}
}

func TestVerifyRejectsBindingBeforeCredentialOrNetworkAccess(t *testing.T) {
	h := newHarness(t)
	other := h.adapter.binding
	other.CredentialRef = "other"
	err := h.adapter.Verify(t.Context(), other)
	var problem domain.Problem
	if !errors.As(err, &problem) || problem.Code != domain.CodeRemoteIdentityMismatch || !errors.Is(err, ErrBinding) || h.vault.calls != 0 || h.dials != 0 {
		t.Fatal("changed binding was not refused before credential access", err)
	}
}

func TestVerifyDockerFailuresAreStateUncertainty(t *testing.T) {
	for _, missingCLI := range []bool{false, true} {
		h := newHarness(t)
		h.host.fail = func(args []string) (Result, bool) {
			if args[0] == "test" && missingCLI || args[len(args)-1] == "version" {
				return Result{ExitCode: 1, Stderr: []byte("private diagnostic")}, true
			}
			return Result{}, false
		}
		err := h.adapter.Verify(t.Context(), h.adapter.binding)
		var problem domain.Problem
		if !errors.As(err, &problem) || problem.Code != domain.CodeProviderStateUnknown || !errors.Is(err, ErrRemoteDocker) || strings.Contains(err.Error(), "private diagnostic") {
			t.Fatal("Docker unavailable became authentication failure", err)
		}
	}
}

type verificationSession struct{ executor }

func (verificationSession) Close() error { return nil }

func TestVerifyResourceReadsPreserveWrappedTransportAndControlFailures(t *testing.T) {
	for _, failAt := range []string{"/bin/sh", "/usr/bin/uname", "/bin/cat", "context", "info"} {
		for _, cause := range []error{ErrUnavailable, context.Canceled, context.DeadlineExceeded} {
			t.Run(failAt+"/"+cause.Error(), func(t *testing.T) {
				h := newHarness(t)
				h.adapter.config.MachineID = resourceDigest("linux-machine-id", h.host.machineID)
				h.adapter.config.EngineID = resourceDigest("docker-engine-id", h.host.engineID)
				h.adapter.dial = func(context.Context, Target, Credential) (session, error) {
					return verificationSession{&scriptedExec{answer: func(args []string, in io.Reader, out io.Writer) (Result, error) {
						for _, arg := range args {
							if arg == failAt {
								return Result{}, fmt.Errorf("private diagnostic: %w", cause)
							}
						}
						return h.host.Run(t.Context(), nil, args, in, out)
					}}}, nil
				}
				err := h.adapter.Verify(t.Context(), h.adapter.binding)
				if !errors.Is(err, cause) || !errors.Is(err, ErrResourceIdentity) || strings.Contains(err.Error(), "private diagnostic") {
					t.Fatal("resource read lost its safe failure chain", err)
				}
				var problem domain.Problem
				if cause == ErrUnavailable && (!errors.As(err, &problem) || problem.Code != domain.CodeProviderUnreachable) {
					t.Fatal("resource transport fault misclassified", err)
				}
			})
		}
	}
}
