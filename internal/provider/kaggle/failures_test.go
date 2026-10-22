package kaggle

import (
	"context"
	"errors"
	"net/http"
	"path"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle/kaggleapi"
)

func assertStepReason(t *testing.T, err error, expected string) {
	t.Helper()
	reason := provider.Reason(err, "")
	if reason != expected {
		t.Fatalf("wanted reason %s; got %s (%v)", expected, reason, err)
	}
	if strings.Contains(err.Error(), "SYNTHETIC_TOKEN") {
		t.Fatal("private error text escaped")
	}
}

func TestExecutionObservationFailuresRetainStep(t *testing.T) {
	for _, tc := range []struct {
		fault, reason string
		sentinel      error
	}{
		{"transport", "execution_call_failed", ErrUnavailable},
		{"response", "execution_response_invalid", ErrProtocol},
		{"lookup", "lookup_rate_limited", ErrUnavailable},
		{"identity", "execution_observation_invalid", nil},
		{"cancel", "execution_interrupted", context.Canceled},
	} {
		t.Run(tc.fault, func(t *testing.T) {
			e := newExecutionFixture(t)
			ref, err := e.remote(executionFound(e.request, "RUNNING"))
			if err != nil {
				t.Fatal(err)
			}
			e.run = func(_ context.Context, _ Config, _ string, _ []byte, request executionRequest) (executionResponse, error) {
				switch tc.fault {
				case "transport":
					return executionResponse{}, errors.New("SYNTHETIC_TOKEN")
				case "response":
					return executionResponse{}, nil
				case "lookup":
					return executionResponse{Protocol: 1, Status: "unknown", Reason: "lookup_rate_limited"}, nil
				case "identity":
					return executionResponse{Protocol: 1, Status: "invalid"}, nil
				}
				return executionFound(request, "RUNNING"), nil
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.fault == "cancel" {
				cancel()
			}
			seen, err := e.Observe(ctx, ref)
			assertStepReason(t, err, tc.reason)
			if tc.sentinel != nil && !errors.Is(err, tc.sentinel) {
				t.Fatal("failure identity lost", err)
			}
			if seen.RemoteActivity == domain.RemoteActivityInactive {
				t.Fatal("failed read proved inactivity")
			}
		})
	}
}

func TestCleanupNativeFailuresRetainStepWithoutRepeatedDeletion(t *testing.T) {
	for _, tc := range []struct{ fault, reason string }{
		{"account_read", "cleanup_account_read_failed"},
		{"account_mismatch", "cleanup_account_mismatch"},
		{"resource_read", "cleanup_resource_read_failed"},
		{"identity", "cleanup_resource_identity_mismatch"},
		{"status", "cleanup_status_read_failed"},
		{"active", "cleanup_execution_active"},
		{"marker_download", "cleanup_marker_download_failed"},
		{"marker_mismatch", "cleanup_marker_mismatch"},
		{"recover", "cleanup_deletion_unconfirmed"},
		{"delete", "cleanup_delete_failed"},
		{"readback", "cleanup_delete_readback_failed"},
		{"visible", "cleanup_resource_still_visible"},
	} {
		t.Run(tc.fault, func(t *testing.T) {
			p, plan, saved, _, f := nativeRuntime(t, false)
			prepared, err := p.Prepare(context.Background(), plan, "prep")
			if err != nil {
				t.Fatal(err)
			}
			*saved = prepared
			e, err := p.attempt(context.Background(), plan.Job.Identity)
			if err != nil {
				t.Fatal(err)
			}
			f.kernel, f.state = &e.executor.request, "COMPLETE"
			remote, err := e.executor.remote(executionFound(e.executor.request, "COMPLETE"))
			if err != nil {
				t.Fatal(err)
			}
			purpose, operation := "execution", domain.OperationID(plan.Job.Identity.IntentID)
			if strings.HasPrefix(tc.fault, "marker_") {
				purpose, operation = "staging", prepared.PreparationID
				remote = provider.RemoteReference{Identity: plan.Job.Identity, Resource: prepared.Resource, Version: "prepared"}
			}
			request := provider.CleanupResourceRequest{CleanupRequest: provider.CleanupRequest{Remote: remote, LedgerID: "res_fixture", CreationOperationID: prepared.PreparationID, Mode: provider.CleanupApply}, Purpose: purpose, OwnershipOperationID: operation, PlanSHA256: plan.Digest()}
			switch tc.fault {
			case "account_mismatch":
				f.owner = "other_account"
			case "identity":
				f.fault = "public-kernel"
			case "active":
				f.state = "RUNNING"
			case "marker_mismatch":
				f.staged["relay-stage.bin"] = []byte("changed")
			case "recover":
				request.RecoverOnly = true
			}
			reads, deletes := 0, 0
			p.runCleanup = func(ctx context.Context, _ Config, mode string, token []byte, q cleanupRequest) (cleanupResponse, error) {
				c, err := kaggleapi.New(token, kaggleapi.Options{APITransport: apiRoundTripFunc(func(r *http.Request) (*http.Response, error) {
					op := path.Base(r.URL.Path)
					if op == "GetKernel" {
						reads++
					}
					if op == "DeleteKernel" {
						deletes++
					}
					fail := tc.fault == "account_read" && op == "IntrospectToken" || tc.fault == "resource_read" && op == "GetKernel" || tc.fault == "status" && op == "GetKernelSessionStatus" || tc.fault == "marker_download" && op == "DownloadDataset" || tc.fault == "delete" && op == "DeleteKernel" || tc.fault == "readback" && op == "GetKernel" && reads >= 3
					if fail {
						status := 403
						if tc.fault == "readback" {
							status = 401
						}
						return jsonResponse(status, `{"private":"SYNTHETIC_TOKEN"}`), nil
					}
					if tc.fault == "visible" && op == "DeleteKernel" {
						return jsonResponse(200, `{}`), nil
					}
					return f.api(r)
				}), StorageTransport: apiRoundTripFunc(f.storage)})
				if err != nil {
					t.Fatal(err)
				}
				defer c.Close()
				return cleanup(ctx, c, mode, q), nil
			}
			out, err := p.CleanupResource(context.Background(), request)
			assertStepReason(t, err, tc.reason)
			if out.Deleted || out.AlreadyAbsent || out.WouldDelete || deletes > 1 {
				t.Fatal("uncertainty became deletion evidence or retry", out, deletes)
			}
		})
	}
}

func TestCleanupCredentialAndProtocolFailuresRetainStep(t *testing.T) {
	for _, tc := range []struct{ fault, reason string }{
		{"credential", "cleanup_credential_unavailable"},
		{"double", "cleanup_credential_callback_invalid"},
		{"call", "cleanup_call_failed"},
		{"response", "cleanup_response_invalid"},
	} {
		t.Run(tc.fault, func(t *testing.T) {
			e := newExecutionFixture(t)
			p := &RuntimeAdapter{config: e.stager.config, binding: provider.BindingSnapshot{Binding: e.plan.Job.Binding}, stager: e.stager,
				load: func(context.Context, provider.Identity) (provider.Plan, provider.Prepared, error) {
					return e.plan, e.prepared, nil
				}}
			p.runCleanup = func(context.Context, Config, string, []byte, cleanupRequest) (cleanupResponse, error) {
				if tc.fault == "call" {
					return cleanupResponse{}, errors.New("SYNTHETIC_TOKEN")
				}
				if tc.fault == "response" {
					return cleanupResponse{Protocol: 2, Status: "deleted"}, nil
				}
				return cleanupResponse{Protocol: 1, Status: "unknown"}, nil
			}
			e.stager.credentials = stagingCredentialFunc(func(_ context.Context, _ ports.CredentialRef, use func([]byte) error) error {
				if tc.fault == "credential" {
					return errors.New("SYNTHETIC_TOKEN")
				}
				err := use([]byte("SYNTHETIC_TOKEN"))
				if tc.fault == "double" {
					_ = use([]byte("SYNTHETIC_TOKEN"))
					return nil
				}
				return err
			})
			out, err := p.CleanupResource(context.Background(), localCleanupRequest(e))
			assertStepReason(t, err, tc.reason)
			if out.Deleted || out.AlreadyAbsent {
				t.Fatal("failed cleanup acquired evidence", out)
			}
		})
	}
}
