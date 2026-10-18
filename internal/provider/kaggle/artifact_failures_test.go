package kaggle

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle/kaggleapi"
)

func assertArtifactReason(t *testing.T, err error, reason string) {
	t.Helper()
	var p domain.Problem
	if !errors.As(err, &p) || p.Code != domain.CodeArtifactCollectionFailed || p.PublicDetails()[domain.ReasonDetail] != reason || !errors.Is(err, ErrArtifactTransfer) {
		t.Fatalf("expected closed reason %s, got %v", reason, err)
	}
	if strings.Contains(err.Error(), "SYNTHETIC_TOKEN") {
		t.Fatal("provider text escaped")
	}
}

func nativeArtifactReader(t *testing.T, fail func(*http.Request) bool) (*ArtifactReader, provider.RemoteReference, *nativeFixture) {
	t.Helper()
	a, ref, _ := artifactFixture(t)
	f := newNativeFixture(t, a.request.Execution.Owner)
	f.kernel = &a.executor.request
	f.state = "COMPLETE"
	f.result(ref.Identity)
	f.outputs["control/stderr.log"] = []byte{}
	f.outputs["control/environment.json"] = []byte(`{"runner":"fixture"}`)
	a.run = func(ctx context.Context, _ Config, mode string, token []byte, request artifactRequest, dst io.Writer) ([]byte, error) {
		c, err := kaggleapi.New(token, kaggleapi.Options{APITransport: apiRoundTripFunc(func(r *http.Request) (*http.Response, error) {
			if fail != nil && fail(r) {
				return jsonResponse(500, `{"private":"SYNTHETIC_TOKEN"}`), nil
			}
			return f.api(r)
		}), StorageTransport: apiRoundTripFunc(f.storage)})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		return artifacts(ctx, c, mode, request, dst)
	}
	return a, ref, f
}

func TestArtifactNativeReadFailuresRetainExactStep(t *testing.T) {
	for _, test := range []struct{ operation, file, reason string }{
		{"IntrospectToken", "", "artifact_account_read_failed"},
		{"GetKernel", "", "artifact_remote_read_failed"},
		{"GetKernelSessionStatus", "", "artifact_status_read_failed"},
		{"DownloadKernelOutput", artifactManifest, "artifact_manifest_download_failed"},
		{"DownloadKernelOutput", "control/stdout.log", "artifact_stdout_download_failed"},
		{"DownloadKernelOutput", "control/stderr.log", "artifact_stderr_download_failed"},
		{"DownloadKernelOutput", "control/environment.json", "artifact_environment_download_failed"},
	} {
		t.Run(test.reason, func(t *testing.T) {
			a, ref, f := nativeArtifactReader(t, func(r *http.Request) bool {
				if path.Base(r.URL.Path) != test.operation {
					return false
				}
				if test.file == "" {
					return true
				}
				raw, err := io.ReadAll(r.Body)
				if err != nil {
					t.Fatal(err)
				}
				r.Body = io.NopCloser(bytes.NewReader(raw))
				var request struct{ FilePath string }
				if json.Unmarshal(raw, &request) != nil {
					t.Fatal("invalid fixture request")
				}
				return request.FilePath == "relay-result/"+test.file
			})
			_, err := a.ListArtifacts(context.Background(), ref)
			assertArtifactReason(t, err, test.reason)
			if f.count("SaveKernel") != 0 || f.count("DeleteKernel") != 0 || f.count("CreateDataset") != 0 {
				t.Fatal("read failure caused remote mutation")
			}
		})
	}
}

func TestArtifactNativeChecksKeepIdentityAndBounds(t *testing.T) {
	for _, fault := range []string{"account", "identity", "final-identity", "running", "manifest", "count", "bytes", "control-count", "control-bytes", "undeclared", "required"} {
		t.Run(fault, func(t *testing.T) {
			a, ref, f := nativeArtifactReader(t, nil)
			reason := ""
			switch fault {
			case "account":
				f.owner, reason = "another-account", "artifact_account_mismatch"
			case "identity":
				f.fault, reason = "public-kernel", "artifact_remote_identity_mismatch"
			case "final-identity":
				f.fault, reason = "kernel-replaced", "artifact_remote_identity_mismatch"
			case "running":
				f.state, reason = "RUNNING", "artifact_not_terminal"
			case "manifest":
				f.outputs[artifactManifest], reason = []byte(`{}`), "artifact_manifest_invalid"
			case "count":
				a.request.MaxFiles, reason = 1, "artifact_file_limit"
			case "bytes":
				a.request.MaxBytes, reason = 1, "artifact_byte_limit"
			case "control-count":
				a.request.MaxFiles, reason = 2, "artifact_file_limit"
			case "control-bytes":
				a.request.MaxBytes = int64(len(f.outputs[artifactManifest]) + len(f.outputs["outputs/answer.txt"]))
				reason = "artifact_byte_limit"
			case "undeclared":
				a.request.Outputs[0].Path, reason = "another.txt", "artifact_output_undeclared"
			case "required":
				a.request.Outputs = append(a.request.Outputs, artifactDeclaration{Path: "missing.txt", Kind: "file", Required: true})
				reason = "artifact_output_missing"
			}
			_, err := a.ListArtifacts(context.Background(), ref)
			assertArtifactReason(t, err, reason)
		})
	}
}

func TestArtifactNativeEmptyLogAndPayloadFetch(t *testing.T) {
	a, ref, f := nativeArtifactReader(t, nil)
	files, err := a.ListArtifacts(context.Background(), ref)
	if err != nil || len(files) != 5 {
		t.Fatal("complete catalog including empty log unavailable", len(files), err)
	}
	for _, file := range files {
		var dst bytes.Buffer
		result, err := a.Fetch(context.Background(), ref, file, &dst, file.Bytes, nil)
		if err != nil || result.Bytes != file.Bytes || !bytes.Equal(dst.Bytes(), f.outputs[file.Path]) {
			t.Fatal("verified fetch failed", file.Path, err)
		}
	}
}

func TestArtifactReaderDropsUnknownProviderDetails(t *testing.T) {
	a, ref, _ := artifactFixture(t)
	a.run = func(context.Context, Config, string, []byte, artifactRequest, io.Writer) ([]byte, error) {
		return nil, provider.Problem(domain.CodeArtifactCollectionFailed, domain.FailureStageResults, "SYNTHETIC_TOKEN").WithDetail(domain.ReasonDetail, "unknown_private_reason")
	}
	_, err := a.ListArtifacts(context.Background(), ref)
	assertArtifactReason(t, err, "artifact_read_failed")
}

func TestArtifactNativePayloadFailureDoesNotPublishBytes(t *testing.T) {
	for _, corrupted := range []bool{false, true} {
		t.Run(map[bool]string{false: "unavailable", true: "changed"}[corrupted], func(t *testing.T) {
			a, ref, f := nativeArtifactReader(t, nil)
			files, err := a.ListArtifacts(context.Background(), ref)
			if err != nil {
				t.Fatal(err)
			}
			var payload provider.Artifact
			for _, file := range files {
				if file.Path == "outputs/answer.txt" {
					payload = file
				}
			}
			// Keep the pinned manifest; only the provider's payload changes.
			reason := "artifact_download_failed"
			delete(f.outputs, payload.Path)
			if corrupted {
				f.outputs[payload.Path] = []byte("bad")
				reason = "artifact_digest_mismatch"
			}
			result, err := a.Fetch(context.Background(), ref, payload, io.Discard, payload.Bytes, nil)
			assertArtifactReason(t, err, reason)
			if result != (provider.TransferResult{}) {
				t.Fatal("failed bytes became verified")
			}
		})
	}
}

func TestArtifactReaderCancelledWaitRetainsContextAndReason(t *testing.T) {
	a, ref, _ := artifactFixture(t)
	a.slot <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := a.ListArtifacts(ctx, ref)
	var problem domain.Problem
	if !errors.Is(err, context.Canceled) || !errors.As(err, &problem) || problem.PublicDetails()[domain.ReasonDetail] != "artifact_runtime_stopped" {
		t.Fatal("cancelled read lost context identity or reason", err)
	}
}
