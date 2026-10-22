package kaggle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle/kaggleapi"
)

func nativeArtifactBatch(t *testing.T, count int) (*ArtifactReader, provider.RemoteReference, []provider.Artifact, *nativeFixture) {
	t.Helper()
	a, ref, _ := artifactFixture(t)
	a.request.Outputs = []artifactDeclaration{{Path: "frames", Kind: "directory", Required: true}}
	f := newNativeFixture(t, a.executor.request.Owner)
	f.kernel, f.state = &a.executor.request, "COMPLETE"
	f.result(ref.Identity)
	var manifest map[string]any
	if json.Unmarshal(f.outputs[artifactManifest], &manifest) != nil {
		t.Fatal("invalid fixture manifest")
	}
	entries := make([]map[string]any, count)
	files := make([]provider.Artifact, count)
	for i := range count {
		name := fmt.Sprintf("frames/frame-%04d.txt", i)
		data := []byte("yes")
		entries[i] = map[string]any{"path": name, "bytes": len(data), "sha256": provider.Digest(data)}
		f.outputs["outputs/"+name] = data
		files[i] = artifactFor(ref, "outputs/"+name, data)
	}
	manifest["artifacts"] = entries
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	f.outputs[artifactManifest] = raw
	f.outputs["scratch/private.txt"] = []byte("must not be fetched")
	a.runBatch = func(ctx context.Context, _ Config, token []byte, r artifactRequest, targets []artifactEntry, visit func(artifactEntry, io.Reader) error) error {
		client, err := f.client(token)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		return artifactBatch(ctx, client, r, targets, visit)
	}
	return a, ref, files, f
}

func TestArtifactBatchSharesConstantControlsAtScale(t *testing.T) {
	for _, count := range []int{1, 200, 500} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			a, ref, files, f := nativeArtifactBatch(t, count)
			credentials := 0
			original := a.executor.stager.credentials
			a.executor.stager.credentials = stagingCredentialFunc(func(ctx context.Context, ref ports.CredentialRef, use func([]byte) error) error {
				credentials++
				return original.WithCredential(ctx, ref, use)
			})
			visits := 0
			err := a.FetchBatch(context.Background(), ref, files, func(file provider.Artifact, reader io.Reader) error {
				if file != files[visits] {
					t.Fatal("batch changed target order/identity", file)
				}
				raw, err := io.ReadAll(reader)
				if err != nil {
					return err
				}
				if string(raw) != "yes" {
					t.Fatal("batch mixed target bytes")
				}
				visits++
				return nil
			})
			if err != nil || visits != count || credentials != 1 {
				t.Fatal("batch incomplete", visits, credentials, err)
			}
			if f.count("IntrospectToken") != 1 || f.count("GetKernel") != 2 || f.count("GetKernelSessionStatus") != 2 || f.count("DownloadKernelOutput") != count+1 || f.transferred != count+1 {
				t.Fatal("metadata/manifest calls scale with file count or unselected bytes fetched", f.calls, f.transferred)
			}
			if f.count("SaveKernel") != 0 || f.count("CreateDataset") != 0 || f.count("DeleteKernel") != 0 || f.count("DeleteDataset") != 0 {
				t.Fatal("collection mutated remote resources")
			}
		})
	}
}

func TestArtifactBatchFinalProofRequiredIncludingEmptyRetry(t *testing.T) {
	for _, empty := range []bool{false, true} {
		for _, fault := range []string{"identity", "status"} {
			t.Run(fmt.Sprintf("%v/%s", empty, fault), func(t *testing.T) {
				a, ref, files, f := nativeArtifactBatch(t, 1)
				if empty {
					files = nil
				}
				if fault == "identity" {
					f.fault = "kernel-replaced"
				}
				original := a.runBatch
				a.runBatch = func(ctx context.Context, c Config, token []byte, r artifactRequest, targets []artifactEntry, visit func(artifactEntry, io.Reader) error) error {
					if fault == "status" {
						client, err := kaggleapi.New(token, kaggleapi.Options{APITransport: apiRoundTripFunc(func(req *http.Request) (*http.Response, error) {
							if strings.HasSuffix(req.URL.Path, "GetKernelSessionStatus") && f.count("GetKernelSessionStatus") == 1 {
								f.state = "RUNNING"
							}
							return f.api(req)
						}), StorageTransport: apiRoundTripFunc(f.storage)})
						if err != nil {
							t.Fatal(err)
						}
						defer client.Close()
						return artifactBatch(ctx, client, r, targets, visit)
					}
					return original(ctx, c, token, r, targets, visit)
				}
				visits := 0
				err := a.FetchBatch(context.Background(), ref, files, func(_ provider.Artifact, reader io.Reader) error {
					visits++
					_, err := io.Copy(io.Discard, reader)
					return err
				})
				reason := "artifact_remote_identity_mismatch"
				if fault == "status" {
					reason = "artifact_not_terminal"
				}
				assertArtifactReason(t, err, reason)
				if visits != len(files) || f.count("GetKernelSessionStatus") != 2 {
					t.Fatal("cached/partial bytes bypassed final proof", visits, f.calls)
				}
			})
		}
	}
}

type waitingArtifactBody struct {
	ctx     context.Context
	first   bool
	stopped chan struct{}
	once    sync.Once
}

func (b *waitingArtifactBody) Read(p []byte) (int, error) {
	if !b.first {
		b.first = true
		p[0] = 'y'
		return 1, nil
	}
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}
func (b *waitingArtifactBody) Close() error { b.once.Do(func() { close(b.stopped) }); return nil }

func TestArtifactBatchVisitorFailureJoinsTransfer(t *testing.T) {
	for _, mode := range []string{"error", "early", "panic", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			a, ref, files, f := nativeArtifactBatch(t, 1)
			stopped := make(chan struct{})
			a.runBatch = func(ctx context.Context, _ Config, token []byte, r artifactRequest, targets []artifactEntry, visit func(artifactEntry, io.Reader) error) error {
				client, err := kaggleapi.New(token, kaggleapi.Options{APITransport: apiRoundTripFunc(f.api), StorageTransport: apiRoundTripFunc(func(req *http.Request) (*http.Response, error) {
					if strings.HasSuffix(req.URL.Path, files[0].Path) {
						return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/octet-stream"}}, Body: &waitingArtifactBody{ctx: req.Context(), stopped: stopped}, ContentLength: 3}, nil
					}
					return f.storage(req)
				})})
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				return artifactBatch(ctx, client, r, targets, visit)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sentinel := errors.New("SYNTHETIC_TOKEN")
			err := a.FetchBatch(ctx, ref, files, func(_ provider.Artifact, reader io.Reader) error {
				var first [1]byte
				if _, err := io.ReadFull(reader, first[:]); err != nil {
					return err
				}
				switch mode {
				case "error":
					return sentinel
				case "panic":
					panic("SYNTHETIC_TOKEN")
				case "cancel":
					cancel()
					return context.Canceled
				}
				return nil
			})
			reason := "artifact_write_failed"
			if mode == "early" {
				reason = "artifact_digest_mismatch"
			}
			assertArtifactReason(t, err, reason)
			if mode == "error" && !errors.Is(err, sentinel) || mode == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatal("visitor failure identity lost", err)
			}
			select {
			case <-stopped:
			default:
				t.Fatal("batch returned before producer/body closed")
			}
			if f.count("DownloadKernelOutput") != 2 {
				t.Fatal("failed batch retried transfer")
			}
		})
	}
}

func TestArtifactBatchBudgetUsesCountAndBytesUnderParent(t *testing.T) {
	a, ref, files, _ := nativeArtifactBatch(t, 500)
	a.policy.Timeout = time.Second
	a.runBatch = func(ctx context.Context, _ Config, _ []byte, _ artifactRequest, _ []artifactEntry, visit func(artifactEntry, io.Reader) error) error {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) < 5*time.Minute {
			t.Fatal("batch retained single-file timeout", time.Until(deadline))
		}
		return artifactFailure("artifact_not_terminal")
	}
	assertArtifactReason(t, a.FetchBatch(context.Background(), ref, files, func(provider.Artifact, io.Reader) error { return nil }), "artifact_not_terminal")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	a.runBatch = func(call context.Context, _ Config, _ []byte, _ artifactRequest, _ []artifactEntry, _ func(artifactEntry, io.Reader) error) error {
		deadline, _ := call.Deadline()
		parent, _ := ctx.Deadline()
		if deadline.After(parent) {
			t.Fatal("batch extended caller budget")
		}
		return artifactFailure("artifact_not_terminal")
	}
	assertArtifactReason(t, a.FetchBatch(ctx, ref, files, func(provider.Artifact, io.Reader) error { return nil }), "artifact_not_terminal")
}

func TestArtifactBatchRejectsChangedAndIncompleteTransfers(t *testing.T) {
	for _, fault := range []string{"pin", "changed", "late-read", "close"} {
		t.Run(fault, func(t *testing.T) {
			a, ref, files, f := nativeArtifactBatch(t, 1)
			reason := "artifact_download_failed"
			switch fault {
			case "pin":
				files[0].SHA256 = provider.Digest([]byte("different pin"))
				reason = "artifact_target_mismatch"
			case "changed":
				f.outputs[files[0].Path] = []byte("bad")
				reason = "artifact_digest_mismatch"
			}
			if fault == "late-read" || fault == "close" {
				a.runBatch = func(ctx context.Context, _ Config, token []byte, r artifactRequest, targets []artifactEntry, visit func(artifactEntry, io.Reader) error) error {
					client, err := kaggleapi.New(token, kaggleapi.Options{APITransport: apiRoundTripFunc(f.api), StorageTransport: apiRoundTripFunc(func(req *http.Request) (*http.Response, error) {
						if strings.HasSuffix(req.URL.Path, files[0].Path) {
							var source io.Reader = strings.NewReader("yes")
							if fault == "late-read" {
								source = io.MultiReader(source, stagingErrorReader{})
							}
							return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/octet-stream"}}, Body: &stagingTestReader{Reader: source, failClose: fault == "close"}, ContentLength: 3}, nil
						}
						return f.storage(req)
					})})
					if err != nil {
						t.Fatal(err)
					}
					defer client.Close()
					return artifactBatch(ctx, client, r, targets, visit)
				}
			}
			visits := 0
			err := a.FetchBatch(context.Background(), ref, files, func(_ provider.Artifact, reader io.Reader) error {
				visits++
				_, err := io.Copy(io.Discard, reader)
				return err
			})
			assertArtifactReason(t, err, reason)
			if fault == "pin" && visits != 0 {
				t.Fatal("mismatched pins reached consumer")
			}
			if f.count("GetKernel") != 1 {
				t.Fatal("failed transfer acquired final proof")
			}
		})
	}
}

func TestArtifactBatchRejectsDuplicateOrMissingCallbacks(t *testing.T) {
	for _, fault := range []string{"duplicate-target", "duplicate-callback", "missing-callback", "wrong-callback"} {
		t.Run(fault, func(t *testing.T) {
			a, ref, files, _ := nativeArtifactBatch(t, 1)
			if fault == "duplicate-target" {
				files = append(files, files[0])
			}
			calls := 0
			a.runBatch = func(_ context.Context, _ Config, _ []byte, _ artifactRequest, targets []artifactEntry, visit func(artifactEntry, io.Reader) error) error {
				calls++
				if fault == "missing-callback" {
					return nil
				}
				target := targets[0]
				if fault == "wrong-callback" {
					target.Path = "outputs/frames/other.txt"
				}
				if err := visit(target, strings.NewReader("yes")); err != nil {
					return err
				}
				return visit(target, strings.NewReader("yes"))
			}
			err := a.FetchBatch(context.Background(), ref, files, func(_ provider.Artifact, reader io.Reader) error { _, err := io.Copy(io.Discard, reader); return err })
			reason := "artifact_target_mismatch"
			if fault == "duplicate-target" {
				reason = "artifact_request_invalid"
			}
			assertArtifactReason(t, err, reason)
			if fault == "duplicate-target" && calls != 0 {
				t.Fatal("invalid targets performed remote work")
			}
		})
	}
}
