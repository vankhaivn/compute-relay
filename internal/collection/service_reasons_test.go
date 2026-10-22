package collection

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

type reasonResolver struct {
	source provider.Provider
	err    error
	panics bool
}

func (r reasonResolver) Resolve(provider.BindingSnapshot) (provider.Provider, error) {
	if r.panics {
		panic("private resolver canary")
	}
	return r.source, r.err
}

type reasonCatalog struct {
	provider.Provider
	err    error
	panics bool
}

func (r reasonCatalog) ListArtifacts(context.Context, provider.RemoteReference) ([]provider.Artifact, error) {
	if r.panics {
		panic("private catalog canary")
	}
	return nil, r.err
}

type reasonBlobs struct {
	BlobStore
	readErr error
	reader  io.ReadCloser
	panics  bool
}

func (b reasonBlobs) Open(context.Context, domain.WorkspaceID, domain.ObjectID) (io.ReadCloser, error) {
	if b.panics {
		panic("private blob canary")
	}
	return b.reader, b.readErr
}

func assertServiceReason(t *testing.T, err error, reason string) Failure {
	t.Helper()
	var f Failure
	if !errors.As(err, &f) || f.Reason != reason || strings.Contains(err.Error(), "canary") {
		t.Fatalf("reason %q: %#v %v", reason, f, err)
	}
	return f
}
func TestCollectionServiceBoundaryReasons(t *testing.T) {
	w, _, _ := manifestFixture(t)
	cause := errors.New("private transport canary")
	for _, tc := range []struct {
		name     string
		resolver reasonResolver
		reason   string
	}{
		{"resolver_error", reasonResolver{err: cause}, "artifact_binding_unavailable"},
		{"resolver_nil", reasonResolver{}, "artifact_binding_unavailable"},
		{"resolver_panic", reasonResolver{panics: true}, "artifact_resolver_panic"},
		{"verify", reasonResolver{source: reliabilitySource{verify: func(context.Context) error { return provider.WithReason(cause, "host_unavailable") }}}, "artifact_account_read_failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := &Engine{resolver: tc.resolver}
			_, err := e.resolve(context.Background(), w)
			f := assertServiceReason(t, err, tc.reason)
			if tc.name == "verify" && (f.Cause != "host_unavailable" || !errors.Is(err, cause)) {
				t.Fatal("lost specific cause", f, err)
			}
		})
	}
	e := &Engine{config: DefaultConfig()}
	for _, tc := range []struct {
		source reasonCatalog
		reason string
	}{
		{reasonCatalog{err: cause}, "artifact_catalog_failed"},
		{reasonCatalog{panics: true}, "artifact_discovery_panic"},
	} {
		_, err := e.discover(context.Background(), tc.source, w)
		assertServiceReason(t, err, tc.reason)
	}
	w.Snapshot = &Snapshot{}
	e.blobs = reasonBlobs{}
	_, err := e.verifyFiles(context.Background(), nil, w)
	assertServiceReason(t, err, "artifact_catalog_invalid")
	e.blobs = reasonBlobs{}
	_, err = e.existing(context.Background(), domain.ObjectMetadata{})
	assertServiceReason(t, err, "artifact_blob_read_failed")
}
func TestCollectionFailurePreservesSpecificCodeReasonAndCausalIdentity(t *testing.T) {
	cause := errors.New("private cause canary")
	for _, code := range []domain.ErrorCode{domain.CodeRemoteIdentityMismatch, domain.CodeResultManifestMissing, domain.CodeArtifactDigestMismatch, domain.CodeArtifactMissing} {
		f := collectionFailure(failure(code, true))
		if f.Code != code || !f.Invalid || !domain.ValidCollectionReason(f.Reason) {
			t.Fatal("lost semantic failure", f)
		}
	}
	p := provider.Problem(domain.CodeArtifactCollectionFailed, domain.FailureStageResults, "safe").WithDetail(domain.ReasonDetail, "artifact_read_failed").WithCause(cause)
	f := collectionFailure(&p)
	if f.Reason != "artifact_read_failed" || !errors.Is(f, cause) {
		t.Fatal("pointer problem lost cause", f)
	}
}
func TestCollectionTransferPanicAndWriteReasons(t *testing.T) {
	w, _, _ := manifestFixture(t)
	file := File{Path: "outputs/a", Object: domain.ObjectMetadata{WorkspaceID: w.Lease.WorkspaceID, ID: "obj_a", SHA256: provider.Digest(nil)}}
	for _, tc := range []struct {
		panics bool
		reason string
	}{{true, "artifact_transfer_panic"}, {false, "artifact_download_failed"}} {
		src := transferSource{fetch: func(io.Writer) (provider.TransferResult, error) {
			if tc.panics {
				panic("private canary")
			}
			return provider.TransferResult{}, errors.New("private canary")
		}}
		e := &Engine{blobs: transferBlobs{put: func(m domain.ObjectMetadata, r io.Reader) (domain.ObjectMetadata, error) {
			_, err := io.Copy(io.Discard, r)
			return m, err
		}}}
		assertServiceReason(t, e.transfer(context.Background(), src, w.Observation.Remote, file, nil), tc.reason)
	}
}

func TestCollectionVerificationPanicHasClosedStep(t *testing.T) {
	w, m, files := manifestFixture(t)
	raw, catalog := snapshotFixture(t, w, m, files)
	snapshot, err := BuildSnapshot(w, raw, catalog, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	w.Snapshot = &snapshot
	e := &Engine{repo: &reliabilityRepository{}, blobs: reasonBlobs{panics: true}, clock: wallClock{}, config: DefaultConfig()}
	_, err = e.verifyFiles(context.Background(), nil, w)
	assertServiceReason(t, err, "artifact_verification_panic")
}
