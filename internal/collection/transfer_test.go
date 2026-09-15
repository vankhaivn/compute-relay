package collection

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

// These fakes test the byte/acknowledgement seam only; they are not adapters or
// persistence fallbacks. Unused methods are inherited and never invoked.
type transferSource struct {
	source
	fetch func(io.Writer) (provider.TransferResult, error)
}

func (s transferSource) FetchArtifact(_ context.Context, _ provider.RemoteReference, _ provider.Artifact, dst io.Writer, _ int64) (provider.TransferResult, error) {
	return s.fetch(dst)
}

type transferBlobs struct {
	BlobStore
	put func(domain.ObjectMetadata, io.Reader) (domain.ObjectMetadata, error)
}

func (b transferBlobs) Put(_ context.Context, m domain.ObjectMetadata, r io.Reader) (domain.ObjectMetadata, error) {
	return b.put(m, r)
}

func TestTransferEOFRequiresCompleteAcknowledgedBytes(t *testing.T) {
	for _, mode := range []string{"success", "late-error", "false-receipt", "overflow"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			w, _, _ := manifestFixture(t)
			data := []byte("bounded transfer fixture")
			meta := domain.ObjectMetadata{WorkspaceID: w.Lease.WorkspaceID, ID: "artifact", Bytes: int64(len(data)), SHA256: provider.Digest(data)}
			file := File{Path: "outputs/fixture", Object: meta}
			var acknowledged, published atomic.Bool
			src := transferSource{fetch: func(dst io.Writer) (provider.TransferResult, error) {
				if _, err := dst.Write(data); err != nil {
					return provider.TransferResult{}, err
				}
				if mode == "overflow" {
					_, _ = dst.Write([]byte("x")) // The adapter ignores the writer error.
				}
				acknowledged.Store(true)
				result := provider.TransferResult{Bytes: meta.Bytes, SHA256: meta.SHA256}
				if mode == "late-error" {
					return result, errors.New("provider acknowledgement unavailable")
				}
				if mode == "false-receipt" {
					result.SHA256 = provider.Digest([]byte("different"))
				}
				return result, nil
			}}
			blobs := transferBlobs{put: func(m domain.ObjectMetadata, r io.Reader) (domain.ObjectMetadata, error) {
				got, err := io.ReadAll(r)
				if err != nil {
					return domain.ObjectMetadata{}, err
				}
				if !acknowledged.Load() || !bytes.Equal(got, data) {
					return domain.ObjectMetadata{}, errors.New("EOF before the full acknowledgement")
				}
				published.Store(true)
				return m, nil
			}}
			engine := &Engine{blobs: blobs}
			err := engine.transfer(ctx, src, w.Observation.Remote, file)
			if (err == nil) != (mode == "success") || published.Load() != (mode == "success") {
				t.Fatal("unacknowledged or invalid bytes reached EOF/publication", mode, err)
			}
		})
	}
}

func TestTransferJoinsProviderAfterBlobFailureOrPanic(t *testing.T) {
	for _, panicPut := range []bool{false, true} {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		w, _, _ := manifestFixture(t)
		data := bytes.Repeat([]byte("x"), 4096)
		file := File{Path: "outputs/fixture", Object: domain.ObjectMetadata{WorkspaceID: w.Lease.WorkspaceID, ID: "artifact", Bytes: int64(len(data)), SHA256: provider.Digest(data)}}
		stopped := make(chan struct{})
		src := transferSource{fetch: func(dst io.Writer) (provider.TransferResult, error) {
			defer close(stopped)
			_, err := dst.Write(data)
			return provider.TransferResult{}, err
		}}
		engine := &Engine{blobs: transferBlobs{put: func(domain.ObjectMetadata, io.Reader) (domain.ObjectMetadata, error) {
			if panicPut {
				panic("private-disk-canary")
			}
			return domain.ObjectMetadata{}, errors.New("disk unavailable")
		}}}
		finished := make(chan error, 1)
		go func() { finished <- engine.transfer(ctx, src, w.Observation.Remote, file) }()
		select {
		case err := <-finished:
			if err == nil {
				t.Fatal("blob failure reported success")
			}
		case <-ctx.Done():
			t.Fatal("blob failure left the transfer pipe blocked")
		}
		select {
		case <-stopped:
		default:
			t.Fatal("worker returned before provider callback ended")
		}
		cancel()
	}
}
