// Package hostimages validates application image archives and prepares a tag-free,
// installation-labelled Docker archive before any remote Docker mutation.
package hostimages

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"sync"
	"syscall"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

const (
	LabelInstallation = "io.compute-relay.installation"
	LabelKind         = "io.compute-relay.image-kind"
	LabelArchive      = "io.compute-relay.archive-sha256"
	LabelSchema       = "io.compute-relay.image-schema"
	LabelMachine      = "io.compute-relay.image-machine"
	LabelEngine       = "io.compute-relay.image-engine"
	ApplicationSchema = "application/1"
)

var (
	ErrInvalidArchive = errors.New("invalid application image archive")
	ErrDigestMismatch = errors.New("application image archive digest or size mismatch")
	ErrLimitExceeded  = errors.New("application image archive exceeds a limit")
	ErrNoSpace        = errors.New("insufficient controller space for application image normalization")
)

// Limits bound controller disk, parser work and expanded layers. A zero Limits
// uses defaults; nonzero overrides may only tighten the defaults.
type Limits struct {
	MaxInputBytes    int64
	MaxOutputBytes   int64
	MaxMetadataBytes int64
	MaxMembers       int
	MaxLayers        int
}

func DefaultLimits() Limits {
	return Limits{MaxInputBytes: 2 << 30, MaxOutputBytes: 4 << 30,
		MaxMetadataBytes: 1 << 20, MaxMembers: 1024, MaxLayers: 128}
}

func (l Limits) resolved() (Limits, error) {
	d := DefaultLimits()
	if l == (Limits{}) {
		return d, nil
	}
	if l.MaxInputBytes < 1 || l.MaxInputBytes > d.MaxInputBytes ||
		l.MaxOutputBytes < 1 || l.MaxOutputBytes > d.MaxOutputBytes ||
		l.MaxMetadataBytes < 1 || l.MaxMetadataBytes > d.MaxMetadataBytes ||
		l.MaxMembers < 1 || l.MaxMembers > d.MaxMembers ||
		l.MaxLayers < 1 || l.MaxLayers > d.MaxLayers {
		return Limits{}, ErrLimitExceeded
	}
	return l, nil
}

// Options identifies the frozen source and the verified target Docker engine.
// MachineID and EngineID are already privacy-preserving SHA-256 identities.
type Options struct {
	ArchiveSHA256  domain.SHA256Digest
	ArchiveBytes   int64
	InstallationID domain.RuntimeInstallationID
	MachineID      domain.SHA256Digest
	EngineID       domain.SHA256Digest
	Architecture   string
	TempDir        string
	Limits         Limits
}

func (o Options) validate() (Limits, error) {
	l, err := o.Limits.resolved()
	if err != nil {
		return Limits{}, err
	}
	if !o.ArchiveSHA256.Valid() || !o.InstallationID.Valid() || !o.MachineID.Valid() || !o.EngineID.Valid() ||
		(o.Architecture != "amd64" && o.Architecture != "arm64") || o.ArchiveBytes < 1 {
		return Limits{}, ErrInvalidArchive
	}
	if o.ArchiveBytes > l.MaxInputBytes {
		return Limits{}, ErrLimitExceeded
	}
	return l, nil
}

// Prepared is a completed local Docker archive. Read sends its bytes to Docker;
// Close always deletes the private temp file, including after a failed load.
// Prepared is not safe for concurrent reads and closes.
type Prepared struct {
	ArchiveSHA256   domain.SHA256Digest
	ImageID         string // Config digest used by the classic Docker image store.
	ManifestID      string // Canonical OCI manifest digest used by the containerd image store.
	RootFSDiffIDs   []string
	NormalizedBytes int64
	file            *os.File
	path            string
	once            sync.Once
	closeErr        error
}

func (p *Prepared) Read(b []byte) (int, error) {
	if p == nil || p.file == nil {
		return 0, os.ErrClosed
	}
	return p.file.Read(b)
}

func (p *Prepared) Close() error {
	if p == nil {
		return nil
	}
	p.once.Do(func() {
		if p.file != nil {
			p.closeErr = p.file.Close()
			p.file = nil
		}
		if p.path != "" {
			if err := os.Remove(p.path); err != nil && !errors.Is(err, os.ErrNotExist) && p.closeErr == nil {
				p.closeErr = err
			}
			p.path = ""
		}
	})
	return p.closeErr
}

// Normalize fully validates the supplied bytes and completes a sanitized archive
// on the controller. Callers must commit their separate one-shot load intent only
// after this succeeds, and must close the result on every path.
func Normalize(ctx context.Context, source io.Reader, opts Options) (_ *Prepared, err error) {
	step := "image_options_invalid"
	defer func() {
		if err != nil {
			err = provider.WithReason(err, step)
		}
	}()
	if ctx == nil || source == nil {
		return nil, ErrInvalidArchive
	}
	limits, err := opts.validate()
	if err != nil {
		return nil, err
	}
	step = "image_source_file_failed"
	input, err := os.CreateTemp(opts.TempDir, "relay-image-source-*")
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = input.Close()
		_ = os.Remove(input.Name())
	}()
	step = "image_source_space_failed"
	if err := ensureFreeSpace(input.Name(), opts.ArchiveBytes); err != nil {
		return nil, err
	}
	step = "image_source_read_failed"
	originalHash := sha256.New()
	n, err := io.CopyN(io.MultiWriter(input, originalHash), contextReader{ctx, source}, opts.ArchiveBytes)
	if err != nil || n != opts.ArchiveBytes {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.Join(ErrDigestMismatch, err)
	}
	step = "image_source_digest_failed"
	var extra [1]byte
	got, readErr := io.ReadFull(contextReader{ctx, source}, extra[:])
	if got != 0 || readErr != io.EOF || hex.EncodeToString(originalHash.Sum(nil)) != string(opts.ArchiveSHA256) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, ErrDigestMismatch
	}
	step = "image_archive_scan_failed"
	entries, err := scanArchive(ctx, input, limits)
	if err != nil {
		return nil, err
	}
	step = "image_archive_parse_failed"
	image, err := parseImage(ctx, input, entries, opts, limits)
	if err != nil {
		return nil, err
	}
	step = "image_output_space_failed"
	if err := ensureFreeSpace(input.Name(), estimatedOutputBytes(image, limits)); err != nil {
		return nil, err
	}
	step = "image_output_file_failed"
	output, err := os.CreateTemp(opts.TempDir, "relay-image-load-*")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = output.Close()
			_ = os.Remove(output.Name())
		}
	}()
	metadata := normalizedMetadata(image)
	step = "image_archive_write_failed"
	size, err := writeDockerArchive(ctx, input, output, image, metadata, limits)
	if err != nil {
		return nil, err
	}
	step = "image_archive_sync_failed"
	if err = output.Sync(); err != nil {
		return nil, err
	}
	step = "image_archive_verify_failed"
	if err = verifyCompletedArchive(ctx, output, image, size, limits); err != nil {
		return nil, err
	}
	step = "image_archive_seek_failed"
	if _, err = output.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return &Prepared{ArchiveSHA256: opts.ArchiveSHA256, ImageID: image.id, ManifestID: metadata.manifestID,
		RootFSDiffIDs: append([]string(nil), image.diffIDs...), NormalizedBytes: size,
		file: output, path: output.Name()}, nil
}

func estimatedOutputBytes(image parsedImage, limits Limits) int64 {
	// Headers and the generated manifest fit in this per-layer allowance. The
	// capped output writer remains authoritative if the estimate is too small.
	estimate := int64(len(image.config)) + 16<<10
	for _, layer := range image.layers {
		if layer.size > limits.MaxOutputBytes-estimate-2048 {
			return limits.MaxOutputBytes
		}
		estimate += layer.size + 2048
	}
	if estimate > limits.MaxOutputBytes {
		return limits.MaxOutputBytes
	}
	return estimate
}

func ensureFreeSpace(path string, bytes int64) error {
	if bytes <= 0 {
		return ErrLimitExceeded
	}
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return err
	}
	blockSize := int64(stat.Bsize)
	if blockSize <= 0 || uint64(stat.Bavail) < uint64((bytes+blockSize-1)/blockSize) {
		return ErrNoSpace
	}
	return nil
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(b []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(b)
}
