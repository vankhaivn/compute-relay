package hostimages

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
)

type cappedWriter struct {
	w     io.Writer
	max   int64
	bytes int64
}

func (c *cappedWriter) Write(b []byte) (int, error) {
	if int64(len(b)) > c.max-c.bytes {
		return 0, ErrLimitExceeded
	}
	n, err := c.w.Write(b)
	c.bytes += int64(n)
	return n, err
}

type archiveDescriptor struct {
	MediaType string           `json:"mediaType"`
	Digest    string           `json:"digest"`
	Size      int64            `json:"size"`
	Platform  *archivePlatform `json:"platform,omitempty"`
}

type archivePlatform struct {
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
}

type archiveMetadata struct {
	configPath, manifestID  string
	layerPaths              []string
	manifest, index, docker []byte
}

func blobPath(digest string) string { return "blobs/sha256/" + strings.TrimPrefix(digest, "sha256:") }

// Both archive views share the same blobs. Supplying an OCI manifest keeps
// containerd from rebuilding it with whichever compressed layers it already has.
func normalizedMetadata(image parsedImage) archiveMetadata {
	m := archiveMetadata{configPath: blobPath(image.id), layerPaths: make([]string, len(image.layers))}
	layers := make([]archiveDescriptor, len(image.layers))
	for i, layer := range image.layers {
		m.layerPaths[i] = blobPath(image.diffIDs[i])
		layers[i] = archiveDescriptor{MediaType: ociTarType, Digest: image.diffIDs[i], Size: layer.size}
	}
	m.manifest, _ = json.Marshal(struct {
		SchemaVersion int                 `json:"schemaVersion"`
		MediaType     string              `json:"mediaType"`
		Config        archiveDescriptor   `json:"config"`
		Layers        []archiveDescriptor `json:"layers"`
	}{2, ociManifestType, archiveDescriptor{MediaType: ociConfigType, Digest: image.id, Size: int64(len(image.config))}, layers})
	hash := sha256.Sum256(m.manifest)
	m.manifestID = "sha256:" + hex.EncodeToString(hash[:])
	m.index, _ = json.Marshal(struct {
		SchemaVersion int                 `json:"schemaVersion"`
		MediaType     string              `json:"mediaType"`
		Manifests     []archiveDescriptor `json:"manifests"`
	}{2, "application/vnd.oci.image.index.v1+json", []archiveDescriptor{{MediaType: ociManifestType,
		Digest: m.manifestID, Size: int64(len(m.manifest)), Platform: &archivePlatform{OS: "linux", Architecture: image.architecture}}}})
	m.docker, _ = json.Marshal([]struct {
		Config   string   `json:"Config"`
		RepoTags []string `json:"RepoTags"`
		Layers   []string `json:"Layers"`
	}{{Config: m.configPath, RepoTags: []string{}, Layers: m.layerPaths}})
	return m
}

func writeDockerArchive(ctx context.Context, input, output *os.File, image parsedImage, metadata archiveMetadata, limits Limits) (int64, error) {
	capped := &cappedWriter{w: output, max: limits.MaxOutputBytes}
	tw := tar.NewWriter(capped)
	for _, dir := range []string{"blobs/", "blobs/sha256/"} {
		if err := writeDir(tw, dir); err != nil {
			return 0, err
		}
	}
	if err := writeBytes(tw, metadata.configPath, image.config); err != nil {
		return 0, err
	}
	for _, file := range []struct {
		name string
		data []byte
	}{{blobPath(metadata.manifestID), metadata.manifest}, {"oci-layout", []byte(`{"imageLayoutVersion":"1.0.0"}`)},
		{"index.json", metadata.index}, {"manifest.json", metadata.docker}} {
		if err := writeBytes(tw, file.name, file.data); err != nil {
			return 0, err
		}
	}
	written := map[string]bool{metadata.configPath: true, blobPath(metadata.manifestID): true}
	for i, layer := range image.layers {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		name := metadata.layerPaths[i]
		if written[name] {
			continue
		}
		written[name] = true
		if err := tw.WriteHeader(&tar.Header{Name: name, Typeflag: tar.TypeReg, Mode: 0o644, Size: layer.size, Format: tar.FormatUSTAR}); err != nil {
			return 0, err
		}
		if err := writeLayer(ctx, tw, input, layer, image.diffIDs[i]); err != nil {
			return 0, err
		}
	}
	if err := tw.Close(); err != nil {
		return 0, err
	}
	return capped.bytes, nil
}

// verifyCompletedArchive reads the closed tar back from disk before a caller can
// commit its one-shot remote load permit. A successful write alone does not prove
// that the bytes subsequently streamed to Docker are the validated image.
func verifyCompletedArchive(ctx context.Context, output *os.File, image parsedImage, size int64, limits Limits) error {
	stat, err := output.Stat()
	if err != nil {
		return err
	}
	if size < 1 || size > limits.MaxOutputBytes || stat.Size() != size {
		return ErrInvalidArchive
	}
	archiveLimits := limits
	archiveLimits.MaxInputBytes = limits.MaxOutputBytes
	entries, err := scanArchive(ctx, output, archiveLimits)
	if err != nil {
		return err
	}
	metadata := normalizedMetadata(image)
	config, err := readMetadata(output, entries, metadata.configPath, limits.MaxMetadataBytes)
	if err != nil || !bytes.Equal(config, image.config) {
		return ErrInvalidArchive
	}
	expected := map[string]bool{metadata.configPath: true}
	for _, file := range []struct {
		name string
		data []byte
	}{{blobPath(metadata.manifestID), metadata.manifest}, {"oci-layout", []byte(`{"imageLayoutVersion":"1.0.0"}`)},
		{"index.json", metadata.index}, {"manifest.json", metadata.docker}} {
		raw, err := readMetadata(output, entries, file.name, limits.MaxMetadataBytes)
		if err != nil || !bytes.Equal(raw, file.data) {
			return ErrInvalidArchive
		}
		expected[file.name] = true
	}
	for i, layer := range image.layers {
		name := metadata.layerPaths[i]
		member, ok := entries.files[name]
		if !ok || member.size != layer.size {
			return ErrInvalidArchive
		}
		digest, err := hashMember(ctx, output, member)
		if err != nil || "sha256:"+digest != image.diffIDs[i] {
			return ErrInvalidArchive
		}
		expected[name] = true
	}
	if err := entries.exactFiles(expected); err != nil {
		return err
	}
	return nil
}

func writeDir(tw *tar.Writer, name string) error {
	return tw.WriteHeader(&tar.Header{Name: name, Typeflag: tar.TypeDir, Mode: 0o755, Format: tar.FormatUSTAR})
}

func writeBytes(tw *tar.Writer, name string, b []byte) error {
	if err := tw.WriteHeader(&tar.Header{Name: name, Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(b)), Format: tar.FormatUSTAR}); err != nil {
		return err
	}
	_, err := tw.Write(b)
	return err
}

func writeLayer(ctx context.Context, tw *tar.Writer, input *os.File, layer layerSource, expected string) error {
	var reader io.Reader = io.NewSectionReader(input, layer.member.offset, layer.member.size)
	var gz *gzip.Reader
	if layer.compressed {
		var err error
		gz, err = gzip.NewReader(reader)
		if err != nil {
			return ErrInvalidArchive
		}
		defer gz.Close()
		reader = gz
	}
	h := sha256.New()
	n, err := io.CopyN(io.MultiWriter(tw, h), contextReader{ctx, reader}, layer.size)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, ErrLimitExceeded) {
		return err
	}
	if err != nil || n != layer.size {
		return ErrInvalidArchive
	}
	if layer.compressed {
		var extra [1]byte
		if n, err := gz.Read(extra[:]); n != 0 || err != io.EOF {
			return ErrInvalidArchive
		}
	}
	if "sha256:"+hex.EncodeToString(h.Sum(nil)) != expected {
		return ErrDigestMismatch
	}
	return nil
}
