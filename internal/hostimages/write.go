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
	"strconv"
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

func writeDockerArchive(ctx context.Context, input, output *os.File, image parsedImage, limits Limits) (int64, error) {
	capped := &cappedWriter{w: output, max: limits.MaxOutputBytes}
	tw := tar.NewWriter(capped)
	configName := image.id[len("sha256:"):] + ".json"
	if err := writeBytes(tw, configName, image.config); err != nil {
		return 0, err
	}
	if err := writeDir(tw, "layers/"); err != nil {
		return 0, err
	}
	paths := make([]string, 0, len(image.layers))
	for i, layer := range image.layers {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		dir := "layers/" + strconv.FormatInt(int64(i), 10) + "/"
		if err := writeDir(tw, dir); err != nil {
			return 0, err
		}
		name := dir + "layer.tar"
		paths = append(paths, name)
		if err := tw.WriteHeader(&tar.Header{Name: name, Typeflag: tar.TypeReg, Mode: 0o644, Size: layer.size, Format: tar.FormatUSTAR}); err != nil {
			return 0, err
		}
		if err := writeLayer(ctx, tw, input, layer, image.diffIDs[i]); err != nil {
			return 0, err
		}
	}
	manifest, err := json.Marshal([]struct {
		Config   string   `json:"Config"`
		RepoTags []string `json:"RepoTags"`
		Layers   []string `json:"Layers"`
	}{{Config: configName, RepoTags: []string{}, Layers: paths}})
	if err != nil {
		return 0, ErrInvalidArchive
	}
	if err := writeBytes(tw, "manifest.json", manifest); err != nil {
		return 0, err
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
	configName := image.id[len("sha256:"):] + ".json"
	config, err := readMetadata(output, entries, configName, limits.MaxMetadataBytes)
	if err != nil || !bytes.Equal(config, image.config) {
		return ErrInvalidArchive
	}
	paths := make([]string, len(image.layers))
	expected := map[string]bool{configName: true, "manifest.json": true}
	for i, layer := range image.layers {
		name := "layers/" + strconv.Itoa(i) + "/layer.tar"
		paths[i] = name
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
	manifest, err := readMetadata(output, entries, "manifest.json", limits.MaxMetadataBytes)
	if err != nil {
		return err
	}
	want, err := json.Marshal([]struct {
		Config   string   `json:"Config"`
		RepoTags []string `json:"RepoTags"`
		Layers   []string `json:"Layers"`
	}{{Config: configName, RepoTags: []string{}, Layers: paths}})
	if err != nil || !bytes.Equal(manifest, want) {
		return ErrInvalidArchive
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
