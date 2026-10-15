package hostimages

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path"
	"slices"
	"strings"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/jsonwire"
)

type member struct {
	offset int64
	size   int64
}

type archiveEntries struct {
	files map[string]member
	dirs  map[string]bool
}

func validOuterPath(name string, directory bool) (string, bool) {
	if directory {
		name = strings.TrimSuffix(name, "/")
	}
	if name == "" || strings.HasPrefix(name, "/") || strings.ContainsRune(name, '\\') ||
		strings.ContainsRune(name, '\x00') || path.Clean(name) != name || name == "." ||
		strings.HasPrefix(name, "../") || name == ".." {
		return "", false
	}
	return name, true
}

func scanArchive(ctx context.Context, f *os.File, limits Limits) (archiveEntries, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return archiveEntries{}, err
	}
	entries := archiveEntries{files: map[string]member{}, dirs: map[string]bool{}}
	tr := tar.NewReader(f)
	count := 0
	var logicalBytes int64
	for {
		if err := ctx.Err(); err != nil {
			return archiveEntries{}, err
		}
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return archiveEntries{}, ErrInvalidArchive
		}
		count++
		if count > limits.MaxMembers {
			return archiveEntries{}, ErrLimitExceeded
		}
		if h.Size < 0 || h.Size > limits.MaxInputBytes-logicalBytes {
			return archiveEntries{}, ErrLimitExceeded
		}
		logicalBytes += h.Size
		for key := range h.PAXRecords {
			if strings.HasPrefix(key, "GNU.sparse.") {
				return archiveEntries{}, ErrInvalidArchive
			}
		}
		dir := h.Typeflag == tar.TypeDir
		name, ok := validOuterPath(h.Name, dir)
		if !ok || entries.dirs[name] || entries.files[name].size != 0 || hasMember(entries.files, name) {
			return archiveEntries{}, ErrInvalidArchive
		}
		switch h.Typeflag {
		case tar.TypeDir:
			if h.Size != 0 {
				return archiveEntries{}, ErrInvalidArchive
			}
			entries.dirs[name] = true
		case tar.TypeReg, tar.TypeRegA:
			if h.Size < 0 {
				return archiveEntries{}, ErrInvalidArchive
			}
			offset, err := f.Seek(0, io.SeekCurrent)
			if err != nil {
				return archiveEntries{}, err
			}
			entries.files[name] = member{offset: offset, size: h.Size}
			if _, err := io.CopyN(io.Discard, contextReader{ctx, tr}, h.Size); err != nil {
				if ctx.Err() != nil {
					return archiveEntries{}, ctx.Err()
				}
				return archiveEntries{}, ErrInvalidArchive
			}
		default:
			return archiveEntries{}, ErrInvalidArchive
		}
	}
	// Tar readers stop at the end marker. Never let hidden data after it affect
	// source identity without being part of the validated structure.
	var tail [32 << 10]byte
	for {
		if err := ctx.Err(); err != nil {
			return archiveEntries{}, err
		}
		n, err := f.Read(tail[:])
		for _, b := range tail[:n] {
			if b != 0 {
				return archiveEntries{}, ErrInvalidArchive
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return archiveEntries{}, err
		}
	}
	return entries, nil
}

func hasMember(m map[string]member, name string) bool { _, ok := m[name]; return ok }

func (e archiveEntries) exactFiles(expected map[string]bool) error {
	if len(e.files) != len(expected) {
		return ErrInvalidArchive
	}
	for name := range e.files {
		if !expected[name] {
			return ErrInvalidArchive
		}
	}
	for dir := range e.dirs {
		found := false
		for file := range expected {
			if strings.HasPrefix(file, dir+"/") {
				found = true
				break
			}
		}
		if !found {
			return ErrInvalidArchive
		}
	}
	return nil
}

func readMetadata(f *os.File, e archiveEntries, name string, limit int64) ([]byte, error) {
	m, ok := e.files[name]
	if !ok || m.size < 1 {
		return nil, ErrInvalidArchive
	}
	if m.size > limit {
		return nil, ErrLimitExceeded
	}
	b := make([]byte, int(m.size))
	if _, err := io.ReadFull(io.NewSectionReader(f, m.offset, m.size), b); err != nil {
		return nil, ErrInvalidArchive
	}
	return b, nil
}

func strictObject(raw []byte, limit int64) (map[string]json.RawMessage, error) {
	if int64(len(raw)) > limit {
		return nil, ErrLimitExceeded
	}
	m, err := jsonwire.Object(raw, int(limit))
	if err != nil {
		return nil, ErrInvalidArchive
	}
	return m, nil
}

func strictArray(raw []byte, limit int64) ([]json.RawMessage, error) {
	if int64(len(raw)) > limit {
		return nil, ErrLimitExceeded
	}
	wrapped := make([]byte, 0, len(raw)+8)
	wrapped = append(wrapped, []byte(`{"array":`)...)
	wrapped = append(wrapped, raw...)
	wrapped = append(wrapped, '}')
	if _, err := jsonwire.Object(wrapped, int(limit)+8); err != nil {
		return nil, ErrInvalidArchive
	}
	var a []json.RawMessage
	if json.Unmarshal(raw, &a) != nil || a == nil {
		return nil, ErrInvalidArchive
	}
	return a, nil
}

func fields(m map[string]json.RawMessage, required, optional []string) error {
	allowed := map[string]bool{}
	for _, name := range required {
		if len(m[name]) == 0 || string(m[name]) == "null" {
			return ErrInvalidArchive
		}
		allowed[name] = true
	}
	for _, name := range optional {
		allowed[name] = true
	}
	for name := range m {
		if !allowed[name] {
			return ErrInvalidArchive
		}
	}
	return nil
}

func stringField(m map[string]json.RawMessage, name string) (string, error) {
	var s string
	if len(m[name]) == 0 || json.Unmarshal(m[name], &s) != nil || s == "" {
		return "", ErrInvalidArchive
	}
	return s, nil
}

func intField(m map[string]json.RawMessage, name string) (int64, error) {
	var n int64
	if len(m[name]) == 0 || json.Unmarshal(m[name], &n) != nil || n < 0 {
		return 0, ErrInvalidArchive
	}
	return n, nil
}

func hashMember(ctx context.Context, f *os.File, m member) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, contextReader{ctx, io.NewSectionReader(f, m.offset, m.size)}); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

type layerSource struct {
	member     member
	compressed bool
	size       int64 // uncompressed bytes
}

type parsedImage struct {
	config  []byte
	id      string
	diffIDs []string
	layers  []layerSource
}

func parseImage(ctx context.Context, f *os.File, e archiveEntries, opts Options, limits Limits) (parsedImage, error) {
	_, docker := e.files["manifest.json"]
	_, oci := e.files["index.json"]
	if docker == oci {
		return parsedImage{}, ErrInvalidArchive
	}
	if docker {
		return parseDocker(ctx, f, e, opts, limits)
	}
	return parseOCI(ctx, f, e, opts, limits)
}

func parseDocker(ctx context.Context, f *os.File, e archiveEntries, opts Options, limits Limits) (parsedImage, error) {
	raw, err := readMetadata(f, e, "manifest.json", limits.MaxMetadataBytes)
	if err != nil {
		return parsedImage{}, err
	}
	items, err := strictArray(raw, limits.MaxMetadataBytes)
	if err != nil || len(items) != 1 {
		return parsedImage{}, ErrInvalidArchive
	}
	m, err := strictObject(items[0], limits.MaxMetadataBytes)
	if err != nil || fields(m, []string{"Config", "Layers"}, []string{"RepoTags"}) != nil {
		return parsedImage{}, ErrInvalidArchive
	}
	configPath, err := stringField(m, "Config")
	if err != nil {
		return parsedImage{}, err
	}
	if _, ok := validOuterPath(configPath, false); !ok {
		return parsedImage{}, ErrInvalidArchive
	}
	var layerPaths []string
	if json.Unmarshal(m["Layers"], &layerPaths) != nil || len(layerPaths) < 1 || len(layerPaths) > limits.MaxLayers {
		return parsedImage{}, ErrInvalidArchive
	}
	if tags := m["RepoTags"]; len(tags) != 0 && string(tags) != "null" {
		var sourceTags []string
		if json.Unmarshal(tags, &sourceTags) != nil || len(sourceTags) > 128 {
			return parsedImage{}, ErrInvalidArchive
		}
	}
	config, err := readMetadata(f, e, configPath, limits.MaxMetadataBytes)
	if err != nil {
		return parsedImage{}, err
	}
	configDigest := strings.TrimSuffix(configPath, ".json")
	if strings.HasPrefix(configPath, "blobs/sha256/") {
		configDigest = strings.TrimPrefix(configPath, "blobs/sha256/")
	}
	if !domain.SHA256Digest(configDigest).Valid() ||
		configPath != configDigest+".json" && configPath != "blobs/sha256/"+configDigest {
		return parsedImage{}, ErrInvalidArchive
	}
	actual := sha256.Sum256(config)
	if hex.EncodeToString(actual[:]) != configDigest {
		return parsedImage{}, ErrDigestMismatch
	}
	image, err := rewriteConfig(config, opts, limits)
	if err != nil {
		return parsedImage{}, err
	}
	if len(image.diffIDs) != len(layerPaths) {
		return parsedImage{}, ErrInvalidArchive
	}
	expected := map[string]bool{"manifest.json": true, configPath: true}
	if _, ok := e.files["repositories"]; ok {
		if _, err := readMetadata(f, e, "repositories", limits.MaxMetadataBytes); err != nil {
			return parsedImage{}, err
		}
		expected["repositories"] = true
	}
	for i, name := range layerPaths {
		if _, ok := validOuterPath(name, false); !ok || expected[name] {
			return parsedImage{}, ErrInvalidArchive
		}
		layer, ok := e.files[name]
		if !ok || layer.size < 1 {
			return parsedImage{}, ErrInvalidArchive
		}
		expected[name] = true
		if strings.HasSuffix(name, "/layer.tar") {
			prefix := strings.TrimSuffix(name, "/layer.tar")
			for _, legacy := range []string{prefix + "/VERSION", prefix + "/json"} {
				if _, ok := e.files[legacy]; ok {
					if _, err := readMetadata(f, e, legacy, limits.MaxMetadataBytes); err != nil {
						return parsedImage{}, err
					}
					expected[legacy] = true
				}
			}
		}
		digest, err := hashMember(ctx, f, layer)
		if err != nil {
			return parsedImage{}, err
		}
		if "sha256:"+digest != image.diffIDs[i] {
			return parsedImage{}, ErrDigestMismatch
		}
		item := layerSource{member: layer, size: layer.size}
		if err := checkReservedRoot(ctx, f, item); err != nil {
			return parsedImage{}, err
		}
		image.layers = append(image.layers, item)
	}
	if err := e.exactFiles(expected); err != nil {
		return parsedImage{}, err
	}
	return image, nil
}

const (
	ociManifestType    = "application/vnd.oci.image.manifest.v1+json"
	dockerManifestType = "application/vnd.docker.distribution.manifest.v2+json"
	ociConfigType      = "application/vnd.oci.image.config.v1+json"
	dockerConfigType   = "application/vnd.docker.container.image.v1+json"
	ociTarType         = "application/vnd.oci.image.layer.v1.tar"
	ociGzipType        = "application/vnd.oci.image.layer.v1.tar+gzip"
	dockerGzipType     = "application/vnd.docker.image.rootfs.diff.tar.gzip"
)

type descriptor struct {
	mediaType string
	digest    string
	size      int64
}

func parseDescriptor(raw []byte, limit int64) (descriptor, map[string]json.RawMessage, error) {
	m, err := strictObject(raw, limit)
	if err != nil || fields(m, []string{"mediaType", "digest", "size"}, []string{"platform", "annotations"}) != nil {
		return descriptor{}, nil, ErrInvalidArchive
	}
	media, err := stringField(m, "mediaType")
	if err != nil {
		return descriptor{}, nil, err
	}
	digest, err := stringField(m, "digest")
	if err != nil || !strings.HasPrefix(digest, "sha256:") || !domain.SHA256Digest(strings.TrimPrefix(digest, "sha256:")).Valid() {
		return descriptor{}, nil, ErrInvalidArchive
	}
	size, err := intField(m, "size")
	if err != nil || size < 1 {
		return descriptor{}, nil, ErrInvalidArchive
	}
	return descriptor{media, digest, size}, m, nil
}

func (d descriptor) blob(ctx context.Context, f *os.File, e archiveEntries, expected map[string]bool) (member, error) {
	name := "blobs/sha256/" + strings.TrimPrefix(d.digest, "sha256:")
	m, ok := e.files[name]
	if !ok || m.size != d.size {
		return member{}, ErrInvalidArchive
	}
	h, err := hashMember(ctx, f, m)
	if err != nil {
		return member{}, err
	}
	if "sha256:"+h != d.digest {
		return member{}, ErrDigestMismatch
	}
	expected[name] = true
	return m, nil
}

func parseOCI(ctx context.Context, f *os.File, e archiveEntries, opts Options, limits Limits) (parsedImage, error) {
	layout, err := readMetadata(f, e, "oci-layout", limits.MaxMetadataBytes)
	if err != nil {
		return parsedImage{}, err
	}
	layoutMap, err := strictObject(layout, limits.MaxMetadataBytes)
	if err != nil || fields(layoutMap, []string{"imageLayoutVersion"}, nil) != nil {
		return parsedImage{}, ErrInvalidArchive
	}
	version, err := stringField(layoutMap, "imageLayoutVersion")
	if err != nil || version != "1.0.0" {
		return parsedImage{}, ErrInvalidArchive
	}
	index, err := readMetadata(f, e, "index.json", limits.MaxMetadataBytes)
	if err != nil {
		return parsedImage{}, err
	}
	indexMap, err := strictObject(index, limits.MaxMetadataBytes)
	if err != nil || fields(indexMap, []string{"schemaVersion", "manifests"}, []string{"mediaType", "annotations"}) != nil ||
		string(indexMap["schemaVersion"]) != "2" {
		return parsedImage{}, ErrInvalidArchive
	}
	manifests, err := strictArray(indexMap["manifests"], limits.MaxMetadataBytes)
	if err != nil || len(manifests) != 1 {
		return parsedImage{}, ErrInvalidArchive
	}
	manifestDesc, descMap, err := parseDescriptor(manifests[0], limits.MaxMetadataBytes)
	if err != nil || !slices.Contains([]string{ociManifestType, dockerManifestType}, manifestDesc.mediaType) {
		return parsedImage{}, ErrInvalidArchive
	}
	if platform, ok := descMap["platform"]; ok {
		p, err := strictObject(platform, limits.MaxMetadataBytes)
		if err != nil || fields(p, []string{"os", "architecture"}, []string{"variant", "os.version", "os.features"}) != nil {
			return parsedImage{}, ErrInvalidArchive
		}
		osName, e1 := stringField(p, "os")
		arch, e2 := stringField(p, "architecture")
		if e1 != nil || e2 != nil || osName != "linux" || arch != opts.Architecture {
			return parsedImage{}, ErrInvalidArchive
		}
	}
	expected := map[string]bool{"oci-layout": true, "index.json": true}
	manifestMember, err := manifestDesc.blob(ctx, f, e, expected)
	if err != nil {
		return parsedImage{}, err
	}
	if manifestMember.size > limits.MaxMetadataBytes {
		return parsedImage{}, ErrLimitExceeded
	}
	manifest, err := readMetadata(f, e, "blobs/sha256/"+strings.TrimPrefix(manifestDesc.digest, "sha256:"), limits.MaxMetadataBytes)
	if err != nil {
		return parsedImage{}, err
	}
	manifestMap, err := strictObject(manifest, limits.MaxMetadataBytes)
	if err != nil || fields(manifestMap, []string{"schemaVersion", "config", "layers"}, []string{"mediaType", "annotations"}) != nil ||
		string(manifestMap["schemaVersion"]) != "2" {
		return parsedImage{}, ErrInvalidArchive
	}
	configDesc, _, err := parseDescriptor(manifestMap["config"], limits.MaxMetadataBytes)
	if err != nil || !slices.Contains([]string{ociConfigType, dockerConfigType}, configDesc.mediaType) {
		return parsedImage{}, ErrInvalidArchive
	}
	configMember, err := configDesc.blob(ctx, f, e, expected)
	if err != nil {
		return parsedImage{}, err
	}
	if configMember.size > limits.MaxMetadataBytes {
		return parsedImage{}, ErrLimitExceeded
	}
	config, err := readMetadata(f, e, "blobs/sha256/"+strings.TrimPrefix(configDesc.digest, "sha256:"), limits.MaxMetadataBytes)
	if err != nil {
		return parsedImage{}, err
	}
	image, err := rewriteConfig(config, opts, limits)
	if err != nil {
		return parsedImage{}, err
	}
	layers, err := strictArray(manifestMap["layers"], limits.MaxMetadataBytes)
	if err != nil || len(layers) < 1 || len(layers) > limits.MaxLayers || len(layers) != len(image.diffIDs) {
		return parsedImage{}, ErrInvalidArchive
	}
	var expanded int64
	for i, raw := range layers {
		d, _, err := parseDescriptor(raw, limits.MaxMetadataBytes)
		if err != nil || !slices.Contains([]string{ociTarType, ociGzipType, dockerGzipType}, d.mediaType) {
			return parsedImage{}, ErrInvalidArchive
		}
		m, err := d.blob(ctx, f, e, expected)
		if err != nil {
			return parsedImage{}, err
		}
		compressed := d.mediaType != ociTarType
		size, diffDigest := m.size, d.digest
		if compressed {
			size, diffDigest, err = measureGzip(ctx, f, m, limits.MaxOutputBytes-expanded)
			if err != nil {
				return parsedImage{}, err
			}
		}
		if size < 1 || size > limits.MaxOutputBytes-expanded {
			return parsedImage{}, ErrLimitExceeded
		}
		if diffDigest != image.diffIDs[i] {
			return parsedImage{}, ErrDigestMismatch
		}
		expanded += size
		item := layerSource{member: m, compressed: compressed, size: size}
		if err := checkReservedRoot(ctx, f, item); err != nil {
			return parsedImage{}, err
		}
		image.layers = append(image.layers, item)
	}
	if err := e.exactFiles(expected); err != nil {
		return parsedImage{}, err
	}
	return image, nil
}

func measureGzip(ctx context.Context, f *os.File, m member, max int64) (int64, string, error) {
	if max < 1 {
		return 0, "", ErrLimitExceeded
	}
	gz, err := gzip.NewReader(io.NewSectionReader(f, m.offset, m.size))
	if err != nil {
		return 0, "", ErrInvalidArchive
	}
	defer gz.Close()
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(contextReader{ctx, gz}, max+1))
	if ctx.Err() != nil {
		return 0, "", ctx.Err()
	}
	if n > max {
		return 0, "", ErrLimitExceeded
	}
	if err != nil {
		return 0, "", ErrInvalidArchive
	}
	return n, "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}
