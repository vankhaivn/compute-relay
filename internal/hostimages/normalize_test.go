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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

type testMember struct {
	name string
	data []byte
}

func testTar(t *testing.T, members ...testMember) []byte {
	t.Helper()
	var b bytes.Buffer
	w := tar.NewWriter(&b)
	for _, m := range members {
		if err := w.WriteHeader(&tar.Header{Name: m.name, Mode: 0o644, Size: int64(len(m.data)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(m.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func digest(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func testLayer(t *testing.T) []byte {
	t.Helper()
	return testTar(t, testMember{"hello.txt", []byte("hello from image\n")})
}

func testConfig(layer []byte, labels map[string]string) []byte {
	config := map[string]any{
		"architecture": "amd64", "os": "linux",
		"rootfs": map[string]any{"type": "layers", "diff_ids": []string{"sha256:" + digest(layer)}},
		"config": map[string]any{"Entrypoint": []string{}, "Labels": labels},
	}
	b, _ := json.Marshal(config)
	return b
}

func dockerFixture(t *testing.T, config, layer []byte, manifest []byte, extra ...testMember) []byte {
	t.Helper()
	configName := digest(config) + ".json"
	if manifest == nil {
		manifest = []byte(fmt.Sprintf(`[{"Config":%q,"RepoTags":["foreign.example/app:latest"],"Layers":["old/layer.tar"]}]`, configName))
	}
	members := []testMember{{"old/layer.tar", layer}, {"manifest.json", manifest}, {configName, config},
		{"repositories", []byte(`{"foreign.example/app":{"latest":"old"}}`)},
		{"old/json", []byte(`{"id":"old"}`)}, {"old/VERSION", []byte("1.0")}}
	return testTar(t, append(members, extra...)...)
}

func testOptions(source []byte, temp string) Options {
	return Options{ArchiveSHA256: domain.SHA256Digest(digest(source)), ArchiveBytes: int64(len(source)),
		InstallationID: "rt_fixture", MachineID: domain.SHA256Digest(strings.Repeat("a", 64)),
		EngineID: domain.SHA256Digest(strings.Repeat("b", 64)), Architecture: "amd64", TempDir: temp}
}

func readOutput(t *testing.T, p *Prepared) map[string][]byte {
	t.Helper()
	data, err := io.ReadAll(p)
	if err != nil || int64(len(data)) != p.NormalizedBytes {
		t.Fatal("incomplete normalized archive", err, len(data), p.NormalizedBytes)
	}
	files := map[string][]byte{}
	r := tar.NewReader(bytes.NewReader(data))
	for {
		h, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if h.Typeflag == tar.TypeReg {
			if _, exists := files[h.Name]; exists {
				t.Fatal("normalized archive repeats a blob", h.Name)
			}
			files[h.Name], err = io.ReadAll(r)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	return files
}

func assertNormalizedViews(t *testing.T, files map[string][]byte, p *Prepared, architecture string) {
	t.Helper()
	type descriptor struct {
		MediaType string            `json:"mediaType"`
		Digest    string            `json:"digest"`
		Size      int               `json:"size"`
		Platform  map[string]string `json:"platform"`
	}
	var index struct {
		SchemaVersion int          `json:"schemaVersion"`
		MediaType     string       `json:"mediaType"`
		Manifests     []descriptor `json:"manifests"`
	}
	if string(files["oci-layout"]) != `{"imageLayoutVersion":"1.0.0"}` || json.Unmarshal(files["index.json"], &index) != nil ||
		index.SchemaVersion != 2 || index.MediaType != "application/vnd.oci.image.index.v1+json" || len(index.Manifests) != 1 {
		t.Fatal("normalized archive has no single OCI image")
	}
	md := index.Manifests[0]
	manifestBytes := files["blobs/sha256/"+strings.TrimPrefix(md.Digest, "sha256:")]
	if md.MediaType != ociManifestType || md.Digest != p.ManifestID || md.Digest != "sha256:"+digest(manifestBytes) ||
		md.Size != len(manifestBytes) || md.Platform["os"] != "linux" || md.Platform["architecture"] != architecture {
		t.Fatal("OCI manifest descriptor differs from frozen digest, size or platform")
	}
	var manifest struct {
		SchemaVersion int          `json:"schemaVersion"`
		MediaType     string       `json:"mediaType"`
		Config        descriptor   `json:"config"`
		Layers        []descriptor `json:"layers"`
	}
	var docker []struct {
		Config   string   `json:"Config"`
		RepoTags []string `json:"RepoTags"`
		Layers   []string `json:"Layers"`
	}
	if json.Unmarshal(manifestBytes, &manifest) != nil || manifest.SchemaVersion != 2 || manifest.MediaType != ociManifestType ||
		json.Unmarshal(files["manifest.json"], &docker) != nil || len(docker) != 1 || len(docker[0].RepoTags) != 0 ||
		len(manifest.Layers) != len(p.RootFSDiffIDs) || len(docker[0].Layers) != len(manifest.Layers) {
		t.Fatal("OCI and Docker views differ")
	}
	config := files[docker[0].Config]
	if docker[0].Config != "blobs/sha256/"+strings.TrimPrefix(p.ImageID, "sha256:") ||
		manifest.Config.Digest != p.ImageID || manifest.Config.Digest != "sha256:"+digest(config) ||
		manifest.Config.Size != len(config) || manifest.Config.MediaType != ociConfigType {
		t.Fatal("OCI and Docker views do not share the exact labelled config")
	}
	expected := map[string]bool{"oci-layout": true, "index.json": true, "manifest.json": true, docker[0].Config: true,
		"blobs/sha256/" + strings.TrimPrefix(p.ManifestID, "sha256:"): true}
	for i, d := range manifest.Layers {
		name := docker[0].Layers[i]
		layer := files[name]
		if name != "blobs/sha256/"+strings.TrimPrefix(d.Digest, "sha256:") || d.Digest != p.RootFSDiffIDs[i] ||
			d.Digest != "sha256:"+digest(layer) || d.Size != len(layer) || d.MediaType != ociTarType {
			t.Fatal("OCI and Docker views do not share verified uncompressed layers")
		}
		expected[name] = true
	}
	if len(files) != len(expected) || bytes.Contains(files["index.json"], []byte("annotations")) || bytes.Contains(manifestBytes, []byte("annotations")) {
		t.Fatal("normalized archive has unreferenced blobs or image names")
	}
}

func assertTempEmpty(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("normalizer left private temp files: %v %v", entries, err)
	}
}

func TestDockerArchiveIsLabelledAndSourceTagsAreStripped(t *testing.T) {
	layer := testLayer(t)
	config := testConfig(layer, map[string]string{"org.example.app": "fixture"})
	source := dockerFixture(t, config, layer, nil)
	dir := t.TempDir()
	opts := testOptions(source, dir)
	p, err := Normalize(context.Background(), bytes.NewReader(source), opts)
	if err != nil {
		t.Fatal(err)
	}
	if p.ArchiveSHA256 != opts.ArchiveSHA256 || len(p.RootFSDiffIDs) != 1 || p.RootFSDiffIDs[0] != "sha256:"+digest(layer) {
		t.Fatal("wrong source or rootfs identity", p)
	}
	files := readOutput(t, p)
	assertNormalizedViews(t, files, p, "amd64")
	if len(files) != 6 || files["repositories"] != nil || files["old/layer.tar"] != nil ||
		files[digest(config)+".json"] != nil || !bytes.Equal(files["blobs/sha256/"+digest(layer)], layer) {
		t.Fatal("source metadata or layer leaked into normalized archive", files)
	}
	var manifest []struct {
		Config   string   `json:"Config"`
		RepoTags []string `json:"RepoTags"`
		Layers   []string `json:"Layers"`
	}
	if json.Unmarshal(files["manifest.json"], &manifest) != nil || len(manifest) != 1 || len(manifest[0].RepoTags) != 0 ||
		manifest[0].Config != "blobs/sha256/"+strings.TrimPrefix(p.ImageID, "sha256:") {
		t.Fatal("source tags survived", manifest)
	}
	labelled := files[manifest[0].Config]
	if p.ImageID != "sha256:"+digest(labelled) {
		t.Fatal("derived image ID is not config SHA-256", p.ImageID)
	}
	var observed struct {
		Config struct {
			Labels map[string]string `json:"Labels"`
		} `json:"config"`
	}
	if json.Unmarshal(labelled, &observed) != nil || observed.Config.Labels[LabelInstallation] != string(opts.InstallationID) ||
		observed.Config.Labels[LabelArchive] != string(opts.ArchiveSHA256) || observed.Config.Labels[LabelMachine] != string(opts.MachineID) ||
		observed.Config.Labels[LabelEngine] != string(opts.EngineID) || observed.Config.Labels["org.example.app"] != "fixture" {
		t.Fatal("ownership labels missing", observed)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal("close is not idempotent", err)
	}
	assertTempEmpty(t, dir)
}

func TestImageLabelsFitCompleteHostInventory(t *testing.T) {
	layer := testLayer(t)
	for _, count := range []int{26, 27} {
		labels := make(map[string]string, count)
		for i := 0; i < count; i++ {
			labels[fmt.Sprintf("org.example.label-%d", i)] = "x"
		}
		source := dockerFixture(t, testConfig(layer, labels), layer, nil)
		prepared, err := Normalize(context.Background(), bytes.NewReader(source), testOptions(source, t.TempDir()))
		if count == 26 {
			if err != nil {
				t.Fatalf("26 source labels should fit 32 total: %v", err)
			}
			_ = prepared.Close()
		} else if !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("27 source labels should fail before load: %v", err)
		}
	}
	tooLarge := dockerFixture(t, testConfig(layer, map[string]string{"org.example.long": strings.Repeat("x", 4096)}), layer, nil)
	if _, err := Normalize(context.Background(), bytes.NewReader(tooLarge), testOptions(tooLarge, t.TempDir())); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("serialized labels over 4096 bytes should fail before load: %v", err)
	}
}

func TestRejectsMismatchedArchiveAndCleansTempFiles(t *testing.T) {
	layer := testLayer(t)
	source := dockerFixture(t, testConfig(layer, nil), layer, nil)
	for _, tc := range []struct {
		name   string
		change func(*Options)
	}{
		{"digest", func(o *Options) { o.ArchiveSHA256 = domain.SHA256Digest(strings.Repeat("f", 64)) }},
		{"short", func(o *Options) { o.ArchiveBytes-- }},
		{"long", func(o *Options) { o.ArchiveBytes++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := testOptions(source, dir)
			tc.change(&opts)
			if _, err := Normalize(context.Background(), bytes.NewReader(source), opts); !errors.Is(err, ErrDigestMismatch) {
				t.Fatal("mismatch reached normalization", err)
			}
			assertTempEmpty(t, dir)
		})
	}
}

func TestRejectsMalformedDockerArchivesBeforeRemoteLoad(t *testing.T) {
	layer := testLayer(t)
	base := testConfig(layer, nil)
	configName := digest(base) + ".json"
	for _, tc := range []struct {
		name   string
		source []byte
		want   error
	}{
		{"reserved-label", dockerFixture(t, testConfig(layer, map[string]string{LabelInstallation: "foreign"}), layer, nil), ErrInvalidArchive},
		{"wrong-platform", dockerFixture(t, bytes.Replace(base, []byte(`"amd64"`), []byte(`"arm64"`), 1), layer, nil), ErrInvalidArchive},
		{"wrong-diffid", dockerFixture(t, bytes.Replace(base, []byte(digest(layer)), []byte(strings.Repeat("c", 64)), 1), layer, nil), ErrDigestMismatch},
		{"declared-entrypoint", dockerFixture(t, bytes.Replace(base, []byte(`"Entrypoint":[]`), []byte(`"Entrypoint":["/bin/sh"]`), 1), layer, nil), ErrInvalidArchive},
		{"declared-volume", dockerFixture(t, bytes.Replace(base, []byte(`"Entrypoint":[]`), []byte(`"Entrypoint":[],"Volumes":{"/data":{}}`), 1), layer, nil), ErrInvalidArchive},
		{"duplicate-config-json", dockerFixture(t, []byte(fmt.Sprintf(`{"os":"linux","os":"linux","architecture":"amd64","rootfs":{"type":"layers","diff_ids":["sha256:%s"]}}`, digest(layer))), layer, nil), ErrInvalidArchive},
		{"duplicate-manifest-key", dockerFixture(t, base, layer, []byte(fmt.Sprintf(`[{"Config":%q,"Config":%q,"Layers":["old/layer.tar"]}]`, configName, configName))), ErrInvalidArchive},
		{"outer-path-traversal", dockerFixture(t, base, layer, nil, testMember{"../outside", []byte("bad")}), ErrInvalidArchive},
		{"duplicate-outer-member", dockerFixture(t, base, layer, nil, testMember{"old/layer.tar", layer}), ErrInvalidArchive},
		{"extra-image", dockerFixture(t, base, layer, []byte(fmt.Sprintf(`[{"Config":%q,"Layers":["old/layer.tar"]},{"Config":%q,"Layers":["old/layer.tar"]}]`, configName, configName))), ErrInvalidArchive},
		{"unexpected-member", dockerFixture(t, base, layer, nil, testMember{"foreign.txt", []byte("bad")}), ErrInvalidArchive},
		{"reserved-staging-file", func() []byte {
			conflicting := testTar(t, testMember{"relay/existing.txt", []byte("occupied")})
			return dockerFixture(t, testConfig(conflicting, nil), conflicting, nil)
		}(), ErrInvalidArchive},
		{"reserved-staging-whiteout", func() []byte {
			conflicting := testTar(t, testMember{".wh.relay", nil})
			return dockerFixture(t, testConfig(conflicting, nil), conflicting, nil)
		}(), ErrInvalidArchive},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			_, err := Normalize(context.Background(), bytes.NewReader(tc.source), testOptions(tc.source, dir))
			if !errors.Is(err, tc.want) {
				t.Fatal("unexpected failure", err, "wanted", tc.want)
			}
			assertTempEmpty(t, dir)
		})
	}
}

func TestLimitsAreEnforcedWithoutLeavingTempFiles(t *testing.T) {
	layer := testLayer(t)
	source := dockerFixture(t, testConfig(layer, nil), layer, nil)
	for _, tc := range []struct {
		name   string
		change func(*Limits)
	}{
		{"input", func(l *Limits) { l.MaxInputBytes = int64(len(source) - 1) }},
		{"output", func(l *Limits) { l.MaxOutputBytes = 512 }},
		{"members", func(l *Limits) { l.MaxMembers = 2 }},
		{"metadata", func(l *Limits) { l.MaxMetadataBytes = 32 }},
		{"layers", func(l *Limits) { l.MaxLayers = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := testOptions(source, dir)
			opts.Limits = DefaultLimits()
			tc.change(&opts.Limits)
			_, err := Normalize(context.Background(), bytes.NewReader(source), opts)
			if !errors.Is(err, ErrLimitExceeded) {
				t.Fatal("limit not enforced", err)
			}
			want := map[string]string{"input": "image_options_invalid", "output": "image_archive_write_failed", "members": "image_archive_scan_failed", "metadata": "image_archive_parse_failed", "layers": "image_options_invalid"}[tc.name]
			if provider.Reason(err, "") != want {
				t.Fatal("lost limit step", tc.name, err)
			}
			assertTempEmpty(t, dir)
		})
	}
}

func TestOCIArchiveGzipNormalizesToDockerArchive(t *testing.T) {
	layer := testLayer(t)
	config := testConfig(layer, map[string]string{"org.example.app": "oci"})
	source := ociFixture(t, config, layer, ociGzipType, "amd64")
	dir := t.TempDir()
	p, err := Normalize(context.Background(), bytes.NewReader(source), testOptions(source, dir))
	if err != nil {
		t.Fatal(err)
	}
	files := readOutput(t, p)
	assertNormalizedViews(t, files, p, "amd64")
	if !bytes.Equal(files["blobs/sha256/"+digest(layer)], layer) || len(files) != 6 || p.ImageID != "sha256:"+digest(files["blobs/sha256/"+strings.TrimPrefix(p.ImageID, "sha256:")]) {
		t.Fatal("OCI conversion changed uncompressed layer or config ID")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	assertTempEmpty(t, dir)
}

func TestOCIRejectsPlatformFormatAndExpansionLimit(t *testing.T) {
	layer := testLayer(t)
	config := testConfig(layer, nil)
	for _, tc := range []struct {
		name   string
		source []byte
		limit  bool
		want   error
	}{
		{"wrong-platform", ociFixture(t, config, layer, ociGzipType, "arm64"), false, ErrInvalidArchive},
		{"unsupported-compression", ociFixture(t, config, layer, "application/vnd.oci.image.layer.v1.tar+zstd", "amd64"), false, ErrInvalidArchive},
		{"expansion", ociFixture(t, config, layer, ociGzipType, "amd64"), true, ErrLimitExceeded},
		{"reserved-staging-file", func() []byte {
			conflicting := testTar(t, testMember{"./relay/existing.txt", []byte("occupied")})
			return ociFixture(t, testConfig(conflicting, nil), conflicting, ociGzipType, "amd64")
		}(), false, ErrInvalidArchive},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := testOptions(tc.source, dir)
			if tc.limit {
				opts.Limits = DefaultLimits()
				opts.Limits.MaxOutputBytes = int64(len(layer) - 1)
			}
			_, err := Normalize(context.Background(), bytes.NewReader(tc.source), opts)
			if !errors.Is(err, tc.want) {
				t.Fatal("unexpected failure", err, "wanted", tc.want)
			}
			assertTempEmpty(t, dir)
		})
	}
}

func ociFixture(t *testing.T, config, layer []byte, layerMedia, platformArch string) []byte {
	t.Helper()
	var zipped bytes.Buffer
	gz := gzip.NewWriter(&zipped)
	if _, err := gz.Write(layer); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	blob := layer
	if layerMedia != ociTarType {
		blob = zipped.Bytes()
	}
	layerDesc := map[string]any{"mediaType": layerMedia, "digest": "sha256:" + digest(blob), "size": len(blob)}
	manifest := map[string]any{"schemaVersion": 2,
		"config": map[string]any{"mediaType": ociConfigType, "digest": "sha256:" + digest(config), "size": len(config)},
		"layers": []any{layerDesc}}
	manifestJSON, _ := json.Marshal(manifest)
	index := map[string]any{"schemaVersion": 2, "manifests": []any{map[string]any{
		"mediaType": ociManifestType, "digest": "sha256:" + digest(manifestJSON), "size": len(manifestJSON),
		"platform": map[string]string{"os": "linux", "architecture": platformArch},
	}}}
	indexJSON, _ := json.Marshal(index)
	return testTar(t,
		testMember{"blobs/sha256/" + digest(blob), blob},
		testMember{"blobs/sha256/" + digest(config), config},
		testMember{"index.json", indexJSON},
		testMember{"oci-layout", []byte(`{"imageLayoutVersion":"1.0.0"}`)},
		testMember{"blobs/sha256/" + digest(manifestJSON), manifestJSON},
	)
}

func TestCloseRemovesOnlyItsOwnTempArchive(t *testing.T) {
	layer := testLayer(t)
	source := dockerFixture(t, testConfig(layer, nil), layer, nil)
	dir := t.TempDir()
	keep := filepath.Join(dir, "keep.txt")
	if err := os.WriteFile(keep, []byte("other"), 0o600); err != nil {
		t.Fatal(err)
	}
	p, err := Normalize(context.Background(), bytes.NewReader(source), testOptions(source, dir))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(keep); err != nil || string(b) != "other" {
		t.Fatal("normalizer modified unrelated local file", err)
	}
}

func TestNormalizationIsDeterministicForFrozenOwnership(t *testing.T) {
	layer := testLayer(t)
	source := dockerFixture(t, testConfig(layer, nil), layer, nil)
	dir := t.TempDir()
	opts := testOptions(source, dir)
	first, err := Normalize(context.Background(), bytes.NewReader(source), opts)
	if err != nil {
		t.Fatal(err)
	}
	a, err := io.ReadAll(first)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := Normalize(context.Background(), bytes.NewReader(source), opts)
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(second)
	if err != nil {
		t.Fatal(err)
	}
	if first.ImageID != second.ImageID || first.ManifestID != second.ManifestID || !bytes.Equal(a, b) {
		t.Fatal("same frozen source and ownership produced different Docker bytes")
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	assertTempEmpty(t, dir)
}

func TestNormalizedArchiveStoresRepeatedLayersOnce(t *testing.T) {
	layer := testLayer(t)
	var config map[string]any
	if err := json.Unmarshal(testConfig(layer, nil), &config); err != nil {
		t.Fatal(err)
	}
	config["rootfs"] = map[string]any{"type": "layers", "diff_ids": []string{"sha256:" + digest(layer), "sha256:" + digest(layer)}}
	configBytes, _ := json.Marshal(config)
	manifest, _ := json.Marshal([]map[string]any{{"Config": digest(configBytes) + ".json", "RepoTags": []string{}, "Layers": []string{"old/layer.tar", "other/layer.tar"}}})
	source := dockerFixture(t, configBytes, layer, manifest, testMember{"other/layer.tar", layer})
	p, err := Normalize(t.Context(), bytes.NewReader(source), testOptions(source, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	files := readOutput(t, p)
	assertNormalizedViews(t, files, p, "amd64")
	if len(files) != 6 || len(p.RootFSDiffIDs) != 2 {
		t.Fatal("repeated layer was copied or lost")
	}
}
