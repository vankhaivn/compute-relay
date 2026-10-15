package hostimages

import (
	"bytes"
	"context"
	"os"
	"testing"
)

func TestCompletedArchiveVerifiesBothViewsAndEveryBlob(t *testing.T) {
	layer := testLayer(t)
	source := dockerFixture(t, testConfig(layer, nil), layer, nil)
	opts := testOptions(source, t.TempDir())
	input, err := os.CreateTemp(t.TempDir(), "source-*")
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	if _, err := input.Write(source); err != nil {
		t.Fatal(err)
	}
	entries, err := scanArchive(t.Context(), input, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	image, err := parseImage(t.Context(), input, entries, opts, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	p, err := Normalize(t.Context(), bytes.NewReader(source), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	files := readOutput(t, p)
	metadata := normalizedMetadata(image)
	for _, tc := range []struct {
		name string
		edit func(map[string][]byte)
	}{
		{"exact", func(map[string][]byte) {}},
		{"config", func(files map[string][]byte) { files[metadata.configPath][0] ^= 1 }},
		{"layer", func(files map[string][]byte) { files[metadata.layerPaths[0]][0] ^= 1 }},
		{"oci-manifest", func(files map[string][]byte) { files[blobPath(metadata.manifestID)][0] ^= 1 }},
		{"oci-index", func(files map[string][]byte) {
			files["index.json"] = bytes.Replace(files["index.json"], []byte(`"size":`), []byte(`"wrong_size":`), 1)
		}},
		{"docker-layer", func(files map[string][]byte) {
			files["manifest.json"] = bytes.Replace(files["manifest.json"], []byte(metadata.layerPaths[0]), []byte(metadata.configPath), 1)
		}},
		{"extra-blob", func(files map[string][]byte) { files["unreferenced"] = []byte("other") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := make(map[string][]byte, len(files))
			for name, data := range files {
				changed[name] = bytes.Clone(data)
			}
			tc.edit(changed)
			var members []testMember
			for name, data := range changed {
				members = append(members, testMember{name, data})
			}
			raw := testTar(t, members...)
			output, err := os.CreateTemp(t.TempDir(), "normalized-*")
			if err != nil {
				t.Fatal(err)
			}
			defer output.Close()
			if _, err := output.Write(raw); err != nil {
				t.Fatal(err)
			}
			err = verifyCompletedArchive(context.Background(), output, image, int64(len(raw)), DefaultLimits())
			if (err == nil) != (tc.name == "exact") {
				t.Fatalf("complete archive verification: %v", err)
			}
		})
	}
}
