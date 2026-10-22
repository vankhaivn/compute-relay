package hostimages

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/provider"
)

func TestImageNormalizationRetainsClosedFailedStep(t *testing.T) {
	layer := testLayer(t)
	valid := dockerFixture(t, testConfig(layer, nil), layer, nil)
	malformed := []byte("private archive canary")
	for _, tc := range []struct {
		name   string
		data   []byte
		change func(*Options)
		reason string
		cause  error
	}{
		{name: "options", data: valid, change: func(o *Options) { o.Architecture = "unknown" }, reason: "image_options_invalid", cause: ErrInvalidArchive},
		{name: "source_file", data: valid, change: func(o *Options) { o.TempDir = filepath.Join(o.TempDir, "missing") }, reason: "image_source_file_failed"},
		{name: "source_digest", data: valid, change: func(o *Options) { o.ArchiveSHA256 = provider.Digest([]byte("different")) }, reason: "image_source_digest_failed", cause: ErrDigestMismatch},
		{name: "scan", data: malformed, reason: "image_archive_scan_failed", cause: ErrInvalidArchive},
		{name: "parse", data: dockerFixture(t, []byte("{}"), layer, nil), reason: "image_archive_parse_failed", cause: ErrInvalidArchive},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := testOptions(tc.data, t.TempDir())
			if tc.change != nil {
				tc.change(&opts)
			}
			_, err := Normalize(context.Background(), bytes.NewReader(tc.data), opts)
			if err == nil || provider.Reason(err, "") != tc.reason || tc.cause != nil && !errors.Is(err, tc.cause) || strings.Contains(err.Error(), "canary") {
				t.Fatalf("wrong closed error: %v %q", err, provider.Reason(err, ""))
			}
		})
	}
}
