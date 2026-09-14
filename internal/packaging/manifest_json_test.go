package packaging

import (
	"archive/tar"
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestManifestRejectsCaseAliasedKeys(t *testing.T) {
	for _, key := range []string{"bundle_version", "files", "path", "bytes", "sha256", "executable"} {
		t.Run(key, func(t *testing.T) {
			data := encodeArchive(t, oneFile(), func(i int, h *tar.Header, b []byte) (*tar.Header, []byte) {
				if i == 0 {
					b = []byte(strings.Replace(string(b), `"`+key+`":`, `"`+strings.ToUpper(key)+`":`, 1))
					h.Size = int64(len(b))
				}
				return h, b
			})
			if _, err := Inspect(context.Background(), bytes.NewReader(data), DefaultLimits()); err == nil {
				t.Fatal("case-aliased JSON field accepted")
			}
		})
	}
}
