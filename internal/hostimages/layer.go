package hostimages

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path"
	"strings"
)

// /relay is the runner's staging root. Reject even a directory or whiteout
// there: a previous layer may otherwise make the target verifier fail before
// the runner starts. This scans streams without buffering layer contents.
func checkReservedRoot(ctx context.Context, f *os.File, layer layerSource) error {
	var source io.Reader = io.NewSectionReader(f, layer.member.offset, layer.member.size)
	if layer.compressed {
		gz, err := gzip.NewReader(source)
		if err != nil {
			return ErrInvalidArchive
		}
		defer gz.Close()
		source = gz
	}
	tr := tar.NewReader(contextReader{ctx, source})
	var logicalBytes int64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return ErrInvalidArchive
		}
		if h.Size < 0 || h.Size > layer.size-logicalBytes {
			return ErrInvalidArchive
		}
		logicalBytes += h.Size
		for key := range h.PAXRecords {
			if strings.HasPrefix(key, "GNU.sparse.") {
				return ErrInvalidArchive
			}
		}
		name := path.Clean(strings.TrimPrefix(h.Name, "./"))
		if name == ".." || strings.HasPrefix(name, "../") || path.IsAbs(name) || reservedPath(name) ||
			name == ".wh.relay" {
			return ErrInvalidArchive
		}
		if h.Typeflag == tar.TypeSymlink || h.Typeflag == tar.TypeLink {
			target := h.Linkname
			if path.IsAbs(target) {
				target = strings.TrimLeft(target, "/")
			} else {
				target = path.Join(path.Dir(name), target)
			}
			if reservedPath(path.Clean(target)) {
				return ErrInvalidArchive
			}
		}
	}
}

func reservedPath(name string) bool {
	return name == "relay" || strings.HasPrefix(name, "relay/")
}
