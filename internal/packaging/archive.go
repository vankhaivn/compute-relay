package packaging

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"strings"
	"time"
)

func header(name string, size int64, executable bool) *tar.Header {
	mode := int64(0o644)
	if executable {
		mode = 0o755
	}
	return &tar.Header{Name: name, Size: size, Typeflag: tar.TypeReg, Mode: mode, ModTime: time.Unix(0, 0).UTC(), Format: tar.FormatUSTAR}
}

// Write emits deterministic gzip/USTAR bytes. On error the destination is incomplete and
// MUST NOT be published. CLI and object import both provide atomic/verified destinations.
func (p *Plan) Write(ctx context.Context, destination io.Writer) (Report, error) {
	if destination == nil {
		return Report{}, ErrInvalid
	}
	if err := p.checkTree(ctx); err != nil {
		return Report{}, err
	}
	sum := sha256.New()
	bounded := &boundWriter{w: io.MultiWriter(destination, sum), left: p.project.limits.MaxCompressedBytes}
	gz := gzip.NewWriter(bounded)
	gz.Header.OS = 255
	tw := tar.NewWriter(gz)
	encoded, _ := manifestBytes(p.preview.Manifest)
	if err := tw.WriteHeader(header(ManifestPath, int64(len(encoded)), false)); err != nil {
		return Report{}, err
	}
	if _, err := tw.Write(encoded); err != nil {
		return Report{}, err
	}
	for _, entry := range p.files {
		if err := ctx.Err(); err != nil {
			return Report{}, err
		}
		if err := tw.WriteHeader(header("code/"+entry.Path, entry.Bytes, entry.Executable)); err != nil {
			return Report{}, ErrPath
		}
		if _, err := copyFile(ctx, p.project.root, entry, tw); err != nil {
			return Report{}, err
		}
	}
	if err := p.checkTree(ctx); err != nil {
		return Report{}, err
	}
	if err := tw.Close(); err != nil {
		return Report{}, err
	}
	if err := gz.Close(); err != nil {
		return Report{}, err
	}
	return Report{Manifest: p.Preview().Manifest, ManifestSHA256: p.preview.ManifestSHA256, Bytes: bounded.n, SHA256: hex.EncodeToString(sum.Sum(nil))}, nil
}

// Inspect validates framing, bounds, all identities and payload digests without extracting
// anything. Only this version's regular-file USTAR subset is accepted. Inspect raw headers
// BEFORE tar.Reader, so hidden PAX/GNU/sparse entries cannot evade entry/type limits.
func Inspect(ctx context.Context, source io.Reader, limits Limits) (Report, error) {
	if err := limits.Validate(); err != nil {
		return Report{}, err
	}
	if source == nil {
		return Report{}, ErrInvalid
	}
	sum := sha256.New()
	raw := &boundReader{r: io.TeeReader(&contextReader{ctx: ctx, r: source}, sum), left: limits.MaxCompressedBytes}
	compressed := bufio.NewReaderSize(raw, BufferBytes)
	gz, err := gzip.NewReader(compressed)
	if err != nil {
		return Report{}, err
	}
	defer gz.Close()
	gz.Multistream(false)
	expanded := &boundReader{r: gz, left: limits.MaxExpandedBytes + limits.MaxManifestBytes + int64(limits.MaxFiles+1)*1024 + 1024}
	h, err := nextHeader(expanded)
	if err != nil {
		return Report{}, err
	}
	if h == nil || h.Name != ManifestPath || h.Size < 1 || h.Size > limits.MaxManifestBytes {
		return Report{}, ErrInvalid
	}
	encoded := make([]byte, h.Size)
	if _, err := io.ReadFull(expanded, encoded); err != nil {
		return Report{}, err
	}
	if err := padding(expanded, h.Size); err != nil {
		return Report{}, err
	}
	if !uniqueJSON(encoded) {
		return Report{}, ErrInvalid
	}
	var wire struct {
		Version *string `json:"bundle_version"`
		Files   []struct {
			Path       *string `json:"path"`
			Bytes      *int64  `json:"bytes"`
			SHA256     *string `json:"sha256"`
			Executable *bool   `json:"executable"`
		} `json:"files"`
	}
	dec := json.NewDecoder(bytes.NewReader(encoded))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&wire); err != nil || wire.Version == nil {
		return Report{}, ErrInvalid
	}
	manifest := Manifest{Version: *wire.Version}
	for _, f := range wire.Files {
		if f.Path == nil || f.Bytes == nil || f.SHA256 == nil || f.Executable == nil {
			return Report{}, ErrInvalid
		}
		manifest.Files = append(manifest.Files, File{Path: *f.Path, Bytes: *f.Bytes, SHA256: *f.SHA256, Executable: *f.Executable})
	}
	if manifest.Version != Version || len(manifest.Files) == 0 || len(manifest.Files) > limits.MaxFiles {
		return Report{}, ErrInvalid
	}
	seen := newPaths()
	var total int64
	last := ""
	for _, f := range manifest.Files {
		if err := seen.add(f.Path); err != nil {
			return Report{}, err
		}
		if len(strings.Split(f.Path, "/")) > limits.MaxDepth || f.Path <= last || f.Bytes < 0 || f.Bytes > limits.MaxExpandedBytes-total || !validDigest(f.SHA256) {
			return Report{}, ErrInvalid
		}
		total += f.Bytes
		last = f.Path
	}
	for _, f := range manifest.Files {
		if err := ctx.Err(); err != nil {
			return Report{}, err
		}
		h, err := nextHeader(expanded)
		if err != nil {
			return Report{}, err
		}
		expected := header("code/"+f.Path, f.Bytes, f.Executable)
		if h == nil || h.Name != expected.Name || h.Size != expected.Size || h.Mode != expected.Mode {
			return Report{}, ErrInvalid
		}
		hash := sha256.New()
		scanner := secretScanner{}
		left := f.Bytes
		buf := make([]byte, BufferBytes)
		for left > 0 {
			if err := ctx.Err(); err != nil {
				return Report{}, err
			}
			n, err := io.ReadFull(expanded, buf[:min(int64(len(buf)), left)])
			if err != nil {
				return Report{}, err
			}
			if err := scanner.check(buf[:n]); err != nil {
				return Report{}, err
			}
			_, _ = hash.Write(buf[:n])
			left -= int64(n)
		}
		if hex.EncodeToString(hash.Sum(nil)) != f.SHA256 {
			return Report{}, ErrInvalid
		}
		if err := padding(expanded, f.Bytes); err != nil {
			return Report{}, err
		}
	}
	end, err := nextHeader(expanded)
	if err != nil {
		return Report{}, err
	}
	if end != nil {
		return Report{}, ErrInvalid
	}
	// Consume gzip footer/checksum, rejecting uncompressed tail, concatenated gzip streams
	// and compressed trailing junk instead of validating only the first convenient member.
	var one [1]byte
	n, err := expanded.Read(one[:])
	if n != 0 || err != io.EOF {
		return Report{}, ErrInvalid
	}
	if _, err := compressed.ReadByte(); err != io.EOF {
		return Report{}, ErrInvalid
	}
	return Report{Manifest: manifest, ManifestSHA256: digest(encoded), Bytes: raw.n, SHA256: hex.EncodeToString(sum.Sum(nil))}, nil
}
func nextHeader(r io.Reader) (*tar.Header, error) {
	var block [512]byte
	if _, err := io.ReadFull(r, block[:]); err != nil {
		return nil, err
	}
	if allZero(block[:]) {
		if _, err := io.ReadFull(r, block[:]); err != nil {
			return nil, err
		}
		if !allZero(block[:]) {
			return nil, ErrInvalid
		}
		return nil, nil
	}
	if block[156] != tar.TypeReg || string(block[257:263]) != "ustar\x00" || string(block[263:265]) != "00" {
		return nil, ErrInvalid
	}
	h, err := tar.NewReader(bytes.NewReader(block[:])).Next()
	if err != nil {
		return nil, ErrInvalid
	}
	if h.Format != tar.FormatUSTAR || h.Linkname != "" || h.Uid != 0 || h.Gid != 0 || h.Uname != "" || h.Gname != "" || h.Size < 0 || h.Mode != 0o644 && h.Mode != 0o755 {
		return nil, ErrInvalid
	}
	return h, nil
}
func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}
func padding(r io.Reader, size int64) error {
	var b [512]byte
	n := (512 - size%512) % 512
	if _, err := io.ReadFull(r, b[:n]); err != nil {
		return err
	}
	if !allZero(b[:n]) {
		return ErrInvalid
	}
	return nil
}
func uniqueJSON(b []byte) bool {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var value func(int) bool
	value = func(depth int) bool {
		if depth > 16 {
			return false
		}
		token, err := d.Token()
		if err != nil {
			return false
		}
		delim, is := token.(json.Delim)
		if !is {
			return true
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				key, e := d.Token()
				s, ok := key.(string)
				if e != nil || !ok || seen[s] {
					return false
				}
				seen[s] = true
				if !value(depth + 1) {
					return false
				}
			}
			end, e := d.Token()
			return e == nil && end == json.Delim('}')
		case '[':
			for d.More() {
				if !value(depth + 1) {
					return false
				}
			}
			end, e := d.Token()
			return e == nil && end == json.Delim(']')
		default:
			return false
		}
	}
	if !value(0) {
		return false
	}
	_, err := d.Token()
	return err == io.EOF
}
