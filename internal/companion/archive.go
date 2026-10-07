package companion

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"time"
)

// Pack creates a deterministic archive. Its source is an explicitly assembled staging
// directory, never a checkout or private installation. Symlinks and special files are rejected.
func Pack(directory, archive string, build Build) (string, error) {
	m := Manifest{Schema: Schema, Build: build}
	err := filepath.WalkDir(directory, func(p string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() || entry.Type()&os.ModeSymlink != 0 {
			return ErrBundle
		}
		name, err := filepath.Rel(directory, p)
		if err != nil {
			return err
		}
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		info, statErr := f.Stat()
		sha, size, hashErr := digest(io.LimitReader(f, MaxPayloadBytes+1))
		f.Close()
		if statErr != nil || hashErr != nil {
			return ErrBundle
		}
		mode := int64(0644)
		if info.Mode().Perm()&0111 != 0 {
			mode = 0755
		}
		m.Files = append(m.Files, File{filepath.ToSlash(name), size, mode, sha})
		if len(m.Files) > MaxFiles {
			return ErrBundle
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	sort.Slice(m.Files, func(i, j int) bool { return m.Files[i].Path < m.Files[j].Path })
	if err = m.Validate(); err != nil {
		return "", err
	}
	data, err := manifestBytes(m)
	if err != nil || len(data) > MaxManifestBytes {
		return "", ErrBundle
	}
	out, err := os.OpenFile(archive, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return "", err
	}
	defer out.Close()
	z := gzip.NewWriter(out)
	w := tar.NewWriter(z)
	header := func(name string, size, mode int64) error {
		return w.WriteHeader(&tar.Header{Name: name, Mode: mode, Size: size, Typeflag: tar.TypeReg, ModTime: time.Unix(0, 0), Format: tar.FormatPAX})
	}
	if err := header(ManifestName, int64(len(data)), 0644); err != nil {
		return "", err
	}
	if _, err := w.Write(data); err != nil {
		return "", err
	}
	for _, item := range m.Files {
		if err := header(item.Path, item.Size, item.Mode); err != nil {
			return "", err
		}
		file, err := os.Open(filepath.Join(directory, filepath.FromSlash(item.Path)))
		if err != nil {
			return "", err
		}
		sha, size, copyErr := digest(io.TeeReader(io.LimitReader(file, item.Size+1), w))
		file.Close()
		if copyErr != nil || sha != item.SHA256 || size != item.Size {
			return "", ErrBundle
		}
	}
	if err := w.Close(); err != nil {
		return "", err
	}
	if err := z.Close(); err != nil {
		return "", err
	}
	if err := out.Sync(); err != nil {
		return "", err
	}
	if _, err := out.Seek(0, 0); err != nil {
		return "", err
	}
	sha, size, err := digest(out)
	if size > MaxArchiveBytes {
		return "", ErrBundle
	}
	return sha, err
}

func validHeader(h *tar.Header, name string, size, mode int64) bool {
	if h.Name != name || h.Size != size || h.Mode != mode || h.Typeflag != tar.TypeReg || h.Linkname != "" {
		return false
	}
	for key := range h.PAXRecords {
		if key != "path" {
			return false
		}
	}
	return validPath(h.Name)
}

// Install requires a trusted external archive checksum. It never merges, upgrades,
// overwrites or initializes private runtime state. An incomplete destination is retained
// on error and has no manifest; callers must choose a new path for any retry.
func Install(archive, expectedSHA, destination string) (Manifest, error) {
	var m Manifest
	if !validHex(expectedSHA, 32) {
		return m, ErrBundle
	}
	f, err := os.Open(archive)
	if err != nil {
		return m, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > MaxArchiveBytes {
		return m, ErrBundle
	}
	sha, _, err := digest(f)
	if err != nil || sha != expectedSHA {
		return m, ErrBundle
	}
	if _, err := f.Seek(0, 0); err != nil {
		return m, err
	}
	compressedHash := sha256.New()
	z, err := gzip.NewReader(io.TeeReader(f, compressedHash))
	if err != nil {
		return m, ErrBundle
	}
	defer z.Close()
	r := tar.NewReader(io.LimitReader(z, MaxPayloadBytes+MaxFiles*2048+MaxManifestBytes))
	h, err := r.Next()
	if err != nil || h.Size > MaxManifestBytes || !validHeader(h, ManifestName, h.Size, 0644) {
		return m, ErrBundle
	}
	m, err = decodeManifest(r)
	if err != nil {
		return m, err
	}
	if err := os.Mkdir(destination, 0700); err != nil {
		return m, err
	}
	root, err := os.OpenRoot(destination)
	if err != nil {
		return m, err
	}
	defer root.Close()
	for _, item := range m.Files {
		h, err := r.Next()
		if err != nil || !validHeader(h, item.Path, item.Size, item.Mode) {
			return m, ErrBundle
		}
		if err := root.MkdirAll(path.Dir(item.Path), 0700); err != nil {
			return m, err
		}
		out, err := root.OpenFile(item.Path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, os.FileMode(item.Mode))
		if err != nil {
			return m, err
		}
		sha, n, copyErr := digest(io.TeeReader(r, out))
		syncErr := out.Sync()
		closeErr := out.Close()
		if copyErr != nil || syncErr != nil || closeErr != nil || n != item.Size || sha != item.SHA256 {
			return m, ErrBundle
		}
		if err := root.Chmod(item.Path, os.FileMode(item.Mode)); err != nil {
			return m, err
		}
	}
	if _, err := r.Next(); err != io.EOF {
		return m, ErrBundle
	}
	// Drain to force gzip trailer/checksum verification; concatenated payloads are rejected.
	trailing, err := io.ReadAll(io.LimitReader(z, 1025))
	if err != nil || len(trailing) > 0 {
		return m, ErrBundle
	}
	// Recheck the exact compressed bytes consumed during extraction as well as
	// the initial pass, so an in-place archive change cannot substitute a manifest.
	if hex.EncodeToString(compressedHash.Sum(nil)) != expectedSHA {
		return m, ErrBundle
	}
	data, err := manifestBytes(m)
	if err != nil {
		return m, err
	}
	out, err := root.OpenFile(ManifestName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return m, err
	}
	_, writeErr := out.Write(data)
	syncErr := out.Sync()
	closeErr := out.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil {
		return m, fmt.Errorf("companion manifest publication incomplete")
	}
	return m, nil
}
