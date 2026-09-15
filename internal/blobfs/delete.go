package blobfs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

// SameRoot supports fail-closed composition of the independent input/result stores.
// It exposes neither an absolute path nor a provider resource.
func (s *Store) SameRoot(other *Store) bool { return other != nil && s.root == other.root }

// Delete removes only the exact immutable identity supplied by a trusted retention
// repository AFTER its tombstone has committed. It is not an authorization API.
// Callers must prevent new references and hold recovery/worker pins before calling.
// The returned bool means the final object was already absent, not remote absence.
func (s *Store) Delete(ctx context.Context, expected domain.ObjectMetadata) (bool, error) {
	s.life.RLock()
	defer s.life.RUnlock()
	if s.closed {
		return false, ErrClosed
	}
	if !expected.Valid() {
		return false, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	base := filepath.Join(s.root, "workspaces", encodedID(string(expected.WorkspaceID)))
	for _, name := range []string{base, filepath.Join(base, "objects"), filepath.Join(base, "temporary")} {
		if err := privateDirectory(name); err != nil {
			if errors.Is(err, ErrNotFound) && name == base {
				return true, nil
			}
			return false, err
		}
	}
	name := encodedID(string(expected.ID))
	final := filepath.Join(base, "objects", name)
	retired := filepath.Join(base, "temporary", "upload-retired-"+name)
	_, err := os.Lstat(final)
	if errors.Is(err, os.ErrNotExist) {
		// A prior committed deletion may have stopped after rename, or after removing
		// only some quarantine files. This exact owned path remains safe to finish.
		if err := removeRetired(retired); err != nil {
			return true, err
		}
		if err := syncDirectory(filepath.Join(base, "temporary")); err != nil {
			return true, ErrUnavailable
		}
		return true, nil
	}
	if err != nil {
		return false, ErrUnavailable
	}
	if _, err := os.Lstat(retired); !errors.Is(err, os.ErrNotExist) {
		// Never guess which of two copies is authoritative, even with the same ID.
		return false, ErrCorrupt
	}
	meta, err := readMetadata(final)
	if err != nil {
		return false, err
	}
	if meta != expected {
		return false, ErrCorrupt
	}
	entries, err := os.ReadDir(final)
	if err != nil {
		return false, ErrUnavailable
	}
	if len(entries) != 2 {
		return false, ErrCorrupt
	}
	for _, e := range entries {
		if e.Name() != "data" && e.Name() != "metadata.json" {
			return false, ErrCorrupt
		}
	}
	data, err := os.Open(filepath.Join(final, "data"))
	if err != nil {
		return false, ErrUnavailable
	}
	hash := sha256.New()
	n, readErr := io.CopyBuffer(hash, &contextReader{ctx: ctx, reader: io.LimitReader(data, expected.Bytes+1)}, make([]byte, bufferBytes))
	closeErr := data.Close()
	if readErr != nil {
		return false, readErr
	}
	if closeErr != nil {
		return false, ErrUnavailable
	}
	if n != expected.Bytes || hex.EncodeToString(hash.Sum(nil)) != string(expected.SHA256) {
		return false, ErrCorrupt
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	// Both paths are beneath the same exclusively locked blob root. Never unlink
	// individual final files: a crash must leave a complete object or quarantine.
	// On Windows, an outstanding reader may prevent this rename; leave the durable
	// tombstone pending and retry later. POSIX readers retain their open descriptor.
	if err := s.rename(final, retired); err != nil {
		return false, ErrUnavailable
	}
	s.used -= meta.Bytes
	s.count--
	if err := syncDirectory(filepath.Join(base, "objects")); err != nil {
		return false, ErrUnavailable
	}
	if err := syncDirectory(filepath.Join(base, "temporary")); err != nil {
		return false, ErrUnavailable
	}
	if err := removeRetired(retired); err != nil {
		return false, err
	}
	if err := syncDirectory(filepath.Join(base, "temporary")); err != nil {
		return false, ErrUnavailable
	}
	return false, nil
}

func removeRetired(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return ErrUnavailable
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ErrCorrupt
	}
	if err := privateDirectory(path); err != nil {
		return err
	}
	if err := os.RemoveAll(path); err != nil {
		return ErrUnavailable
	}
	return nil
}
