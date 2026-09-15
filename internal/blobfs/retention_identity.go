package blobfs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

// RetentionIdentity binds pending deletion tickets to this physical store instance,
// not merely a path that could later contain a different directory. Preserve this
// small identity file when backing up or moving an entire blob store.
func (s *Store) RetentionIdentity(ctx context.Context) (string, error) {
	s.life.RLock()
	defer s.life.RUnlock()
	if s.closed {
		return "", ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	path := filepath.Join(s.root, ".retention-id")
	info, err := os.Lstat(path)
	if err == nil {
		if !info.Mode().IsRegular() || info.Size() != 70 || !privateMode(info.Mode()) || checkPrivatePath(path) != nil {
			return "", ErrCorrupt
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return "", ErrUnavailable
		}
		id := strings.TrimSuffix(string(raw), "\n")
		if !strings.HasPrefix(id, "blob_") || !domain.SHA256Digest(strings.TrimPrefix(id, "blob_")).Valid() {
			return "", ErrCorrupt
		}
		return id, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", ErrUnavailable
	}
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", ErrUnavailable
	}
	id := "blob_" + hex.EncodeToString(nonce[:])
	file, err := os.CreateTemp(s.root, ".retention-id-")
	if err != nil {
		return "", ErrUnavailable
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if _, err := file.WriteString(id + "\n"); err != nil {
		return "", ErrUnavailable
	}
	if err := file.Sync(); err != nil {
		return "", ErrUnavailable
	}
	if err := file.Close(); err != nil {
		return "", ErrUnavailable
	}
	if err := s.rename(file.Name(), path); err != nil {
		return "", ErrUnavailable
	}
	if err := syncDirectory(s.root); err != nil {
		return "", ErrUnavailable
	}
	return id, nil
}
