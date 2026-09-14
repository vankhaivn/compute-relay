package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"

	"github.com/vankhaivn/compute-relay/internal/statefs"
)

// A durable identity marker detects accidental DB deletion/replacement instead of
// silently initializing a new installation in an established state directory.
func checkInitializedFile(root, database string) error {
	path := filepath.Join(root, ".installation")
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if _, err := readInstallation(path); err != nil {
		return err
	}
	info, err := os.Stat(database)
	if err != nil || info.Size() == 0 {
		return ErrCorrupt
	}
	return nil
}
func readInstallation(path string) (string, error) {
	if err := statefs.CheckFile(path); err != nil {
		return "", err
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() > 128 {
		return "", ErrCorrupt
	}
	raw, err := os.ReadFile(path)
	if err != nil || !validID(string(raw)) {
		return "", ErrCorrupt
	}
	return string(raw), nil
}
func recordInstallation(ctx context.Context, db *sql.DB, root string) error {
	var id string
	if err := db.QueryRowContext(ctx, "SELECT installation_id FROM runtime_installation WHERE singleton=1").Scan(&id); err != nil {
		return ErrCorrupt
	}
	return writeInstallation(root, id)
}

func writeInstallation(root, id string) error {
	path := filepath.Join(root, ".installation")
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		existing, err := readInstallation(path)
		if err != nil {
			return err
		}
		if existing != id {
			return ErrCorrupt
		}
		return nil
	}
	// Only an interrupted marker under the exclusive root lock is eligible for removal.
	temp := path + ".pending"
	if err := statefs.CheckFile(temp); err == nil {
		if err := os.Remove(temp); err != nil {
			return ErrUnavailable
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := statefs.WriteNew(temp, []byte(id)); err != nil {
		return err
	}
	if err := os.Rename(temp, path); err != nil {
		return ErrUnavailable
	}
	return statefs.SyncDir(root)
}
