package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
)

// ContractLock makes contract changes explicit and reviewable.
type ContractLock struct {
	LockVersion int          `json:"lock_version"`
	Files       []LockedFile `json:"files"`
}

// LockedFile records the identity of one JSON contract or fixture.
type LockedFile struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

// BuildLock computes the canonical lock for JSON files beneath api/ except the lock itself.
func BuildLock(root string) (ContractLock, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return ContractLock{}, fmt.Errorf("resolve repository root: %w", err)
	}
	apiRoot := filepath.Join(root, "api")
	lock := ContractLock{LockVersion: 1}
	err = filepath.WalkDir(apiRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(path) != ".json" || entry.Name() == filepath.Base(lockPath) {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(data)
		lock.Files = append(lock.Files, LockedFile{
			Path:   filepath.ToSlash(relative),
			SHA256: hex.EncodeToString(digest[:]),
			Bytes:  int64(len(data)),
		})
		return nil
	})
	if err != nil {
		return ContractLock{}, fmt.Errorf("build contract lock: %w", err)
	}
	sort.Slice(lock.Files, func(left, right int) bool { return lock.Files[left].Path < lock.Files[right].Path })
	if len(lock.Files) == 0 {
		return ContractLock{}, errors.New("contract lock would contain no files")
	}
	return lock, nil
}

// ValidateLock requires the committed lock to equal a fresh canonical snapshot.
func ValidateLock(root string) error {
	var committed ContractLock
	if err := readStrictJSON(filepath.Join(root, filepath.FromSlash(lockPath)), &committed); err != nil {
		return fmt.Errorf("read contract lock: %w", err)
	}
	if committed.LockVersion != 1 {
		return fmt.Errorf("unsupported contract lock version %d", committed.LockVersion)
	}
	actual, err := BuildLock(root)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(committed, actual) {
		return errors.New("api/contract.lock.json is stale; run the contract-lock developer task")
	}
	return nil
}

// WriteLock atomically publishes the canonical contract lock.
func WriteLock(root string) error {
	lock, err := BuildLock(root)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(lock, "", "  ")
	if err != nil {
		return fmt.Errorf("encode contract lock: %w", err)
	}
	data = append(data, '\n')
	path := filepath.Join(root, filepath.FromSlash(lockPath))
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, data, 0o600); err != nil {
		return fmt.Errorf("write temporary contract lock: %w", err)
	}
	if err := os.Rename(temporary, path); err != nil {
		_ = os.Remove(temporary)
		return fmt.Errorf("publish contract lock: %w", err)
	}
	return nil
}
