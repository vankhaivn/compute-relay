// Package localstorage measures Relay-owned local files without exposing paths.
package localstorage

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Inventory struct {
	InputsBytes   int64     `json:"inputs_bytes"`
	ResultsBytes  int64     `json:"results_bytes"`
	DatabaseBytes int64     `json:"database_bytes"`
	OtherBytes    int64     `json:"other_bytes"`
	TotalBytes    int64     `json:"total_bytes"`
	MeasuredAt    time.Time `json:"measured_at"`
}

// Reader caches logical file sizes for a minute. It never opens file contents or
// follows symlinks, and no measured path is returned to an application.
type Reader struct {
	root  string
	mu    sync.Mutex
	saved Inventory
}

func New(root string) *Reader { return &Reader{root: root} }
func (r *Reader) Measure(ctx context.Context, refresh bool) (Inventory, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !refresh && !r.saved.MeasuredAt.IsZero() && time.Since(r.saved.MeasuredAt) < time.Minute {
		return r.saved, nil
	}
	var out Inventory
	err := filepath.WalkDir(r.root, func(path string, d fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(r.root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		size := info.Size()
		out.TotalBytes += size
		switch {
		case strings.HasPrefix(rel, "inputs/"):
			out.InputsBytes += size
		case strings.HasPrefix(rel, "results/"):
			out.ResultsBytes += size
		case rel == "state/runtime.db" || rel == "state/runtime.db-wal" || rel == "state/runtime.db-shm":
			out.DatabaseBytes += size
		default:
			out.OtherBytes += size
		}
		return nil
	})
	if err != nil {
		return Inventory{}, err
	}
	out.MeasuredAt = time.Now().UTC()
	r.saved = out
	return out, nil
}
