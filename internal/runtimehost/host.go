// Package runtimehost composes durable local services, without provider workers.
package runtimehost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/statefs"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
)

var ErrState = errors.New("local runtime state is missing, locked or inconsistent; preserve the directory")
var ErrRequest = errors.New("invalid local operator request")

const markerName = ".compute-relay-runtime"

type layout struct {
	Version        int    `json:"version"`
	InstallationID string `json:"installation_id"`
	InputsID       string `json:"inputs_id"`
	ResultsID      string `json:"results_id"`
}

type Status struct {
	Mode            string `json:"mode"`
	InstallationID  string `json:"installation_id"`
	SchemaVersion   int    `json:"schema_version"`
	DispatchEnabled bool   `json:"dispatch_enabled"`
}

// Host owns every local store lock. Close only after all users/HTTP handlers stop.
// Administrative commands require exclusive host access, not a token granting
// an application an HTTP administration route.
type Host struct {
	root     *statefs.Root
	store    *sqlite.Store
	inputs   *blobfs.Store
	results  *blobfs.Store
	access   *auth.Service
	identity layout
}

func Initialize(ctx context.Context, path string) (status Status, err error) {
	if path == "" || ctx.Err() != nil {
		return status, ErrRequest
	}
	if _, err := statefs.PrivateDir(path, true); err != nil {
		return status, ErrState
	}
	h, err := open(ctx, path, true)
	if err != nil {
		return status, err
	}
	defer func() { err = errors.Join(err, h.Close()) }()
	raw, err := json.Marshal(h.identity)
	if err != nil || statefs.WriteNew(filepath.Join(h.root.Path, markerName), raw) != nil || statefs.SyncDir(h.root.Path) != nil {
		return status, ErrState // Do not remove/reinitialize an incomplete installation.
	}
	return h.Status(ctx)
}

func Open(ctx context.Context, path string) (*Host, error) { return open(ctx, path, false) }

func open(ctx context.Context, path string, create bool) (_ *Host, err error) {
	if path == "" || ctx.Err() != nil || statefs.CheckDir(path) != nil {
		return nil, ErrState
	}
	h := &Host{}
	defer func() {
		if err != nil {
			_ = h.Close()
		}
	}()
	h.root, err = statefs.Open(path)
	if err != nil {
		return nil, ErrState
	}
	if !create {
		raw, e := readPrivate(filepath.Join(h.root.Path, markerName), 1024)
		if e != nil || json.Unmarshal(raw, &h.identity) != nil {
			return nil, ErrState
		}
		canonical, e := json.Marshal(h.identity)
		if e != nil || !bytes.Equal(raw, canonical) || h.identity.Version != 1 || !domain.InstallationID(h.identity.InstallationID).Valid() || h.identity.InputsID == h.identity.ResultsID {
			return nil, ErrState
		}
		for _, name := range []string{"state", "inputs", "results"} {
			if statefs.CheckDir(filepath.Join(h.root.Path, name)) != nil {
				return nil, ErrState
			}
		}
		for _, name := range []string{"state/runtime.db", "state/.compute-relay-state", "inputs/.retention-id", "results/.retention-id"} {
			if statefs.CheckFile(filepath.Join(h.root.Path, filepath.FromSlash(name))) != nil {
				return nil, ErrState
			}
		}
	}
	h.store, err = sqlite.Open(ctx, filepath.Join(h.root.Path, "state"), sqlite.DefaultOptions())
	if err != nil {
		return nil, ErrState
	}
	h.inputs, err = blobfs.New(filepath.Join(h.root.Path, "inputs"), blobfs.Limits{MaxObjectBytes: 2 << 30, MaxTotalBytes: 8 << 30})
	if err != nil {
		return nil, ErrState
	}
	h.results, err = blobfs.New(filepath.Join(h.root.Path, "results"), blobfs.Limits{MaxObjectBytes: 4 << 30, MaxTotalBytes: 8 << 30})
	if err != nil {
		return nil, ErrState
	}
	info, err := h.store.Info(ctx)
	if err != nil {
		return nil, ErrState
	}
	inputID, err := h.inputs.RetentionIdentity(ctx)
	if err != nil {
		return nil, ErrState
	}
	resultID, err := h.results.RetentionIdentity(ctx)
	if err != nil {
		return nil, ErrState
	}
	actual := layout{1, string(info.InstallationID), inputID, resultID}
	if !create && actual != h.identity {
		return nil, ErrState
	}
	h.identity = actual
	h.access, err = auth.New(h.store, h.store, nil)
	if err != nil {
		return nil, ErrState
	}
	return h, nil
}

func (h *Host) Close() error {
	if h == nil {
		return nil
	}
	var err error
	if h.results != nil {
		err = errors.Join(err, h.results.Close())
	}
	if h.inputs != nil {
		err = errors.Join(err, h.inputs.Close())
	}
	if h.store != nil {
		err = errors.Join(err, h.store.Close())
	}
	if h.root != nil {
		err = errors.Join(err, h.root.Close())
	}
	if err != nil {
		return ErrState
	}
	return nil
}
func (h *Host) Status(ctx context.Context) (Status, error) {
	if h == nil || h.store == nil {
		return Status{}, ErrState
	}
	info, err := h.store.Info(ctx)
	if err != nil || string(info.InstallationID) != h.identity.InstallationID {
		return Status{}, ErrState
	}
	return Status{Mode: "local-admission-only", InstallationID: h.identity.InstallationID, SchemaVersion: int(info.SchemaVersion)}, nil
}

// Internal marker bytes are generated once, not user configuration. Require exact
// canonical bytes so aliases, duplicate/extra/missing fields cannot be accepted.
func readPrivate(path string, limit int64) ([]byte, error) {
	if statefs.CheckFile(path) != nil {
		return nil, ErrState
	}
	before, err := os.Lstat(path)
	if err != nil || before.Size() > limit {
		return nil, ErrState
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, ErrState
	}
	opened, statErr := file.Stat()
	if statErr != nil || !os.SameFile(before, opened) {
		_ = file.Close()
		return nil, ErrState
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, limit+1))
	after, statErr := file.Stat()
	closeErr := file.Close()
	if readErr != nil || statErr != nil || closeErr != nil || int64(len(raw)) > limit || int64(len(raw)) != opened.Size() || after.Size() != opened.Size() || !after.ModTime().Equal(opened.ModTime()) || !os.SameFile(after, opened) {
		return nil, ErrState
	}
	return raw, nil
}
