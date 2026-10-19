package kaggleapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/vankhaivn/compute-relay/internal/statefs"
)

const cooldownFile = "kaggle-api-cooldowns.json"

var ErrPacingState = errors.New("Kaggle API pacing state unavailable")

type savedCooldowns struct {
	Version  int                  `json:"version"`
	Provider time.Time            `json:"provider"`
	Accounts map[string]time.Time `json:"accounts"`
}

// OpenRequestPacer pins the installation's existing state lock. Only server
// cooldowns survive restart; request spacing and credentials are never persisted.
func OpenRequestPacer(root *statefs.Root) (*RequestPacer, error) {
	pin, err := root.Retain()
	if err != nil {
		return nil, ErrPacingState
	}
	p := newRequestPacer()
	p.root = pin
	if err := p.load(); err != nil {
		_ = pin.Close()
		return nil, ErrPacingState
	}
	return p, nil
}

func (p *RequestPacer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	if p.root != nil && p.root.Close() != nil {
		return ErrPacingState
	}
	return nil
}

func (p *RequestPacer) load() error {
	path := filepath.Join(p.root.Path, cooldownFile)
	if err := statefs.CheckFile(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, 128<<10))
	if err != nil {
		return err
	}
	var saved savedCooldowns
	if json.Unmarshal(raw, &saved) != nil || saved.Version != 1 || saved.Accounts == nil || len(saved.Accounts) > maxRequestScopes {
		return ErrPacingState
	}
	canonical, err := json.Marshal(saved)
	if err != nil || !bytes.Equal(raw, canonical) {
		return ErrPacingState
	}
	p.providerUntil = saved.Provider
	p.scopes = make(map[string]*requestScope)
	for account, until := range saved.Accounts {
		if !accountName.MatchString(account) || until.IsZero() {
			return ErrPacingState
		}
		if until.After(p.clock()) {
			p.scopes[account] = &requestScope{until: until}
		}
	}
	return nil
}

func (p *RequestPacer) saveLocked() error {
	saved := savedCooldowns{Version: 1, Provider: p.providerUntil, Accounts: make(map[string]time.Time)}
	now := p.clock()
	for account, scope := range p.scopes {
		if accountName.MatchString(account) && scope.until.After(now) {
			saved.Accounts[account] = scope.until
		}
	}
	raw, err := json.Marshal(saved)
	if err != nil || statefs.CheckDir(p.root.Path) != nil {
		return ErrPacingState
	}
	path := filepath.Join(p.root.Path, cooldownFile)
	if err := statefs.CheckFile(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return ErrPacingState
	}
	file, err := os.CreateTemp(p.root.Path, ".kaggle-cooldown-*")
	if err != nil {
		return ErrPacingState
	}
	defer os.Remove(file.Name())
	_, writeErr := file.Write(raw)
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil || os.Rename(file.Name(), path) != nil || statefs.SyncDir(p.root.Path) != nil {
		return ErrPacingState
	}
	return nil
}
