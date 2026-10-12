package workloadsecret

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

// Pool serializes material changes with short durable authority transactions.
// Its lock is never held across provider I/O. Every participant takes this lock
// before a database transaction, so supply/revoke/start cannot restore used bytes.
type Pool struct {
	mu      sync.Mutex
	key     []byte
	entries map[Key]Material
	closed  bool
}

func NewPool(key []byte) (*Pool, error) {
	if len(key) != 32 {
		return nil, ErrRequest
	}
	return &Pool{key: append([]byte(nil), key...), entries: map[Key]Material{}}, nil
}
func cloneValues(values map[string][]byte) map[string][]byte {
	copy := make(map[string][]byte, len(values))
	for name, value := range values {
		copy[name] = append([]byte(nil), value...)
	}
	return copy
}
func (p *Pool) drop(key Key) { m := p.entries[key]; m.Clear(); delete(p.entries, key) }
func (p *Pool) prune(now time.Time) {
	for key, material := range p.entries {
		expires, ok := CanonicalTime(material.ExpiresAt)
		if !ok || !now.Before(expires) {
			p.drop(key)
		}
	}
}
func (p *Pool) available(key Key, id domain.OperationID, now time.Time) bool {
	p.prune(now)
	m, ok := p.entries[key]
	return !p.closed && ok && m.AuthorizationID == id && id.Valid()
}

// Supply commits only a keyed fingerprint and metadata. Exact explicit replay
// may restore unused material; the repository decides whether it remains usable.
func (p *Pool) Supply(key Key, request Supply, now time.Time, persist func(string, Metadata) (Receipt, bool, error)) (Receipt, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || !key.Valid() || key.AttemptID != request.AttemptID || request.Validate() != nil {
		return Receipt{}, ErrRequest
	}
	p.prune(now)
	raw, _ := json.Marshal(struct {
		Version string
		Key     Key
		Request Supply
	}{"workload-secret/v1", key, request})
	mac := hmac.New(sha256.New, p.key)
	_, _ = mac.Write(raw)
	clear(raw)
	fingerprint := hex.EncodeToString(mac.Sum(nil))
	metadata := Metadata{request.AttemptID, request.ReadinessRevision, request.ExpiresAt, request.Names()}
	receipt, usable, err := persist(fingerprint, metadata)
	if err != nil {
		return Receipt{}, err
	}
	if receipt.Validate() != nil || receipt.Key() != key {
		return Receipt{}, ErrState
	}
	if !usable {
		p.drop(key)
		return receipt, nil
	}
	if _, exists := p.entries[key]; !exists && len(p.entries) >= MaxHeld {
		return Receipt{}, ErrLimit
	}
	p.drop(key)
	p.entries[key] = Material{receipt.AuthorizationID, key, request.ExpiresAt, cloneValues(request.Values)}
	return receipt, nil
}
func (p *Pool) WithAvailability(key Key, id domain.OperationID, now time.Time, action func(bool) error) error {
	if p == nil {
		return action(false)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return action(p.available(key, id, now))
}
func (p *Pool) Available(key Key, id domain.OperationID, now time.Time) bool {
	available := false
	_ = p.WithAvailability(key, id, now, func(value bool) error { available = value; return nil })
	return available
}

// Claim returns a private borrowed copy only after the start/consumption barrier
// commits. A failed barrier leaves the unused original available for the caller.
func (p *Pool) Claim(key Key, id domain.OperationID, now time.Time, commit func() error) (Material, error) {
	if p == nil {
		return Material{}, ErrUnavailable
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.available(key, id, now) {
		return Material{}, ErrUnavailable
	}
	if err := commit(); err != nil {
		return Material{}, err
	}
	m := p.entries[key]
	m.Values = cloneValues(m.Values)
	p.drop(key)
	return m, nil
}
func (p *Pool) Revoke(key Key, id domain.OperationID, persist func() (Receipt, error)) (Receipt, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	r, err := persist()
	if err == nil {
		if m, ok := p.entries[key]; ok && m.AuthorizationID == id {
			p.drop(key)
		}
	}
	return r, err
}

// Sweep clears expired material and material whose attempt has been cancelled,
// cleaned up or consumed. An unavailable state store fails closed for plaintext.
func (p *Pool) Sweep(now time.Time, usable func(Key, domain.OperationID) bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.prune(now)
	for key, m := range p.entries {
		if !usable(key, m.AuthorizationID) {
			p.drop(key)
		}
	}
}
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for key := range p.entries {
		p.drop(key)
	}
	clear(p.key)
	p.closed = true
}
