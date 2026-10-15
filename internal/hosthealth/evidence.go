package hosthealth

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

func snapshotDigest(snapshot Snapshot) string {
	raw, err := json.Marshal(snapshot)
	if err != nil || snapshot.Validate() != nil {
		return ""
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// Evidence is an opaque process confirmation. It carries no provider identity,
// policy or remote authority and cannot be reconstructed from persisted JSON.
// Checking it takes only the service mutex, so SQLite can validate its current
// rows without another SQLite call or network work inside the transaction.
type Evidence struct {
	service          *Service
	resource, digest string
	observed, since  time.Time
}

func (Evidence) String() string   { return "[host health confirmation]" }
func (Evidence) GoString() string { return "[host health confirmation]" }

func (s *Service) Evidence(resource string) Evidence {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.machines[resource]
	if m == nil || m.failed || m.confirmed.IsZero() || m.since.IsZero() || m.digest == "" {
		return Evidence{}
	}
	return Evidence{service: s, resource: resource, digest: m.digest, observed: m.confirmed, since: m.since}
}

// View verifies the exact current observation and continuous process window.
// A newer sample, failed probe, restart or changed clock invalidates old proof.
func (e Evidence) View(resource string, history []Snapshot, now time.Time) View {
	v := View{ResourceID: resource, History: history, UnknownReason: string(ReasonNotReobserved)}
	if len(history) == 0 {
		v.UnknownReason = string(ReasonNotObserved)
		return v
	}
	latest := history[0]
	v.Latest = &latest
	if e.service == nil || resource != e.resource || snapshotDigest(latest) != e.digest || !latest.ObservedAt.Equal(e.observed) {
		return v
	}
	e.service.mu.Lock()
	m := e.service.machines[resource]
	current := m != nil && !m.failed && m.digest == e.digest && m.confirmed.Equal(e.observed) && m.since.Equal(e.since)
	if current && now.Before(e.observed) {
		m.confirmed, m.since, m.digest = time.Time{}, time.Time{}, ""
	}
	e.service.mu.Unlock()
	if !current {
		return v
	}
	if now.Before(e.observed) {
		v.UnknownReason = string(ReasonClockChanged)
		return v
	}
	if now.Sub(e.observed) > StaleAfter {
		v.UnknownReason = string(ReasonStale)
		return v
	}
	v.Fresh, v.UnknownReason, v.ReobservedSince = true, "", e.since
	return v
}
