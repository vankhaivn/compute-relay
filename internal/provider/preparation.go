package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

// InputSnapshot contains immutable object identities, never host paths or source URLs.
// Adapters receive a workspace-scoped BlobStore at composition, not credentials in jobs.
// The digest is compatible with runner/python/relay_runner/contract.py:input_digest.
type InputSnapshot struct {
	Bundle domain.ObjectMetadata `json:"bundle"`
	Inputs []FrozenInput         `json:"inputs"`
}
type FrozenInput struct {
	Name   string                `json:"name"`
	Target string                `json:"target"`
	Object domain.ObjectMetadata `json:"object"`
}

func (s InputSnapshot) Validate(w domain.WorkspaceID) error {
	if !s.Bundle.Valid() || s.Bundle.WorkspaceID != w || s.Bundle.Bytes > 100<<20 || len(s.Inputs) > 64 {
		return errors.New("invalid frozen input snapshot")
	}
	names, paths := map[string]bool{}, []string{}
	var total int64
	for _, in := range s.Inputs {
		if !in.Object.Valid() || in.Object.WorkspaceID != w || in.Object.Bytes > 2<<30 || in.Object.Bytes > (4<<30)-total || !SafeArtifactPath(in.Target) || len(in.Name) < 1 || len(in.Name) > 64 || names[strings.ToLower(in.Name)] {
			return errors.New("invalid frozen input")
		}
		for i, c := range in.Name {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || i > 0 && (c == '.' || c == '_' || c == '-')) {
				return errors.New("invalid input name")
			}
		}
		// Runner v1 paths are printable ASCII; do not produce a different JSON encoding.
		for _, c := range in.Target {
			if c < 32 || c >= 127 {
				return errors.New("unsupported input target encoding")
			}
		}
		p := strings.ToLower(in.Target)
		for _, old := range paths {
			if old == p || strings.HasPrefix(old, p+"/") || strings.HasPrefix(p, old+"/") {
				return errors.New("overlapping frozen inputs")
			}
		}
		paths = append(paths, p)
		names[strings.ToLower(in.Name)] = true
		total += in.Object.Bytes
	}
	return nil
}
func (s InputSnapshot) Clone() InputSnapshot {
	s.Inputs = append(s.Inputs[:0:0], s.Inputs...)
	return s
}
func (s InputSnapshot) InputDigest() domain.SHA256Digest {
	// Field order is alphabetical, as in Python sort_keys=True. Staging paths and IDs
	// are deliberately excluded. HTML escaping would disagree for a target containing '&'.
	type entry struct {
		Bytes  int64               `json:"bytes"`
		Name   string              `json:"name"`
		SHA256 domain.SHA256Digest `json:"sha256"`
		Target string              `json:"target"`
	}
	values := make([]entry, 0, len(s.Inputs))
	for _, in := range s.Inputs {
		values = append(values, entry{in.Object.Bytes, in.Name, in.Object.SHA256, in.Target})
	}
	sort.Slice(values, func(i, j int) bool { return values[i].Target < values[j].Target })
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if enc.Encode(values) != nil {
		return ""
	}
	return Digest(bytes.TrimSuffix(b.Bytes(), []byte("\n")))
}

// PreparationObserver is read-only, including when no resource can be found. A miss
// never authorizes another Prepare call. Adapters without it cannot enter orchestration.
type PreparationObserver interface {
	ReconcilePreparation(context.Context, Plan, domain.OperationID) (PreparationObservation, error)
}
type PreparationObservation struct {
	Status   ReconciliationStatus
	Prepared *Prepared
}

func (o PreparationObservation) Validate(plan Plan, id domain.OperationID) error {
	switch o.Status {
	case ReconciliationFound:
		if o.Prepared == nil {
			return errors.New("missing preparation")
		}
		return o.Prepared.Validate(plan, id)
	case ReconciliationUnknown, ReconciliationNotFound:
		if o.Prepared != nil {
			return errors.New("unresolved preparation contains resource")
		}
		return nil
	default:
		return errors.New("invalid preparation observation")
	}
}
func (p Prepared) Validate(plan Plan, id domain.OperationID) error {
	if plan.Job.Validate() != nil || !id.Valid() || p.Identity != plan.Job.Identity || p.PreparationID != id || p.PlanSHA256 != plan.Digest() || !safeText(p.Resource, 1024) {
		return errors.New("preparation identity mismatch")
	}
	return nil
}

// BindingSnapshot is a non-secret immutable provider configuration identity. Values
// behind a credential reference may rotate; the adapter must verify the same account.
type BindingSnapshot struct {
	Binding       domain.ProviderBinding `json:"binding"`
	AccountScope  string                 `json:"account_scope"`
	CredentialRef string                 `json:"credential_ref,omitempty"`
}

func (s BindingSnapshot) Valid() bool {
	return s.Binding.Validate() == nil && domain.ObjectID(s.AccountScope).Valid() && len(s.CredentialRef) <= 1030 && !strings.ContainsAny(s.CredentialRef, "\x00\r\n")
}

// VerifyBinding must check the effective configured revision and actual credential
// account through the supported adapter path. A config label alone is not live evidence.
type BindingVerifier interface {
	VerifyBinding(context.Context, BindingSnapshot) error
}

// SnapshotRegistry retains exact revisions needed by accepted jobs and recovery. It
// neither replaces old entries nor falls back to a current profile/instance alias.
type SnapshotRegistry struct {
	mu      sync.RWMutex
	entries map[BindingSnapshot]Provider
}

func NewSnapshotRegistry() *SnapshotRegistry {
	return &SnapshotRegistry{entries: make(map[BindingSnapshot]Provider)}
}
func (r *SnapshotRegistry) Register(s BindingSnapshot, p Provider) error {
	if !s.Valid() || nilProvider(p) || p.Describe().InstanceID != s.Binding.ProviderInstanceID {
		return errors.New("invalid provider snapshot")
	}
	if _, ok := p.(BindingVerifier); !ok {
		return errors.New("provider cannot verify account binding")
	}
	if _, ok := p.(PreparationObserver); !ok {
		return errors.New("provider cannot reconcile staging")
	}
	check := NewRegistry()
	if err := check.Register(p); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.entries[s]; ok {
		return errors.New("provider snapshot already registered")
	}
	r.entries[s] = p
	return nil
}
func (r *SnapshotRegistry) Resolve(s BindingSnapshot) (Provider, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.entries[s]
	if !ok {
		return nil, errors.New("frozen provider revision unavailable; no fallback")
	}
	return p, nil
}
