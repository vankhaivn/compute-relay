package provider

import (
	"errors"
	"reflect"
	"sort"
	"sync"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

// Registry wires explicitly configured instances. It performs no discovery, credential
// resolution, default selection, or fallback. Constructors remain at the composition root.
type Registry struct {
	mu        sync.RWMutex
	instances map[domain.ProviderInstanceID]Provider
}

func NewRegistry() *Registry {
	return &Registry{instances: make(map[domain.ProviderInstanceID]Provider)}
}
func (r *Registry) Register(p Provider) error {
	if nilProvider(p) {
		return errors.New("nil provider")
	}
	d := p.Describe()
	if !safeText(d.Type, 128) || !d.InstanceID.Valid() || !safeText(d.Version, 128) {
		return errors.New("invalid provider descriptor")
	}
	if !d.Capabilities.Valid() {
		return errors.New("invalid provider capabilities")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.instances == nil {
		r.instances = make(map[domain.ProviderInstanceID]Provider)
	}
	if _, exists := r.instances[d.InstanceID]; exists {
		return errors.New("provider instance already registered")
	}
	r.instances[d.InstanceID] = p
	return nil
}
func (r *Registry) Lookup(id domain.ProviderInstanceID) (Provider, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.instances[id]
	if !ok {
		return nil, Problem(domain.CodeConfigurationInvalid, domain.FailureStageLocalRuntime, "provider instance is not registered; no fallback was selected")
	}
	return p, nil
}
func (r *Registry) Instances() []domain.ProviderInstanceID {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]domain.ProviderInstanceID, 0, len(r.instances))
	for id := range r.instances {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
func nilProvider(p Provider) bool {
	if p == nil {
		return true
	}
	v := reflect.ValueOf(p)
	switch v.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Interface, reflect.Func, reflect.Chan:
		return v.IsNil()
	}
	return false
}
func (d Descriptor) Support(name domain.CapabilityName) domain.CapabilitySupport {
	return d.Capabilities.Support(name)
}
