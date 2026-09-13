package provider

import (
	"context"
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
	seen := make(map[domain.CapabilityName]bool)
	for _, cap := range d.Capabilities {
		if err := cap.Validate(); err != nil {
			return err
		}
		if seen[cap.Name] {
			return errors.New("duplicate provider capability")
		}
		seen[cap.Name] = true
	}
	if d.Support(domain.CapabilityBatchExecution) != domain.CapabilitySupportSupported {
		return errors.New("provider must support finite batch execution")
	}
	_, cancel := p.(Canceller)
	_, logs := p.(LogReader)
	_, quota := p.(QuotaReader)
	if (d.Support(domain.CapabilityRemoteCancellation) == domain.CapabilitySupportSupported && !cancel) ||
		((d.Support(domain.CapabilityLogsWhileRunning) == domain.CapabilitySupportSupported || d.Support(domain.CapabilityLogsAfterCompletion) == domain.CapabilitySupportSupported) && !logs) ||
		(d.Support(domain.CapabilityQuotaReporting) == domain.CapabilitySupportSupported && !quota) {
		return errors.New("advertised optional capability has no implementation")
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
	for _, cap := range d.Capabilities {
		if cap.Name == name {
			return cap.Support
		}
	}
	return domain.CapabilitySupportUnknown
}

// RequestCancellation preserves unsupported intent without claiming remote termination.
func RequestCancellation(ctx context.Context, p Provider, remote RemoteReference, operation domain.OperationID) (CancellationOutcome, error) {
	if err := ctx.Err(); err != nil {
		return CancellationOutcome{}, err
	}
	if err := remote.Validate(); err != nil {
		return CancellationOutcome{}, err
	}
	if !operation.Valid() {
		return CancellationOutcome{}, errors.New("invalid cancellation operation")
	}
	if nilProvider(p) || p.Describe().InstanceID != remote.Identity.InstanceID {
		return CancellationOutcome{}, errors.New("provider instance mismatch")
	}
	cancel, ok := p.(Canceller)
	if !ok || p.Describe().Support(domain.CapabilityRemoteCancellation) != domain.CapabilitySupportSupported {
		return CancellationOutcome{Status: domain.CancellationManual}, nil
	}
	outcome, err := cancel.Cancel(ctx, remote, operation)
	if err != nil {
		return outcome, err
	}
	return outcome, outcome.Validate()
}
func ReadAvailableLogs(ctx context.Context, p Provider, remote RemoteReference, page PageRequest) (LogPage, error) {
	if err := ctx.Err(); err != nil {
		return LogPage{}, err
	}
	if err := remote.Validate(); err != nil {
		return LogPage{}, err
	}
	if err := page.Validate(); err != nil {
		return LogPage{}, err
	}
	if nilProvider(p) || p.Describe().InstanceID != remote.Identity.InstanceID {
		return LogPage{}, errors.New("provider instance mismatch")
	}
	logs, ok := p.(LogReader)
	if !ok {
		return LogPage{Source: "provider", Availability: "unavailable"}, nil
	}
	result, err := logs.ReadLogs(ctx, remote, page)
	if err != nil {
		return result, err
	}
	return result, result.Validate(page)
}
func ReadAvailableQuota(ctx context.Context, p Provider) (QuotaObservation, error) {
	if err := ctx.Err(); err != nil {
		return QuotaObservation{}, err
	}
	if nilProvider(p) {
		return QuotaObservation{}, errors.New("nil provider")
	}
	quota, ok := p.(QuotaReader)
	if !ok {
		return QuotaObservation{Status: QuotaUnknown}, nil
	}
	result, err := quota.ReadQuota(ctx)
	if err != nil {
		return result, err
	}
	return result, result.Validate()
}
