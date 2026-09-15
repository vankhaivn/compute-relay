package fake

import (
	"context"
	"errors"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

// Bound is explicitly composed test infrastructure. Its account check is a fixture,
// never evidence that an actual credential belongs to an actual provider account.
type Bound struct {
	*Basic
	binding provider.BindingSnapshot
}

func NewBound(b *Backend, clock ports.Clock, binding provider.BindingSnapshot) (*Bound, error) {
	if !binding.Valid() {
		return nil, errors.New("invalid fixture binding")
	}
	p, err := New(b, clock, binding.Binding.ProviderInstanceID)
	if err != nil {
		return nil, err
	}
	return &Bound{p, binding}, nil
}
func (p *Bound) VerifyBinding(ctx context.Context, s provider.BindingSnapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.binding != s {
		return identityError()
	}
	return nil
}
func (p *Basic) ReconcilePreparation(ctx context.Context, plan provider.Plan, operation domain.OperationID) (provider.PreparationObservation, error) {
	if err := ctx.Err(); err != nil {
		return provider.PreparationObservation{}, err
	}
	if plan.Job.Validate() != nil || !operation.Valid() || plan.Job.Identity.InstanceID != p.instance {
		return provider.PreparationObservation{}, identityError()
	}
	p.backend.mu.Lock()
	defer p.backend.mu.Unlock()
	r, ok := p.backend.records[attemptKey(plan.Job.Identity)]
	if !ok || r.deleted {
		return provider.PreparationObservation{Status: provider.ReconciliationNotFound}, nil
	}
	if r.plan.Digest() != plan.Digest() || r.prepared.PreparationID != operation {
		return provider.PreparationObservation{}, identityError()
	}
	prepared := r.prepared
	return provider.PreparationObservation{Status: provider.ReconciliationFound, Prepared: &prepared}, nil
}
