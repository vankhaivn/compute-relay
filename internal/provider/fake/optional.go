package fake

import (
	"context"
	"errors"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

// Complete exposes optional fixture capabilities for contract testing. Basic remains a
// truly minimal Provider; a caller cannot type-assert optional methods on it.
type Complete struct{ *Basic }

func NewComplete(b *Backend, clock ports.Clock, instance domain.ProviderInstanceID) (*Complete, error) {
	base, err := New(b, clock, instance)
	if err != nil {
		return nil, err
	}
	base.complete = true
	return &Complete{Basic: base}, nil
}

var _ provider.Canceller = (*Complete)(nil)
var _ provider.LogReader = (*Complete)(nil)
var _ provider.QuotaReader = (*Complete)(nil)

func (p *Complete) Describe() provider.Descriptor {
	d := p.Basic.Describe()
	for i := range d.Capabilities {
		switch d.Capabilities[i].Name {
		case domain.CapabilityRemoteCancellation, domain.CapabilityLogsWhileRunning, domain.CapabilityLogsAfterCompletion, domain.CapabilityQuotaReporting:
			d.Capabilities[i].Support = domain.CapabilitySupportSupported
		}
	}
	return d
}
func (p *Complete) Cancel(ctx context.Context, remote provider.RemoteReference, operation domain.OperationID) (provider.CancellationOutcome, error) {
	if err := ctx.Err(); err != nil {
		return provider.CancellationOutcome{}, err
	}
	if !operation.Valid() {
		return provider.CancellationOutcome{}, errors.New("invalid cancellation operation")
	}
	p.backend.mu.Lock()
	defer p.backend.mu.Unlock()
	r, err := p.lookup(remote)
	if err != nil {
		return provider.CancellationOutcome{}, err
	}
	if p.state(r).Terminal() {
		return provider.CancellationOutcome{Status: domain.CancellationTooLate}, nil
	}
	r.cancelRequested = true
	return provider.CancellationOutcome{Status: domain.CancellationAccepted, TerminationConfirmed: false}, nil
}
func (p *Complete) ReadLogs(ctx context.Context, remote provider.RemoteReference, page provider.PageRequest) (provider.LogPage, error) {
	if err := ctx.Err(); err != nil {
		return provider.LogPage{}, err
	}
	if err := page.Validate(); err != nil {
		return provider.LogPage{}, err
	}
	p.backend.mu.Lock()
	defer p.backend.mu.Unlock()
	r, err := p.lookup(remote)
	if err != nil {
		return provider.LogPage{}, err
	}
	lines := []string{"fixture log: no workload was executed", "fixture log: no credentials or network used"}
	start, err := pageOffset(remote, page.Cursor, len(lines))
	if err != nil {
		return provider.LogPage{}, err
	}
	end := start + page.Limit
	if end > len(lines) {
		end = len(lines)
	}
	availability := "delayed"
	if p.state(r) == domain.ExecutionRunning {
		availability = "live"
	} else if p.state(r).Terminal() {
		availability = "after_completion"
	}
	result := provider.LogPage{Source: "provider", Availability: availability, Lines: append([]string(nil), lines[start:end]...)}
	if end < len(lines) {
		result.NextCursor = cursor(remote, end)
	}
	return result, nil
}
func (p *Complete) ReadQuota(ctx context.Context) (provider.QuotaObservation, error) {
	if err := ctx.Err(); err != nil {
		return provider.QuotaObservation{}, err
	}
	// Synthetic values are NOT an account allowance or an estimate from elapsed time.
	limit, used, remaining := 10.0, 1.0, 9.0
	return provider.QuotaObservation{Status: provider.QuotaKnown, Resource: "fixture", Unit: "hours", Limit: &limit, Used: &used, Remaining: &remaining,
		ObservedAt: p.clock.Now(), Source: "fake fixture", Precision: "synthetic"}, nil
}
