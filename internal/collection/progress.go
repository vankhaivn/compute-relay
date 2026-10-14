package collection

import (
	"context"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

// A progress acknowledgement failure preserves the accepted ticket, just like a
// pin/publication acknowledgement failure. It must not become a failed operation.
type progressWriteError struct{ error }

type progressTracker struct {
	engine                              *Engine
	work                                Work
	completed, current, received, total int64
	last                                time.Time
}

func newProgressTracker(e *Engine, w Work) *progressTracker {
	p := &progressTracker{engine: e, work: w}
	for _, file := range w.Snapshot.Files {
		if file.Role == "output" {
			p.total += file.Object.Bytes
		}
	}
	return p
}

func (p *progressTracker) checkpoint(ctx context.Context, stage string, force bool) error {
	now := p.engine.clock.Now().UTC()
	if !force && now.Sub(p.last) < time.Second {
		return nil
	}
	sample := domain.CollectionProgress{Scope: "selected_output_bytes", Generation: p.work.Lease.Generation,
		BytesCompleted: p.completed + p.current, BytesTotal: &p.total, ObservedAt: now}
	sample.BytesReceived = &p.received
	if err := p.engine.repo.RecordCollectionProgress(ctx, p.work, stage, sample); err != nil {
		return progressWriteError{err}
	}
	p.last = now
	return nil
}

func (p *progressTracker) stream(ctx context.Context, size int64) func(int64) error {
	base, previous := p.received, int64(0)
	return func(n int64) error {
		if n < previous || n > size {
			return ErrInvalid
		}
		previous, p.current, p.received = n, n, base+n
		return p.checkpoint(ctx, "transferring", false)
	}
}
