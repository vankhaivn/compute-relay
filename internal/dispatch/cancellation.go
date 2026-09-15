package dispatch

import (
	"context"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

// CancellationControl belongs to the exact attempt and shares its dispatch fence.
// Started is a durable one-shot gate, not a promise the provider received the call.
type CancellationControl struct {
	Operation domain.Operation
	Started   bool
}
type CancellationAction struct {
	Begin   bool
	Outcome *provider.CancellationOutcome
	Code    domain.ErrorCode
}
type CancellationRepository interface {
	CommitCancellation(context.Context, Handle, domain.OperationID, CancellationAction, time.Time) (Work, error)
}

func (s *session) commitCancellation(action CancellationAction) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	repo, ok := s.e.repo.(CancellationRepository)
	if !ok || s.current.Cancellation == nil {
		return ErrConflict
	}
	base := context.Background()
	if action.Begin {
		base = s.ctx
	}
	ctx, stop := context.WithTimeout(base, 5*time.Second)
	defer stop()
	work, err := repo.CommitCancellation(ctx, s.current.Handle, s.current.Cancellation.Operation.ID, action, s.e.clock.Now())
	if err == nil {
		s.current = work
	}
	return err
}

// cancelOnce is considered only after effective binding verification and an exact
// remote reference. Restarted callers with Started=true observe; they never replay.
func (s *session) cancelOnce(p provider.Provider) (bool, error) {
	w := s.work()
	if w.Cancellation == nil || w.Cancellation.Started || w.Journal.Phase != Submitted || w.Journal.Remote == nil {
		return false, nil
	}
	canceller, ok := p.(provider.Canceller)
	verified := false
	for _, cap := range p.Describe().Capabilities {
		if cap.Name == domain.CapabilityRemoteCancellation && cap.Validate() == nil && cap.Support == domain.CapabilitySupportSupported && len(cap.Conditions) == 0 && (cap.Evidence == domain.EvidenceImplementedOffline || cap.Evidence == domain.EvidencePassedLive) {
			verified = true
		}
	}
	if !ok || !verified {
		return true, s.commitCancellation(CancellationAction{Code: domain.CodeRemoteCancelUnsupported})
	}
	if err := s.commitCancellation(CancellationAction{Begin: true}); err != nil {
		return true, err
	}
	// Only this acknowledged transition grants a call. No error path re-arms it.
	outcome, err := control(s.ctx, s.e.config.ControlTimeout, func(ctx context.Context) (provider.CancellationOutcome, error) {
		return canceller.Cancel(ctx, *w.Journal.Remote, w.Cancellation.Operation.ID)
	})
	if err != nil || outcome.Validate() != nil {
		return true, s.commitCancellation(CancellationAction{Code: domain.CodeRemoteExecutionUnresolved})
	}
	return true, s.commitCancellation(CancellationAction{Outcome: &outcome})
}

func cancellationPending(state domain.AttemptState) bool {
	return state.Cancellation == domain.CancellationRequested || state.Cancellation == domain.CancellationRequesting || state.Cancellation == domain.CancellationAccepted
}

// Expected ownership loss is local contention, not a reason to stop unrelated
// workers. Corruption, I/O failures and invalid transitions remain fatal to Run.
func controlContention(err error) bool {
	return errors.Is(err, ErrConflict) || errors.Is(err, ErrStaleObservation)
}
