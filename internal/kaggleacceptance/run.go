package kaggleacceptance

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle"
)

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now().UTC() }

var processOnce sync.Once
var processID string
var processErr error

// Stable within one process, new after exec. It is local restart evidence, not a
// provider session token or an attestation against the trusted host owner.
func processNonce() (string, error) {
	processOnce.Do(func() { processID, processErr = randomNonce() })
	return processID, processErr
}

type dependencies struct {
	clock        ports.Clock
	process      string
	fixture      bool
	makeProvider func(*session, bool, ports.Clock) (provider.Provider, error)
	pause        func(context.Context) error
}

func actualProvider(s *session, allow bool, clock ports.Clock) (provider.Provider, error) {
	r := s.record
	resolver, err := credentials.NewEnvironment([]ports.CredentialRef{r.Config.CredentialRef}, os.LookupEnv)
	if err != nil {
		return nil, err
	}
	scope := kaggle.AcceptanceScope{Binding: r.binding(), WorkspaceID: Workspace, JobID: r.Receipt.JobID, AttemptID: r.Receipt.AttemptID, SpecificationSHA256: r.SpecificationSHA256}
	return kaggle.NewAcceptanceAdapter(r.Config, scope, resolver, s.inputs, clock, s.original, r.MachineShape, allow)
}

func Run(ctx context.Context, o Options) (Report, error) {
	if !o.valid() {
		return Report{}, ErrPermission
	}
	process, err := processNonce()
	if err != nil {
		return Report{}, err
	}
	return runWith(ctx, o, dependencies{clock: wallClock{}, process: process, makeProvider: actualProvider, pause: func(ctx context.Context) error {
		timer := time.NewTimer(time.Second)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	}})
}

func runWith(parent context.Context, o Options, d dependencies) (report Report, err error) {
	if !o.valid() || d.clock == nil || d.makeProvider == nil || d.pause == nil || !domain.SHA256Digest(d.process).Valid() {
		return report, ErrPermission
	}
	ctx, cancel := context.WithTimeout(parent, o.MaxWait)
	defer cancel()
	s, err := openSession(ctx, o, d.fixture)
	if err != nil {
		return report, err
	}
	defer func() {
		if closeErr := s.close(); closeErr != nil {
			report.Status = "local-close-failed"
			report.Evidence = "not-tested"
			err = errors.Join(err, ErrState)
		}
	}()
	report, err = stateReport(ctx, s)
	if err != nil || o.Mode == "prepare" {
		return report, err
	}
	if o.Mode == "status" {
		return verifyPublication(ctx, s)
	}
	j, err := s.journal(ctx)
	if err != nil {
		return report, ErrState
	}
	if o.Mode == "submit" && j.SubmitStarted {
		report.Status = "resume-required"
		return report, nil
	}
	if o.Mode != "submit" {
		var mark submissionMark
		if !j.SubmitStarted || j.Plan == nil || readRecord(filepath.Join(s.root.Path, "submission-process.json"), &mark) != nil || !validMark(mark, j.Plan.Digest(), j.Plan.Job.Identity.IntentID) {
			return report, ErrState
		}
		if mark.ProcessNonce == d.process {
			report.Status = "restart-required"
			return report, nil
		}
	}
	p, err := d.makeProvider(s, o.Mode == "submit", d.clock)
	if err != nil {
		return report, ErrState
	}
	registry := provider.NewSnapshotRegistry()
	if err := registry.Register(s.record.binding(), p); err != nil {
		return report, err
	}
	if o.Mode == "submit" {
		quota, err := provider.ReadAvailableQuota(ctx, p)
		if err != nil || quota.Status != provider.QuotaKnown || quota.Remaining == nil || *quota.Remaining < 120 {
			report.Status = "quota-blocked"
			return report, nil
		}
		if err := s.store.RecordQuota(ctx, s.record.Config.AccountName, quota); err != nil {
			return report, err
		}
	}
	repo := &markedRepository{Repository: s.store, s: s, allow: o.Mode == "submit", process: d.process}
	engine, err := dispatch.New(repo, registry, s.inputs, nil, d.clock, dispatch.DefaultConfig())
	if err != nil {
		return report, err
	}
	collector, err := collection.New(s.store, registry, s.results, d.clock, collection.DefaultConfig())
	if err != nil {
		return report, err
	}
	if o.Mode == "collect" {
		// A nil input reader disables compute retry in this service. This path can
		// request collection only; it never creates another attempt or reads inputs.
		service, err := operations.New(s.access, s.store, nil, d.clock, admission.DefaultLimits())
		if err != nil {
			return report, err
		}
		raw, _ := json.Marshal(map[string]any{"attempt_id": s.record.Receipt.AttemptID})
		if _, err := service.Submit(ctx, s.actor, Workspace, s.record.Receipt.JobID, domain.OperationCollect, o.CollectionKey, raw); err != nil {
			return report, err
		}
	}
	for polls := 0; polls < 1800; polls++ {
		if err := ctx.Err(); err != nil {
			report.Status = "local-budget-exceeded"
			return report, err
		}
		report, err = stateReport(ctx, s)
		if err != nil {
			return report, err
		}
		j, err = s.journal(ctx)
		if err != nil {
			return report, err
		}
		if report.Result == domain.ResultAvailable {
			if o.Mode != "submit" {
				if err := ensureResume(ctx, s, p, d); err != nil {
					return report, err
				}
			}
			return verifyPublication(ctx, s)
		}
		if o.Mode == "submit" && j.SubmitStarted {
			report.Status = "resume-required"
			return report, nil
		}
		if j.Phase == dispatch.Attention || j.Phase == dispatch.Failed || j.Phase == dispatch.Rejected || j.Phase == dispatch.Prevented {
			report.Status = "needs-attention"
			return report, nil
		}
		if j.Phase == dispatch.Collectible {
			if o.Mode == "submit" {
				report.Status = "resume-required"
				return report, nil
			}
			worked, err := collector.RunOnce(ctx)
			if err != nil {
				report, _ = stateReport(ctx, s)
				return report, err
			}
			if !worked && (report.Result == domain.ResultIncomplete || report.Result == domain.ResultInvalid) {
				report.Status = "collection-retry-required"
				return report, nil
			}
		} else {
			_, err := engine.RunOnce(ctx, "acceptance_worker")
			if err != nil {
				report, _ = stateReport(ctx, s)
				return report, err
			}
		}
		if err := d.pause(ctx); err != nil {
			report.Status = "local-budget-exceeded"
			return report, err
		}
	}
	report.Status = "local-budget-exceeded"
	return report, context.DeadlineExceeded
}

func ensureResume(ctx context.Context, s *session, p provider.Provider, d dependencies) error {
	j, err := s.journal(ctx)
	if err != nil || j.Plan == nil || j.Remote == nil || !j.SubmitStarted {
		return ErrEvidence
	}
	var start submissionMark
	if readRecord(filepath.Join(s.root.Path, "submission-process.json"), &start) != nil || !validMark(start, j.Plan.Digest(), j.Plan.Job.Identity.IntentID) || start.ProcessNonce == d.process {
		return ErrEvidence
	}
	path := filepath.Join(s.root.Path, "resume-process.json")
	if _, err := os.Lstat(path); err == nil {
		var previous submissionMark
		if readRecord(path, &previous) != nil || !validMark(previous, j.Plan.Digest(), j.Plan.Job.Identity.IntentID) || previous.ProcessNonce == start.ProcessNonce {
			return ErrEvidence
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return ErrState
	}
	// An independent post-reopen exact-identity observation is required even if
	// collection publication had committed before an acknowledgement was lost.
	observed, err := p.Observe(ctx, *j.Remote)
	if err != nil || observed.Validate(*j.Remote) != nil || !observed.Execution.Terminal() {
		return ErrEvidence
	}
	mark := submissionMark{1, d.process, j.Plan.Digest(), j.Plan.Job.Identity.IntentID, d.clock.Now().UTC()}
	return writeRecord(path, mark)
}
