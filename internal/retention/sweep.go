package retention

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

type DeletionPage struct {
	Items     []Deletion
	NextAfter int64
}

type Repository interface {
	BindRetentionStores(context.Context, string, string) error
	ExpireRetention(context.Context, time.Time, Policy, int) (SweepReport, error)
	PendingRetention(context.Context, int64, int) (DeletionPage, error)
	CompleteRetention(context.Context, Deletion, time.Time) error
}

type BlobDeleter interface {
	RetentionIdentity(context.Context) (string, error)
	Delete(context.Context, domain.ObjectMetadata) (bool, error)
}

type Clock interface{ Now() time.Time }

type Sweeper struct {
	mu      sync.Mutex
	repo    Repository
	inputs  BlobDeleter
	results BlobDeleter
	clock   Clock
}

func nilDependency(value any) bool {
	if value == nil {
		return true
	}
	switch v := reflect.ValueOf(value); v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		return v.IsNil()
	default:
		return false
	}
}

func NewSweeper(repo Repository, inputs, results BlobDeleter, clock Clock) (*Sweeper, error) {
	if nilDependency(repo) || nilDependency(inputs) || nilDependency(results) || nilDependency(clock) {
		return nil, ErrInvalid
	}
	return &Sweeper{repo: repo, inputs: inputs, results: results, clock: clock}, nil
}

// SweepOnce is a finite explicit composition call, never an automatic admission,
// GET or migration side effect. Pass the returned cursor to reach later pending
// objects even when an earlier object cannot be deleted. Zero restarts a pass.
func (s *Sweeper) SweepOnce(ctx context.Context, policy Policy, after int64, limit int) (SweepReport, int64, error) {
	if !policy.Valid() || after < 0 || limit < 1 || limit > 100 {
		return SweepReport{}, after, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	inputID, err := s.inputs.RetentionIdentity(ctx)
	if err != nil {
		return SweepReport{}, after, err
	}
	resultID, err := s.results.RetentionIdentity(ctx)
	if err != nil {
		return SweepReport{}, after, err
	}
	if inputID == resultID || !domain.ObjectID(inputID).Valid() || !domain.ObjectID(resultID).Valid() {
		return SweepReport{}, after, ErrInvalid
	}
	if err := s.repo.BindRetentionStores(ctx, inputID, resultID); err != nil {
		return SweepReport{}, after, err
	}
	report, err := s.repo.ExpireRetention(ctx, s.clock.Now(), policy, limit)
	if err != nil {
		// An uncertain tombstone commit does not authorize filesystem I/O.
		return SweepReport{}, after, err
	}
	page, err := s.repo.PendingRetention(ctx, after, limit)
	if err != nil {
		return report, after, err
	}
	if len(page.Items) > limit || (len(page.Items) > 0 && page.NextAfter <= after) {
		return report, after, ErrInvalid
	}
	var failures []error
	for _, d := range page.Items {
		if !d.Valid() {
			return report, after, ErrInvalid
		}
		store := s.inputs
		if d.Kind == Result {
			store = s.results
		}
		call, done := context.WithTimeout(ctx, time.Minute)
		absent, err := store.Delete(call, d.Object)
		done()
		if err != nil {
			report.Pending++
			failures = append(failures, err)
			continue
		}
		if err := s.repo.CompleteRetention(ctx, d, s.clock.Now()); err != nil {
			report.Pending++
			failures = append(failures, err)
			continue
		}
		if absent {
			report.AlreadyAbsent++
		} else {
			report.Deleted++
		}
	}
	return report, page.NextAfter, errors.Join(failures...)
}
