package provider

import "context"

type preparationProgressKey struct{}

// WithPreparationProgress lets an adapter that can observe its own upload report absolute
// staged input bytes during Prepare. Reports are observational and never affect the outcome.
func WithPreparationProgress(ctx context.Context, report func(completed, total int64)) context.Context {
	return context.WithValue(ctx, preparationProgressKey{}, report)
}

// ReportPreparationProgress is a no-op when the caller did not ask for progress.
func ReportPreparationProgress(ctx context.Context, completed, total int64) {
	if report, ok := ctx.Value(preparationProgressKey{}).(func(int64, int64)); ok && report != nil {
		report(completed, total)
	}
}
