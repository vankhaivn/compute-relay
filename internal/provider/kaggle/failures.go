package kaggle

import (
	"context"
	"errors"

	"github.com/vankhaivn/compute-relay/internal/provider"
)

func contextStep(err error, prefix string) error {
	reason := prefix + "_interrupted"
	if errors.Is(err, context.DeadlineExceeded) {
		reason = prefix + "_timeout"
	}
	return provider.WithReason(err, reason)
}
