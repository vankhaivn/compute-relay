package dispatch

import (
	"context"
	"errors"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"strings"
	"testing"
	"time"
)

func TestDispatchControlNamesEveryFailedBoundary(t *testing.T) {
	for _, step := range []string{"verify", "validate", "prepare", "reconcile_preparation", "revalidate_preparation", "submit", "reconcile_submission", "observe", "cancel"} {
		t.Run(step, func(t *testing.T) {
			cause := errors.New("private-canary")
			_, err := control(t.Context(), time.Second, step, func(context.Context) (int, error) { return 0, cause })
			if !errors.Is(err, cause) || provider.Reason(err, "") != step+"_failed" || strings.Contains(err.Error(), "private") {
				t.Fatal("lost closed boundary", err)
			}
			_, err = control(t.Context(), time.Second, step, func(context.Context) (int, error) { panic("private-canary") })
			if provider.Reason(err, "") != step+"_panic" || strings.Contains(err.Error(), "private") {
				t.Fatal("lost panic boundary", err)
			}
		})
	}
}
