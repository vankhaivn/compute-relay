package provider

import (
	"errors"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/worker"
)

type localBusy struct{}

func (localBusy) Error() string   { return "private store diagnostic" }
func (localBusy) Temporary() bool { return true }

func TestLocalStateErrorPreservesSafeTextAndAllCauses(t *testing.T) {
	fatal := errors.New("private invariant diagnostic")
	for _, cause := range []error{localBusy{}, fatal, errors.Join(localBusy{}, fatal)} {
		err := LocalStateError{Err: cause}
		if !IsLocalStateError(err) || !errors.Is(err, cause) || strings.Contains(err.Error(), "private") {
			t.Fatal("local origin or safe text was lost", err)
		}
		if worker.Classify(err) != worker.Classify(cause) {
			t.Fatal("local wrapper changed retry/stop policy", err)
		}
	}
}
