// Package cleanup previews and applies explicit, attempt-scoped remote deletion.
// Admission, receipts and public records belong to operations.
package cleanup

import "errors"

var (
	ErrInvalid   = errors.New("invalid cleanup record")
	ErrLeaseLost = errors.New("cleanup ownership expired or changed")
	// ErrDeferred keeps an interrupted deletion running until its lease expires and a later
	// claim can observe the provider again. It is not a failure of local state.
	ErrDeferred = errors.New("cleanup recovery deferred until the provider is reachable")
)
