// Package cleanup previews and applies explicit, attempt-scoped remote deletion.
// Admission, receipts and public records belong to operations.
package cleanup

import "errors"

var (
	ErrInvalid   = errors.New("invalid cleanup record")
	ErrLeaseLost = errors.New("cleanup ownership expired or changed")
)
