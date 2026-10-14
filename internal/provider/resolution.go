package provider

import "errors"

// LocalStateError preserves failures reading durable local state across provider
// boundaries. Its text is safe to expose; workers inspect the cause to retry or stop.
type LocalStateError struct{ Err error }

func (LocalStateError) Error() string   { return "local provider state could not be read" }
func (e LocalStateError) Unwrap() error { return e.Err }

func IsLocalStateError(err error) bool {
	var local LocalStateError
	return errors.As(err, &local)
}
