// Package credentials resolves only explicitly configured sources. It never scans
// home directories, logs credential values or changes the operator's environment.
package credentials

import (
	"context"
	"errors"
	"regexp"
	"strings"

	"github.com/vankhaivn/compute-relay/internal/ports"
)

const MaxBytes = 8192

var (
	ErrInvalid      = errors.New("invalid credential configuration")
	ErrUnconfigured = errors.New("credential source is not configured")
	ErrUnavailable  = errors.New("configured credential is unavailable")
	environmentName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)
)

// Environment holds references and a lookup function, never a cached secret.
// The supplied lookup must be concurrency-safe. os.LookupEnv is suitable for the
// explicitly composed runtime; tests should provide their own source.
type Environment struct {
	names  map[ports.CredentialRef]string
	lookup func(string) (string, bool)
}

var _ ports.CredentialResolver = (*Environment)(nil)

func EnvironmentName(ref ports.CredentialRef) (string, bool) {
	text := string(ref)
	name, ok := strings.CutPrefix(text, "env:")
	return name, ok && environmentName.MatchString(name)
}

func NewEnvironment(refs []ports.CredentialRef, lookup func(string) (string, bool)) (*Environment, error) {
	if lookup == nil || len(refs) == 0 || len(refs) > 64 {
		return nil, ErrInvalid
	}
	r := &Environment{names: make(map[ports.CredentialRef]string), lookup: lookup}
	for _, ref := range refs {
		name, ok := EnvironmentName(ref)
		if !ok || r.names[ref] != "" {
			return nil, ErrInvalid
		}
		r.names[ref] = name
	}
	return r, nil
}

func (r *Environment) WithCredential(ctx context.Context, ref ports.CredentialRef, use func([]byte) error) error {
	if r == nil || use == nil {
		return ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	name, ok := r.names[ref]
	if !ok {
		return ErrUnconfigured
	}
	value, ok := r.lookup(name)
	if err := ctx.Err(); err != nil {
		return err
	}
	if !ok || len(value) == 0 || len(value) > MaxBytes {
		return ErrUnavailable
	}
	secret := []byte(value)
	defer clear(secret) // Includes callback error/panic. OS environment/string copies are not erasable here.
	if err := use(secret); err != nil {
		return err
	}
	return ctx.Err()
}
