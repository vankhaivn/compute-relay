package kaggle

import (
	"context"
	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/ports"
)

type fixtureCredentialResolver struct {
	refs   map[ports.CredentialRef]bool
	lookup func(string) (string, bool)
}

func fixtureCredentials(refs []ports.CredentialRef, lookup func(string) (string, bool)) (*fixtureCredentialResolver, error) {
	return &fixtureCredentialResolver{refs: func() map[ports.CredentialRef]bool {
		m := map[ports.CredentialRef]bool{}
		for _, ref := range refs {
			m[ref] = true
		}
		return m
	}(), lookup: lookup}, nil
}

func (r *fixtureCredentialResolver) WithCredential(ctx context.Context, ref ports.CredentialRef, use func([]byte) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !r.refs[ref] {
		return credentials.ErrUnconfigured
	}
	value, ok := r.lookup(string(ref))
	if !ok || len(value) == 0 || len(value) > 8192 {
		return credentials.ErrUnavailable
	}
	raw := []byte(value)
	defer clear(raw)
	return use(raw)
}
