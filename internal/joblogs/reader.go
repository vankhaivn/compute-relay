// Package joblogs reads bounded output for an explicitly authorized original attempt.
package joblogs

import (
	"context"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

var (
	ErrInvalid     = errors.New("invalid log request")
	ErrNotFound    = errors.New("log attempt not found")
	ErrUnavailable = errors.New("logs unavailable")
)

// Attempt contains only the immutable provider binding and committed remote identity.
// A nil remote means submission has not yielded an accepted reference.
type Attempt struct {
	Binding provider.BindingSnapshot
	Remote  *provider.RemoteReference
}
type Repository interface {
	ReadLogAttempt(context.Context, domain.WorkspaceID, string, domain.JobID, domain.AttemptID) (Attempt, error)
}
type Resolver interface {
	Resolve(provider.BindingSnapshot) (provider.Provider, error)
}
type Reader struct {
	access   *auth.Service
	repo     Repository
	resolver Resolver
}

func NewReader(access *auth.Service, repo Repository, resolver Resolver) (*Reader, error) {
	if access == nil || repo == nil {
		return nil, ErrInvalid
	}
	return &Reader{access: access, repo: repo, resolver: resolver}, nil
}

// Read is observational: no scheduling, authorization, reconciliation or collection writes.
func (r *Reader) Read(ctx context.Context, p auth.Principal, w domain.WorkspaceID, job domain.JobID, attempt domain.AttemptID, request provider.PageRequest) (provider.LogPage, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	fresh, err := r.access.Revalidate(ctx, p)
	if err != nil {
		return provider.LogPage{}, err
	}
	if err = auth.Require(fresh, w, auth.Read); err != nil {
		return provider.LogPage{}, err
	}
	if !job.Valid() || !attempt.Valid() || request.Limit < 1 || request.Limit > 100 || request.Validate() != nil {
		return provider.LogPage{}, ErrInvalid
	}
	target, err := r.repo.ReadLogAttempt(ctx, w, fresh.TokenID(), job, attempt)
	if err != nil {
		return provider.LogPage{}, err
	}
	page := provider.LogPage{Source: "provider", Availability: "unavailable", Lines: []string{}}
	if target.Remote != nil && r.resolver != nil {
		remote := *target.Remote
		if !target.Binding.Valid() || remote.Validate() != nil || remote.Identity.WorkspaceID != w || remote.Identity.JobID != job || remote.Identity.AttemptID != attempt || remote.Identity.InstanceID != target.Binding.Binding.ProviderInstanceID {
			return provider.LogPage{}, ErrUnavailable
		}
		var adapter provider.Provider
		var resolveErr error
		if contextual, ok := r.resolver.(interface {
			ResolveContext(context.Context, provider.BindingSnapshot) (provider.Provider, error)
		}); ok {
			adapter, resolveErr = contextual.ResolveContext(ctx, target.Binding)
		} else {
			adapter, resolveErr = r.resolver.Resolve(target.Binding)
		}
		if resolveErr != nil || adapter == nil {
			if ctx.Err() != nil {
				return provider.LogPage{}, ctx.Err()
			}
			return provider.LogPage{}, ErrUnavailable
		}
		if _, supported := adapter.(provider.LogReader); supported {
			verifier, ok := adapter.(provider.BindingVerifier)
			if !ok || verifier.VerifyBinding(ctx, target.Binding) != nil {
				if ctx.Err() != nil {
					return provider.LogPage{}, ctx.Err()
				}
				return provider.LogPage{}, ErrUnavailable
			}
			page, err = provider.ReadAvailableLogs(ctx, adapter, remote, request)
			if err != nil {
				if ctx.Err() != nil {
					return provider.LogPage{}, ctx.Err()
				}
				if errors.Is(err, provider.ErrLogCursorInvalid) {
					return provider.LogPage{}, provider.ErrLogCursorInvalid
				}
				if errors.Is(err, provider.ErrLogCursorReset) {
					return provider.LogPage{}, provider.ErrLogCursorReset
				}
				var failure provider.LogReadFailure
				if errors.As(err, &failure) && failure.Valid() {
					return provider.LogPage{}, failure
				}
				return provider.LogPage{}, ErrUnavailable
			}
		}
	}
	if err = ctx.Err(); err != nil {
		return provider.LogPage{}, err
	}
	// Revocation while the provider was being read must prevent releasing its bytes.
	fresh, err = r.access.Revalidate(ctx, p)
	if err != nil {
		return provider.LogPage{}, err
	}
	if err = auth.Require(fresh, w, auth.Read); err != nil {
		return provider.LogPage{}, err
	}
	if page.Lines == nil {
		page.Lines = []string{}
	}
	return page, nil
}
