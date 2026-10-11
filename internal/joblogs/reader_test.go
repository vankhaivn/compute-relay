package joblogs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/testsupport"
)

type logRepo struct {
	target Attempt
	calls  int
}

func (r *logRepo) ReadLogAttempt(_ context.Context, _ domain.WorkspaceID, _ string, _ domain.JobID, _ domain.AttemptID) (Attempt, error) {
	r.calls++
	return r.target, nil
}

type logResolver struct {
	adapter provider.Provider
	calls   int
}

func (r *logResolver) Resolve(provider.BindingSnapshot) (provider.Provider, error) {
	r.calls++
	return r.adapter, nil
}

type logProvider struct {
	provider.Provider
	read     func(context.Context) (provider.LogPage, error)
	verified int
}

func (p *logProvider) Describe() provider.Descriptor {
	return provider.Descriptor{InstanceID: "fixture"}
}
func (p *logProvider) VerifyBinding(context.Context, provider.BindingSnapshot) error {
	p.verified++
	return nil
}
func (p *logProvider) ReadLogs(ctx context.Context, _ provider.RemoteReference, _ provider.PageRequest) (provider.LogPage, error) {
	return p.read(ctx)
}
func logSetup(t *testing.T) (*Reader, *auth.Service, auth.Principal, *logRepo, *logResolver) {
	t.Helper()
	m := testsupport.NewMemory(auth.Workspace{ID: "ws_fixture", Enabled: true})
	access, err := auth.New(m, m, nil)
	if err != nil {
		t.Fatal(err)
	}
	secret, _, err := access.Issue(context.Background(), "ws_fixture", []auth.Scope{auth.Read}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	p, err := access.Authenticate(context.Background(), secret.Reveal())
	if err != nil {
		t.Fatal(err)
	}
	job := fake.ExampleJob("fixture")
	repo := &logRepo{target: Attempt{Binding: provider.BindingSnapshot{Binding: job.Binding, AccountScope: "fixture_account", CredentialRef: "fixture_credential"}, Remote: &provider.RemoteReference{Identity: job.Identity, Resource: "original-resource", Version: "1"}}}
	resolver := &logResolver{}
	reader, err := NewReader(access, repo, resolver)
	if err != nil {
		t.Fatal(err)
	}
	return reader, access, p, repo, resolver
}
func TestReadRevalidatesAuthorityBeforeAndAfterProviderBytes(t *testing.T) {
	r, access, p, repo, resolver := logSetup(t)
	adapter := &logProvider{read: func(ctx context.Context) (provider.LogPage, error) {
		if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 10*time.Second {
			t.Error("provider read lacked deadline")
		}
		if err := access.Revoke(ctx, p.TokenID()); err != nil {
			t.Fatal(err)
		}
		return provider.LogPage{Source: "payload", Availability: "live", Lines: []string{"must not escape"}}, nil
	}}
	resolver.adapter = adapter
	page, err := r.Read(context.Background(), p, "ws_fixture", "job_fixture", "att_fixture", provider.PageRequest{Limit: 100})
	if !errors.Is(err, auth.ErrUnauthenticated) || len(page.Lines) != 0 || adapter.verified != 1 {
		t.Fatal("revoked output released", page, err)
	}
	_, err = r.Read(context.Background(), p, "ws_fixture", "job_fixture", "att_fixture", provider.PageRequest{Limit: 100})
	if !errors.Is(err, auth.ErrUnauthenticated) || repo.calls != 1 {
		t.Fatal("revoked caller reached repository", err, repo.calls)
	}
}
func TestReadUnavailableAndOriginalIdentity(t *testing.T) {
	for _, mode := range []string{"before-submit", "no-resolver", "no-logs", "wrong-job", "wrong-attempt", "wrong-instance"} {
		t.Run(mode, func(t *testing.T) {
			r, _, p, repo, resolver := logSetup(t)
			switch mode {
			case "before-submit":
				repo.target.Remote = nil
			case "no-resolver":
				r.resolver = nil
			case "no-logs":
				resolver.adapter = &struct{ provider.Provider }{nil}
			case "wrong-job":
				repo.target.Remote.Identity.JobID = "another-job"
			case "wrong-attempt":
				repo.target.Remote.Identity.AttemptID = "another-attempt"
			case "wrong-instance":
				repo.target.Remote.Identity.InstanceID = "another-instance"
			}
			page, err := r.Read(context.Background(), p, "ws_fixture", "job_fixture", "att_fixture", provider.PageRequest{Limit: 100})
			if mode == "wrong-job" || mode == "wrong-attempt" || mode == "wrong-instance" {
				if !errors.Is(err, ErrUnavailable) || resolver.calls != 0 {
					t.Fatal("substituted identity reached provider", err)
				}
			} else if err != nil || page.Availability != "unavailable" || len(page.Lines) != 0 || page.Lines == nil || page.NextCursor != "" {
				t.Fatal("invented logs", page, err)
			}
		})
	}
}
func TestReadCancellationAndMalformedProviderOutput(t *testing.T) {
	r, _, p, _, resolver := logSetup(t)
	adapter := &logProvider{read: func(ctx context.Context) (provider.LogPage, error) {
		<-ctx.Done()
		return provider.LogPage{}, ctx.Err()
	}}
	resolver.adapter = adapter
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*20)
	defer cancel()
	if _, err := r.Read(ctx, p, "ws_fixture", "job_fixture", "att_fixture", provider.PageRequest{Limit: 100}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	adapter.read = func(context.Context) (provider.LogPage, error) {
		return provider.LogPage{Source: "private-provider-name", Availability: "live", Lines: []string{"unsafe"}}, nil
	}
	if page, err := r.Read(context.Background(), p, "ws_fixture", "job_fixture", "att_fixture", provider.PageRequest{Limit: 100}); !errors.Is(err, ErrUnavailable) || len(page.Lines) > 0 {
		t.Fatal(page, err)
	}
	adapter.read = func(context.Context) (provider.LogPage, error) {
		return provider.LogPage{Source: "payload", Availability: "live", Lines: []string{"progress"}, NextCursor: "opaque", Truncated: true}, nil
	}
	page, err := r.Read(context.Background(), p, "ws_fixture", "job_fixture", "att_fixture", provider.PageRequest{Limit: 100})
	if err != nil || page.Lines[0] != "progress" || page.NextCursor != "opaque" || !page.Truncated {
		t.Fatal(page, err)
	}
}
func TestReadWrongWorkspaceNeverReachesRepository(t *testing.T) {
	r, _, p, repo, _ := logSetup(t)
	if _, err := r.Read(context.Background(), p, "other", "job_fixture", "att_fixture", provider.PageRequest{Limit: 100}); !errors.Is(err, auth.ErrForbidden) || repo.calls != 0 {
		t.Fatal(err)
	}
}
