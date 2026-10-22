package hoststorage

import (
	"context"
	"errors"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

type previewRepository struct {
	CleanupRepository
	err error
}

func (r previewRepository) FindStorageOrphan(context.Context, domain.WorkspaceID, string, string, string) (Candidate, error) {
	return Candidate{}, r.err
}
func (r previewRepository) RecordStoragePreview(context.Context, domain.WorkspaceID, string, Candidate, TargetObservation) (Preview, error) {
	return Preview{}, nil
}

type previewResolver struct {
	p   provider.Provider
	err error
}

func (r previewResolver) Resolve(provider.BindingSnapshot) (provider.Provider, error) {
	return r.p, r.err
}

type previewProvider struct {
	provider.Provider
	verify, observe error
	panics          bool
}

func (p previewProvider) Verify(context.Context, provider.BindingSnapshot) error { return p.verify }
func (p previewProvider) ObserveStorageTarget(context.Context, Target) (TargetObservation, error) {
	if p.panics {
		panic("private canary")
	}
	return TargetObservation{}, p.observe
}

func TestStoragePreviewNamesFailedReadStep(t *testing.T) {
	cause := errors.New("private canary")
	for _, tc := range []struct {
		name     string
		repoErr  error
		resolver previewResolver
		want     string
	}{
		{name: "target", repoErr: cause, want: "storage_preview_target_failed"},
		{name: "resolve", resolver: previewResolver{err: cause}, want: "storage_preview_binding_unavailable"},
		{name: "verify", resolver: previewResolver{p: previewProvider{verify: provider.WithReason(cause, "host_key_mismatch")}}, want: "host_key_mismatch"},
		{name: "observe", resolver: previewResolver{p: previewProvider{observe: cause}}, want: "storage_preview_observation_failed"},
		{name: "panic", resolver: previewResolver{p: previewProvider{panics: true}}, want: "storage_preview_observation_failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := NewService(previewRepository{err: tc.repoErr}, tc.resolver)
			_, err := s.Preview(context.Background(), "a", "token", "connection", "orphan")
			if err == nil || provider.Reason(err, "") != tc.want {
				t.Fatal("missing failure reason", err)
			}
			if tc.name != "panic" && !errors.Is(err, cause) {
				t.Fatal("lost error identity")
			}
		})
	}
}
