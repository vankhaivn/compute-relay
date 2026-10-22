package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

type reasonCollectionProvider struct {
	*collectionProvider
	stage string
	fault error
}

func (p reasonCollectionProvider) ListArtifacts(ctx context.Context, remote provider.RemoteReference) ([]provider.Artifact, error) {
	if p.stage == "catalog" {
		return nil, p.fault
	}
	return p.collectionProvider.ListArtifacts(ctx, remote)
}
func (p reasonCollectionProvider) Fetch(ctx context.Context, remote provider.RemoteReference, a provider.Artifact, dst io.Writer, limit int64, progress func(int64) error) (provider.TransferResult, error) {
	if p.stage == "manifest" && a.Path == collection.ManifestPath || p.stage == "payload" && strings.HasPrefix(a.Path, "outputs/") {
		return provider.TransferResult{}, p.fault
	}
	return p.collectionProvider.Fetch(ctx, remote, a, dst, limit, progress)
}

func TestCollectionReasonSurvivesEveryProviderReadBoundary(t *testing.T) {
	for _, stage := range []string{"catalog", "manifest", "payload"} {
		for _, reason := range []string{"artifact_download_failed", "private_provider_canary", "private https://provider.invalid/token"} {
			t.Run(stage+"/"+reason, func(t *testing.T) {
				f, id, p, blobs := collectionFixture(t)
				problem := provider.Problem(domain.CodeArtifactCollectionFailed, domain.FailureStageResults, "private-message-canary").WithDetail(domain.ReasonDetail, reason).WithDetail("response", "private-detail-canary").WithCause(errors.New("private-cause-canary"))
				registry := provider.NewSnapshotRegistry()
				binding := provider.BindingSnapshot{Binding: f.profile.Binding, AccountScope: f.profile.AccountScope, CredentialRef: f.profile.CredentialRef}
				requireNoError(t, registry.Register(binding, reasonCollectionProvider{p, stage, problem}))
				engine, err := collection.New(f.s, registry, blobs, f.clock, collection.DefaultConfig())
				requireNoError(t, err)
				worked, err := engine.RunOnce(context.Background())
				var failure collection.Failure
				if !worked || !errors.As(err, &failure) || failure.Invalid {
					t.Fatal("provider failure changed retry/invalid semantics", err)
				}
				want := map[string]string{"catalog": "artifact_catalog_failed", "manifest": "artifact_manifest_download_failed", "payload": "artifact_download_failed"}[stage]
				if domain.ValidCollectionReason(reason) {
					want = reason
				}
				if failure.Reason != want {
					t.Fatal("unexpected failure reason", failure.Reason)
				}
				_, actor := authorizationService(t, f, "a", auth.Read)
				job, err := f.s.ReadJob(t.Context(), "a", actor.TokenID(), id.JobID)
				requireNoError(t, err)
				var raw string
				requireNoError(t, f.s.db.QueryRow("SELECT record FROM operations WHERE kind='collect'").Scan(&raw))
				op, err := decodeOperation(raw, 8192)
				requireNoError(t, err)
				for _, stored := range []*domain.Problem{job.Problem, op.Operation.Failure} {
					if stored == nil || stored.Code != domain.CodeArtifactCollectionFailed || stored.Details[domain.ReasonDetail] != want || !stored.SafeOperationRetry || !stored.ComputeMayHaveStarted || stored.RecommendedAction != domain.RecommendedActionCollect {
						t.Fatal("stored collection diagnostic changed", stored)
					}
					public, err := json.Marshal(stored)
					requireNoError(t, err)
					if strings.Contains(string(public), "private") || errors.Unwrap(*stored) != nil {
						t.Fatal("provider text reached stored public problem")
					}
				}
				assertNoNewCompute(t, f)
			})
		}
	}
}

func TestCollectionLocalStateReadFailuresRemainFatal(t *testing.T) {
	for _, stage := range []string{"catalog", "manifest", "payload"} {
		t.Run(stage, func(t *testing.T) {
			f, id, p, blobs := collectionFixture(t)
			cause := errors.New("local durable corruption")
			registry := provider.NewSnapshotRegistry()
			binding := provider.BindingSnapshot{Binding: f.profile.Binding, AccountScope: f.profile.AccountScope, CredentialRef: f.profile.CredentialRef}
			requireNoError(t, registry.Register(binding, reasonCollectionProvider{p, stage, provider.LocalStateError{Err: cause}}))
			engine, err := collection.New(f.s, registry, blobs, f.clock, collection.DefaultConfig())
			requireNoError(t, err)
			worked, err := engine.RunOnce(context.Background())
			if !worked || !provider.IsLocalStateError(err) || !errors.Is(err, cause) {
				t.Fatal("lost fatal local-state identity", err)
			}
			if f.journal(t, id).Problem != nil || controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='collect' AND status='failed'") != 0 {
				t.Fatal("local-state failure became collection evidence")
			}
		})
	}
}

func TestFailCollectionRejectsUnknownReasonBeforeMutation(t *testing.T) {
	f, _, _, _ := collectionFixture(t)
	work, err := f.s.ClaimCollection(t.Context(), time.Minute, 1)
	requireNoError(t, err)
	for _, failure := range []collection.Failure{
		{Code: domain.CodeArtifactCollectionFailed, Reason: "private_provider_canary"},
		{Code: domain.CodeArtifactDigestMismatch, Reason: "artifact_digest_mismatch", Cause: "private /path"},
	} {
		if err := f.s.FailCollection(t.Context(), *work, failure); !errors.Is(err, collection.ErrInvalid) {
			t.Fatal("accepted invalid reason", err)
		}
	}
	if controlCount(t, f.s, "SELECT count(*) FROM operations WHERE kind='collect' AND status='failed'") != 0 {
		t.Fatal("invalid reason mutated operation")
	}
}
