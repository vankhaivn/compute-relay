package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/retention"
)

type cleanupPreviewProbe struct {
	binding provider.BindingSnapshot
	outcome provider.CleanupOutcome
	after   func() error
	calls   int
	request provider.CleanupRequest
}

func (p *cleanupPreviewProbe) ResolvePreview(b provider.BindingSnapshot) (retention.PreviewSource, error) {
	if b != p.binding {
		return nil, errors.New("frozen binding remapped")
	}
	return p, nil
}
func (p *cleanupPreviewProbe) VerifyBinding(_ context.Context, b provider.BindingSnapshot) error {
	if b != p.binding {
		return errors.New("wrong account")
	}
	return nil
}
func (p *cleanupPreviewProbe) Preview(_ context.Context, request provider.CleanupRequest) (provider.CleanupOutcome, error) {
	p.calls++
	p.request = request
	if request.Mode != provider.CleanupDryRun || !request.LedgerID.Valid() || !request.CreationOperationID.Valid() || !request.ResultsCollected {
		return provider.CleanupOutcome{}, errors.New("unsafe preview request")
	}
	if p.after != nil {
		if err := p.after(); err != nil {
			return provider.CleanupOutcome{}, err
		}
	}
	return p.outcome, nil
}

func TestCleanupPreviewUsesLedgerAndNeverApplies(t *testing.T) {
	ctx := context.Background()
	f, id, adapter, blobs := collectionFixture(t)
	runCollection(t, collectionEngine(t, f, adapter, blobs, f.s))
	_, actor, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	var execution, staging domain.ProviderResourceID
	for purpose, destination := range map[string]*domain.ProviderResourceID{"execution": &execution, "staging": &staging} {
		if err := f.s.db.QueryRow("SELECT resource_id FROM provider_resources WHERE workspace_id='a' AND purpose=?", purpose).Scan(destination); err != nil {
			t.Fatal(err)
		}
	}
	binding := provider.BindingSnapshot{Binding: f.profile.Binding, AccountScope: f.profile.AccountScope, CredentialRef: f.profile.CredentialRef}
	probe := &cleanupPreviewProbe{binding: binding, outcome: provider.CleanupOutcome{AlreadyAbsent: true}}
	preview, err := retention.NewPreviewer(f.s, probe, f.clock)
	if err != nil {
		t.Fatal(err)
	}
	if r, err := preview.Preview(ctx, "a", actor.TokenID(), execution, retention.DefaultPolicy()); err != nil || r.Outcome != "retained" || probe.calls != 0 {
		t.Fatal("window did not retain provider resource", r, err)
	}
	f.clock.advance(8 * 24 * time.Hour)
	newProfile := f.profile
	newProfile.Binding.ConfigurationRevision = "replacement"
	newProfile.AccountScope = "another_account"
	if err := f.s.PutProfile(ctx, newProfile, true); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if r, err := preview.Preview(ctx, "a", actor.TokenID(), execution, retention.DefaultPolicy()); err != nil || r.Outcome != "already_absent" {
			t.Fatal("owned absence is not idempotent", r, err)
		}
	}
	if probe.calls != 2 || probe.request.Remote != *f.journal(t, id).Remote || controlCount(t, f.s, "SELECT count(*) FROM cleanup_previews") != 1 || controlCount(t, f.s, "SELECT count(*) FROM provider_resources WHERE cleanup_state!='pinned'") != 0 {
		t.Fatal("preview changed ledger or target identity")
	}
	if r, err := preview.Preview(ctx, "a", actor.TokenID(), staging, retention.DefaultPolicy()); err != nil || r.Outcome != "retained" || r.Reason != "staging_preview_unavailable" || probe.calls != 2 {
		t.Fatal("staging used execution cleanup port", r, err)
	}
	if _, err := preview.Preview(ctx, "a", actor.TokenID(), "res_execution_prefix_guess", retention.DefaultPolicy()); err == nil || probe.calls != 2 {
		t.Fatal("prefix substituted for ownership", err)
	}
	_, foreign, _ := controlService(t, f, "b", auth.Operate)
	if _, err := preview.Preview(ctx, "b", foreign.TokenID(), execution, retention.DefaultPolicy()); err == nil || probe.calls != 2 {
		t.Fatal("foreign ledger visible", err)
	}
	_, reader, _ := controlService(t, f, "a", auth.Read)
	if _, err := preview.Preview(ctx, "a", reader.TokenID(), execution, retention.DefaultPolicy()); !errors.Is(err, auth.ErrForbidden) {
		t.Fatal("read scope authorized cleanup preview", err)
	}
	assertNoNewCompute(t, f)
}

func TestCleanupPreviewRechecksPinsAuthorityAndOutcome(t *testing.T) {
	for _, mode := range []string{"hold", "revoke", "deleted", "contradictory"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			f, id, adapter, blobs := collectionFixture(t)
			runCollection(t, collectionEngine(t, f, adapter, blobs, f.s))
			_, actor, _ := controlService(t, f, "a", auth.Operate)
			f.clock.advance(8 * 24 * time.Hour)
			var resource domain.ProviderResourceID
			if err := f.s.db.QueryRow("SELECT resource_id FROM provider_resources WHERE purpose='execution'").Scan(&resource); err != nil {
				t.Fatal(err)
			}
			probe := &cleanupPreviewProbe{binding: provider.BindingSnapshot{Binding: f.profile.Binding, AccountScope: f.profile.AccountScope, CredentialRef: f.profile.CredentialRef}, outcome: provider.CleanupOutcome{WouldDelete: true}}
			switch mode {
			case "hold":
				probe.after = func() error { return f.s.SetRetentionHold(ctx, "a", "attempt", string(id.AttemptID), "operator-review", true, f.clock.Now()) }
			case "revoke":
				probe.after = func() error { return f.s.RevokeToken(ctx, actor.TokenID()) }
			case "deleted":
				probe.outcome.Deleted = true
			case "contradictory":
				probe.outcome.AlreadyAbsent = true
			}
			preview, err := retention.NewPreviewer(f.s, probe, f.clock)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := preview.Preview(ctx, "a", actor.TokenID(), resource, retention.DefaultPolicy()); err == nil {
				t.Fatal("stale or invalid preview accepted")
			}
			if controlCount(t, f.s, "SELECT count(*) FROM cleanup_previews") != 0 {
				t.Fatal("invalid preview persisted")
			}
			assertNoNewCompute(t, f)
		})
	}
}
