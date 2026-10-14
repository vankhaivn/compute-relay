package kaggle

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

type journalConnectionAdapter struct{ clock *stagingClock }

func (journalConnectionAdapter) Descriptor() connections.Descriptor {
	return connections.Descriptor{Type: "kaggle", Label: "Journal fixture", Fields: []connections.Field{{Name: "api_token", Label: "Token", Required: true, WriteOnly: true, MaxBytes: 8192}}, Capabilities: connections.Capabilities{Accelerators: []string{"cpu"}, RemoteCancel: "unsupported", Quota: "supported"}}
}
func (journalConnectionAdapter) Encode(fields map[string]string) ([]byte, error) {
	return []byte(fields["api_token"]), nil
}
func (a journalConnectionAdapter) Verify(context.Context, []byte) (connections.Verification, error) {
	limit, used, left := 3600.0, 0.0, 3600.0
	return connections.Verification{CanonicalAccount: "fixture_account", Quota: provider.QuotaObservation{Status: provider.QuotaKnown, Resource: "gpu", Unit: "seconds", Limit: &limit, Used: &used, Remaining: &left, ObservedAt: a.clock.Now(), Source: "synthetic", Precision: "lower_bound"}}, nil
}
func (journalConnectionAdapter) Profile(binding domain.ProviderBinding, account string) admission.Profile {
	p := admission.DefaultProfile(binding, account)
	p.Accelerator = "cpu"
	return p
}

// A generated alias must reject repointing; disabling it still retains its exact
// original revision for recovery. Ordinary admission uses the managed service.
func replaceJournalProfile(t *testing.T, f *stagingJournalFixture, p admission.Profile, enabled bool) error {
	t.Helper()
	raw, err := json.Marshal(p)
	if err != nil {
		return err
	}
	db, err := sql.Open("sqlite", filepath.Join(f.root, "runtime.db"))
	if err != nil {
		return err
	}
	defer db.Close()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.Exec(`INSERT INTO profile_revisions VALUES(?,?,?)`, p.Binding.Profile, p.Binding.ConfigurationRevision, string(raw)); err != nil {
		return err
	}
	if _, err = tx.Exec(`UPDATE profiles SET revision=? WHERE profile=?`, p.Binding.ConfigurationRevision, p.Binding.Profile); err == nil || !strings.Contains(err.Error(), "managed profile alias cannot be repointed") {
		return errors.New("managed fixture alias accepted a changed revision")
	}
	if _, err = tx.Exec(`UPDATE profiles SET enabled=? WHERE profile=?`, enabled, p.Binding.Profile); err != nil {
		return err
	}
	return tx.Commit()
}
