package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

type unsafeConnectionCause struct{ reason string }

func (e unsafeConnectionCause) Error() string         { return "private provider diagnostic" }
func (e unsafeConnectionCause) FailureReason() string { return e.reason }

func TestConnectionFailureCauseSurvivesReopenWithoutChangingSelectionPolicy(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		failure                        error
		cause, problem, authentication string
		retained                       bool
	}{
		{"transport", provider.WithReason(errors.New("private provider diagnostic"), "ssh_channel_open_failed"), "ssh_channel_open_failed", "provider_unavailable", "verified", true},
		{"credential", provider.WithReason(connections.ErrCredentialRejected, "account_credential_rejected"), "account_credential_rejected", "credential_rejected", "rejected", false},
		{"untyped", errors.New("private provider diagnostic"), "", "provider_unavailable", "verified", true},
		{"unsafe_typed", unsafeConnectionCause{"private provider diagnostic"}, "", "provider_unavailable", "verified", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newManagedStateFixture(t)
			c := f.create(t, "a", "cause-create", managedStateSecret)
			op := f.action(t, "a", c, "cause-check", "check", "")
			f.discovery.fail = tc.failure
			f.run(t)
			f.restartManaged(t)
			saved, err := f.service.Operation(t.Context(), f.principals["a"], "a", op.ID)
			requireNoError(t, err)
			if saved.Status != "failed" || saved.Problem == nil || *saved.Problem != tc.problem || (saved.Cause == nil) != (tc.cause == "") || saved.Cause != nil && *saved.Cause != tc.cause {
				t.Fatal("lost failure classification or cause", saved)
			}
			raw, err := json.Marshal(saved)
			requireNoError(t, err)
			if strings.Contains(string(raw), "private provider diagnostic") || tc.cause == "" && strings.Contains(string(raw), `"cause"`) {
				t.Fatal("untyped diagnostic escaped", string(raw))
			}
			after := f.get(t, "a", c.ID)
			if after.Authentication != tc.authentication || (after.Selection != nil) != tc.retained || tc.retained && !reflect.DeepEqual(after.Selection, c.Selection) {
				t.Fatal("cause changed authentication or frozen selection", after)
			}
		})
	}
}

func TestConnectionFailureCauseMigrationPreservesExistingOperation(t *testing.T) {
	f := newManagedStateFixture(t)
	c := f.create(t, "a", "legacy-cause-create", managedStateSecret)
	op := f.action(t, "a", c, "legacy-cause-check", "check", "")
	f.discovery.fail = errors.New("untyped old failure")
	f.run(t)
	oldPath := copyFixtureAtSchema(t, f.s, 39)
	upgraded, err := Open(t.Context(), oldPath, DefaultOptions())
	requireNoError(t, err)
	defer upgraded.Close()
	saved, err := upgraded.ReadConnectionOperation(t.Context(), "a", f.principals["a"].TokenID(), op.ID)
	requireNoError(t, err)
	if saved.Cause != nil || saved.Problem == nil || *saved.Problem != "provider_unavailable" || saved.Status != "failed" {
		t.Fatal("migration changed old operation", saved)
	}
	after, err := upgraded.ReadConnection(t.Context(), "a", f.principals["a"].TokenID(), c.ID)
	requireNoError(t, err)
	if !reflect.DeepEqual(after.Selection, c.Selection) {
		t.Fatal("migration changed selection")
	}
}

func TestConnectionFailureCauseValidationAndRetryClearing(t *testing.T) {
	f := newManagedStateFixture(t)
	c := f.create(t, "a", "cause-validation-create", managedStateSecret)
	op := f.action(t, "a", c, "cause-validation-remove", "remove", "")
	for _, cause := range []string{"raw provider diagnostic", "Uppercase", "_leading", strings.Repeat("a", 65)} {
		if err := f.s.FailConnection(t.Context(), op.ID, "credential_store_unavailable", cause); !errors.Is(err, ErrInvalid) {
			t.Fatal("accepted unsafe cause", err)
		}
		if _, err := f.s.db.Exec(`UPDATE managed_connection_operations SET status='failed',cause=? WHERE operation_id=?`, cause, op.ID); err == nil {
			t.Fatal("database accepted unsafe cause")
		}
	}
	requireNoError(t, f.s.FailConnection(t.Context(), op.ID, "credential_store_unavailable", "vault_delete_failed"))
	f.restartManaged(t)
	saved, err := f.service.Operation(t.Context(), f.principals["a"], "a", op.ID)
	requireNoError(t, err)
	if saved.Cause == nil || *saved.Cause != "vault_delete_failed" {
		t.Fatal("remove failure lost cause")
	}
	f.clock.advance(31 * time.Second)
	pending, found, err := f.s.NextConnectionOperation(context.Background())
	requireNoError(t, err)
	if !found || pending.Operation.ID != op.ID || pending.Operation.Cause != nil || pending.Operation.Problem != nil || pending.Operation.Status != "running" {
		t.Fatal("retry retained stale failure", pending)
	}
	requireNoError(t, f.s.FinishConnection(t.Context(), pending, connections.Completion{}))
	saved, err = f.service.Operation(t.Context(), f.principals["a"], "a", op.ID)
	requireNoError(t, err)
	if saved.Status != "succeeded" || saved.Cause != nil {
		t.Fatal("successful retry retained stale cause", saved)
	}
}
