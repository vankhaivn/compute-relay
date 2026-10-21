package sqlite

import (
	"context"
	"errors"

	"github.com/vankhaivn/compute-relay/internal/connections"
	"reflect"
	"testing"
)

func TestTransientConnectionCheckKeepsVerifiedSelection(t *testing.T) {
	f := newManagedStateFixture(t)
	c := f.create(t, "a", "transient-check-create", managedStateSecret)
	op := f.action(t, "a", c, "transient-check", "check", "")
	pending := f.get(t, "a", c.ID)
	if !reflect.DeepEqual(pending.Selection, c.Selection) {
		t.Fatal("pending check removed selection")
	}
	f.discovery.fail = errors.New("temporary provider outage")
	worked, err := f.service.RunOnce(context.Background())
	if !worked || err != nil {
		t.Fatal("fault was not injected", worked, err)
	}
	after := f.get(t, "a", c.ID)
	if after.Authentication != "verified" || !reflect.DeepEqual(after.Selection, c.Selection) {
		t.Fatal("transient check erased frozen selection", after)
	}
	saved, err := f.service.Operation(context.Background(), f.principals["a"], "a", op.ID)
	if err != nil || saved.Status != "failed" || saved.Problem == nil || *saved.Problem != "provider_unavailable" {
		t.Fatal("check fault was hidden", saved, err)
	}
	// Admission can use the previously verified identity; dispatch verifies it again.
	f.admit(t, "a", after, "transient-check-admit")
	f.discovery.fail = nil
	f.action(t, "a", after, "transient-check-recover", "check", "")
	f.run(t)
	recovered := f.get(t, "a", c.ID)
	if recovered.Selection == nil || recovered.Authentication != "verified" || recovered.Selection.Profile == c.Selection.Profile {
		t.Fatal("successful check did not renew alias")
	}
}

func TestRejectedCredentialCheckDisablesPreviousSelection(t *testing.T) {
	f := newManagedStateFixture(t)
	c := f.create(t, "a", "rejected-check-create", managedStateSecret)
	f.action(t, "a", c, "rejected-check", "check", "")
	f.discovery.fail = connections.ErrCredentialRejected
	f.run(t)
	after := f.get(t, "a", c.ID)
	if after.Authentication != "rejected" || after.Selection != nil {
		t.Fatal("rejected credential retained eligibility", after)
	}
	var enabled bool
	requireNoError(t, f.s.db.QueryRow(`SELECT enabled FROM profiles WHERE profile=?`, c.Selection.Profile).Scan(&enabled))
	if enabled {
		t.Fatal("rejected profile remained enabled")
	}
}
