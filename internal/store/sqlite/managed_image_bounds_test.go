package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func seedLegacyImageOperation(t *testing.T, f *managedStateFixture, c connections.Connection, i int, stage string) domain.OperationID {
	t.Helper()
	id := domain.OperationID(fmt.Sprintf("op_legacy_image_%d", i))
	stamp := f.clock.Now().UTC().Format(time.RFC3339Nano)
	tx, err := f.s.db.BeginTx(context.Background(), nil)
	requireNoError(t, err)
	defer tx.Rollback()
	installation, _ := json.Marshal(connections.ImageInstallation{RecipeSHA256: string(provider.Digest([]byte("fixture recipe"))), Architecture: "amd64"})
	_, err = tx.Exec(`INSERT INTO managed_connection_operations
(operation_id,workspace_id,connection_id,action,connection_revision,token_id,key_sha256,stage,status,credential_key,created_at,updated_at,installation)
VALUES(?,?,?,'install_image',1,?,?,?,'accepted','',?,?,?)`, id, c.Workspace, c.ID,
		f.principals[c.Workspace].TokenID(), provider.Digest([]byte(id)), stage, stamp, stamp, installation)
	requireNoError(t, err)
	receipt, _ := json.Marshal(connections.Operation{ID: id, Workspace: c.Workspace, ConnectionID: c.ID, Action: "install_image", ConnectionRevision: 1, Status: "accepted", CreatedAt: f.clock.Now(), UpdatedAt: f.clock.Now()})
	_, err = tx.Exec(`INSERT INTO receipts(workspace_id,namespace,scope,key_sha256,request_sha256,canonical_version,subject_id,receipt)
VALUES(?,'connection','',?,?,'',?,?)`, c.Workspace, provider.Digest([]byte(id)), provider.Digest([]byte("fixture request")), id, string(receipt))
	requireNoError(t, err)
	requireNoError(t, tx.Commit())
	return id
}

func TestRunnerImageLegacyOverCapKeepsRecoveryAndBlocksFreshBuilds(t *testing.T) {
	f, _ := resourceFixture(t)
	ctx := context.Background()
	lan := f.create(t, "a", "legacy-image-lan", "SYNTHETIC_LAN")
	vpn := f.create(t, "b", "legacy-image-vpn", "SYNTHETIC_VPN")
	other := f.create(t, "a", "legacy-image-other", "SYNTHETIC_OTHER_HOST")
	var unknown, waiting domain.OperationID
	for i := 0; i <= provider.RunnerImageCountLimit; i++ {
		connection, stage := lan, "done"
		if i%2 == 1 {
			connection = vpn
		}
		if i == 0 {
			stage = "installing"
		} else if i == 1 {
			stage = "ready"
		}
		id := seedLegacyImageOperation(t, f, connection, i, stage)
		if i == 0 {
			unknown = id
		} else if i == 1 {
			waiting = id
		}
	}
	otherID := seedLegacyImageOperation(t, f, other, 100, "ready")
	if started, err := f.s.BeginConnectionInstall(ctx, unknown); err != nil || started {
		t.Fatal("count policy rearmed or blocked observation of the original intent", started, err)
	}
	assertBlocked := func() {
		t.Helper()
		started, err := f.s.BeginConnectionInstall(ctx, waiting)
		if started || !errors.Is(err, provider.ErrResourceCountLimit) || provider.Reason(err, "") != "runner_image_count_limit" {
			t.Fatal("legacy queued install exceeded the physical-host bound", started, err)
		}
		var stage string
		requireNoError(t, f.s.db.QueryRow(`SELECT stage FROM managed_connection_operations WHERE operation_id=?`, waiting).Scan(&stage))
		if stage != "ready" {
			t.Fatal("count refusal committed a mutation intent")
		}
	}
	assertBlocked()
	if started, err := f.s.BeginConnectionInstall(ctx, otherID); err != nil || !started {
		t.Fatal("one physical machine blocked another", started, err)
	}
	f.restartManaged(t)
	assertBlocked()
}
