package sqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func TestHostHealthUpgradeFromV28PreservesStateAndArtifactBytes(t *testing.T) {
	ctx := context.Background()
	fixture := "testdata/baseline-b8dc5b4"
	script, err := os.ReadFile(filepath.Join(fixture, "runtime.sql"))
	requireNoError(t, err)
	sourceRoot := loadStateScript(t, string(script))
	source, err := Open(ctx, sourceRoot, DefaultOptions())
	requireNoError(t, err)
	defer source.Close()
	root := copyFixtureAtSchema(t, source, 28)
	original, err := connect(filepath.Join(root, databaseName))
	requireNoError(t, err)
	var version int
	requireNoError(t, original.QueryRow("PRAGMA user_version").Scan(&version))
	if version != 28 {
		t.Fatal("upgrade fixture is not v28", version)
	}
	queries := []string{
		"SELECT * FROM runtime_installation",
		"SELECT * FROM workspaces ORDER BY workspace_id",
		"SELECT * FROM api_token_hashes ORDER BY token_id",
		"SELECT * FROM jobs ORDER BY workspace_id,job_id",
		"SELECT * FROM attempts ORDER BY workspace_id,job_id,attempt_id",
		"SELECT * FROM managed_connections ORDER BY connection_id",
		"SELECT * FROM managed_profile_runtime ORDER BY profile,revision",
		"SELECT * FROM managed_resource_accounts ORDER BY account_scope",
		"SELECT * FROM receipts ORDER BY namespace,scope,workspace_id,key_sha256",
		"SELECT * FROM attempt_grants ORDER BY kind,workspace_id,job_id,attempt_id",
		"SELECT * FROM operations ORDER BY operation_seq",
		"SELECT * FROM events ORDER BY workspace_id,job_id,sequence",
		"SELECT * FROM artifacts ORDER BY path",
		"SELECT * FROM collection_publications ORDER BY workspace_id,job_id,attempt_id",
	}
	before := make([][][]any, len(queries))
	for i, q := range queries {
		before[i] = upgradeRows(t, original, q)
	}
	if len(before[3]) == 0 || len(before[4]) == 0 || len(before[5]) == 0 || len(before[12]) != 5 {
		t.Fatal("upgrade fixture is missing retained state")
	}
	rows, err := original.Query("SELECT workspace_id,object_id,bytes,sha256 FROM artifacts ORDER BY path")
	requireNoError(t, err)
	metadata := []domain.ObjectMetadata{}
	for rows.Next() {
		var object domain.ObjectMetadata
		requireNoError(t, rows.Scan(&object.WorkspaceID, &object.ID, &object.Bytes, &object.SHA256))
		metadata = append(metadata, object)
	}
	requireNoError(t, rows.Err())
	rows.Close()
	blobs, err := blobfs.New(filepath.Join(root, "results"), blobfs.Limits{MaxObjectBytes: 1 << 20, MaxTotalBytes: 8 << 20})
	requireNoError(t, err)
	for _, object := range metadata {
		raw, err := os.ReadFile(filepath.Join(fixture, "artifacts", string(object.ID)))
		requireNoError(t, err)
		if _, err := blobs.Put(ctx, object, bytes.NewReader(raw)); err != nil {
			t.Fatal(err)
		}
	}
	requireNoError(t, blobs.Close())
	requireNoError(t, original.Close())
	upgraded, err := Open(ctx, root, DefaultOptions())
	requireNoError(t, err)
	defer upgraded.Close()
	requireNoError(t, upgraded.db.QueryRow("PRAGMA user_version").Scan(&version))
	if version != len(migrations) || version < 29 {
		t.Fatal("health migration was not applied", version)
	}
	for i, q := range queries {
		if after := upgradeRows(t, upgraded.db, q); !reflect.DeepEqual(before[i], after) {
			t.Fatal("v28 upgrade changed original rows", q)
		}
	}
	var samples int
	requireNoError(t, upgraded.db.QueryRow("SELECT count(*) FROM host_health_history").Scan(&samples))
	if samples != 0 {
		t.Fatal("upgrade manufactured host health observations")
	}
	raw, err := os.ReadFile(filepath.Join(fixture, "requests.json"))
	requireNoError(t, err)
	var requests baselineFixture
	requireNoError(t, json.Unmarshal(raw, &requests))
	access, err := auth.New(upgraded, upgraded)
	requireNoError(t, err)
	if _, err := access.Authenticate(ctx, requests.Token); err != nil {
		t.Fatal("v28 token no longer authenticates", err)
	}
	blobs, err = blobfs.New(filepath.Join(root, "results"), blobfs.Limits{MaxObjectBytes: 1 << 20, MaxTotalBytes: 8 << 20})
	requireNoError(t, err)
	defer blobs.Close()
	for _, object := range metadata {
		stream, err := blobs.Open(ctx, object.WorkspaceID, object.ID)
		requireNoError(t, err)
		raw, err := io.ReadAll(stream)
		closeErr := stream.Close()
		if err != nil || closeErr != nil || int64(len(raw)) != object.Bytes || provider.Digest(raw) != object.SHA256 {
			t.Fatal("v28 published artifact changed", err, closeErr)
		}
	}
	requireNoError(t, upgraded.Ready(ctx))
}
