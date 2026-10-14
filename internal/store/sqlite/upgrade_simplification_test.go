package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func TestSimplificationUpgradePreservesStateAndArtifactBytes(t *testing.T) {
	for _, fixture := range []struct {
		name, directory, artifacts string
		version                    int
	}{
		{"v14", baselineDir, "testdata/upgrade-v14-artifacts", 14},
		{"v22", "testdata/baseline-b8dc5b4", "testdata/baseline-b8dc5b4/artifacts", 22},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			ctx := context.Background()
			script, err := os.ReadFile(filepath.Join(fixture.directory, "runtime.sql"))
			if err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(filepath.Join(fixture.directory, "requests.json"))
			if err != nil {
				t.Fatal(err)
			}
			var requests baselineFixture
			if err := json.Unmarshal(raw, &requests); err != nil {
				t.Fatal(err)
			}
			root := loadStateScript(t, string(script))
			original, err := connect(filepath.Join(root, databaseName), false)
			if err != nil {
				t.Fatal(err)
			}
			var version int
			if err := original.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != fixture.version {
				t.Fatalf("fixture schema=%d want %d: %v", version, fixture.version, err)
			}
			queries := []string{
				"SELECT workspace_id,job_id,request,request_sha256,active_attempt_id FROM jobs ORDER BY workspace_id,job_id",
				"SELECT workspace_id,job_id,attempt_id,number,nonce FROM attempts ORDER BY workspace_id,job_id,attempt_id",
				"SELECT connection_id,workspace_id,provider_type,label,canonical_account,account_scope FROM managed_connections ORDER BY connection_id",
				"SELECT workspace_id,job_id,attempt_id,artifact_id,object_id,path,role,bytes,sha256,media_type FROM artifacts ORDER BY path",
				"SELECT workspace_id,job_id,attempt_id,operation_id,snapshot_sha256,phase,file_count,verified_at FROM collection_publications ORDER BY workspace_id,job_id,attempt_id",
			}
			before := make([][][]any, len(queries))
			for i, query := range queries {
				before[i] = upgradeRows(t, original, query)
				if len(before[i]) == 0 {
					t.Fatal("empty fixture", query)
				}
			}
			blobs, err := blobfs.New(filepath.Join(root, "results"), blobfs.Limits{MaxObjectBytes: 1 << 20, MaxTotalBytes: 8 << 20})
			if err != nil {
				t.Fatal(err)
			}
			rows, err := original.Query("SELECT workspace_id,object_id,bytes,sha256 FROM artifacts ORDER BY path")
			if err != nil {
				t.Fatal(err)
			}
			var metadata []domain.ObjectMetadata
			for rows.Next() {
				var object domain.ObjectMetadata
				if err := rows.Scan(&object.WorkspaceID, &object.ID, &object.Bytes, &object.SHA256); err != nil {
					t.Fatal(err)
				}
				metadata = append(metadata, object)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			rows.Close()
			if len(metadata) != 5 {
				t.Fatalf("published artifacts=%d want 5", len(metadata))
			}
			for _, object := range metadata {
				data, err := os.ReadFile(filepath.Join(fixture.artifacts, string(object.ID)))
				if err != nil {
					t.Fatal(err)
				}
				if int64(len(data)) != object.Bytes || provider.Digest(data) != object.SHA256 {
					t.Fatal("fixture bytes differ from original publication", object.ID)
				}
				if _, err := blobs.Put(ctx, object, bytes.NewReader(data)); err != nil {
					t.Fatal(err)
				}
			}
			// Close the restored historical blob store before migrating the installation.
			if err := blobs.Close(); err != nil {
				t.Fatal(err)
			}
			if err := original.Close(); err != nil {
				t.Fatal(err)
			}
			s, err := Open(ctx, root, DefaultOptions())
			if err != nil {
				t.Fatal("upgrade", err)
			}
			defer s.Close()
			if err := s.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != len(migrations) {
				t.Fatalf("upgraded schema=%d want %d: %v", version, len(migrations), err)
			}
			var applied int
			if err := s.db.QueryRow("SELECT count(*) FROM schema_migrations").Scan(&applied); err != nil || applied != version {
				t.Fatalf("migration history=%d want %d: %v", applied, version, err)
			}
			for i, query := range queries {
				if after := upgradeRows(t, s.db, query); !reflect.DeepEqual(before[i], after) {
					t.Fatalf("upgrade changed original rows for %s\nbefore %v\nafter %v", query, before[i], after)
				}
			}
			handler := baselineHandler(t, s)
			for _, want := range requests.Requests {
				// Schema v14 predates the additive scheduling status field.
				reason := map[string]string{"granted status": "account_capacity", "queued status": "workspace_fifo"}[want.Name]
				if fixture.version == 14 && reason != "" {
					var response map[string]any
					if err := json.Unmarshal(want.Response, &response); err != nil {
						t.Fatal(err)
					}
					response["scheduling"] = map[string]string{"reason": reason}
					want.Response, err = json.Marshal(response)
					if err != nil {
						t.Fatal(err)
					}
				}
				status, got := baselineCall(t, handler, requests.Token, want)
				if status != want.Status || !sameJSON(t, got, want.Response) {
					t.Errorf("%s changed: status %d want %d\ngot %s\nwant %s", want.Name, status, want.Status, got, want.Response)
				}
			}
			access, err := auth.New(s, s, nil)
			if err != nil {
				t.Fatal(err)
			}
			principal, err := access.Authenticate(ctx, requests.Token)
			if err != nil {
				t.Fatal(err)
			}
			var clockMillis int64
			if err := s.db.QueryRow("SELECT clock_ms FROM scheduler_control WHERE singleton=1").Scan(&clockMillis); err != nil {
				t.Fatal(err)
			}
			connections, err := s.ListConnections(ctx, "a", principal.TokenID(), time.UnixMilli(clockMillis))
			if err != nil || len(connections) != len(before[2]) {
				t.Fatalf("connections unreadable: %v %v", connections, err)
			}
			blobs, err = blobfs.New(filepath.Join(root, "results"), blobfs.Limits{MaxObjectBytes: 1 << 20, MaxTotalBytes: 8 << 20})
			if err != nil {
				t.Fatal(err)
			}
			defer blobs.Close()
			reader, err := collection.NewReader(access, s, blobs)
			if err != nil {
				t.Fatal(err)
			}
			publications := upgradeRows(t, s.db, "SELECT workspace_id,job_id,attempt_id FROM collection_publications")
			if len(publications) != 1 {
				t.Fatalf("publications=%d want 1", len(publications))
			}
			p := publications[0]
			w, job, attempt := domain.WorkspaceID(p[0].(string)), domain.JobID(p[1].(string)), domain.AttemptID(p[2].(string))
			result, err := reader.Read(ctx, principal, w, job, attempt)
			if err != nil || result.Phase != "completed" || len(result.Files) != len(metadata) {
				t.Fatalf("collection unreadable: %+v %v", result, err)
			}
			for _, file := range result.Files {
				meta, stream, err := reader.Open(ctx, principal, w, job, attempt, file.ID)
				if err != nil {
					t.Fatal(err)
				}
				data, err := io.ReadAll(stream)
				closeErr := stream.Close()
				if err != nil || closeErr != nil || int64(len(data)) != meta.Object.Bytes || provider.Digest(data) != meta.Object.SHA256 {
					t.Fatalf("artifact %s failed verification: %v %v", file.Path, err, closeErr)
				}
			}
		})
	}
}

func upgradeRows(t *testing.T, db *sql.DB, query string) [][]any {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var result [][]any
	for rows.Next() {
		values := make([]any, len(columns))
		targets := make([]any, len(columns))
		for i := range values {
			targets[i] = &values[i]
		}
		if err := rows.Scan(targets...); err != nil {
			t.Fatal(err)
		}
		result = append(result, values)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}
