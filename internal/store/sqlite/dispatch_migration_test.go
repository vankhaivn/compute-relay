package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/statefs"
)

func TestDispatchMigrationPreservesExistingQueueAndDoesNotInventIntents(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "schema-four")
	root, err := statefs.Open(path)
	requireNoError(t, err)
	if err = initializeRoot(root.Path); err != nil {
		root.Close()
		t.Fatal(err)
	}
	if err = statefs.WriteNew(filepath.Join(root.Path, databaseName), nil); err != nil {
		root.Close()
		t.Fatal(err)
	}
	db, err := connect(filepath.Join(root.Path, databaseName))
	if err != nil {
		root.Close()
		t.Fatal(err)
	}
	legacy := &Store{db: db, root: root, options: DefaultOptions()}
	defer legacy.Close()
	err = migrate(ctx, db, migrations[:4])
	requireNoError(t, err)
	schedulerSeed(t, legacy, "one", "a", "p", "account")
	var installation string
	err = db.QueryRow("SELECT installation_id FROM runtime_installation WHERE singleton=1").Scan(&installation)
	requireNoError(t, err)
	var sequence int64
	err = db.QueryRow("SELECT queue_seq FROM scheduler_queue WHERE job_id='job_one'").Scan(&sequence)
	requireNoError(t, err)
	if _, err = db.Exec("UPDATE scheduler_queue SET dispatch_barrier=1 WHERE queue_seq=?", sequence); err != nil {
		t.Fatal(err)
	}
	err = legacy.Close()
	requireNoError(t, err)
	upgraded, err := Open(ctx, path, DefaultOptions())
	requireNoError(t, err)
	defer upgraded.Close()
	info, err := upgraded.Info(ctx)
	if err != nil || info.InstallationID != installation || info.SchemaVersion != len(migrations) {
		t.Fatal("migration changed installation", info, err)
	}
	var got int64
	var barrier bool
	if err = upgraded.db.QueryRow("SELECT queue_seq,dispatch_barrier FROM scheduler_queue WHERE job_id='job_one'").Scan(&got, &barrier); err != nil || got != sequence || !barrier {
		t.Fatal("migration erased prior safety barrier", got, barrier, err)
	}
	for _, query := range []string{"SELECT count(*) FROM dispatch_journals", "SELECT count(*) FROM provider_resources", "SELECT count(*) FROM submission_intents"} {
		var n int
		if err = upgraded.db.QueryRow(query).Scan(&n); err != nil || n != 0 {
			t.Fatal("migration fabricated external evidence", n, err)
		}
	}
	c, err := upgraded.ClaimRecovery(ctx, "migration")
	if err != nil || c != nil {
		t.Fatal("unknown historical barrier was silently rearmed", c, err)
	}
}
