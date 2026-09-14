package sqlite

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/scheduler"
	"github.com/vankhaivn/compute-relay/internal/statefs"
)

func TestSchedulerMigrationBackfillsStableAcceptanceOrder(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy")
	root, err := statefs.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = initializeRoot(root.Path); err != nil {
		root.Close()
		t.Fatal(err)
	}
	if err = statefs.WriteNew(filepath.Join(root.Path, databaseName), nil); err != nil {
		root.Close()
		t.Fatal(err)
	}
	db, err := connect(filepath.Join(root.Path, databaseName), false)
	if err != nil {
		root.Close()
		t.Fatal(err)
	}
	legacy := &Store{db: db, root: root, options: DefaultOptions()}
	defer legacy.Close()
	if err = migrate(ctx, db, migrations[:3]); err != nil {
		t.Fatal(err)
	}
	schedulerSeed(t, legacy, "z-first", "a", "p", "account")
	schedulerSeed(t, legacy, "a-second", "a", "p", "account")
	var installation string
	if err = db.QueryRow("SELECT installation_id FROM runtime_installation WHERE singleton=1").Scan(&installation); err != nil {
		t.Fatal(err)
	}
	if err = legacy.Close(); err != nil {
		t.Fatal(err)
	}
	upgraded, err := Open(ctx, path, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer upgraded.Close()
	rows, err := upgraded.db.Query("SELECT job_id FROM scheduler_queue ORDER BY queue_seq")
	if err != nil {
		t.Fatal(err)
	}
	var order []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		order = append(order, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(order, []string{"job_z-first", "job_a-second"}) {
		t.Fatal("migration reordered equal-time admissions", order)
	}
	info, err := upgraded.Info(ctx)
	if err != nil || info.InstallationID != installation || info.SchemaVersion != len(migrations) {
		t.Fatal("migration changed identity", info, err)
	}
	r, err := upgraded.ClaimNext(ctx, "migration", schedulerNow)
	if err != nil || r.Claim != nil {
		t.Fatal("upgrade started work without opt-in", r, err)
	}
	if err = upgraded.ConfigureScheduler(ctx, scheduler.DefaultSettings()); err != nil {
		t.Fatal(err)
	}
	c := schedulerClaim(t, upgraded, "migration", schedulerNow)
	if c.JobID != "job_z-first" {
		t.Fatal("backfilled FIFO", c)
	}
}
