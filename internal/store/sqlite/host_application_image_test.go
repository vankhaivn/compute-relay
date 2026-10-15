package sqlite

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hostimages"
)

func TestApplicationImageLoadPermitSurvivesRestartAndIsNeverRepeated(t *testing.T) {
	f := newAdmission(t)
	ctx := context.Background()
	receipt := submit(t, f, "image-ledger")
	owner := hostimages.Owner{WorkspaceID: "a", JobID: receipt.JobID, AttemptID: receipt.AttemptID, ObjectID: "code"}
	key := hostimages.Key{InstallationID: "rt_image_fixture", MachineID: domain.SHA256Digest(strings.Repeat("a", 64)),
		EngineID: domain.SHA256Digest(strings.Repeat("b", 64)), ArchiveSHA256: domain.SHA256Digest(strings.Repeat("c", 64)), ArchiveBytes: 7}
	_, err := f.s.db.ExecContext(ctx, `INSERT INTO host_application_image_pins
	 (workspace_id,job_id,attempt_id,installation_id,machine_id,engine_id,archive_sha256,archive_bytes,object_id,created_at)
	 VALUES(?,?,?,?,?,?,?,?,?,?)`, owner.WorkspaceID, owner.JobID, owner.AttemptID, key.InstallationID, key.MachineID,
		key.EngineID, key.ArchiveSHA256, key.ArchiveBytes, owner.ObjectID, time.Now().UTC().Format(time.RFC3339Nano))
	requireNoError(t, err)
	pin, err := f.s.PinnedImage(ctx, owner.WorkspaceID, owner.JobID, owner.AttemptID)
	if err != nil || pin == nil || pin.Key != key || pin.ObjectID != owner.ObjectID || pin.Released {
		t.Fatalf("frozen image pin was not readable: pin=%+v err=%v", pin, err)
	}

	g, elected, err := f.s.ClaimImage(ctx, owner, key)
	if err != nil || !elected || g.State != hostimages.StatePreparing {
		t.Fatalf("first claim: generation=%+v elected=%v err=%v", g, elected, err)
	}
	imageID := "sha256:" + strings.Repeat("d", 64)
	diffIDs := []string{"sha256:" + strings.Repeat("e", 64)}
	if begun, err := f.s.BeginImageLoad(ctx, g, imageID, diffIDs); err != nil || !begun {
		t.Fatalf("first durable load permit: begun=%v err=%v", begun, err)
	}
	if begun, err := f.s.BeginImageLoad(ctx, g, imageID, diffIDs); err != nil || begun {
		t.Fatalf("duplicate load permit: begun=%v err=%v", begun, err)
	}
	requireNoError(t, f.s.Close())
	reopened, err := Open(ctx, f.root, DefaultOptions())
	requireNoError(t, err)
	defer reopened.Close()
	active, err := reopened.ActiveImage(ctx, key)
	if err != nil || active == nil || active.State != hostimages.StateLoading || active.ExpectedImageID != imageID ||
		len(active.RootFSDiffIDs) != 1 || active.RootFSDiffIDs[0] != diffIDs[0] || active.LoadingAt.IsZero() {
		t.Fatalf("lost load intent after restart: generation=%+v err=%v", active, err)
	}
	if reused, elected, err := reopened.ClaimImage(ctx, owner, key); err != nil || elected || reused.ID != g.ID {
		t.Fatalf("reclaim sent another load: generation=%+v elected=%v err=%v", reused, elected, err)
	}
	if err := reopened.MarkImageUnknown(ctx, *active); err != nil {
		t.Fatal(err)
	}
	if unknown, err := reopened.ActiveImage(ctx, key); err != nil || unknown == nil || unknown.State != hostimages.StateUnknown {
		t.Fatalf("unknown outcome did not block replacement: generation=%+v err=%v", unknown, err)
	}
	if err := reopened.MarkImageReady(ctx, *active); err != nil {
		t.Fatal(err)
	}
	ready, err := reopened.ActiveImage(ctx, key)
	if err != nil || ready == nil || ready.State != hostimages.StateReady || ready.ExpectedImageID != imageID {
		t.Fatalf("read-only recovery did not resolve generation: generation=%+v err=%v", ready, err)
	}
	if err := reopened.RetireMissingImage(ctx, *ready); err != nil {
		t.Fatal(err)
	}
	next, elected, err := reopened.ClaimImage(ctx, owner, key)
	if err != nil || !elected || next.ID == g.ID {
		t.Fatalf("observed missing image did not open a new generation: generation=%+v elected=%v err=%v", next, elected, err)
	}
	var loads int
	requireNoError(t, reopened.db.QueryRowContext(ctx, `SELECT count(*) FROM host_application_image_loads WHERE generation_id=?`, g.ID).Scan(&loads))
	if loads != 1 {
		t.Fatalf("generation issued %d load permits", loads)
	}
}
