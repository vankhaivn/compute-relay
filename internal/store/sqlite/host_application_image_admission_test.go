package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
)

func imageAdmissionJob(profile, object, digest string, bytes int64) []byte {
	raw := string(cacheJob(profile, "", ""))
	image := fmt.Sprintf(`"application_image":{"object_id":%q,"sha256":%q,"bytes":%d},`, object, digest, bytes)
	return []byte(strings.Replace(raw, `"execution":`, image+`"execution":`, 1))
}

func TestApplicationImageAdmissionFreezesExactOwnedObjectAndHostKey(t *testing.T) {
	f := cacheAdmissionFixture(t)
	ctx := context.Background()
	digest := strings.Repeat("e", 64)
	requireNoError(t, f.s.CommitObject(ctx, domain.ObjectMetadata{WorkspaceID: "a", ID: "image", SHA256: domain.SHA256Digest(digest), Bytes: 77}))
	requireNoError(t, f.s.CommitObject(ctx, domain.ObjectMetadata{WorkspaceID: "b", ID: "foreign-image", SHA256: domain.SHA256Digest(digest), Bytes: 77}))
	for _, tc := range []struct {
		name   string
		object string
		digest string
		bytes  int64
	}{
		{"wrong-digest", "image", strings.Repeat("f", 64), 77},
		{"wrong-size", "image", digest, 78},
		{"foreign", "foreign-image", digest, 77},
	} {
		_, err := f.service.Submit(ctx, f.p, "a", "image-"+tc.name, imageAdmissionJob("cache-host-a", tc.object, tc.digest, tc.bytes))
		if !errors.Is(err, admission.ErrInputs) {
			t.Fatalf("%s reached acceptance: %v", tc.name, err)
		}
	}
	if tableCount(t, f.s, "jobs") != 0 {
		t.Fatal("mismatched or foreign image created a job")
	}
	request := imageAdmissionJob("cache-host-a", "image", digest, 77)
	receipt, err := f.service.Submit(ctx, f.p, "a", "image-valid", request)
	requireNoError(t, err)
	pin, err := f.s.PinnedImage(ctx, "a", receipt.JobID, receipt.AttemptID)
	if err != nil || pin == nil || pin.ObjectID != "image" || pin.Released || pin.Key.InstallationID == "" ||
		pin.Key.MachineID != cacheMachine || pin.Key.EngineID != cacheEngine || pin.Key.ArchiveSHA256 != domain.SHA256Digest(digest) || pin.Key.ArchiveBytes != 77 {
		t.Fatalf("admission did not freeze application image: pin=%+v err=%v", pin, err)
	}
	var record admission.Record
	err = f.s.write(ctx, func(ctx context.Context, tx *sql.Tx, _ time.Time) error {
		var err error
		record, err = readJobRecord(ctx, tx, "a", receipt.JobID)
		return err
	})
	requireNoError(t, err)
	found := false
	for _, ref := range record.Objects {
		if ref.Role == "application_image" {
			found = ref.Object.ID == "image" && ref.Object.SHA256 == domain.SHA256Digest(digest) && ref.Object.Bytes == 77
		}
	}
	if !found {
		t.Fatal("frozen job record lost application image role")
	}
	replay, err := f.service.Submit(ctx, f.p, "a", "image-valid", request)
	requireNoError(t, err)
	if !replay.Replay || replay.JobID != receipt.JobID || tableCount(t, f.s, "jobs") != 1 {
		t.Fatal("image admission replay changed frozen job")
	}
}

func TestApplicationImageAdmissionRequiresCurrentVerifiedSSHHost(t *testing.T) {
	f := newAdmission(t)
	ctx := context.Background()
	digest := strings.Repeat("e", 64)
	requireNoError(t, f.s.CommitObject(ctx, domain.ObjectMetadata{WorkspaceID: "a", ID: "image", SHA256: domain.SHA256Digest(digest), Bytes: 77}))
	_, err := f.service.Submit(ctx, f.p, "a", "image-non-ssh", imageAdmissionJob("default-gpu", "image", digest, 77))
	if !errors.Is(err, admission.ErrRequirements) || tableCount(t, f.s, "jobs") != 0 {
		t.Fatalf("non-SSH provider accepted application image: %v", err)
	}
}
