package api

import (
	"strings"
	"testing"
	"time"
)

func TestArtifactPaginationBindsSnapshotTargetAndCanonicalOffset(t *testing.T) {
	target := artifactTarget{"app", "job_one", "att_one"}
	files := []artifactFile{{"art_a", "control/execution-result.json", "manifest", 2, strings.Repeat("a", 64)}, {"art_b", "outputs/answer.txt", "output", 3, strings.Repeat("b", 64)}}
	at := time.Date(2026, 9, 17, 0, 0, 0, 0, time.UTC)
	digest := artifactSnapshot(target, "completed", at, files)
	if offset, err := artifactOffset(artifactCursor(digest, 1), digest, len(files)); err != nil || offset != 1 {
		t.Fatal("valid cursor rejected", err)
	}
	for _, cursor := range []string{artifactCursor(digest, 0), artifactCursor(digest, 2), "ar1_" + digest + "_01", artifactCursor(strings.Repeat("f", 64), 1)} {
		if _, err := artifactOffset(cursor, digest, len(files)); err == nil {
			t.Fatal("accepted stale/noncanonical cursor")
		}
	}
	other := target
	other.AttemptID = "att_two"
	if artifactSnapshot(other, "completed", at, files) == digest {
		t.Fatal("retargeted snapshot")
	}
	files[1].Bytes++
	if artifactSnapshot(target, "completed", at, files) == digest {
		t.Fatal("changed files retained snapshot identity")
	}
}
