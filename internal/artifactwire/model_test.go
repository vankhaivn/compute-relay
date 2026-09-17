package artifactwire

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func fixture() (Target, []File, time.Time) {
	return Target{"app", "job_one", "att_one"}, []File{{"art_a", "control/execution-result.json", "manifest", 2, strings.Repeat("a", 64)}, {"art_b", "outputs/answer.txt", "output", 3, strings.Repeat("b", 64)}}, time.Date(2026, 9, 17, 0, 0, 0, 0, time.UTC)
}
func TestPagesBindCompleteSnapshotTargetAndCanonicalOffset(t *testing.T) {
	target, files, at := fixture()
	digest := Snapshot(target, "completed", at, files)
	p := Page{target, "completed", at, digest, 2, files[:1], Cursor(digest, 1)}
	raw, _ := json.Marshal(p)
	decoded, err := DecodePage(raw, target, "", 1)
	if err != nil || decoded.NextCursor != p.NextCursor || decoded.Artifacts[0] != files[0] {
		t.Fatal(decoded, err)
	}
	p.Artifacts, p.NextCursor = files[1:], ""
	raw, _ = json.Marshal(p)
	if _, err := DecodePage(raw, target, Cursor(digest, 1), 1); err != nil {
		t.Fatal(err)
	}
	for _, cursor := range []string{Cursor(digest, 0), Cursor(digest, 2), "ar1_" + digest + "_01", Cursor(strings.Repeat("f", 64), 1)} {
		if _, err := DecodePage(raw, target, cursor, 1); err == nil {
			t.Fatal("accepted stale/noncanonical cursor")
		}
	}
	other := target
	other.AttemptID = "att_two"
	if _, err := DecodePage(raw, other, Cursor(digest, 1), 1); err == nil || Snapshot(other, "completed", at, files) == digest {
		t.Fatal("retargeted a page")
	}
	files[1].Bytes++
	if Snapshot(target, "completed", at, files) == digest {
		t.Fatal("changed files retained snapshot identity")
	}
}
func TestMetadataRejectsMissingNullDuplicateAndWrongIdentity(t *testing.T) {
	target, files, at := fixture()
	raw, _ := json.Marshal(Metadata{target, "failed", at, files[1]})
	if m, err := DecodeMetadata(raw, target, "art_b"); err != nil || m.Artifact != files[1] || m.ResultPhase != "failed" {
		t.Fatal(m, err)
	}
	for _, mutation := range []struct{ from, to string }{
		{`"bytes":3`, `"bytes":null`}, {`"bytes":3`, `"bytes":3,"bytes":3`},
		{`"bytes":3`, `"Bytes":3`}, {`"bytes":3`, `"bytes":-1`},
		{`"attempt_id":"att_one"`, `"attempt_id":"att_other"`},
		{`"outputs/answer.txt"`, `"outputs/../private"`},
	} {
		changed := bytes.Replace(raw, []byte(mutation.from), []byte(mutation.to), 1)
		if _, err := DecodeMetadata(changed, target, "art_b"); err == nil {
			t.Fatal("accepted invalid metadata", mutation.to)
		}
	}
	future := bytes.Replace(raw, []byte(`"bytes":3`), []byte(`"bytes":3,"Bytes":99,"future":9007199254740993`), 1)
	if m, err := DecodeMetadata(future, target, "art_b"); err != nil || m.Artifact.Bytes != 3 {
		t.Fatal("unknown field changed an exact known key", err)
	}
}
func TestPageRequiresExplicitCompletePagination(t *testing.T) {
	target, files, at := fixture()
	p := Page{target, "completed", at, Snapshot(target, "completed", at, files), 2, files, ""}
	raw, _ := json.Marshal(p)
	for _, replacement := range []string{`"next_cursor":null`, `"ignored_cursor":""`} {
		changed := bytes.Replace(raw, []byte(`"next_cursor":""`), []byte(replacement), 1)
		if _, err := DecodePage(changed, target, "", 2); err == nil {
			t.Fatal("missing end-of-page acknowledgement")
		}
	}
	p.Artifacts = p.Artifacts[:1]
	raw, _ = json.Marshal(p)
	if _, err := DecodePage(raw, target, "", 2); err == nil {
		t.Fatal("silently incomplete page")
	}
}
