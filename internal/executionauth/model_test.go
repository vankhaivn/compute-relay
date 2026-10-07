package executionauth

import (
	"strings"
	"testing"
)

const requestJSON = `{"attempt_id":"attempt_1","max_remote_wall_seconds":10,"authorize_private_staging":true,"authorize_compute":true}`

func TestParseStrictAndDigest(t *testing.T) {
	r, err := Parse([]byte(requestJSON))
	if err != nil {
		t.Fatal(err)
	}
	original, _ := r.Digest("job_1")
	reordered, err := Parse([]byte(`{"authorize_compute":true,"authorize_private_staging":true,"max_remote_wall_seconds":10,"attempt_id":"attempt_1"}`))
	if err != nil {
		t.Fatal(err)
	}
	same, _ := reordered.Digest("job_1")
	other, _ := r.Digest("job_2")
	if original != same || original == other {
		t.Fatal("route or canonical request digest mismatch")
	}
	for _, raw := range []string{
		`null`, `{}`, requestJSON + `{}`, strings.Replace(requestJSON, `"attempt_id"`, `"Attempt_ID"`, 1),
		strings.Replace(requestJSON, `"attempt_id":"attempt_1"`, `"attempt_id":"attempt_1","attempt_id":"attempt_1"`, 1),
		strings.Replace(requestJSON, `:10`, `:0`, 1), strings.Replace(requestJSON, `:10`, `:86401`, 1), strings.Replace(requestJSON, `:10`, `:1.5`, 1),
		strings.Replace(requestJSON, `:10`, `:null`, 1), strings.Replace(requestJSON, `:true`, `:false`, 1), strings.Replace(requestJSON, `:true`, `:null`, 1),
		strings.Replace(requestJSON, `"attempt_1"`, `null`, 1), strings.Replace(requestJSON, `"attempt_1"`, `"\ud800"`, 1),
		strings.Replace(requestJSON, `"attempt_id"`, `"extra":1,"attempt_id"`, 1),
	} {
		if _, err := Parse([]byte(raw)); err == nil {
			t.Fatalf("invalid request accepted: %s", raw)
		}
	}
}
