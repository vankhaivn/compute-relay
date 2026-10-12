package admission

import (
	"strings"
	"testing"
	"time"
)

func TestAbsoluteDeadlineIsCanonicalAndImmutable(t *testing.T) {
	with := func(value string) []byte {
		return []byte(strings.Replace(validJob, `"timeouts":{`, `"timeouts":{"execution_deadline":`+value+`,`, 1))
	}
	r, err := Parse(with(`"2026-10-12T15:00:00Z"`))
	if err != nil {
		t.Fatal(err)
	}
	s := r.Spec()
	if s.Deadline() != time.Date(2026, 10, 12, 15, 0, 0, 0, time.UTC) || s.Expired(s.Deadline().Add(-time.Nanosecond)) || !s.Expired(s.Deadline()) {
		t.Fatal("deadline boundary changed")
	}
	// Parsing an old snapshot remains valid after expiry; only new effects check time.
	if _, err = Parse(r.Canonical()); err != nil {
		t.Fatal(err)
	}
	old, _ := Parse([]byte(validJob))
	if old.Spec().Expired(time.Now().Add(100*365*24*time.Hour)) || strings.Contains(string(old.Canonical()), "execution_deadline") {
		t.Fatal("legacy relative-only semantics changed")
	}
	for _, value := range []string{`null`, `123`, `""`, `"2026-10-12T15:00:00+00:00"`, `"2026-10-12T15:00:00.001Z"`, `"2026-02-30T15:00:00Z"`, `"0001-02-01T00:00:00Z"`, `"2026-10-12t15:00:00z"`} {
		if _, err := Parse(with(value)); err == nil {
			t.Fatalf("accepted noncanonical deadline %s", value)
		}
	}
}
