package executionauth

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestGateRequestsRejectImplicitAndMutableAuthority(t *testing.T) {
	prepare := `{"attempt_id":"attempt_1","max_preparation_seconds":60,"ready_until":"2099-10-12T01:00:00Z","max_staged_bytes":1024,"authorize_private_staging":true}`
	start := `{"attempt_id":"attempt_1","readiness_revision":"` + strings.Repeat("a", 64) + `","max_remote_wall_seconds":60,"execution_deadline":"2099-10-12T01:00:00Z","authorize_compute":true}`
	for _, row := range []struct {
		name, raw string
		parse     func([]byte) error
	}{
		{"prepare", prepare, func(raw []byte) error { _, err := ParsePrepare(raw); return err }},
		{"start", start, func(raw []byte) error { _, err := ParseStart(raw); return err }},
	} {
		t.Run(row.name, func(t *testing.T) {
			if err := row.parse([]byte(row.raw)); err != nil {
				t.Fatal(err)
			}
			for _, raw := range []string{
				strings.Replace(row.raw, `"attempt_id":"attempt_1"`, `"attempt_id":"attempt_1","attempt_id":"attempt_1"`, 1),
				strings.Replace(row.raw, `"attempt_id":"attempt_1",`, "", 1),
				strings.Replace(row.raw, `"attempt_id":"attempt_1"`, `"attempt_id":null`, 1),
				strings.Replace(row.raw, `"attempt_id":"attempt_1"`, `"attempt_id":"attempt_1","unknown":true`, 1),
				strings.Replace(row.raw, `true}`, `false}`, 1),
				strings.Replace(row.raw, `01:00:00Z`, `01:00:00+00:00`, 1),
				row.raw + "{}",
			} {
				if row.parse([]byte(raw)) == nil {
					t.Fatalf("accepted invalid authority %s", raw)
				}
			}
		})
	}
	p, _ := ParsePrepare([]byte(prepare))
	original := GateDigest("prepare", "job_1", p)
	p.MaxPreparationSeconds++
	if original == GateDigest("prepare", "job_1", p) || original == GateDigest("prepare", "job_2", p) {
		t.Fatal("authority digest not bound")
	}
}

func TestGateExpiryAndReceiptSemantics(t *testing.T) {
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	g := Gate{Request: PrepareRequest{ReadyUntil: now.Add(time.Minute).Format(time.RFC3339)}}
	if g.Expired(now) || !g.Expired(now.Add(time.Minute)) {
		t.Fatal("hold boundary is not exclusive")
	}
	r := GateReceipt{AuthorizationID: "grant_1", WorkspaceID: "workspace_1", JobID: "job_1", AttemptID: "attempt_1", Kind: "prepare", Status: Granted, CreatedAt: now}
	if r.Validate() != nil {
		t.Fatal("invalid original receipt")
	}
	raw, _ := json.Marshal(r)
	if strings.Contains(string(raw), "authorize_compute") {
		t.Fatal("preparation receipt implies compute")
	}
	r.Status = Consumed
	if r.Validate() == nil {
		t.Fatal("missing consumption time accepted")
	}
	r.ConsumedAt = &now
	if r.Validate() != nil {
		t.Fatal("valid consumption rejected")
	}
}
