package jsonwire_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

func TestAuthorityAndControlParsersRejectAmbiguousFields(t *testing.T) {
	revision := strings.Repeat("a", 64)
	for _, row := range []struct {
		name, raw string
		parse     func([]byte) error
	}{
		{"execution", `{"attempt_id":"attempt_1","max_remote_wall_seconds":10,"authorize_private_staging":true,"authorize_compute":true}`, func(raw []byte) error { _, e := executionauth.Parse(raw); return e }},
		{"prepare", `{"attempt_id":"attempt_1","max_preparation_seconds":60,"ready_until":"2099-10-12T01:00:00Z","max_staged_bytes":1024,"authorize_private_staging":true}`, func(raw []byte) error { _, e := executionauth.ParsePrepare(raw); return e }},
		{"start", `{"attempt_id":"attempt_1","readiness_revision":"` + revision + `","max_remote_wall_seconds":60,"execution_deadline":"2099-10-12T01:00:00Z","authorize_compute":true}`, func(raw []byte) error { _, e := executionauth.ParseStart(raw); return e }},
		{"secret", `{"attempt_id":"attempt_1","readiness_revision":"` + revision + `","expires_at":"2099-10-12T01:00:00Z","values":{"ingest":"YQ=="}}`, func(raw []byte) error { r, e := workloadsecret.ParseSupply(raw); r.Clear(); return e }},
		{"control", `{"attempt_id":"attempt_1","reason":"explicit"}`, func(raw []byte) error { _, e := operations.Parse(domain.OperationCancel, raw); return e }},
		{"cleanup", `{"attempt_id":"attempt_1","discard_uncollected":false}`, func(raw []byte) error { _, e := operations.Parse(domain.OperationCleanup, raw); return e }},
	} {
		t.Run(row.name, func(t *testing.T) {
			if e := row.parse([]byte(row.raw)); e != nil {
				t.Fatal("valid request", e)
			}
			for _, bad := range []string{
				strings.Replace(row.raw, `"attempt_id":`, `"\u0061ttempt_id":"attempt_1","attempt_id":`, 1),
				strings.Replace(row.raw, `"attempt_id":`, `"Attempt_ID":`, 1),
				strings.Replace(row.raw, `"attempt_1"`, `null`, 1),
				strings.Replace(row.raw, `"attempt_1"`, `"\ud800"`, 1),
				strings.Replace(row.raw, `"attempt_id":`, `"unknown":{"a":1,"\u0061":2},"attempt_id":`, 1),
				row.raw + ` {}`,
			} {
				if e := row.parse([]byte(bad)); e == nil {
					t.Fatal("ambiguous request accepted", bad)
				}
			}
			// Field order and surrounding whitespace remain valid.
			var fields map[string]json.RawMessage
			if e := json.Unmarshal([]byte(row.raw), &fields); e != nil {
				t.Fatal(e)
			}
			reordered, e := json.Marshal(fields)
			if e != nil || row.parse(append([]byte(" \n"), reordered...)) != nil {
				t.Fatal("valid reordered request rejected", e)
			}
		})
	}
}
