package api_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

func TestManagedHTTPWorkloadSecretsScopesStrictBodiesAndNoEcho(t *testing.T) {
	f := setupManagedHTTP(t, true)
	base := "/v1/workspaces/a/jobs/job_absent/workload-secrets"
	canary := "synthetic-http-workload-secret/with+url?chars=only"
	encoded := base64.StdEncoding.EncodeToString([]byte(canary))
	raw, _ := json.Marshal(workloadsecret.Supply{AttemptID: "attempt_absent", ReadinessRevision: domain.SHA256Digest(strings.Repeat("a", 64)), ExpiresAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339), Values: map[string][]byte{"ingest": []byte(canary)}})
	for _, who := range []string{"read", "write", "operate", "manage", "b"} {
		f.call(t, "POST", base, who, "secret-supply", string(raw), 403)
		f.call(t, "GET", base+"/secret_absent", who, "", "", 403)
		f.call(t, "POST", base+"/secret_absent/revoke", who, "secret-revoke", `{"attempt_id":"attempt_absent"}`, 403)
	}
	for _, body := range []string{`{}`, strings.Replace(string(raw), `"values":`, `"unexpected":true,"values":`, 1), strings.Replace(string(raw), `"ingest":`, `"ingest":"`+encoded+`","ingest":`, 1), strings.Replace(string(raw), `"values":{`, `"values":null,"ignored":{`, 1)} {
		_, response := f.call(t, "POST", base, "execute", "secret-malformed", body, 400)
		if bytes.Contains(response, []byte(canary)) || bytes.Contains(response, []byte(encoded)) {
			t.Fatal("secret echoed in failure")
		}
	}
	f.call(t, "POST", base, "execute", "secret-oversized", strings.Repeat(" ", workloadsecret.MaxRequestBytes+1), 413)
	f.call(t, "POST", base, "execute", "", string(raw), 400)
	f.call(t, "POST", base, "execute", "secret-supply", string(raw), 404)
	f.call(t, "GET", base+"/secret_absent", "execute", "", "", 404)
	f.call(t, "POST", base+"/secret_absent/revoke", "execute", "secret-revoke", `{"attempt_id":"attempt_absent"}`, 404)
	f.call(t, "POST", base+"/secret_absent/revoke", "execute", "secret-revoke", `{"attempt_id":"attempt_absent","values":{}}`, 400)
	f.call(t, "GET", base, "execute", "", "", 405)
	f.call(t, "POST", base+"/secret_absent", "execute", "secret-read", `{}`, 405)
	if f.adapter.verifies.Load() != 0 {
		t.Fatal("secret HTTP calls touched provider")
	}
}
