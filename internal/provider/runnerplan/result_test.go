package runnerplan

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/provider"
)

func resultFixture() (provider.Identity, map[string]any) {
	id := provider.Identity{InstallationID: "runtime", WorkspaceID: "workspace", JobID: "job", AttemptID: "attempt", InstanceID: "fake", IntentID: "intent", ResourceKey: "resource", Nonce: strings.Repeat("n", 32), BundleSHA256: provider.Digest([]byte("code")), InputManifestSHA256: provider.Digest([]byte("inputs"))}
	return id, map[string]any{"manifest_version": "1", "job_id": id.JobID, "attempt_id": id.AttemptID, "attempt_nonce": id.Nonce, "runner_version": "fixture", "bundle_sha256": id.BundleSHA256, "input_manifest_sha256": id.InputManifestSHA256, "started_at": "2026-10-14T00:00:00Z", "finished_at": "2026-10-14T00:00:01Z", "phase": "completed", "exit_code": 0, "timed_out": false, "resource_check": map[string]any{"gpu_required": false, "gpu_verified": false}, "artifacts": []any{map[string]any{"path": "answer.txt", "bytes": 3, "sha256": provider.Digest([]byte("yes"))}}, "error": nil}
}

func TestResultContractRejectsUntrustedClaims(t *testing.T) {
	tests := []struct {
		name   string
		change func(map[string]any)
	}{
		{"unknown field", func(m map[string]any) { m["extra"] = true }},
		{"missing field", func(m map[string]any) { delete(m, "timed_out") }},
		{"identity", func(m map[string]any) { m["job_id"] = "other" }},
		{"timestamp order", func(m map[string]any) { m["finished_at"] = "2026-10-13T00:00:00Z" }},
		{"timeout", func(m map[string]any) { m["timed_out"] = true }},
		{"failed without error", func(m map[string]any) { m["phase"] = "failed" }},
		{"gpu", func(m map[string]any) {
			m["resource_check"] = map[string]any{"gpu_required": true, "gpu_verified": false}
		}},
		{"unsafe path", func(m map[string]any) { m["artifacts"].([]any)[0].(map[string]any)["path"] = "a:b" }},
		{"negative bytes", func(m map[string]any) { m["artifacts"].([]any)[0].(map[string]any)["bytes"] = -1 }},
		{"oversized bytes", func(m map[string]any) { m["artifacts"].([]any)[0].(map[string]any)["bytes"] = int64(1<<32) + 1 }},
		{"digest", func(m map[string]any) { m["artifacts"].([]any)[0].(map[string]any)["sha256"] = "bad" }},
		{"collision", func(m map[string]any) {
			m["artifacts"] = []any{map[string]any{"path": "A", "bytes": 0, "sha256": provider.Digest(nil)}, map[string]any{"path": "a/b", "bytes": 0, "sha256": provider.Digest(nil)}}
		}},
	}
	id, m := resultFixture()
	raw, _ := json.Marshal(m)
	if _, err := ParseResult(id, raw); err != nil {
		t.Fatal(err)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			id, m := resultFixture()
			test.change(m)
			raw, _ := json.Marshal(m)
			if _, err := ParseResult(id, raw); err == nil {
				t.Fatal("untrusted result accepted")
			}
		})
	}
	raw = append(raw[:len(raw)-1], []byte(`,"phase":"failed"}`)...)
	if _, err := ParseResult(id, raw); err == nil {
		t.Fatal("duplicate result field accepted")
	}
}
