package appclient

import "encoding/json"

// The existing wire operation kind for the retry route is retry_compute. The
// response still names the SOURCE attempt and carries a distinct new_attempt_id.
func controlIdentity(m map[string]json.RawMessage, r Request) bool {
	kind := r.Action
	if kind == "retry" {
		kind = "retry_compute"
		next := text(m, "new_attempt_id")
		if !ValidID(next) || next == r.Attempt {
			return false
		}
	}
	return text(m, "job_id") == r.ID && text(m, "attempt_id") == r.Attempt && text(m, "kind") == kind
}
