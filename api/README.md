# API contracts

The [JSON schemas](schemas/) describe Relay's HTTP/JSON requests, receipts and projections.
[Examples](examples/) include valid documents and deliberately invalid documents under
`examples/invalid/`. The HTTP API uses `/v1`; job/status documents retain the stable
`compute-connector/v1alpha1` identifier, result manifests use version `1`, and bundle manifests
use `compute-relay/bundle/v1`.

Run `go test ./internal/contracts` to compile every schema, validate every example and compare
shared enums with the Go domain values. Example names begin with their schema name; `job.*`
uses `job-spec`. Route-fragment tests also reject bodies for a different operation. The runtime
embeds these same schemas for admission, connection requests and result validation.

Update schemas and their positive and negative examples together when changing a contract.
Preserve stable versions and the Studio wire contract. Schema validation alone cannot prove
provider authorization, current workspace authority, verified file bytes or successful stream
completion. Actual HTTP behavior is covered by the API tests, including the Studio flow in
`internal/api/studio_contract_test.go`.

## Safety semantics

Workspace tokens authorize reads and mutations separately. Job creation and control requests
use an `Idempotency-Key`; a replay returns the original receipt and changed requests conflict.
Validation and status reads grant no compute authority. Cancellation records intent; reconciliation
observes; collection requests transfer work. Cleanup targets one settled attempt, requires explicit
consent to discard uncollected results, and reports confirmed per-resource outcomes.

Attempt authorization, preparation, start and workload-secret grants bind explicit attempts and
finite budgets. Workload-secret values are never durable job fields or authorization receipts.
Unknown quotas, capacity, release evidence and provider state remain unknown.

Artifact and log reads select an explicit original `attempt_id`, including historical attempts.
Logs are bounded observational pages; preserve cursors, and show a continuity gap after
`LOG_CURSOR_RESET`. Artifact content requires verified bytes and the final
`X-Compute-Relay-Verified` trailer. Neither EOF nor a log message proves job completion or result
publication.

Parsers reject duplicate keys, invalid Unicode, trailing values and closed-schema violations.
Contextual runtime checks additionally enforce budgets, reserved environment names, ownership,
selection policy and path collisions. Result publication binds nonce/input hashes, required
outputs and GPU/phase consistency.
