# Schema v22 upgrade fixture

This write-once fixture was generated from commit
`b8dc5b4a6d034759ad03be5333a94c331fbab635` (schema v22). Production source was unchanged.

Generation used `TestWriteBaselineState` in `baseline_state_test.go` with the destination changed
from `baseline-9591372` to this directory, and an additional export immediately before
`dumpState`: query every published artifact's `object_id`, read its bytes through
`results.Open`, and write them to `artifacts/<object_id>`. The command was:

```sh
COMPUTE_RELAY_WRITE_BASELINE=1 go test ./internal/store/sqlite -run '^TestWriteBaselineState$' -count=1
```

The existing fixture helpers seed a synthetic managed connection and immutable profile, then use
real admission, authorization, dispatch, operation and collection services with the offline fake
provider. The resulting state includes a completed collected job, a completed job awaiting manual
collection, a running job, an authorized queued job and an unapproved queued job. It includes the
original admission, authorization and collection replay receipts. No external provider was used.

`runtime.sql` is the complete logical database dump in the existing fixture format, including its
original migration history. `requests.json` contains authenticated HTTP reads and replay requests
and their recorded responses. Its token authenticates only against the synthetic fixture database.
`artifacts/` contains all five published files read from the original blob store, including the
runner manifest, logs, provenance and selected output. Metadata and SHA-256 digests remain in SQL.
Do not regenerate this directory to update expected behavior; add a new baseline instead.
