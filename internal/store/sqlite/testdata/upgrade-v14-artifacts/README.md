# Exact artifact supplement for the schema v14 baseline

The write-once `baseline-9591372` fixture contains SQL and HTTP receipts but did not export its
physical result blobs. This separate supplement leaves that directory untouched.

The runner manifest was extracted verbatim from `collection_snapshots.snapshot.manifest` in the
original SQL. The other four files use the exact synthetic payloads from `completedOutputs`:
`{"fixture":true}`, `fixture output\n`, an empty stderr, and `fixture bytes\n` repeated 4096 times.
Every file's byte count and SHA-256 digest was checked against the original `artifacts` row before
writing it under its original object ID. These are reconstructed historical fixture bytes, not a
newly executed v14 workload.

The upgrade test restores these files before opening the historical state root, then verifies
published metadata and reads every file through the authenticated collection service after
migration. This qualifies preservation of synthetic state and bytes; it does not qualify a live
provider or operating-system credential vault.
