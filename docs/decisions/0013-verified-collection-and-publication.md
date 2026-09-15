# ADR-0013: Immutable result pins and fenced artifact publication

- Status: proposed; implementation in PR #16, pending owner review/merge
- Date: 2026-09-15
- Task: M3-06
- Requirements: JOB-05, PRV-02, VER-02; preserves DUR-01/03 and DOM-02/03
- Builds on: ADR-0008, ADR-0011 and ADR-0012

## Context

M3-04 records exact terminal execution evidence without claiming verified results. M3-05
adds durable transfer-only collect tickets. Downloading some files, seeing a successful
provider wrapper or receiving a successful transfer response is not sufficient to publish
job success. Filesystem publication and SQLite commit are separate failure boundaries.

A restart or explicit collection retry must not select a newer run, rerun compute, replace
frozen inputs, overwrite attempt history or expose a partial artifact set.

## Decision

Implement a separately composed `internal/collection` engine using only binding verification,
listing and fetch methods. Reuse the existing embedded result schema, provider identity
contract, create-only blob store and SQLite transaction helpers; add no dependency.

Acquire a collection-specific lease tied to the exact operation and attempt revision.
The engine consumes an existing accepted collect ticket or creates the first ticket only
when explicitly run for terminal collectible work with no collection history. It does not
automatically replace a committed failed ticket. No migration, HTTP GET or admission starts
this engine or grants permission to create compute.

Validate the complete bounded listing and result manifest against the frozen specification
and remote identity. Persist one immutable snapshot per attempt before transferring output
bytes. Reuse that same snapshot across operation retries; metadata-only discovery is not
permission to refresh a pinned result.

Use a dedicated collector-owned blob root, separate from uploads. File IDs derive from the
full remote reference, path, size and digest. Remote paths never become local paths and no
archive is extracted. Stream through a size/digest-checking writer; send EOF only after a
successful provider return and matching transfer receipt. Reopen and independently hash all
pinned local blobs before issuing an internal verification value.

Migration 7 atomically publishes the result record, complete artifact set, state/events,
operation outcome and lease release. Publication requires the current lease and a matching
verification value whose fields are not exported. This protects normal package composition,
not malicious same-process code, unsafe reflection or cryptographic remote attestation.

Retain complete pre-commit blobs after interruption. Recovery reclaims an expired accepted
ticket, reloads its pin and reuses only rehashed files. A lost publication acknowledgement
is not followed by a failure mutation. A committed transfer failure requires a new explicit
collect request; neither path repeats compute. Partial-byte range resume is deferred.

Keep execution observations, local cancellation intent, release evidence, payload outcome
and artifact availability separate. Failed payload manifests/logs remain useful verified
results. A successful wrapper cannot turn a failed payload into a successful job. Existing
HTTP control/status schemas remain unchanged; artifact HTTP and production CLI composition
remain separate gates.

## Alternatives rejected

- Publish each file immediately through the objects API: leaks partial results and conflates
  uploads with attempt-bound outputs.
- Commit metadata before downloading: advertises unavailable/unverified artifacts.
- Hold a SQL transaction while downloading: blocks the one-connection store and makes
  network failure a database transaction lifetime.
- Re-list and replace metadata after a failed transfer: risks associating a newer execution
  with an old attempt.
- Repeat compute to recover missing results: violates explicit retry and quota boundaries.
- Introduce a broker, cloud bucket or new archive subsystem: unnecessary for the initial
  SQLite/local-filesystem model.

## Consequences and explicit limits

Result pins and complete blobs remain retained until M3-07 supplies ownership-safe cleanup.
A database-only backup is not a complete result backup. Missing/corrupt cached bytes cannot
be silently replaced with changed content. Provider availability and callback cooperation
are still required; a fixed pool does not forcibly kill a Go callback.

The existing manifest lists files, not directory-presence entries. Nonempty declared
directories have per-file and aggregate verification; empty required directories fail closed
instead of claiming presence. A marker file can express that requirement without changing
the manifest version. This is documented as a limitation, not live-provider evidence.

## Verification

Core tests cover strict JSON/Unicode, identity and frozen GPU/output requirements, path
collisions, missing required files, directory aggregates and false-success combinations.
Real SQLite/blob component tests cover pagination, partial/late-error/digest failures,
concurrent/stale leases, cancellation races, profile remapping and authorized reads.

Fault tests exercise actual SQL disk-full rollback, schema-6 upgrade, lost pin/publication
acknowledgements, complete blobs before metadata and a real subprocess kill after pin commit.
They assert that one original attempt/submission remains. Provider bytes are synthetic and
no uploaded command executes. Local decoder-only evidence and full pinned-stack offline CI
are reported separately in the collection guide and PR #16. No live credentials or compute
are involved. Stop after PR #16 for owner merge; M3-07 is not part of this decision.
