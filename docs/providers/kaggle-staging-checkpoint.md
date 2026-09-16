# M4-02 staging checkpoint: planning and read-only verification

> **Status:** partial preservation checkpoint on `checkpoint/m4-02-offline-staging`.
> This is not a completed M4-02 adapter or a merge-ready replacement for PR #20.
> **Base reviewed:** `52b1655c6d194e961492dcf4b241b459663b2ed8` (PR #19 merged).
> **Prepared locally and preserved on GitHub:** 2026-09-16.
> No provider credential, live request or remote resource mutation was used.
> **Requirements:** DAT-04, OPS-05, SEC-03; preserves the M3 one-shot preparation rule.

## Preservation and active implementation handoff

The owner requested pushing the previously exported checkpoint after GitHub write actions
became available. On re-reading the repository, PR #20 already existed on
`feat/m4-02-private-staging`, observed at `c7c5081f3b929dbe7c22afaefaa52b1176841ca1`.
It contains a different, newer staging implementation in `internal/provider/kaggle/`,
including a direct mapping from `provider.Plan` and SDK/process source files.

To preserve both workstreams, the four local Go source/test files were uploaded byte-for-byte
on this separate checkpoint branch, in code commit
`f6cb6c0fe368313737d4f6fac61a43e944126767`. Documentation is a separate commit.
The active PR branch and `main` are not updated by this preservation push. No duplicate
implementation PR is opened; PR #20 remains the M4-02 integration/review workstream.

**Do not blindly merge or cherry-pick this package into PR #20.** Its provisional resource
slug includes the complete manifest/content digest. The active PR's `buildStagingPlan`
instead derives its resource name from the prewritten attempt/preparation identity so that
changed payload/configuration cannot select a new target under the same intent. File names,
manifest format and types also differ. Any reuse must explicitly reconcile these semantics
and preserve the existing durable one-shot gate, rather than install two competing planners.
This branch is recoverable source and test material, not an authorization to switch formats.

The original archive's checksums all verified before upload. GitHub blob IDs matched the
four locally calculated blob IDs:

| File under `internal/provider/kaggle/staging/` | Git blob ID |
|---|---|
| `plan.go` | `eb998dcb1560092f0ce4438263b50bfb040b23e8` |
| `plan_test.go` | `cda547ab92c4363631caa1784767a921a5c47146` |
| `verify.go` | `b554b878a545bc6d7a24867d9af534d40d7c724b` |
| `verify_test.go` | `cdfe0d33db0dcd17a588f2b086043f28be97e38f` |

The remaining sections describe this preserved package only, not the completion status of
the different active PR implementation. Its current status must be read from PR #20.

## Delivered in this checkpoint

`internal/provider/kaggle/staging` is an adapter-internal, standard-library-only
package. It builds deterministic attempt-bound staging manifests and verifies an
already existing dataset through an injected **read-only** source. It does not
implement `provider.Provider`, an upload transport, or a durable store.

`NewPlan` takes the original installation/workspace/job/attempt/instance/revision,
preparation operation, nonce and parent-plan digest plus frozen object metadata.
Missing identities are errors; recovery must never manufacture new identities.
The caller must map these values from the existing M3 records. No duplicate durable
ledger or new credential/configuration API is introduced by this package.

The input content digest uses the same sorted-target, alphabetical-key, ASCII JSON
recipe as `provider.InputSnapshot.InputDigest`. A checked Python-derived golden
value and an HTML-sensitive target test cover that compatibility. Input ordering
cannot change the plan. Mutating caller-owned input arrays or returned byte/file
slices cannot change an existing plan.

The plan binds every object ID, workspace, length and SHA-256, along with the
original parent-plan digest. It checks the existing 100 MiB bundle, 2 GiB per input,
4 GiB total input and 64-input ceilings, and rejects duplicate names, conflicting
object identity and overlapping/unsafe logical targets. These are local planning
limits, not measured provider limits. Lower operator/transport limits still apply.

Physical transfer names are generated (`bundle.bin`, `input-000.bin`, etc.). Logical
input targets are data in `relay-staging.json`, never host filesystem paths. The
opaque dataset slug is deterministic and 44 ASCII characters long. Its seed binds
the account and complete manifest, including operation/attempt/configuration identity.
The manifest version is provisional pending the full staging transport/ledger review.

Metadata selects `copyright-authors` and explicitly preserves existing ownership
and licenses. It is not a relicensing mechanism. Crucially, generated metadata does
**not** contain a fabricated `isPrivate` field: a future SDK create operation must
separately force private creation and disable content conversion/extraction. This
checkpoint does not create anything and does not claim metadata alone ensures privacy.

## Readiness is independent from creation acknowledgement

`Plan.Assess` consumes only `Inspect`, `List` and `Open` methods. There is no create,
upload, version, submit, cleanup or mutation-retry method in that interface.

Absent, unknown or still-processing observations remain non-ready. An upload
acknowledgement is not an input to successful verification. A missing resource
never returns a new-create permit and must not reset the M3 preparation gate.

For readiness, the source must supply an explicitly private dataset, server-verified
account, exact dataset identity and first version. A previously persisted dataset
ID cannot be replaced silently. Unknown visibility, a different account, replacement
ID, manually created version or failed processing stops readiness.

The source contract then requires bounded, complete pagination and exact regular-
file names/sizes. Extra/missing/duplicate entries, symlinks, cursor cycles and malformed
cursors fail. Verification first downloads and hashes the expected identity manifest,
then streams and hashes every payload. Catalog hashes are not trusted as byte proof.
Every read must finish at the declared length with successful EOF and Close; a late
read/close error after the last byte is still failure. No partial-byte resume exists.

The dataset is inspected again after all transfers. Identity, visibility, account,
version or readiness changes invalidate the whole assessment. Only a successful
assessment yields the verified pin and plan digest. A zero assessment is not ready.
These checks are consistency safeguards in a trusted process, not remote attestation.

The future official transport must substantiate authenticated account, explicit
privacy, exact version, regular-file semantics and normalized phase mappings. The
package does not assume that Kaggle exposes any particular raw status or digest
field. A transport that cannot establish these facts must remain unready. Source
errors are sanitized; they are not reflected as provider exceptions or private paths.

The assessment has a ten-minute context budget, bounded pages and 32 KiB streaming
buffers. Readers/callbacks must cooperate with cancellation; the package does not
forcibly terminate a stuck callback. This bound is not an overall-job deadline or
a provider performance promise. Existing schema validation and bundle inspection
remain upstream responsibilities; synthetic unit bytes are not valid workload bundles.

## Integration still required for this preserved package

These were the checkpoint's remaining tasks before the active PR was discovered. They
are not a directive to replace PR #20's existing implementation. Reconcile the formats and
resource naming described above before selecting any reusable test or verification logic.

1. Map the actual frozen `provider.Plan`, binding snapshot and preparation operation
   into this plan and persist its exact target before the first provider-side effect.
   Review the provisional wire format and mapping in an ADR before activation.
2. Implement the audited, pinned official-client upload/create path with explicit
   operator authorization, private visibility, original-byte preservation, finite
   limits and no hidden replay. Account verification must precede all provider calls.
3. Implement exact-version read-only SDK inspection, full file pagination and byte
   retrieval. Verify that the SDK/provider can actually supply all required evidence;
   do not manufacture the normalized Source assertions from configuration labels.
4. Integrate observations and resource identity with the existing M3-04/M3-07 journal
   and ownership ledger. Test creation response loss, crash boundaries, stale fencing,
   missing resources, account rotation and delayed readiness with real SQLite.
5. Run pinned-stack repository/native/SDK CI. Synchronize the living plan, roadmap,
   capability evidence and operator procedure in a separate documentation commit.
   Live K-03/K-04 and M1-08 remain separately gated; no credential is requested in chat.

No public endpoint, production command, schema, migration, dependency, runner asset,
existing dispatch implementation or capability descriptor is changed by this checkpoint.
The preservation push does not change canonical roadmap status, mark M4-02 complete,
make PR #20 ready, or start M4-03.

## Validation performed

On local Linux with Go 1.23.2, the **actual complete new package** was tested directly,
without alternate drivers, interface stubs or dependency replacements:

```text
GO111MODULE=off GOTOOLCHAIN=local go test -race -count=10 -v .
GO111MODULE=off GOTOOLCHAIN=local go vet .
GO111MODULE=off GOTOOLCHAIN=local go test -cover .
GO111MODULE=off GOTOOLCHAIN=local go test -run='^$' -fuzz=FuzzPlanTargetValidation -fuzztime=3s -parallel=2 .
```

These commands were run from the new staging package directory, not the repository
root. They are explicitly limited local evidence, not a change to the repository's
Go 1.27.1 pin. Full repository/SDK/SQLite integration and native Windows/macOS runs
were not performed. The accompanying checkpoint archive includes exact command logs,
checksums and the list of source files tested. No test calls Kaggle or creates compute.

During the preservation push, all archive checksums were verified again, `gofmt -l *.go`
reported no differences, and the exact package was rerun with
`GO111MODULE=off GOTOOLCHAIN=local go test -race -count=10 .` and
`GO111MODULE=off GOTOOLCHAIN=local go vet .`; both passed on Go 1.23.2 linux/amd64.
The earlier fuzz/coverage results were not relabeled as fresh runs. No full repository
CI result is claimed for this preservation branch, and PR #20's checks are not reused as
qualification of different source bytes.

The focused pinned-toolchain command is `go test -race ./internal/provider/kaggle/staging`;
full `devtool check`, `devtool test-race` and adapter integration tests still need to run
for any proposed integration of this source.

## Sources and evidence boundary

The following primary files were read through the GitHub connector on 2026-09-16:

- [Kaggle CLI v2.2.4 dataset metadata](https://github.com/Kaggle/kaggle-cli/blob/v2.2.4/docs/datasets_metadata.md): title/slug length, ID and license metadata. File SHA: `b5187d1b4f4feade1509a24e99458ca3e4e06403`.
- [Current repository preparation contracts](https://github.com/vankhaivn/compute-relay/blob/52b1655c6d194e961492dcf4b241b459663b2ed8/internal/provider/preparation.go): frozen inputs, input digest, read-only preparation recovery and exact binding snapshots.
- [Current provider contract](https://github.com/vankhaivn/compute-relay/blob/52b1655c6d194e961492dcf4b241b459663b2ed8/internal/provider/contract.go): prewritten identity and independent Prepared readiness/privacy assertions.
- [Approved proposal](https://github.com/vankhaivn/compute-relay/blob/52b1655c6d194e961492dcf4b241b459663b2ed8/docs/proposal.md): sections 8.5, 10.6, 13.4–13.6, 20.5 and 23.6.
- [Active PR #20 staging plan at the observed head](https://github.com/vankhaivn/compute-relay/blob/c7c5081f3b929dbe7c22afaefaa52b1176841ca1/internal/provider/kaggle/staging_plan.go): incompatible provisional format and stable attempt/preparation-derived resource naming.

No API-field or raw-status mapping is inferred from these sources beyond the stated
metadata shape. A metadata format review is not proof that private upload/readiness
works for an account. The live acceptance gates remain unchanged.
