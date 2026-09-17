# One-shot Kaggle execution

The per-attempt Executor packages original inputs into fixed remote source, submits once under
durable authority and observes exact identity. The [acceptance utility](kaggle-acceptance.md)
composes it for one experiment; general worker/provider registration remains unfinished.

## Source and authority

`NewExecutor(stager, policy, plan, prepared, allowSubmit)` clones the original validated plan,
private/ready version-1 staging reference and policy. It constructs inert source locally,
not credentials or provider calls. The unchanged five runner modules are checked against their
source lock and explicitly embedded without caches. The runner manifest binds original input
IDs/names/targets/digests, command/resources/network/time/outputs, nonce, staging and GPU policy.
Credentials, source URLs, profile/job labels and host paths are excluded; explicitly supplied
workload environment is still user data, not a universal secret-scanning result.

A stable `cre-` name derives from prewritten intent/attempt identity, not content. Source/policy
changes conflict under that name instead of evading uncertainty with a new resource.
Only an invocation receiving a successful **new** M3 BeginSubmission commit may call Submit.
Operator enablement and the atomic per-instance latch are additional checks, not durable permits.
Reconstructing the executor after restart must never rearm submission.

## Submission and exact reads

Submit re-verifies the entire original private staging reference before kernel work. After local
pins and explicit account verification, exact-resource HTTP 404 or the precise Kaggle
`kernels.get` 403 `PERMISSION_DENIED` absence response can lead to one SaveKernel wire call on a
newly authorized path. Other authorization/permission errors fail closed. An exact existing
kernel is read, not updated. The request is a private Python script with finite requested timeout
and explicit CPU/GPU/internet; no TPU, extra sources, custom image, priority, interactive session
or paid fallback is exposed.

Acceptance requires a usable version-1 receipt and exact numeric kernel ID/source/private
metadata readback. Kaggle's `author` field is a human display name, so account/resource identity
is bound by the canonical `owner/slug` reference plus exact slug, not display-name equality. A
timeout, HTTP/receipt error, nonzero exit or lost acknowledgement after a possible save remains
submission-unknown. Reconciliation sends original identity/source digest, not another save.
Observation additionally checks the first-pinned numeric ID.

Validate raw metadata rather than SDK defaults, read version 1 explicitly, read its status and
recheck current metadata. `GetKernelSessionStatus` intentionally sends only account username and
kernel slug because the live Kaggle endpoint rejects `versionLabel`; version-1 identity remains
verified by the surrounding `GetKernel` reads. The same status-request contract is used by logs
and artifact collection. New source/version/account/privacy/ID or additional sources fail closed.

| Raw state | Execution / activity |
|---|---|
| QUEUED | queued / possible |
| RUNNING | running / active |
| COMPLETE | succeeded / inactive |
| ERROR | failed / inactive |
| CANCEL_REQUESTED, CANCEL_ACKNOWLEDGED, NEW_SCRIPT or missing/new/failed status | unknown / possible |

Release evidence stays `not_observable`. Terminal observation opens [collection](../collection.md),
not final business success. The latest raw unknown may coexist with stronger confirmed attempt
state. Existing fences reject stale nonterminal updates after terminal facts.

## Remote bootstrap and limits

Attached Kaggle datasets may be mounted as
`/kaggle/input/datasets/<owner>/<dataset-slug>/` or exposed through the legacy
`/kaggle/input/<dataset-slug>` form. The generated bootstrap therefore carries the frozen dataset
owner as well as slug, checks the nested owner path first and the legacy path second, and only when
both are absent inspects one bounded owner namespace level under `/kaggle/input/datasets`. That
fallback must resolve to exactly one matching slug. Provider-owned symlink/volume mounts may
resolve, but the selected mount must be a directory and the resolved marker must remain a regular
file inside that resolved dataset root. Missing, ambiguous or excessive layouts, marker escape
and changed marker bytes fail before any runner module is loaded.

The runner modules receive resolved staging/root paths and contain no additional absolute Kaggle
input-layout dependency. Results remain rooted at a fresh `/kaggle/working/relay-result`
directory. Monitoring uses provider APIs rather than container paths, while artifact collection
addresses provider output with the matching relative `relay-result/` prefix; regression coverage
keeps that prefix aligned with the bootstrap result directory. Duplicate in-process invocation is
rejected and signal handlers are restored. The original runner verifies bundle/input bytes and
bounded execution. The control plane never runs the generated script. [Artifact retrieval](kaggle-artifacts.md)
selects only agreed controls/outputs, not arbitrary code/input/scratch under the result tree.

Default policy is CPU-only, no internet, 1,800-second wall ceiling and one-minute local invocation.
GPU requires explicit T4/P100 shape; no CPU upgrade or GPU fallback. New GPU saves recheck the
pay-to-scale flag in the returned GPU quota object: omitted scalar false and explicit false are
equivalent under the [pinned quota schema](kaggle-operations.md#quota). Missing quota objects,
explicit true, null or non-booleans block save. This is a free-policy recheck, not numeric quota
scheduling or a reservation; the acceptance harness separately requires known remaining time.
Policy bounds are 1–86,400 wall seconds and one second–five minutes per invocation.

Source/payload/request bounds are 2 MiB/1 MiB/3 MiB; token 8 KiB, each output stream 16 KiB.
The fixed isolated leaf has exact RPC allowlisting, at most ten armed calls, no retries/redirects/
ambient proxies, verified TLS, 5/10-second connect/read limits and parent/watchdog deadlines.
Private source/token travel only on stdin. Cooperative callbacks/OS operations remain trusted.

## Unresolved provider guarantees

SaveKernel is an upsert without exposed atomic create-only/CAS: another actor can race absence
and save. Post-save checks cannot undo that effect; no remote exactly-once guarantee is made.
An external same-version rerun with unchanged observable identity is not ruled out. Local
ObservedAt is read time, not a server execution timestamp or session attestation.

Dataset attachment names owner/slug, not a guaranteed immutable mount. Pre-save verification
and remote hashes detect changes but do not lock provider state. Source reconstruction depends
on original binary/bootstrap/runner and configuration; upgrades can prevent verification of older
work. Preserve the compatible executable/configuration rather than changing pins or resubmitting.
See [ADR-0017](../decisions/0017-one-shot-kaggle-execution.md),
[operational mappings](kaggle-operations.md) and [recovery](../recovery.md).
