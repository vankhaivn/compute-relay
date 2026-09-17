# Domain model and state interpretation

The domain defines provider-neutral identity and state, not SDK response shapes or storage
paths. See [architecture](architecture.md), [API schemas](../api/README.md) and
[recovery](recovery.md) for composition and operational use.

## Identity

An installation owns workspaces, immutable input objects, jobs and attempts. A job records the
accepted specification and frozen profile revision/account/input references. Each attempt has
its own nonce, submission intent and provider resource reference. Artifacts bind an exact
attempt/resource/version/path/length/digest, never an implicit latest attempt.

A profile name is an alias for future admission; remapping it cannot alter accepted work.
An operation identifies a requested control action. Its acceptance receipt is immutable while
its current status can advance. Idempotency keys identify a request, not authorization.

## Independent dimensions

| Dimension | Meaning |
|---|---|
| Orchestration | Local progress and business outcome, including collection/recovery work. |
| Execution | Evidence about remote command execution. |
| Result | Whether verified outputs are available, incomplete, invalid or expired. |
| Cancellation | Requested/acknowledged/confirmed cancellation evidence, not synonymous with execution. |
| Remote activity | Possible/active/inactive activity used for conservative capacity decisions. |
| Release evidence | What can actually be observed about hardware release. |

A provider terminal observation opens collection, not final success. Successful execution can
coexist with missing or expired results. A completed control operation does not prove remote
termination. The latest journal observation can be unknown while stronger confirmed attempt
execution/activity evidence is retained. State/event updates are atomic and fenced; late polls
cannot regress terminal evidence.

## Problems and capabilities

Problems expose a stable code, stage and safe action guidance. Interpret
`compute_may_have_started`, `safe_operation_retry` and `recommended_action` independently;
a generic retryable boolean is insufficient. Public responses must not leak provider exceptions,
credential references, private paths or arbitrary internal causes.

Capabilities separate support (`supported`, `unsupported`, `unknown`) from evidence and
conditions. Offline implementation, upstream documentation and account-scoped live verification
are not interchangeable. Missing quota, logs or session identity remain absent/unknown rather
than inheriting optimistic SDK defaults. Never infer termination/release from local elapsed time.
