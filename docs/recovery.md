# Safe recovery

Preserve the original receipt, request/key, attempt, state directory, provider binding and
compatible executable before diagnosing failure. Do not delete intent rows, reset roots, clear
capacity or submit a replacement merely because a response is missing.

## Read the right fact

Operation acceptance, current operation state, remote execution, artifact availability and
hardware release are independent. A 202 is a local receipt. Provider terminal status opens
collection, not verified result availability. Cancellation intent is not termination. Local
shutdown/timeout is not remote cancellation. The latest raw unknown can coexist with stronger
confirmed running/activity evidence.

| Observed boundary | Safe action |
|---|---|
| Lost admission/control response | Explicitly replay the identical request and original key under current authority; then read current state. Do not generate a new key automatically. |
| Failed before an intent committed | No external permit exists; inspect the actual transaction outcome before retrying local work. |
| Intent committed, acknowledgement lost or external call uncertain | Reload the journal and observe original identity only, even if a lookup returns not found. |
| Staging pending/private status unknown | Observe original preparation. Do not recreate/upload/version it automatically. |
| Remote active/unknown | Retain account capacity and recovery material; inspect/reconcile the recorded resource. |
| Cancellation unsupported or target missing | Keep manual-required/unknown termination. Never delete resources or substitute IDs to pretend cancellation. |
| Terminal execution, results missing | Collect for the same attempt, not compute retry. |
| Interrupted accepted collection | Reclaim after the transfer lease under normal fencing; reuse the original result pin. |
| Committed failed collection | One explicit new collect request/key for the same attempt; old-key replay remains the old receipt. |
| Complete bytes but failed final transfer acknowledgement | Keep destination unpublished; complete bytes alone are not success. |
| Publication acknowledgement lost | Read committed result state; do not duplicate publication/events. |
| Download receipt failed after local linking | Respect `download_may_be_published`; inspect the chosen file, never overwrite/delete it automatically. |
| Result expired | Preserve history. Do not refetch inputs or rerun compute to resurrect expired results. |
| Identity/source/version/root mismatch | Stop and retain evidence; do not adopt a replacement or rewrite markers to bypass the check. |

Use [application controls](application-cli.md), [artifact delivery](artifact-delivery.md) and
[collection](collection.md) for available commands/contracts. Normal serve cannot run missing
workers. A queued job there is not a live-provider outage.

## Fixed Kaggle experiment

Use its original acceptance binary/root/configuration. After `resume-required`, start a new
process with `resume --allow-read-only`; the utility never grants another submission on resume.
A failed transfer uses its explicit collect mode. Do not point normal serve at that directory.
Missing process records or changed executable bytes cannot be repaired from plausible artifacts.
See the [acceptance runbook](providers/kaggle-acceptance.md).

Source/version/nonce checks reduce accidental mixups, not malicious-host attestation or proof that
a provider never reran the same version. Upstream upsert and mount limits remain. Do not infer an
exact remote execution count, provider timeout enforcement or hardware release without evidence.

## Backup and incident handling

Preserve SQLite with its committed WAL state, both blob roots and all identity/process markers.
A database-only backup does not restore deleted bytes or stop remote work. Never activate original
and restored copies concurrently against one provider identity. Review restored grants/revocations
before exposing work. Follow [storage](storage.md), not ad-hoc database/file copies.

For an unexpected failure, record the source/binary SHA, exact check/command, actual result,
possible committed/remote effects and a sanitized reproduction in
[validation results](development/validation-results.md). Retain raw state/logs privately. The
[next agent's bug template](development/bug-report-template.md) distinguishes hypotheses from
observations and requires a retest before closing a fix.
