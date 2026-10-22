# Kaggle

Relay talks to Kaggle's HTTPS API directly from Go; no Kaggle CLI or local Python is needed. Each
attempt becomes one private dataset (inputs) and one private kernel (the runner).

## Connect

Create a connection with `provider_type: "kaggle"` and the write-only `api_token` field
([Connections](connections.md)). Relay discovers the account and reads quota without starting
compute. Replacing the credential must keep the same account.

The machine type comes from `serve --managed-machine-shape`: `cpu` (default), `NvidiaTeslaT4` or
`NvidiaTeslaP100`. CPU is always an explicit choice, never a fallback when no GPU is available. After
changing the flag, run a connection `check` to get a selection for it.

## Limits and behavior

| Topic | Behavior |
|---|---|
| Wall time | At most 86400 s (Kaggle's session limit). |
| Inputs | Up to 256 inputs, staged in one private attempt dataset with a marker file (≤2 MiB). Inputs of 256 MiB or more go to [shared datasets](#shared-input-datasets). |
| Staging checks | Uploads are SHA-checked against the frozen digest; readiness and recovery compare metadata only, and the runner rehashes every input before use. File lists request 200 entries per page, with at most 16 pages; missing/null arrays, non-progressing pages and incomplete lists never confirm readiness. |
| Staging visibility | After an acknowledged or uncertain create, Relay polls the exact owner/slug, version 1 and marker for up to 2 minutes. Dispatch retains a separate durable ten-minute observation grace from its first preparation fault. |
| Pre-submit recheck | Just before the one submission, Relay rereads ready staging. Any read that does not confirm it (unanswered, missing name, changed or partial metadata, unready, non-private) is read again from 15 s backoff (doubling, at most 2 min) for up to 10 minutes. Auth, kernel and quota read refusals before `SaveKernel` share that window. It ends `staging_not_confirmed`, `staging_recheck_failed` or the refusal reason, with a `cause`, never submitting. |
| Privacy | A dataset read as non-private is read again 3 times, 5 s apart, before it is reported for attention. |
| Create acknowledgement | CreateDataset has one send and up to 2 minutes to answer; ordinary metadata requests retain a 15-second deadline. Caller budgets can shorten both. |
| Request pacing | API requests are admitted at least 500 ms apart per configured account, plus 0–100 ms jitter, and at least 100 ms apart across the provider. Concurrent callers share admission through response headers. |
| Read retries | Metadata, logs and download tickets retry HTTP 429, temporary 5xx and transport failures, with up to three sends per read. HTTP 429 honors `Retry-After` (seconds or HTTP date); missing or invalid delays use 30 s then 60 s. Other temporary read faults wait 1 s then 2 s. Waiting uses the caller's budget, capped at one hour; each HTTP request keeps its own timeout. Mutations remain one-shot. |
| GPU quota | A GPU authorization needs fresh, sufficient known quota. Connections to one account share it. No reset time is reported. |
| Internet | Disabled unless the policy allows it **and** the job requests it. |
| GPU proof | Success requires the runner's hardware evidence, not just a requested GPU or a quota deduction. |
| Unsupported | `prepare_then_start`, absolute deadlines, workload secrets, `application_image`, SSH cache. |
| Downloads | Only from `storage.googleapis.com` and `www.kaggleusercontent.com` over HTTPS. Batch collection shares one account/manifest read and initial/final kernel identity and terminal checks across exact pinned file downloads. It transfers selected files only, including on recovery; an empty retry still verifies remote identity and terminal state. |
| Collection budget | The batch has a shared finite budget: the configured control allowance plus one second per selected file and per MiB, capped at two hours and shortened by the caller. Each transfer retains its own timeout; complete temporary bytes are published only after the batch returns verified success. |
| Cancellation | May stay manual-required when Kaggle gives no usable session target. |
| Failure reasons | A stopped preparation or collection carries a closed `reason`, e.g. `shared_create_failed_http_500` or `artifact_manifest_download_failed`. Unresolved staging reads and pre-submit stops add `cause`, e.g. `staging_lookup_failed_http_429`, `shared_lookup_failed_http_403`, `staging_files_mismatch` or `lookup_rate_limited`; a terminal `staging_unconfirmed` also keeps the first failed step as `first_cause`. |
| Cleanup | Explicit; never touches shared datasets. Deletion is by owner/name, so a resource replaced between check and delete could be deleted instead. A 403 alone (for example after a lost acknowledgement) stays `unknown`. |

A lost staging or submission response leaves the original intent for observation; a "not found"
read never triggers another create or submit. When staging has never acquired a confirmed dataset
identity, reconciliation reads use the control-call budget (one minute by default) within a
durable ten-minute grace starting at the first preparation fault, after upload. A restart does
not reset that grace or permit uploading again. After the grace, an answered read that still
cannot confirm the resource (a missing name or a different identity) ends the unsubmitted attempt
as `failed` with reason `staging_unconfirmed`, releasing its account slot. Reads Kaggle leaves
unanswered (429, outage, timeout) keep observing until the attempt's preparation limit (one hour
by default), then end the same way. Either ending still means unknown, not absent. A recorded private dataset that is still processing remains a separate pending state.
Historical attention attempts without a saved grace start it on their first unsuccessful
observation after an explicit reconciliation request.

Submission reconciliation derives the kernel name from the frozen attempt identity and validates
version 1, source digest, owner, privacy and mounted inputs before normal observation can resume.
If an exact lookup fails, it scans every page of the authenticated owner's unfiltered kernel
inventory. Finding the name resumes identity verification. Only complete inventories can establish
absence; two complete absent observations at least two minutes apart settle the original attempt
as rejected with `submission_absent_owned_list`. Partial, malformed, rate-limited or unavailable
inventories retain uncertainty and never permit another submission of the original attempt. Missing
or null kernel arrays are malformed, not empty inventories; only an explicit complete list can
establish absence.
The consumer may then admit a new attempt under its replacement policy.

Unresolved lookups retain closed access, authentication, rate-limit, timeout, interruption or
invalid-response reasons. Five failed observations reach attention. At attention, an explicit
owner resolution can attest the exact notebook absent or present through the normal reconciliation
operation. Absence settles with `submission_absent_owner_confirmed`; presence follows the original
identity and prevents later inventory misses from overruling that attestation. Original submission
HTTP status, allowlisted provider error type and operation survive later observations. Raw provider
messages and bodies are never public diagnostics. A 403 or 404 lookup alone is not absence proof.

Terminalization preserves the original intent and unknown resource evidence. Cleanup preview can
observe this settled attempt using its frozen staging plan. Exact discovery records the numeric
identity for ordinary cleanup without reopening the failed attempt. An exact HTTP 404 displays
`absent`; it creates no deletion target or durable absence receipt. Missing-name 403 remains
unknown. Until identity is confirmed, the ledger stays `creation_unresolved` and local input pins
remain. Confirmed partial resources use exact-attempt cleanup; shared inputs remain untouched.
Mutations never retry because of HTTP 429. A file upload that failed before its dataset was created
can use a fresh upload ticket up to five times, waiting 15, 30, 60 and 120 seconds within the
original preparation budget. This never repeats `CreateDataset` or a kernel submission.

Rate-limit cooldowns apply across all API endpoints and clients for the same account, including
credential replacement. A limit before account discovery applies provider-wide. Mutations share
pacing and record 429 cooldowns but still send at most once. Storage-host transfers keep their own
bounded transfer policy; no partial transfer is retried by this gate.

Managed Relay retains account/provider cooldown deadlines in the private
`state/kaggle-api-cooldowns.json` file under its existing installation lock. It contains no tokens,
credential fingerprints or provider response text. Restart retains the deadline. Invalid or
unwritable pacing state refuses further API requests; preserve the state directory for diagnosis.
Standalone native clients share only process-local pacing. Short calls that cannot wait return 429
without sending; preparation can wait within its existing authorization budget. Previously stopped
staging remains observation-only and never recreates an absent dataset or authorizes a new attempt.

Runtime binding verification reads fresh account identity without fetching quota. Explicit connection
checks and GPU admission still read fresh quota. An ordinary verified state observation uses five
API reads; a verified log page uses six. Both retain separate account verification at each credential
resolution and kernel identity checks before and after status/log reads; no identity results are
cached across calls.

Log reads have a 45-second authenticated route budget (40 seconds inside the account monitor).
The native stream has up to 15 seconds for connection setup and replay to the verified cursor;
only then does its two-second live-tail window begin. Complete lines preserve cursor continuity,
while an unreadable stream with no verified new lines returns unavailable rather than claiming
an idle log. Kernel identity is still checked before and after releasing a page. A delayed log
read says nothing about whether the kernel is progressing; inspect its observed execution state
and the timestamps of real workload progress separately.

Temporary verification outages are `PROVIDER_UNREACHABLE`; malformed responses are
`PROVIDER_STATE_UNKNOWN`. After confirmed submission, both keep read-only observation active
with bounded calls and backoff, even after repeated failures or a Relay restart. Rejected credentials,
denied access, unavailable local credentials and account mismatches keep distinct problem codes and
attention gates. Existing attempts already parked for attention need an explicit reconcile.

## Shared input datasets

An input of at least 256 MiB (for example an application runtime archive) is staged once per
installation and account as its own private dataset, named from its SHA-256 and byte count. Later
attempts with the same bytes mount it instead of uploading again. A kernel mounts its attempt
dataset plus at most 7 shared ones; further large inputs stay in the attempt dataset.

- A missing shared dataset is created by the preparation that needs it. If the create response is
  lost, Relay only observes; it never creates it twice.
- An existing one is used only if name, version 1, privacy, marker and exact file list match;
  otherwise preparation stops without replacing it.
- Attempt cleanup and retention never delete shared datasets. They count toward the account's
  Kaggle storage until you delete them in Kaggle — only when no waiting attempt mounts them; the
  next attempt that needs one uploads it again.
