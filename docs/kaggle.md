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
| Staging checks | Uploads are SHA-checked against the frozen digest; readiness and recovery compare metadata only, and the runner rehashes every input before use. |
| Staging visibility | Kaggle may hide a new dataset for a while; Relay polls read-only for up to 2 minutes, then stops with `staging_not_visible`. |
| Request pacing | API requests are admitted at least 500 ms apart per configured account, plus 0–100 ms jitter, and at least 100 ms apart across the provider. Concurrent callers share admission through response headers. |
| Rate limits | Metadata, logs and download tickets honor HTTP 429 `Retry-After` (seconds or HTTP date), with up to three sends per read. Missing or invalid delays use 30 s then 60 s. Waiting uses the caller's budget, capped at one hour; each HTTP request keeps its own timeout. |
| GPU quota | A GPU authorization needs fresh, sufficient known quota. Connections to one account share it. No reset time is reported. |
| Internet | Disabled unless the policy allows it **and** the job requests it. |
| GPU proof | Success requires the runner's hardware evidence, not just a requested GPU or a quota deduction. |
| Unsupported | `prepare_then_start`, absolute deadlines, workload secrets, `application_image`, SSH cache. |
| Downloads | Only from `storage.googleapis.com` and `www.kaggleusercontent.com` over HTTPS. |
| Cancellation | May stay manual-required when Kaggle gives no usable session target. |
| Failure reasons | A stopped preparation or collection carries a closed `reason`, e.g. `shared_create_failed_http_500` or `artifact_manifest_download_failed`. |
| Cleanup | Explicit; never touches shared datasets. Deletion is by owner/name, so a resource replaced between check and delete could be deleted instead. A 403 alone (for example after a lost acknowledgement) stays `unknown`. |

A lost staging or submission response leaves the original intent for observation; a "not found"
read never triggers another create or submit. Mutations never retry because of HTTP 429. A file
upload that failed before its dataset was created can be sent once more with a new ticket.

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
