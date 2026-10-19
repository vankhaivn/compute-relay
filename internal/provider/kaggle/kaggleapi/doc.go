// Package kaggleapi implements the fixed Kaggle wire operations used by Relay.
//
// The wire reference is kagglesdk 0.1.35: KaggleHttpClient.call serializes the
// generated request fields as camel-case JSON and posts to api.kaggle.com/v1.
// Enum requests use their names, empty repeated fields are omitted, and optional
// false kernel flags remain explicit. The existing Relay Python bridge fixtures
// provide representative responses; they are not live captures of this client.
//
// Mutating RPCs send once. Allowlisted metadata reads retry HTTP 429 at most twice,
// honoring Retry-After within the caller's budget (at most one hour overall).
// Calls never use cookies, environment proxies or automatic redirects. File downloads may explicitly
// exchange one API Location header for one request to an approved storage host.
// Account credentials never enter that storage request. Upload references and
// signed URLs are opaque and errors contain only fixed local classifications.
//
// This package does not authorize work or establish attempt identity. Its caller
// must verify account, original resource identity, privacy, version and terminal
// state before collecting or deleting. A successful save/create receipt is not
// readiness evidence. A transport error after sending a mutation is ambiguous;
// callers must preserve the attempt and reconcile by observation, never retry.
// A precise missing-name 403 on Get is not proof of completed deletion.
//
// Artifact listing remains manifest based: download the version-1 runner result
// manifest with DownloadKernelOutput, validate the frozen identity and declared
// outputs, and fetch bounded control files. The SDK's unused output-list RPC
// cannot replace the runner's digest/size catalog. Writer destinations must remain
// unpublished until their receipts and the final identity/state reads verify.
package kaggleapi
