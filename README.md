# Compute Relay

A self-hosted runtime for finite compute jobs, with a provider-neutral HTTP/JSON API.
Run it on your own machine with your own provider account. Kaggle is the first provider;
applications do not need the Kaggle SDK or provider credentials.

**Pre-release:** the local server supports uploads, durable job admission, controls and
verified downloads of published artifacts. It does **not** start compute or collection
workers. Kaggle execution is currently available through a separate, fixed GPU acceptance
experiment; real-provider acceptance has not yet been recorded.

## Build and start locally

Use the toolchain required by [`go.mod`](go.mod). No local GPU or Docker is required for
the control plane. Build from the repository root:

```sh
git clone https://github.com/vankhaivn/compute-relay.git
cd compute-relay
go build -trimpath -o compute-relay ./cmd/compute-relay
./compute-relay help
```

On Windows, build `compute-relay.exe` and use absolute Windows paths in the commands below.
Installation and token directories must be private and outside the source checkout. Replace
`/absolute/private` with an existing directory you control; `runtime` must not exist yet.

```sh
./compute-relay init --root /absolute/private/runtime
./compute-relay workspace create --root /absolute/private/runtime --id app
./compute-relay token issue --root /absolute/private/runtime --workspace app --scope read --scope write --scope operate --ttl 24h --output /absolute/private/runtime/app-token
```

The token is delivered to the new private file, not printed. To admit jobs, first apply an
admission profile and grant it to the workspace using [application commands](docs/application-cli.md).
Do that before starting the server; local profile/token/workspace administration requires the
installation lock. Uploads alone do not require a profile.

```sh
./compute-relay serve --root /absolute/private/runtime --listen 127.0.0.1:7331
```

`serve` runs in the foreground and reports `local-admission-only` with `dispatch_enabled=false`.
Use a second terminal for application HTTP commands: upload a bundle, validate and submit a job.
**A successful admission is not remote execution.** New installations do not generate artifacts
automatically. Stop `serve` before any further local administration.

## Choose your next step

| Goal | Guide |
|---|---|
| Set up the local server and manage access | [Local runtime](docs/local-runtime.md) |
| Upload, validate, submit and inspect jobs | [Application CLI](docs/application-cli.md) |
| List and download already published results | [Artifact delivery](docs/artifact-delivery.md) |
| Test your account with a bounded, real GPU job | [Operator validation checklist](docs/development/validation-checklist.md) |
| Hand test results or a reproducible failure to another agent | [Validation results](docs/development/validation-results.md) |
| Understand what is implemented and what remains | [Current status](docs/status.md) |
| Develop the runtime | [Contributing](CONTRIBUTING.md) and [documentation map](docs/README.md) |

## Safety and recovery

Provider credentials stay on the operator's machine. Never commit tokens, runtime databases,
private inputs or raw provider responses. Live tests require explicit authorization and
finite budgets; they can leave private staging and execution resources on the account.

Unknown submission outcomes are recovered by observing the original attempt, not submitting
again. Cancellation intent is not proof that remote compute stopped. Use the
[recovery guide](docs/recovery.md) instead of deleting state or resetting an uncertain job.

The reference workflow requires no paid connector service or mandatory paid infrastructure.
Provider allowances, eligibility and terms are the operator's responsibility; no unlimited
GPU, immediate allocation or automatic paid fallback is promised.

## License

[Apache-2.0](LICENSE). Third-party services, data, models and dependencies retain their own
terms. See [Security](SECURITY.md) for private reporting and [Support](SUPPORT.md) for help.
