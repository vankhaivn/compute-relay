# Remote runner

The Python program Relay ships to remote compute. It runs one attempt on Linux with Python 3.11+,
writes bounded results and exits. Standard library only; GPU checks use PyTorch if the provider
image has it. Relay embeds these files (`assets.go`) — the Relay host never runs them.

What a job sees (`CC_*` variables, layout) is documented in [Jobs](../docs/jobs.md#what-the-workload-sees);
deadline behavior in [Prepare and start](../docs/prepare-start.md#execution-deadline).

## Files

| File | Responsibility |
|---|---|
| `python/run.py` | Entry point: `--validate-only`, `--execute`, `--supervise` |
| `python/relay_runner/contract.py` | Strict manifest decoding and limits |
| `python/relay_runner/files.py` | Input verification, safe bundle extraction, output hashing |
| `python/relay_runner/process.py` | Process-group supervision, bounded logs |
| `python/relay_runner/deadline.py` | Deadline checks against UTC and `CLOCK_BOOTTIME` |
| `python/relay_runner/main.py` | Prepare, set up, check resources, execute, finalize |
| `contract.py`, `examples/` | Adapter-to-runner contract `compute-relay/runner/v1` |
| `check.py`, `tests/` | Offline checks and CPU fixtures |

## Run locally

Validation does not execute anything:

```sh
python runner/python/run.py --manifest runner/examples/request.valid.json --validate-only
```

`--execute` **runs the manifest's commands on the current machine** — use it only on a disposable
host, never on the control plane:

```sh
python runner/python/run.py --manifest attempt.json --execute --staging-root staged --work-root attempt-new --network-mode disabled
```

Attempt layout: `code/`, `inputs/`, `outputs/`, `scratch/` (HOME, optional venv), `control/`
(manifest, provenance, logs). The result is `control/execution-result.json`
([result manifest](../api/schemas/result-manifest.v1alpha1.schema.json)).

| Exit code | Meaning |
|---|---|
| 0 | Completed result recorded |
| 1 | Unsuccessful result recorded |
| 64 | Rejected input or could not finalize |
| 124 | Absolute deadline reached (`--supervise`, SSH) |

The runner does not enforce networking (the provider must), does not install PyTorch, and never
falls back to CPU. Its process-level deadline enforcement cannot stop a suspended host, SIGKILL/OOM
or a workload that escapes its process group.

## Checks

```sh
python runner/check.py
go test ./runner
```
