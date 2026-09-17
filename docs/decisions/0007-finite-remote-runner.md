# ADR-0007: One finite remote runner per attempt

Status: accepted. Requirements: JOB-02/03/05, SEC-02.

## Decision

Run the generic Python/Linux runner remotely on one resolved attempt, then exit. Verify frozen
manifest/input identity, enforce strict bundle extraction in a new work directory, bound setup,
command process groups, logs and finalization, and hash declared outputs into a result manifest.
Use argument arrays and the explicit execution kind, not implicit shell evaluation or compute retry.

Expose only the bounded `CC_*` workload environment and approved provider runtime variables.
Provider/application credentials and source URLs do not enter the manifest or workload. GPU
requirements must be checked rather than silently falling back to CPU. Missing prerequisites
remain failure. The runner does not poll for more work or call back to the control plane.

## Reason and consequences

Keeping one finite remote asset avoids an always-on worker and local admission execution.
The runner is not an arbitrary OS sandbox or hardware reservation. Provider termination, output
availability and hardware release remain separate from payload success/local signals. Explicit
`--execute` runs code on its host and is never a control-plane validation command. Only declared
outputs and controls qualify for collection; private scratch/code are not automatic artifacts.

See [runner contract and limits](../../runner/README.md), [collection](../collection.md) and
[execution](../providers/kaggle-execution.md). Preserve the source/asset lock when packaging.
