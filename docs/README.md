# Using Compute Relay

User and operator documentation. Architecture, ADRs, provider internals, validation and planning
are under [development](development/README.md).

| I want to... | Read |
|---|---|
| Know what exists now and its limits | [Current status](status.md) |
| Set it up from a fresh clone | **[Getting started](getting-started.md)** |
| Install the native companion without a checkout | [Companion distribution](companion-distribution.md) |
| Run bounded GPU jobs through Kaggle | **[Kaggle runtime](kaggle-runtime.md)** |
| Manage connections from a local application | **[Managed connections](managed-connections.md)** |
| Run CPU jobs in Docker on my own machine over SSH | **[SSH hosts](ssh-hosts.md)** |
| Stage an SSH attempt before explicitly starting it | [Preparation gates](preparation-gates.md) |
| Supply temporary secret values to a gated SSH workload | [Workload secrets](workload-secrets.md) |
| Bound time-sensitive execution across queueing and disconnects | [Execution deadlines](execution-deadlines.md) |
| Run a finite H.264/AAC stream-copy delivery | [RTMPS example](../examples/finite-rtmps/README.md) |
| Administer the runtime, workspaces and tokens | [Local runtime](local-runtime.md) |
| Package code and upload inputs | [Bundles and import](packaging-and-import.md) |
| Configure profiles, validate/admit jobs and issue controls | [Application CLI](application-cli.md) |
| Read or download published results | [Artifact delivery](artifact-delivery.md) |
| Recover safely after an uncertain response or state | [Recovery](recovery.md) |
| Inspect and explicitly delete an attempt's remote data | [Remote cleanup](remote-cleanup.md) |
| Check toolchains, hosts and filesystem assumptions | [Compatibility](compatibility.md) |
