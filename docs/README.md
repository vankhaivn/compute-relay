# Using Compute Relay

This directory is the user/operator documentation. If you cloned the repository because you want
to understand or run Compute Relay, stay here; architecture history, ADRs, provider research,
validation procedures and implementation planning are under [development](development/README.md).

## Start here

| I want to... | Read |
|---|---|
| Understand what the project does and its current limit | [Current status](status.md) |
| Set it up from a fresh clone | **[Getting started](getting-started.md)** |
| Run bounded GPU jobs through Kaggle | **[Kaggle runtime](kaggle-runtime.md)** |
| Operate an existing installation | **[Operator runbook](runbook.md)** |
| Manage the local runtime, workspaces and tokens | [Local runtime](local-runtime.md) |
| Package/upload code and inputs | [Bundles and import](packaging-and-import.md) |
| Configure profiles, validate/admit jobs and issue controls | [Application CLI](application-cli.md) |
| Read or download published results | [Artifact delivery](artifact-delivery.md) |
| Recover safely after an uncertain response/state | [Recovery](recovery.md) |
| Check supported toolchains, hosts and filesystem assumptions | [Compatibility](compatibility.md) |

## Two serve modes

With no provider flags, `compute-relay serve` is **local-admission-only**: it accepts immutable
objects/jobs and serves already published artifacts without provider effects.

With the complete explicit Kaggle configuration, GPU authorization and finite per-process attempt
budget described in [Kaggle runtime](kaggle-runtime.md), the same server starts bounded dispatch
and collection workers. It stages and submits each authorized attempt once, reconciles the original
identity after uncertainty/restart and publishes verified results.

The integrated path is pre-release. The underlying fixed Kaggle acceptance workflow has live
qualification evidence; a new account/environment/workload still requires scoped re-qualification
before its support is claimed.

## For contributors and maintainers

Use [Development documentation](development/README.md) for architecture, component contracts,
ADRs, provider internals, research, requirements, validation, roadmap and remaining implementation
work. Start contribution work with [CONTRIBUTING.md](../CONTRIBUTING.md) and
[AGENTS.md](../AGENTS.md).
