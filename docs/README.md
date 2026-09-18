# Using Compute Relay

This directory is the user/operator documentation. If you cloned the repository because you want
to understand or run Compute Relay, stay here; architecture history, ADRs, provider research,
validation procedures and implementation planning are under [development](development/README.md).

## Start here

| I want to... | Read |
|---|---|
| Understand what the project does and its current limit | [Current status](status.md) |
| Set it up from a fresh clone | **[Getting started](getting-started.md)** |
| Operate an existing installation | **[Operator runbook](runbook.md)** |
| Manage the local runtime, workspaces and tokens | [Local runtime](local-runtime.md) |
| Package/upload code and inputs | [Bundles and import](packaging-and-import.md) |
| Configure profiles, validate/admit jobs and issue controls | [Application CLI](application-cli.md) |
| Read or download already published results | [Artifact delivery](artifact-delivery.md) |
| Recover safely after an uncertain response/state | [Recovery](recovery.md) |
| Check supported toolchains, hosts and filesystem assumptions | [Compatibility](compatibility.md) |

## The important current boundary

The normal server is **local-admission-only**. It authenticates applications, stores immutable
objects, validates/admit jobs, records explicit controls, and serves already published artifacts.
It does **not** start provider/scheduler/dispatch/collection workers, so an admitted job does not
automatically execute remotely.

The repository also contains a fixed Kaggle GPU acceptance utility that has been live-qualified
for a bounded provider workflow. That is a maintainer/operator qualification path, not the normal
application runbook. Its documentation is under
[development/provider docs](development/providers/README.md).

## For contributors and maintainers

Use [Development documentation](development/README.md) for architecture, component contracts,
ADRs, provider internals, research, requirements, validation, roadmap and remaining implementation
work. Start contribution work with [CONTRIBUTING.md](../CONTRIBUTING.md) and
[AGENTS.md](../AGENTS.md).
