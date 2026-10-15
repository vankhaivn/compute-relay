# Documentation

Compute Relay runs finite jobs through a local managed HTTP runtime. Start with the walkthrough,
then use the topic that owns the operation. API schemas and examples live under [api](../api/README.md).

| Topic | Contents |
|---|---|
| [Getting started](getting-started.md) | Build, private installation, tokens and HTTP job flow. |
| [Local runtime](local-runtime.md) | Serving, authorization, state and shutdown. |
| [Managed connections](managed-connections.md) | Credentials, selections, capacity and attempt authorization. |
| [Kaggle](kaggle-runtime.md) | Native adapter, private resources, GPU and evidence limits. |
| [SSH hosts](ssh-hosts.md) | Host identity, Docker image, limits and execution. |
| [Host cache and application images](host-cache.md) | Digest-and-size inputs, read-only SSH cache, and optional tag-free application images. |
| [Remote attempt retention](retention.md) | Admission-declared cleanup after verified collection or a bounded uncollected lifetime. |
| [Host storage budget and inventory](host-storage.md) | Machine-scoped SSH budget, storage view, inventory and explicit orphan cleanup. |
| [Bundles](packaging-and-import.md) | Explicit packaging and immutable uploads. |
| [Execution gates and deadlines](execution-gates.md) | Separate preparation/start and finite UTC bounds. |
| [Workload secrets](workload-secrets.md) | Attempt-scoped memory-only supply and delivery. |
| [Artifact delivery](artifact-delivery.md) | Verified collection, publication and HTTP content. |
| [Remote cleanup](remote-cleanup.md) | Exact-target preview, authorization and outcomes. |
| [Recovery](recovery.md) | Ambiguity, backups, retention and upgrades. |
| [Compatibility](compatibility.md) | Supported host boundaries and qualification. |
| [Design](design.md) | Invariants, domain model, durable boundaries and fault cases. |
| [Development](development.md) | Toolchain, checks, CI and commit convention. |
