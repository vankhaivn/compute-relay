# Compute Relay

Compute Relay is a self-hosted control plane for finite remote compute jobs. Applications use a
local HTTP/JSON API to upload inputs, admit jobs, authorize attempts and read verified results.
Relay keeps durable receipts and the original provider identity through failures and restarts.
The provider adapters are Kaggle and Docker hosts reached over SSH.

The control plane is a single Go executable. Its Python runner is embedded and sent to remote
compute; Relay does not need a local Python interpreter. Docker is required on SSH execution hosts,
not on the Relay host. Accounts and infrastructure belong to the operator.

```sh
git clone https://github.com/vankhaivn/compute-relay.git
cd compute-relay
go build -trimpath -o compute-relay ./cmd/compute-relay
./compute-relay help
```

Build with Go 1.27.1, pinned in [go.mod](go.mod). Start with
[Getting started](docs/getting-started.md), then [Managed connections](docs/managed-connections.md).
Serving starts the managed runtime; each attempt still needs explicit, finite authorization.
An installation with no connections makes no provider calls.

The project is unreleased. [Compatibility](docs/compatibility.md) distinguishes implemented
behavior from live qualification. [Documentation](docs/README.md) covers operations and design;
[CONTRIBUTING.md](CONTRIBUTING.md) explains checks and changes.

Licensed under [Apache-2.0](LICENSE). Provider services, data, models and dependencies retain their
own terms and licenses.
