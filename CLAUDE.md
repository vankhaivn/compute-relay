# Claude instructions

Follow [AGENTS.md](AGENTS.md), the canonical repository instructions. Start with
[current status](docs/status.md) and the guide for the requested task. Consult the
[approved proposal](docs/proposal.md), requirement IDs and relevant ADRs when changing design
or scope; ordinary usage and validation do not require reading completed development history.

For operator testing, read [the validation checklist](docs/development/validation-checklist.md)
and [existing results and bugs](docs/development/validation-results.md). Fill non-secret values,
inspect one check at a time and record actual outcomes before proceeding. Preserve failures,
original binaries and uncertain remote state. Do not treat a validation request as authorization
for every live effect or change code during a validation-only run.

Keep credentials private, preserve provider-neutral contracts and one-shot recovery, and follow
the Git and documentation rules in AGENTS.md. Do not add AI/model co-author trailers.
