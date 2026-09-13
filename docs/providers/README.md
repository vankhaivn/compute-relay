# Provider documentation

Provider documents describe adapter-specific configuration, capabilities, evidence, limits, and operational recovery while keeping the public application contract provider-neutral.

The implemented offline [provider contract and deterministic fake](contract.md) document the
M2-04 interfaces, registry, reusable tests and local smoke command. This is not live Kaggle
support or a production local execution provider.

A provider document should include:

- supported and tested client versions;
- authentication modes and credential-reference handling;
- capability matrix with `supported`, `unsupported`, or `unknown` plus evidence level;
- input/staging privacy and readiness behavior;
- submission and execution identity semantics;
- raw-to-domain state and error mapping;
- timeout, cancellation, logs, quota, and artifact behavior;
- recovery from ambiguous submission and local restart;
- cleanup ownership and manual operator procedures;
- known provider/account/version limitations; and
- date of last live verification.

Applications must not require provider slugs, provider filesystem paths, notebook metadata, provider credentials, or adapter-specific SDKs for their normal lifecycle.

The initial Kaggle document should be written from [`../research/kaggle-feasibility.md`](../research/kaggle-feasibility.md) after the relevant evidence exists. Do not copy optimistic assumptions into a support matrix.
