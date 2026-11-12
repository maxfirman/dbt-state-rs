# Documentation index

Documentation for **dbt-state-rs**, an open-source reimplementation of the dbt
State ("query cache") gRPC decision service. Domain terms are defined in
[GLOSSARY.md](../GLOSSARY.md).

Reading order for a newcomer: **overview → protocol → architecture → testing**.
For hands-on work: **development → harness**. For local DuckDB runs:
**duckdb**.

## Core

| Doc | What it covers |
|---|---|
| [overview.md](overview.md) | What dbt State is, the client/server split, project goals. |
| [protocol.md](protocol.md) | gRPC services and messages, decision semantics, the complete `decision_description` and rejection-code list, Result copy lifecycle, selectors, deferral, client facts. |
| [architecture.md](architecture.md) | Server modules, registered services, submit flow, physical scope rule, store tables, lifecycle. |
| [development.md](development.md) | Toolchain, build/lint/test commands, running the server and UI, pointing the dbt client at the server. |
| [testing.md](testing.md) | Validation commands, test strategy, evidence rules and release gate, `replay_gaps.json`. |
| [harness.md](harness.md) | Recording proxy and how real traffic is captured. |
| [correctness-handoff.md](correctness-handoff.md) | Open hosted questions, known divergences, deployment gaps, reuse invariants. |
| [hosted-behavior.md](hosted-behavior.md) | Hosted behavior matrix: what the hosted service does and whether this server reproduces it. |
| [sql-normalization.md](sql-normalization.md) | How SQL text enters the Snowflake logic identity. |
| [explain-catalog.md](explain-catalog.md) | `dbt state explain` trees, lines and badges, and how a shape is selected. |
| [ui.md](ui.md) | The read-only web console: pages, running it, data it reads. |

## DuckDB

| Doc | What it covers |
|---|---|
| [duckdb.md](duckdb.md) | The local DuckDB workflow: quick start and supported features. |
| [duckdb-evidence.md](duckdb-evidence.md) | DuckDB freshness evidence and reuse eligibility. |
| [duckdb-identity-and-copy.md](duckdb-identity-and-copy.md) | DuckDB SQL identity subset and Result copies. |
| [duckdb-incremental.md](duckdb-incremental.md) | Supported DuckDB incremental models. |
| [duckdb-generated.md](duckdb-generated.md) | Generated DuckDB physical-state checks. |
| [integrations/duckdb/README.md](../integrations/duckdb/README.md) | The `dbt-state-duckdb` Python package. |

## Decision records

| ADR | Decision |
|---|---|
| [0001](adr/0001-duckdb-support-and-conformance.md) | Add DuckDB for local dbt workflows and independent reuse testing. |
| [0002](adr/0002-duckdb-local-workflow.md) | Integrate DuckDB through Python dbt and copy only eligible results. |
| [0003](adr/0003-duckdb-observed-change-evidence.md) | Use observed changes as DuckDB freshness evidence. |
| [0004](adr/0004-shared-sql-identity-with-dialect-rules.md) | Share SQL identity machinery with explicit warehouse semantics. |
| [0005](adr/0005-re-derive-explain-tree-on-demand.md) | Re-derive `dbt state explain` trees on demand; record the matched execution. |
| [0006](adr/0006-persist-the-decision-reason.md) | Persist the Decision reason; it selects the Explain tree shape. |
| [0007](adr/0007-lag-tolerance-and-upstream-producers.md) | Freshness against recorded upstream epochs and producers, gated by Lag tolerance. |
| [0008](adr/0008-store-failures-audit-relation-by-elimination.md) | Recognize an aliased `store_failures` audit relation as the one unnamed table in the test's schema. |

## Other

- [Databricks table/view acceptance](databricks-baseline.md): isolated hosted
  replay and real-client Rust runs with independent complete physical results;
  [dated evidence](reports/databricks-baseline-2026-11-12.md).
- [Databricks auxiliary RPC evidence](reports/databricks-client-auxiliary-2026-11-12.md):
  pinned-client inventory, exact public replay, real-client hosted/Rust checks.
- [golden/README.md](../golden/README.md): the golden fixture corpus.
- [agents/](agents/): agent instructions for the issue tracker, triage labels
  and domain docs.
