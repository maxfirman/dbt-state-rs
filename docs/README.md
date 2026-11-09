# Documentation index

Documentation for **dbt-state-rs**, an open-source reimplementation of the dbt
State ("query cache") gRPC decision service.

| Doc | What it covers |
|---|---|
| [correctness-handoff.md](correctness-handoff.md) | Current correctness status, live-verified behavior, remaining tasks and validation limits. |
| [overview.md](overview.md) | What dbt State is, how the client/server split works, project goals. |
| [protocol.md](protocol.md) | The gRPC protocol reference: services, messages, decision semantics, observed wire shapes. |
| [architecture.md](architecture.md) | Server internals: decision engine, Postgres store/schema, service wiring. |
| [development.md](development.md) | Toolchain, build/run, Postgres, pointing the dbt client at the server. |
| [harness.md](harness.md) | The recording proxy + how real golden traffic is captured. |
| [testing.md](testing.md) | Testing strategy: conformance replay, property tests, differential fuzzing. |
| [conformance-plan.md](conformance-plan.md) | Handoff plan for a versioned coverage ledger, independent state-machine tests and release gates. |
| [ui.md](ui.md) | The read-only web console (Topcoat): what it shows, how to run it, design. |
| [research.md](research.md) | Reverse-engineering notes and provenance from real traffic. |
| [GLOSSARY.md](../GLOSSARY.md) | Domain language, including hosted conformance, reuse safety and freshness evidence. |
| [ADR 0001](adr/0001-duckdb-support-and-conformance.md) | DuckDB workflow and acceptance-gate decisions, Snowflake compatibility policy and implementation status. |
| [ADR 0002](adr/0002-duckdb-local-workflow.md) | Python client integration decisions, local environment scope, copy capabilities and validated coverage. |
| [ADR 0003](adr/0003-duckdb-observed-change-evidence.md) | DuckDB observed-change freshness, durable evidence and reset after backup restoration. |
| [ADR 0004](adr/0004-shared-sql-identity-with-dialect-rules.md) | Shared SQL identity and copy lifecycle with explicit warehouse semantics. |
| [ADR 0005](adr/0005-re-derive-explain-tree-on-demand.md) | `dbt state explain` trees are re-derived on demand; the decision records its matched execution. |
| [ADR 0006](adr/0006-persist-the-decision-reason.md) | The decision reason is persisted with each node decision and selects the Explain tree shape. |

Reading order for a newcomer: **overview → protocol → architecture → testing**.
For hands-on work: **development → harness**.
