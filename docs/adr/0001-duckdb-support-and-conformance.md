# Add DuckDB for local dbt workflows and independent reuse testing

## Implementation status — 2026-11-07

The local workflow and its acceptance gate are implemented. The
[workflow guide](../duckdb.md) covers the supported real-dbt workloads;
[generated checks](../duckdb-generated.md) describe the independent result
checks and replayable histories. [CI](../../.github/workflows/ci.yml) runs
Snowflake capture replay, the fixed DuckDB matrix and bounded generation,
with larger generated runs scheduled separately. The original five-minute
runtime target remains a goal, rather than a measured CI guarantee.

The sequencing below records the implementation plan that led to this work.

## Original decision and implementation plan

DuckDB support must enable a real local dbt project to use dbt-state-rs for
BUILD, SKIP and copying results between environments. A harness-owned protocol
driver is a useful intermediate milestone but does not complete this feature.
DuckDB is intended for local exploration and testing; production warehouse
support is not the objective of this work.

Share decision machinery across warehouses while retaining warehouse-specific
rules where necessary. Existing captured Snowflake behavior remains the default
contract. Evaluate each intentional divergence individually against its
simplification benefit and document it explicitly; correct results and
organization/warehouse isolation remain requirements. This extends the hosted
conformance objective in `docs/overview.md` without treating DuckDB behavior as
evidence of official dbt State behavior.

DuckDB integration tests must compare actual State-guided results with an
independent reference execution, accounting explicitly for policy-permitted
staleness. They must also assert reuse in eligible scenarios so that always
rebuilding cannot pass. Keep the captured Snowflake tests as the evidence for
hosted conformance. The initial client, environment scope, supported workloads
and copy contract are recorded in [ADR 0002](0002-duckdb-local-workflow.md).
[ADR 0003](0003-duckdb-observed-change-evidence.md) defines the freshness evidence
contract and [ADR 0004](0004-shared-sql-identity-with-dialect-rules.md) defines the
shared SQL identity direction. Detailed mechanics require validation during
implementation.

Every pull request must run fixed DuckDB end-to-end scenarios and a bounded,
reproducible set of generated operation sequences alongside Snowflake capture
replay. Use isolated DuckDB files and Postgres schemas with no cloud
credentials. Failures retain their seed and a replayable trace. Larger
generated runs belong in scheduled or manually invoked jobs. Start with an
additional CI runtime target of approximately five minutes and measure it;
this is a target, not an established performance claim.

Validate a thin real-client compatibility slice before broad server
refactoring. Establish durable evidence and recovery semantics next, then
consolidate shared Rust logic, complete CTAS and the supported dbt workloads,
and make the independent correctness suite a CI gate. This order addresses
client feasibility and trustworthy evidence before relying on the expanded
reuse implementation; tests accompany each milestone.
