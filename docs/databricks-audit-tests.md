# Databricks stored-failure audits

The bounded #144 supplement covers dbt 2.0.11 on serverless Unity Catalog with
one managed Delta source and configured table/view audit aliases in a separate
audit schema. Both kinds pass, repeat, fail after a genuine source UPDATE,
repeat that failure, recover after another UPDATE and repeat recovery. The
query remains `SELECT id,v FROM <source> WHERE v < 0`, with duplicate failing
rows and a NULL non-failing source row. Rendered comparison and zero lag apply.

Hosted builds each changed-source result and reuses each unchanged result.
Ordinary requests retain genuine source epochs and report the audit output
when it exists. Speculative requests remain undecided. Complete results,
runtimes, native status/exit, Explain and independent typed failure bags are
checked. Read-only State-disabled native `dbt show` references create no audit
objects. Every cell also checks the original source bag, audit schema/type,
MANAGED/Delta properties for tables, full table history or view definition.
A view is live over its source; its physical rows are not a cached outcome.

[ADR 0011](adr/0011-databricks-direct-test-audit-output.md) bounds the new input
proof. Unsupported syntax keeps all inputs and can rebuild conservatively.
Existing source-less/default-name/alias error and recovery histories remain
unchanged; they are not rerun or generalized by this supplement.

Hosted raw evidence is under
`~/.local/state/dbt-state-rs/databricks-144-20261115/{hosted,hosted-continuation}`.
The first driver stopped after a successful failing table command because its
post-command SQL guard omitted native replacement's exact owned `DROP TABLE
IF EXISTS`. The certified 34-RPC prefix is retained. Continuation admits that
exact replacement, finishes saved command physical/status/Explain checks, then
runs only the seven missing managed commands against unchanged hosted State.
The fixture concatenates the original chronological 34 and 47 RPCs byte for
byte, without synthetic hydration, omitted statuses or response masks: 81 RPCs,
12 ordinary submits, six confirms and 12 Explain calls; SHA-256
`e3a17d60d05f5fb1b8f2df1b36d27e559b7a6166eced720d687425b907acc922`.
All twelve original Explain trees pass the final full structural/absolute-time
guards offline; exact relative-time fidelity is checked by public replay.

Per backend the bounded graph adds one UUID source in existing
`ds_20261112a134ef_prod` and two UUID audits in existing
`ds_20261112a134ef_ref_dbt_test__audit`; no schema allocation or cleanup.
Original ownership digests, all three schema IDs/owners, full paginated
inventory, collision/capacity and both native compile manifests are checked
before DDL. Source mutation budget is CREATE, INSERT and two UPDATEs; only
native replacement writes the exact audit outputs. Older graphs stay intact.

Run `python3 experiments/databricks_audit_upstream_test.py` and the ordinary
`databricks_audit_upstream` harness test. The ignored real-client test requires
a fresh exact UUID and the observed contract. Its native capture retains actual
audit clocks before State cleanup. Slow real-dbt DuckDB acceptance is deferred
for this Databricks effort; ordinary Rust/protocol regression checks remain.

The fresh Rust production-library campaign passed all 12 managed observations
and 12 independent read-only references (217.32s), with six BUILD and six
own-runtime SKIP results matching hosted. Its complete 81-RPC capture SHA-256 is
`248c0cf433b9bcba9fae1ed3daac21728c38bcf1f2c32dd240dbf08d0c12bb21`,
with all 12 actual submit clocks retained in `rust/native-clocks.json` before
its isolated State schema was dropped. All BUILD/SKIP Explain guards and
physical/view-definition/history checks ran in the native campaign. Hosted
inventory was prod61→62/audit4→6; Rust62→63/audit6→8; ref101 remained unchanged.
The hosted UUID is `d4e2a030ec5f4b67902870349cb1fbff`, Rust
`371d3a09cfc94a0fab62b2e02b11d97b`, each prefixed `a144_` with suffixes
`inputs`, `table`, `view`. All older inventories were preserved.

The complete hosted replay reproduced false input/Explain mismatches before
the new filter and passed every response and tree afterward (81 RPCs, 2.40s).
The ordinary public safety test confirms an actual fixture result and requires
source epoch/name changes, unknown epochs, duplicate/extra reports, changed
producer logic at equal epoch and foreign org/namespace/full UID to rebuild.
Changing only the audit output epoch still reuses its own complete cached
result. Parsed/transitive reads and uncertain grammar retain their inputs.
