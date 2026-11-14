# Databricks source-less cached failures — 2026-11-14

This is an independently validated partial slice of #143. **#143 remains
INCOMPLETE and blocked by #138's cross-full-UID hosted reuse conflict.** This
work does not promote a dependency or establish full Databricks support. No
production rule was widened to match that conflict. Audit materializations,
relations, aliases and schemas belong to #144 and were not exercised here.

The pinned client is dbt 2.0.11, SHA-256
`f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31`, on the
existing Databricks Free Edition serverless warehouse, Unity Catalog,
`cdo_demo_db`. The selected singular test has no sources or model dependencies,
explicit `store_failures=false`, `compare_unrendered_code=false`, severity
`ERROR`, `fail_calc=count(*)`, and `error_if` / `warn_if` both `> 0`.

## Primary results

A fresh full UID, project and SQL nonce establish the initial declaration.
Every request preserves the same authoritative full UID and connection
namespace within the four-cell history. The independent declared row multiset
is `[(1,2),(1,2),(2,NULL)]` with ordered INT columns `id,v`. A literal filter
selects either no rows or all three. The change to failure changes rendered SQL
and raw body/node hashes under false comparison mode. All configuration,
context, namespace and full UID fields stay fixed; `request-field-delta.json`
retains that exact audit. A byte-identical body with changed rendering remains
a separate comparison-mode boundary. This is not the previous #138 true-template case
that intentionally reused a passing result after a variable change.

| Actual client cell | Hosted ordinary/speculative behavior | Complete payload `(failures,error,warn)` | Raw exit / run_results status |
| --- | --- | --- | --- |
| pass first | speculative UNDECIDED, ordinary BUILD, successful Confirm | `(0,false,false)` | `0 / pass` |
| unchanged pass | speculative SKIP | `(0,false,false)` | `0 / reused` |
| changed SQL, fail first | speculative UNDECIDED, ordinary BUILD, successful Confirm | `(3,true,true)` | `1 / fail` |
| unchanged fail | speculative SKIP | `(3,true,true)` | `1 / fail` |

A logical failing test is a successful SQL computation. The initial failing
BUILD has a successful ConfirmExecution carrying the complete failing result.
The cached failure also exits 1 and reports `fail`; it does not use the passing
cache's `reused` status spelling. These numeric exits and statuses were learned
from the actual pinned runtime and are retained in
`experiments/databricks_cached_tests_contract.json`. The runner preserves raw
exit codes; it does not turn them into zero or accept arbitrary nonzero exits.
Both required reuses echo the respective confirmed runtime and all three typed
fields (integer failures, boolean error and warn). Every cell includes its
first-call complete Explain tree, full response and captured OK status.

Actual wire evidence is DBT_DATA_TEST 8, target_table null, empty tables and
query_dependencies, exact full UID, and false compare mode. Explicit false
configuration emits `store_failures: false` and
`store_failures_as: "ephemeral"` in semantic extras. The standard client still
probes DESCRIBE HISTORY of the exact absent derived audit name and its exact
INFORMATION_SCHEMA.TABLES entry. Those bounded read-only metadata probes are
retained with their raw statuses; they are not physical test inputs. Main and
aggregate test SQL read literals only. No custom macros/materialization or
hooks, schema creation, DDL/DML, cleanup, or reads of physical user/audit data
were used. The exact audit name is checked absent before dbt executes.

## Independent checks and retained evidence

Each cell has an independent complete typed row SELECT, a separate aggregate
SELECT and a State-disabled dbt test with the same literal/config declaration.
Duplicate rows and NULLs are preserved; the aggregate schema is BIGINT/BOOLEAN/
BOOLEAN and the row schema is INT/INT. Separate owned recording canaries certify
zero State RPCs for all four disabled tests and all four compile prechecks.
All invocations use a project containing exactly the one selected singular
test and project configuration. The mutation allowlist is empty.

The hosted history is retained under
`~/.local/state/dbt-state-rs/databricks-143-20261114/hosted-second/`, including
12 dbt invocation artifacts (four managed, four disabled references, four
compile prechecks), raw query logs, native manifests/run_results, complete
SQL API envelopes, operation timestamps and before/after inventories.
`golden/fixtures/databricks_cached_test_results.jsonl` is the immutable complete
24-RPC managed history, SHA-256
`2ad28d24f8d48fcaf19e782825fcb05922db22d4a67f09681cd684cd6d03c0f2`.
It contains every initial publication, validation/telemetry, speculation and
four Explain calls. No masks, replay gaps or unknown-baseline exceptions were
added. The four trees contain no INFO or unspecified marker/badge.

Warehouse inventory was exactly unchanged at 100/97 objects in the two existing
leased demo schemas. The interval was coordinated with #141 while its warehouse
SQL was idle; #158's concurrent work was metadata-only. No preexisting object,
including the original incremental experiments, was changed or removed.

The initial `hosted-first/` pilot is retained separately with six certified
RPCs and unchanged inventory. Its pass BUILD succeeded, but a runner glob
matched both `pass_first` and `reference_pass_first` query-log directories and
stopped the acceptance check. The runner now matches the complete label.
The successful history starts with a different fresh UID and nonce, so the
failed pilot is not a hidden initial publication in the accepted fixture.

## Scope and validation

Exact hosted replay uses the production gRPC router and an empty isolated
Postgres-backed State store. Crafted public controls additionally verify that
an abandoned pending failing execution remains BUILD/UNDECIDED after restart,
and that a confirmed complete failing payload and runtime survive restart and
reuse through both ordinary and speculative RPCs. Different full UID, org and
connection namespace remain misses, with a positive original-scope control.
These are Rust persistence/safety tests, not hosted restart or actual-client
ordinary-repeat reachability claims.

The dedicated runner's offline fault probes reject stale/malformed/incomplete
typed payloads, wrong counts, lost duplicates, NULL replacement, wrong schema
or column order, physical relation data reads, DDL/DML, wrong audit metadata
names, a successful exit for an executed logical failure, unexplained nonzero
exits, wrong test UID and runtime error statuses. Existing capture certification
continues to reject missing/corrupt/incomplete sidecars.

WARN severity, threshold/config changes, alternate count/NULL aggregation,
true-template failing-cache behavior, audit objects and unit-test caching are
not new evidence from this slice. No unit-test run or State caching claim is
made. Further expansion is deferred until this first vertical is complete.

The initial exact replay on integration `e8e0f8d` passes all 114 corpora in
214.48 seconds, including the new fixture's 20 non-Explain assertions and four
full Explain trees. There was **zero production red** and no server change.
The new public persistence/isolation controls pass (two tests, one opt-in
ignore). The current tracked Explain census is 779 trees / 125 files, with no
empty or unmapped tree or unspecified enum; INFO stays at 59 lines.

The real-client Rust acceptance passes in 40.25 seconds against production
integration `d4ae8705305095aa0de2d9171366a732ecb188e0`, with #141's FULL-only
Explain candidate still unintegrated. Its four managed cells and eight
zero-RPC reference/precheck canaries match the observed hosted contract. Both
positive reuses preserve complete payloads and their own confirmed runtime.
The 24-RPC capture is retained in the sibling `rust-current/` archive with SHA-256
`4449f2b8a9fa118704419a17d07527b45060f8e626b15599cf5d69f18bd1f2d1`.
`current-source-bin/` retains the exact test/proxy/probe executables, their
SHA-256s and a 65-file production source manifest. This is a pinned-source live
run, not a claim to have rerun the client on later source.

The Rust interval began after #141 drained its complete target-lifecycle pair,
with inventory 100/99, and ended with exact equality at 100/99. All owned
proxies were stopped before releasing the interval to the coordinator. No
warehouse calls or cleanup were performed by this slice after that boundary.

Final gates at synced integration `d4ae870` pass: 662 workspace tests, zero
failures and 42 opt-in ignores; all 114 hosted corpora and the supplementary
complete 24-RPC Rust trace replay; 310 library tests with an invalid DATABASE_URL;
strict workspace/all-targets/fuzz Clippy; formatting and whitespace checks.
All 17 credential-free Python groups pass. Their first all-groups invocation
retains a missing-grpcio environment failure; the owned persistent Python
configuration was corrected and that group reran successfully. No repository
dependency changed. The final runner is byte-identical to the archived live
runner, SHA-256 `539cac7a8758bd1514f0822a6bc5ae5dcb60ca7ce685753cb3303f69e815ff03`.

The complete DuckDB 25/25 gate is reused from #138's final-source receipt
`/tmp/dbt-state-132-context/identity138-duckdb-final.log` and its
[dated evidence](databricks-dependency-template-2026-11-14.md).
`git diff 1233907 d4ae870` is empty for production server/proto/protocol sources,
Cargo manifests/lock, harness source, DuckDB integration, and both DuckDB gate
scripts. This slice changes tests, runner and documentation only; it performed
no additional DuckDB cloud or local acceptance campaign.

At 16:23:44 UTC the coordinator separately completed removal of seven retired
#140 reference outputs after this slice released its accepted 100/99 interval.
Its independent receipt records 100/92 with every other object unchanged.
That subsequent housekeeping does not rewrite this slice's historical inventory
proof and was not performed by the #143 implementer. No later warehouse calls
were made by this slice.
