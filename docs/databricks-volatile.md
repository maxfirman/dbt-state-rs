# Databricks Volatile evaluation (#198)

Captured 2026-11-19 through pinned dbt 2.0.11 and hosted dbt State, on the
Databricks Free Edition Serverless Starter Warehouse, Unity Catalog and managed
Delta tables. The SQL runtime reports `dbsql_version: 2026.45`.

## Evidence

`golden/fixtures/databricks_volatile_evaluation.jsonl` is the complete, certified
29-RPC history: eight ordinary submits, six successful execution confirmations,
four speculative submits, two validation calls, seven telemetry batches and two
native `dbt state explain --verbose` calls explaining all eight decisions.
Fixture SHA-256: `ee41de27692643810fa2957b9fe7eaad4408cfe98a50ef57ad43f49b75e62a74`.

| Cell | Submit / Explain entries (zero-based) | Observed behavior |
|---|---|---|
| True first model build | 5 / 15 | EXECUTE, codes 6 / 6; `RAND()` keyed by the Target relation. Explain lists the missing table and no volatile lines. |
| 2: unchanged model rerun | 21 / 28 | EXECUTE, codes 6 / 6, same map shape; Explain prints the rendered value, non-deterministic query not tolerated, and model query changed. |
| 6: passing singular data test, first / rerun | 10, 22 / 15, 28 | EXECUTE both times, codes 6 / 6; `RAND()` under `__target_query__`. Explain prints the value and test query changed above the upstream listing. |
| 7: table downstream, first / rerun | 9, 26 / 15, 28 | First EXECUTE, then SKIP with `is_stale=true`, within one-hour Lag tolerance. Empty map. Explain says upstream queries unchanged, upstream data outdated, and within tolerance. |
| Deterministic control, evaluation off | 4, 20 / 15, 28 | First EXECUTE, then fresh SKIP; empty maps. Supplies the test input. |

The same model SQL and upstream definition are sent on both runs. The consumer
SKIP is evidence that this upstream rebuild is data drift within Lag tolerance,
not an Upstream producer logic change. No identity, producer or reuse-refusal
rule changes are needed. Rendering remains telemetry after the decision.

Independent SQL API reads checked complete typed rows, the unique output literal,
and managed Delta properties after both builds:

| Run | State-rendered model `RAND()` | Warehouse model value | Warehouse consumer value |
|---|---|---|---|
| First | `0.7029237228139773` | `0.6458789029656377` | `0.6458789029656377` |
| Rerun | `0.32878844711181066` | `0.46502475932217` | `0.6458789029656377` |

The model's written values differ from State's rendered values on both builds:
the Warehouse executes the original SQL. The consumer retains its first value
on the SKIP. The passing test writes no Target relation; both actual
`run_results.json` outcomes report `pass`, zero failures. Its State-rendered
values were `0.3087601569109618` and `0.7997112382714784`.

The same five-invocation scenario also passed against the updated Rust server
and real Databricks in a separate fresh history. Its certified local capture
has the same 29-RPC method counts. Rust telemetry values `0.4034455556479022`
and `0.5214139626089704` differed from Warehouse model values
`0.24591389802939545` and `0.7545877737284201`; the consumer retained
`0.24591389802939545` on its SKIP. Both data tests passed. Raw local acceptance
artifacts, including server binary provenance, are retained under
`~/projects/databricks-volatile-198-rust-20261119/`, separately from the hosted
fixture. This local capture is not promoted as hosted evidence.

## Reproduce

The runner creates an isolated project and fresh UUID-named objects in an
existing evidenced schema lease. A unique output literal prevents equivalents
from earlier attempts becoming hidden hosted history. Its schema macro keeps
even non-storing tests in the existing leased container. No schema CREATE or
DROP occurred; all writes stayed in `cdo_demo_db`, and preexisting inventories
were preserved. Only its own proxy is stopped; run-owned relations are retained.

```bash
cargo build -p dbt-state-harness --bin record-proxy
python3 experiments/databricks_volatile.py \
  --dbt /path/to/dbt-2.0.11 --proxy target/debug/record-proxy \
  --schema-lease /path/to/lease.json --work-dir /path/to/fresh-capture
python3 experiments/databricks_volatile_test.py
```

To repeat against an owned Rust server, add `--rust-url http://127.0.0.1:PORT`
and use a fresh work directory; no replay clock is enabled in the live server.

The default profile is `databricks` in `~/.dbt/profiles.yml`; warehouse ID
`47fd3fa312624d91`. The binary's pinned SHA-256 is checked. Tokens stay in the
existing CLI configuration and secret environment variables. A lease names
`catalog: cdo_demo_db`, distinct `prod` and `ref` roles in existing
`ds_<run-id>_ref` schemas, their `preexisting_objects`, and `source_evidence`
paths with SHA-256 digests. Each evidence file lists `retained_schemas` and each
schema's observed `full_name`, `owner` and `schema_id`, checked again live.

Raw evidence on the capture machine is under
`~/projects/databricks-volatile-198-live-20261119-d/`: certified JSONL and sidecar,
client/runtime provenance, native logs and Explain output, run results, SQL API
responses, before/after inventories and per-run physical checks. Lease evidence
is in `~/.dbt/leases/volatile198/`. Earlier attempts are retained separately;
the original inactive-compute restriction cleared after the maintainer signed
into the web console. None of those incomplete attempts is promoted.

## Regression coverage and limits

`conformance.rs::databricks_volatile_evaluation_exact_public_history` compares
every captured RPC, full response and status, including all eight Explain
trees. The fixture also joins the ordinary corpus replay. Only opaque IDs and
validated runtime value formats are normalized; Databricks names, relative
times, timestamps, codes, descriptions, markers and badges remain exact. No
replay gap or missing-history exemption is added.

`volatile_evaluation.rs` covers the codes and maps of all eight decisions
(`databricks_cells_return_hosted_codes_and_maps`), exact Explain for all eight
before and after a server restart
(`databricks_cells_explain_as_hosted_including_after_a_restart`), and empty
maps in neighbouring uncaptured cells (`databricks_uncaptured_cells_render_nothing`
and `databricks_refused_reruns_render_only_unchanged`).
The existing shared rendering, persistence and Explain code is reused;
Databricks adds only captured rendering cells (`warehouse/databricks.rs`), all
with evaluation on and values in the node's own SQL: a first build, an
unchanged rerun refused at Reuse qualification for its volatile SQL alone
(`Outcome::RefusedVolatileSqlUnchangedRerun`,
`reuse_candidate::refuses_volatile_sql_alone`) for a FULL table and a data
test without a Target relation, and the earlier query-changed FULL rebuild
(#192). Because Reuse qualification is checked before the Target relation,
the volatile refusal also answers a rerun after the table was dropped or
modified externally, or after an upstream changed. Only the unchanged rerun
was captured (entries 21 and 22: the Target relation as built, the test's
upstream at its recorded epoch), so a rerun renders only when the refused
build is an ordinary build, a table's Target relation is as built, a data
test's cached results are complete, and every upstream has the epoch and
Upstream producer recorded at the refused build
(`reuse_candidate::Selection::unchanged_rerun`). Every other refusal keeps
an empty map and prints no volatile lines. Identity and fingerprints continue to use
original SQL. Reuse qualification remains conservative.

Still uncaptured (tracked in #213): other Databricks Volatile functions/forms, evaluation off for
volatile SQL, volatile query dependencies, stored-failure tests, a data test's
logic change, a refusal at Reuse qualification for another cause (among them
under `compare_unrendered_code`), the volatile refusal of a table after it was
dropped or modified externally or of a node over changed inputs, other node kinds and decision reasons, and
Databricks outside-tolerance consumer behavior.
These cases are not promoted to verified support. Any future identity, producer
or relaxed-refusal implication still requires the maintainer's decision under
the ticket's check-in rule.

## Validation (2026-11-19)

- All 32 volatile protocol tests pass, including exact restart trees.
- The new strict 29-RPC replay and the full 144-corpus replay pass.
- All 25 fixed/generated DuckDB acceptance cases pass, including oracle mutants.
- The 15 runner/shared offline tests and 25 Explain census checks pass.
- Workspace typechecking, formatting, and strict all-target fuzz Clippy pass.
- `cargo test --workspace --no-fail-fast` completes with 819 passed, 76 ignored
  and one pre-existing failure:
  `explain_skip_tree_matches_hosted_oracle_when_history_seeded` expects the
  "within lag tolerance" description but receives "at least one upstream
  dependency is unchanged". This assertion also failed before the implementation
  and in isolation during preparation. No exemption or unrelated fix was added;
  the entire workspace gate therefore remains red despite the passing #198 slice.

After merging main's #201 cell vocabulary (#209), the reruns are the
`Outcome::RefusedVolatileSql` cells, narrowed in review to
`Outcome::RefusedVolatileSqlUnchangedRerun`. `cargo test --workspace --no-fail-fast`
completes with 848 passed, 76 ignored and no failure (main fixed the
`explain_skip_tree_matches_hosted_oracle_when_history_seeded` clock
dependence), including the 145-fixture corpus replay. Strict all-target fuzz
Clippy, formatting, `experiments/databricks_volatile_test.py` and all 25
DuckDB acceptance cases pass.

Rust checks used `CARGO_INCREMENTAL=0`, `CARGO_PROFILE_DEV_DEBUG=0`,
`CARGO_PROFILE_TEST_DEBUG=0` and an isolated `CARGO_TARGET_DIR` to stay within
the machine's build-artifact quota. No test was filtered from the full workspace
run. Live server acceptance used the ordinary system clock.
