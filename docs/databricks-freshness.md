# Databricks freshness evidence

Ticket #140 remains **incomplete**. Its actual-client source-operation and
same-logic recorded-rebuild cells have independent physical checks and exact
public replay. Crafted missing-evidence requests expose a hosted/Safety
conflict; these are retained diagnostic evidence, not acceptance fixtures or
pinned-client reachability claims. No conservative freshness rule is weakened.
See the [dated report](reports/databricks-freshness-2026-11-12.md).

The runners reuse the #135 guarded campaign, pinned dbt 2.0.11, owned proxy
process, complete SQL result reader and schema ownership ledger. Their exact
allowlist contains only each new run's `f_<run>_inputs`, `consumer`, `producer`,
`recorded_consumer` relations in the two leased
run-owned demo schemas. Existing relations and schemas are preserved. Every
source mutation is inside `cdo_demo_db`; no schema CREATE/DROP is attempted.

`experiments/databricks_freshness.py` holds SQL/configuration fixed, uses zero
Lag tolerance and ANY, and runs first build plus INSERT, UPDATE, DELETE, no-op
UPDATE, ADD COLUMN, property and comment changes. Every operation requires its
hosted-characterized build and an unchanged follow-up SKIP. An explicit
unchanged control also requires SKIP. Both rebuilding and reuse steps compare
all rows, duplicate multiplicity, NULLs, ordered BIGINT/INT/STRING schema and
MANAGED/Delta properties against literal expected rows, an independent SELECT,
and a separate State-disabled dbt reference project/profile. Metadata samples
retain catalog metadata and Delta history before/immediately after each
operation, after building and after unchanged reuse. Every SQL statement in
the current runner records start/completion wall clocks.

`experiments/databricks_recorded_freshness.py` builds a producer/consumer graph,
requires unchanged reuse, changes source data, then runs the producer with
`--state-mode write-only`. The actual RecordExecutions is retained; subsequent
consumer rebuild and unchanged producer/consumer reuse are mandatory. A table
`--full-refresh` attempt used ordinary submit/confirm instead, and is retained
as a rejected recording hypothesis.

`experiments/databricks_freshness_boundaries.py` is explicitly crafted public
gRPC: equal/older/+1ms, missing current/recorded/both epochs, omitted/duplicate
inputs and delayed-old then new evidence. Each cell owns a separate namespace
and unique SQL. It performs no warehouse writes. Its diagnostic captures are
not promoted when exact replay conflicts with conservative missing-evidence
behavior. `experiments/databricks_freshness_diagnostic.py` combines an actual
client build with a deliberately omitted-source request, independently proves
the resulting stale physical output, then repairs it using the actual client.
It also retains close-spaced warehouse-write attempts and the actual client's
missing-relation failure, without claiming either demonstrates client-emitted
missing or equal-change epochs.

Run a hosted campaign with the same flags as the baseline:

```bash
uv run --with pyyaml python experiments/databricks_freshness.py \
  --dbt /path/to/pinned/dbt-2.0.11 --proxy /path/to/record-proxy \
  --probe /path/to/state-probe --schema-lease /path/to/lease.json \
  --work-dir /path/to/new-hosted-evidence
```

For Rust acceptance, set `DBT_STATE_DATABRICKS_PYTHON`,
`DBT_STATE_DATABRICKS_DBT`, `DBT_STATE_DATABRICKS_SCHEMA_LEASE`,
`DBT_STATE_DATABRICKS_QUERY_IDENTITY` and a fresh
`DBT_STATE_DATABRICKS_ARTIFACTS` path, then run either ignored test in
`crates/harness/tests/databricks_freshness.rs`. Each test owns its isolated
Postgres schema and ephemeral production-router listener. Normal CI runs the
complete hosted fixtures, fixed fixture inventories and credential-free
physical/lease fault probes without cloud credentials.

Anchored Explain timestamps are rendered at the persisted decision instant,
recovered from the matched build clock plus recorded build age; a later Explain
does not age the text. Gap durations are humanized. For new Databricks Submit
captures only, replay recovers the characterized hosted instant from a UUIDv7
decision ID and validates version, RFC4122 variant and captured request/response
bounds. Unsupported IDs or missing/invalid timing evidence fail. Production
continues treating IDs as opaque. Full Explain text, absolute timestamps,
relative ages, structure, descriptions and statuses are compared exactly;
there is no temporal mask or comparator. Other warehouses' replay clocks are
unchanged. Unanchored copy timestamps remain separate unfinished coverage.

The [2026-11-14 bounded follow-up](reports/databricks-freshness-followup-2026-11-14.md)
adds submit-frozen missing-current/both-missing Explain defaults and identical
known current upstream normalization for rendered FULL submits without query
dependencies. Unknown/conflicting duplicates and omitted-source evidence retain
the conservative BUILD floor. `experiments/databricks_freshness_followup.py`
checks an actual repeated-source query and explicitly crafted duplicates over
real warehouse data. Its opt-in test is
`databricks_freshness_followup::real_dbt_repeated_source_and_crafted_duplicate_match_independent_results`;
set `DBT_STATE_DATABRICKS_RUN_ID` to a fresh YYYYMMDD plus six hex digits as well
as the existing live-test environment variables. #140 remains incomplete.
