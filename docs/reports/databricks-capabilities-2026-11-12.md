# Databricks capability survey — 2026-11-12

This is a live capability survey for #134, under spec #132. It characterizes
the official client, hosted State and the warehouse. It makes **no Rust
Databricks support or conformance claim**. Copied table/seed rebuild failures
and snapshot `new_record` deletion records with missing business keys remain
blockers for downstream support; a permission failure or an
unexercised operation would not establish hosted rejection.

## Pinned environment

- Official dbt **2.0.11**, separately selected binary; SHA-256
  `f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31`.
- Databricks Free Edition, Serverless Starter SQL Warehouse
  `47fd3fa312624d91`, Unity Catalog, managed Delta tables and views.
  `current_version()` returned DBSQL **2026.44**, `dbr_version: null`.
- Databricks CLI **1.26.0**. SQL Statement Execution used
  `databricks api post /api/2.0/sql/statements`, with complete inline JSON
  results and ordered column type metadata; partial/truncated results fail.
- dbt_utils **1.3.1**, package lock hash
  `324340f91fa6209d83effd377f2cbe828d3bd667`. The adapter macro inventory comes
  from this binary's manifest, not a separately versioned Python adapter.
- The client advertised `append`, `delete+insert`, `merge`,
  `insert_overwrite`, `replace_where`, and `microbatch`.
- Every experiment relation used an owned schema under `cdo_demo_db`.
  SQL sessions otherwise defaulted to `workspace.default`; generated SQL
  therefore used full catalog/schema/relation names. Native
  incremental materializations also used session-local temporary views; their
  exact unqualified temporary-view SQL is retained. Persistent experiment
  writes stayed in the guarded demo schemas. View dependency definitions retained the actual `workspace`/`default` context received on
  the wire. Profiles and tokens were loaded internally; credentials and raw
  transport dumps are excluded from retained evidence.

## Reproduction and isolation

[The runner](../../experiments/databricks_survey.py) is separate from the
Snowflake runner; its Snowflake guards are unchanged. It checks the binary
digest, source profile catalog, workspace and warehouse, then creates fresh
run-owned schemas, a project copy, per-invocation artifact/log directories,
and an ephemeral local port with its own recording proxy process. It never
restores the shared demo checkout or stops a process it did not start.
Warehouse objects and successful/failed artifacts are retained for diagnosis.
A nonempty artifact directory or existing run schemas prevents a fresh run.

```bash
cargo build -p dbt-state-harness --bin record-proxy
uv run --with pyyaml python experiments/databricks_survey.py \
  --dbt /path/to/pinned/dbt-2.0.11 \
  --proxy "$PWD/target/debug/record-proxy" \
  --work-dir /path/to/new-survey-directory
```

`--plan` requires no credentials. `--sections` selects independent campaigns;
the default runs the baseline, explicit clone diagnostic, all four State copy
surfaces, six incrementals, six snapshot strategy/delete-mode combinations,
and freshness cases. `--sections seed-rebuild-diagnostic` repeats the isolated
clone-versus-managed seed rebuild experiment. `--sections snapshot-reference`
runs State-disabled `new_record` controls for both snapshot strategies. Cloud
access is opt-in; ordinary `cargo test --workspace` runs only the stdlib Python guard/comparison tests.

Each campaign retains `configuration.json`, ordered `operations.json`, exact
client commands and return codes, compiled artifacts and emitted SQL,
per-invocation wire windows, complete typed physical rows and relation
properties, proxy JSONL plus its integrity sidecar, and a certification report.
Certification requires graceful proxy shutdown and identical started,
recorded and durable RPC counts. Failed experiments remain in the report.
The companion [evidence index](databricks-capabilities-2026-11-12.json) pins
capture digests and points to the retained local artifacts. Six campaigns are
archived at
`/home/maxfirman/.local/state/dbt-state-rs/databricks-134-20261112`: 185 dbt
invocations, 1,067 certified RPCs and 8,601 retained files. The State-disabled
snapshot reference contributes eight invocations and **zero RPCs**; its empty
proxy session certifies clean shutdown, not hosted policy.

Original summaries are retained unchanged, including exploratory guard/oracle
failures. The index distinguishes their recorded statuses from the final
assessments: default-policy stale results needed zero-lag follow-ups; the first
information-schema guard refused freshness reads before mutation; a freshness
update comparison initially treated row order as meaningful; and the early
`new_record` summaries predated the current business-key oracle. No raw
evidence was rewritten to turn a failed cell green.

## Fresh jaffle-shop baseline

The fresh project/schema identity was `survey_20261112a134ef` /
`cdo_demo_db.ds_20261112a134ef_prod`; seed schemas used the `_raw` suffix.
The complete manifest's relation destinations, including generated test audit
schemas, were checked against the owned catalog/schema set.

| Invocation | Hosted observations | Client result |
| --- | --- | --- |
| Initial build | 45 `ready_to_execute`; each minted request had an accepted `ConfirmExecution` | 18 model/seed successes; 30 passes including 3 unit tests |
| Unchanged build | Same 45 node identities, each `skip_execution`; no new confirmations | 45 reused; 3 unit tests passed |

The 45 State workloads were six seeds, 12 models and 27 data tests. Unit tests
are counted separately and are not claimed to be State-cached. The baseline
validator correlates each node's initial pending request and confirmation;
an enabled-State banner or aggregate count alone cannot satisfy it.

## Incremental and snapshot capabilities

The workload uses `(id BIGINT, v STRING, p DATE, ts TIMESTAMP)`, two initial
rows in distinct partitions/days, a changed existing row where appropriate,
and new rows. Every strategy has first/unchanged/changed hosted invocations
and a separate State-disabled target with a genuine existing-target DML run.
Append filters new IDs; replacement strategies exercise a bounded partition
and retain the other partition. Microbatch declares event time on both source
and model and fixes the window to 2026-11-01 through 2026-11-03.

The first exploratory campaign left incremental/snapshot Lag tolerance at
its client default, **2700 seconds**. Changed inputs legitimately produced
`skip_execution`, `is_stale: true`, with the description “model was a no-op
because its query is up to date and its data is within lag tolerance.” Its
unchanged physical results are retained as policy evidence. Those results
cannot prove strategy DML; the separate State-disabled target and explicit
`state.lag_tolerance: 0s` campaign establish actual DML capability.

| Incremental strategy | Wire `execution_type` | Actual existing-target DML | Final capability status |
| --- | --- | --- | --- |
| append | 2 | INSERT BY NAME | observed-success |
| delete+insert | 5 | INSERT / REPLACE ON unique key | observed-success |
| merge | 3 | MERGE | observed-success |
| insert_overwrite | 4 | INSERT / REPLACE ON partition | observed-success |
| replace_where | 11 | INSERT BY NAME / REPLACE WHERE predicate | observed-success |
| microbatch | 6 | Two per-day INSERT BY NAME / REPLACE WHERE windows | observed-success |

Every zero-lag hosted changed-input result and independent existing-target DML
result matched the complete literal schema and row multiset, including the
untouched partition. The exact `semantic_extras`, predicates and event-time
window fields are retained; these observed execution types are not copied
from Snowflake assumptions.

| Snapshot strategy | `hard_deletes` | Final physical-result status |
| --- | --- | --- |
| timestamp | ignore | observed-success |
| timestamp | invalidate | observed-success |
| timestamp | new_record | unverified/blocked — deletion loses business key |
| check | ignore | observed-success |
| check | invalidate | observed-success |
| check | new_record | unverified/blocked — deletion loses business key |

Snapshots exercise timestamp (`updated_at=ts`) and check (`check_cols=['v']`),
with `hard_deletes` ignore, invalidate and new_record independently. Complete
physical SCD versions, validity columns and deletion records are retained;
a current-row count alone is insufficient. The zero-lag run checks both an
update plus new row and a subsequent source delete. Ignore and invalidate
pass the current independent SCD oracle. Both `new_record` strategies execute
successfully but produce a deletion record whose `id`, `v`, `p` and `ts` are
all NULL, while `dbt_is_deleted=true`. The deleted business key was **2**. A
record that cannot identify the deleted business entity fails the physical
contract; a fifth row alone does not satisfy it. The [official snapshot
example](https://docs.getdbt.com/docs/build/snapshots?version=2#hard-deletes-opt-in)
retains the entity's key in its deletion record.

Separate State-disabled timestamp/check reference executions reproduce the
same missing key. This isolates a pinned-client/adapter physical-result failure
from a hosted State decision mismatch. The original zero-lag summary's
`observed-success` labels predate the key-preservation oracle and are superseded
by the blocked assessments in the evidence index. The runner's current oracle
and regression test reject these records; neither the feature scope nor the
expected key was weakened.

## Actual freshness observations

For the source Delta table, the following operations all changed the pinned
client's submitted epoch and caused the zero-lag dependent table to execute.
Complete typed row multisets matched an independent `SELECT id,v` after each
operation, including the update whose physical row order changed.

| Operation | Delta version before → after | Submitted source epoch (ms) |
| --- | --- | --- |
| INSERT | 1 → 2 | 1794447410000 |
| UPDATE | 2 → 3 | 1794447428000 |
| DELETE | 3 → 4 | 1794447446000 |
| No-op `UPDATE v=v` | 4 → 5 | 1794447464000 |
| `ALTER TABLE ADD COLUMN` | 5 → 6 | 1794447481000 |
| Metadata-only `SET TBLPROPERTIES` | 6 → 7 | 1794447499000 |
| `CREATE OR REPLACE TABLE` | 7 → 8 | 1794447517000 |
| DROP/recreate | 8 → 0 | 1794447537000 |

In these Delta observations, the submitted epoch equals the
`DESCRIBE HISTORY ... LIMIT 1` timestamp in milliseconds, with second
precision. It differs from the information-schema `last_altered` millisecond
value: e.g. INSERT reports `01:36:49.704Z` there, but history and wire report
`01:36:50.000Z`. The client query logs explicitly identify “Extracting
freshness from Delta history.” The catalog table metadata exposed no
`last_data_change` column. These are observed values, not an assumption that
Delta version numbers themselves are the freshness contract. No missing,
equal or delayed source epoch occurred in these sequential, separated writes;
subsecond races and longer metadata delays remain unverified.

An external UPDATE to the dependent target caused hosted execution with
“model was executed because the table was modified after the cached
execution.” A dependent view continued to reuse its unchanged definition
while reading current source rows. Replacing a source view rebuilt its table
dependent: the wire carried the base Delta input and the changed view
`query_dependencies` definition, not an epoch for that source view. Own-view
relation epochs were millisecond-valued, so the Delta precision observation
must not be generalized to every relation kind.

## State copy observations and rebuild blockers

All four initial copy surfaces were granted by hosted State and executed
successfully; full typed rows/schema matched their sources and subsequent
unchanged invocations reused the copied results. All granted DDL was
`CREATE OR REPLACE TABLE <target> SHALLOW CLONE <source>`.

| Surface | Actual RPC | Initial outcome |
| --- | --- | --- |
| Incremental merge pre-copy | `RegisterClone` | observed-success |
| Check snapshot pre-copy | `RegisterClone` | observed-success |
| FULL table, `pre_clone: never` | `SubmitEnrichedSQL` | observed-success |
| Seed | `SubmitValues` | observed-success |

Both `RegisterClone` requests sent `clone_source_table_type: null` and
`table_properties: null`, while hosted returned both `ready_to_clone` and its
identical `ready_to_clone_v1` mirror. Thus a requirement for explicit source
Delta metadata would reject these real client requests; source Delta format
was verified independently in the warehouse. Successful copy confirmations
and later submissions are in the same capture as each source build.
The explicit warehouse clone diagnostic is separate from these State paths.

Zero-lag incremental and snapshot follow-ups successfully adopted changed
source values. The FULL table's changed-source rebuild and the seed's
changed-values load are both **warehouse-capability-failed**. Default-Lag-
tolerance stale responses are separately retained. The seed's failed SQL is:

```sql
create or replace table <owned copied seed> (`id` bigint, `v` string)
using delta;
```

The table rebuild likewise fails its emitted `CREATE OR REPLACE TABLE ...
USING delta AS SELECT ...`. Neither emitted statement contains an explicit
`CLUSTER BY AUTO`, but the warehouse rejects them with `CLUSTER_BY_AUTO_UNSUPPORTED_TABLE_TYPE_ERROR`, SQLSTATE 56038:
“CLUSTER BY AUTO is only supported on UC Managed tables.” Both source and copy
have Delta `clusterByAuto=true`. `DESCRIBE TABLE EXTENDED` calls both `MANAGED`,
whereas `cdo_demo_db.information_schema.tables` reports the source/control as
`MANAGED` and the copy as **`MANAGED_SHALLOW_CLONE`**.

The independent seed diagnostic reproduced the changed-load failure, repeated it
after a delay, and reproduced it with State disabled against the clone.
The State-disabled ordinary managed control successfully loaded the same
changed CSV and produced all expected rows. This distinguishes a copied-target
warehouse limitation from hosted rejection, permissions, credential failure,
or ordinary seed SQL failure. The analogous State-disabled FULL-table rebuild
was not separately probed; its exact hosted-guided warehouse failure remains
recorded. Failed invocations did not acquire successful execution confirmations. Exact failed SQL, metadata and before/after physical
rows are retained.

No hosted DDL was altered and no admitted copy was disabled to hide this gap.
Dropping the owned clone and loading a new managed relation is a possible
fallback to investigate, not an observed hosted contract or an accepted scope
change. Copy rebuild/fallback support remains blocked pending an evidenced
solution or an explicit scope decision. The successful initial table and seed
copies do not establish usable complete rebuild lifecycles.

## Limits and downstream handoff

This report establishes the exercised capabilities and exposes copied
table/seed rebuild failures and both snapshot `new_record` physical-result
blockers. No observed warehouse permission denial or hosted policy rejection
is claimed; warehouse SQL failures and client physical-result failures remain
separate categories. It does not establish Rust response fidelity, SQL identity,
all freshness races, policy boundaries, copy chains/limits, source removal,
full Explain/selector/deferral coverage or generated safety. Those remain the
subsequent #132 slices. The observed null copy-source type, shallow-clone
subtype, default stale reuse and actual freshness epochs must inform those
slices rather than importing Snowflake assumptions.
