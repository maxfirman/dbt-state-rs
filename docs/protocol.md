# Protocol reference

The dbt State protocol is the gRPC **"query cache"** protocol, proto package
`com.fivetran.query_cache` (plus `grpc.health.v1`). The authoritative `.proto`
files are Apache-2.0, published by dbt Labs, and vendored under
[`proto/query_cache_protobuf/query_cache/`](../proto/query_cache_protobuf/query_cache).

This page describes the wire contract and the results the server returns.
Engine internals are in [architecture.md](architecture.md); open questions and
pinned divergences are in [correctness-handoff.md](correctness-handoff.md).
Per-scenario hosted observations are in [hosted-behavior.md](hosted-behavior.md).

## Transport and auth

- Default endpoint `api.state.dbt.com:443` (TLS). The client uses TLS when the
  URL is https or ends in `:443`. Otherwise it opens an insecure channel and
  sends **no** OAuth token, which is how the client is pointed at a local server.
- Client configuration: `RUN_CACHE_*` environment variables (for example
  `RUN_CACHE_API_URL`, `RUN_CACHE_API_SECURE`) and
  `DBT_ENGINE_MANAGE_STATE=true`.
- Hosted auth: an OAuth token exchange of the dbt Cloud credential at
  `https://auth.state.dbt.com/token` yields an `id_token`, sent as
  `authorization: Bearer <id_token>` with `x-organization-id: <org_id>` (parsed
  from the token scope `runcache:scope:org:<ORG_ID>:...`).
- Client request metadata: `x-request-id`, `x-session-id`,
  `x-submitted-at-epoch`, `x-system-user-id`, `x-os-name`, and in newer clients
  `x-dbt-invocation-id`.
- The server reads three of them:
  - `x-organization-id` scopes every store query. When absent the tenant is
    `local`. `DBT_STATE_REQUIRE_ORG_HEADER=true` rejects requests without a
    non-empty value (`UNAUTHENTICATED`). This checks presence only; it does not
    authenticate the organization.
  - `x-dbt-invocation-id` and `x-session-id` group captured decisions for the UI.
- `x-dbt-state-replay-now-ms` is a test-only header. The server honors it only
  under `Clock::Replay` (`crates/server/src/lib.rs`), where it sets the clock
  that build age is measured against. Never enable that clock in a deployment.

## Services

| Service | RPCs | Server status |
|---|---|---|
| `SQL` | `SubmitEnrichedSQL`, `SubmitValues`, `SubmitEnrichedSQLSpeculative` | Submits decided; speculative SKIPs only an unchanged data test with no upstreams and is otherwise `Undecided` ([below](#speculative-submits-sqlsubmitenrichedsqlspeculative), #103) |
| `Execution` | `ConfirmExecution`, `RecordExecutions`, `ResolveDeferredRelations` | Implemented |
| `Clone` | `RegisterClone` | Implemented; non-Snowflake DDL unverified |
| `ClientValidation` | `ValidateClientVersion` | Returns `is_supported=true` for every version |
| `Explain` | `GetExplainMessages`, `GetUpstreamDependencyChanges` | `GetExplainMessages` re-derived on demand ([explain-catalog.md](explain-catalog.md)); `GetUpstreamDependencyChanges` is an empty stub (see below) |
| `SelectorService` | `GetStateSelection` | Implemented |
| `ClientTelemetry` | `SubmitTelemetryBatch` | Acknowledges batches with `success=true`; no hosted metering persistence |
| `Health` (`grpc.health.v1`) | `Check`, `Watch` | Always `SERVING` |

The client treats `ValidateClientVersion` as fail-open: an RPC error is logged
as a warning and dbt runs without dbt State
(`validate_client_version_fail_open` in the client's `dbt-run-cache` crate).

Pinned dbt 2.0.11 calls `ValidateClientVersion` and `SubmitTelemetryBatch` in
the Databricks survey. Telemetry carries `session_start`,
`enriched_sql_prepared` and `session_end` events, including unsuccessful
invocations. Hosted returns OK with `is_supported=true` and `success=true`
respectively; auxiliary replay compares the complete responses, statuses and
application metadata. Session-end duration preserves seconds/nanoseconds and
optional presence in new captures. Older captures omitted that field; it is
not reconstructed. The deprecated standalone `RegisterSessionStart` was not
called and remains unimplemented. OAuth, accounts and billing remain outside
the self-hosted service. See the dated [auxiliary RPC evidence](reports/databricks-client-auxiliary-2026-11-12.md).

Wire service names must be preserved exactly. The service named `Clone`
collides with Rust's `std::clone::Clone`; the proto build patches the generated
impl and keeps the on-wire name (see [architecture.md](architecture.md#proto-crate)).

## The decision RPC: `SQL.SubmitEnrichedSQL`

### Request `SubmitEnrichedSQLRequest`

`target_table?`, `dialect`, `default_catalog`, `default_schema?`,
`execution_type`, `sql` (raw compiled SQL), `tables[]` =
`{name, last_modified_epoch?}` (per-relation metadata, including the node's own
target relation when it exists), `query_dependencies[]` =
`{name, query, default_catalog, default_schema}`, `semantic_extras{}`,
`freshness_tolerance_seconds`, `lenient_dependencies[]`,
`tolerate_nondeterminism`, `labels{}` (`dbt_node_name`, `dbt_node_fqn`,
`dbt_node_unique_id`), `clone_time_travel_limit?`, `clone_table_properties?`,
`stale_upstream_policy` (`ANY=0`, `ALL=1`), `clone_chain_depth_limit?`,
`dbt_node_state` = `{node_unique_id, node_hash, node_body_hash?,
node_configs_hash?, node_contract_hash?, project_id?, …}`,
`compare_unrendered_code`, `table_namespace?`, `ignore_external_modifications`,
`allow_clones?`, `is_defer_to_profile`, `defer_enabled`.

Only a data test (`DBT_DATA_TEST`) may leave `target_table` unset or `""`;
client 2.0.11 always sets it for other nodes. The server rejects any other
submit without a target with `INVALID_ARGUMENT` before deciding or recording
anything (#81). Hosted answers `INVALID_ARGUMENT` "Failed to process SQL" for
`""` and `INTERNAL` for unset (crafted d3 / d3e); the unset case is an
[accepted divergence](correctness-handoff.md#accepted-divergences).

Hosted also answers `INVALID_ARGUMENT` "Failed to process SQL" for empty SQL,
for every node kind, and for a query dependency with an empty name or query
(crafted `experiments/malformed_requests.py`, #104). This server accepts these
requests and builds them, then fails reuse qualification on every resubmit
(`no_reuse_candidate.failed_reuse_qualification`). It also rebuilds a request
whose query dependency is listed twice, which hosted SKIPs
(`explain_malformed_requests.jsonl`). Both are crafted-only
[accepted divergences](correctness-handoff.md#accepted-divergences) (#114).

DuckDB note: for the optional local DuckDB extension, `last_modified_epoch` is
a durable file/generation-wide ordinal, not Unix milliseconds, and freshness
compares ordering only. See the
[evidence contract](duckdb-evidence.md#ordering-and-build-age).
Other warehouses use timestamps.

### What the client computes

The client sends raw compiled SQL. The server owns the fingerprint, the State
history match and the decision. The client derives:

- `execution_type` (`execution_type_from_input` in the client's
  `request_builder.rs`), first match wins: data test → `DBT_DATA_TEST`; view →
  `VIEW`; custom materialization → `DBT_CUSTOM`; snapshot → `SNAPSHOT`;
  incremental model without `--full-refresh` → its strategy upper-cased (`+`
  becomes `_`, default `append`; `merge` without `unique_key` → `APPEND`);
  otherwise `FULL`. A snapshot run under `--full-refresh` is sent as `FULL`
  (captured with client 2.0.11, `explain_snapshot_history.jsonl` entry 58).
  - **Unset strategy** (client 2.0.11 on Snowflake,
    `explain_incremental_append_merge_history.jsonl` `u73`): an incremental
    with a `unique_key` and no `incremental_strategy` is sent as `APPEND` (2),
    the request builder's default, although the Snowflake adapter's default
    strategy is merge. Its Explain trees are the APPEND ones.
  - **`--full-refresh` incremental** (client 2.0.11, same fixture entries
    69–70): the client sends no Submit. After the build it sends a write-only
    `Execution.RecordExecutions` with the request built for a full refresh,
    so `execution_type` is `FULL` (1), and the rebuilt Target relation's
    epoch. Hosted answers `records_stored: 1`. The next ordinary run submits
    the model as APPEND/MERGE again; hosted and this server rebuild it with
    "the table was modified after the cached execution", judged against the
    last APPEND/MERGE build (rule 1 of
    [Current decision semantics](#current-decision-semantics)).
- `semantic_extras`, from these node config keys only (client 2.0.11 also
  sends `unique_key` and `lookback` for incrementals,
  `explain_incremental_append_merge_history.jsonl`). SQL nodes:
  `on_schema_change`, `incremental_predicates`, `merge_update_columns`,
  `merge_exclude_columns`, `severity`, `limit`, `where`, `fail_calc`,
  `warn_if`, `error_if`, `store_failures`, `store_failures_as`,
  `auto_liquid_cluster`, `databricks_tags`. Seeds: `column_types`,
  `quote_columns`, `delimiter`.
- `tables` for a data test: the relations it reads, with no target. With
  `store_failures: true` it also holds the test's audit relation (in the
  test's database and schema, named after the test or its configured alias)
  once a run has written it, whether a table or a `store_failures_as: view`
  view (client 2.0.11, `explain_data_test_history.jsonl`,
  `explain_store_failures_audit.jsonl`). `default_schema` is the test's own
  schema, also when that is a separate audit schema
  (`explain_store_failures_audit_schema.jsonl`); no request field carries
  the alias (`explain_store_failures_audit_alias.jsonl`). The server drops
  the audit relation from the test's inputs when the request names it
  (#88, #106), and under a configured alias when it is the only reported
  table in the test's schema that the test's SQL and query dependencies
  never mention (#123, [ADR 0008](adr/0008-store-failures-audit-relation-by-elimination.md)).
- `values_hash` for seeds: the md5 of the seed file bytes.
- For a microbatch model, the run's resolved event-time window, added to the
  `semantic_extras` as `__microbatch_event_time_start` and
  `__microbatch_event_time_end` (raw ISO-8601 strings, e.g.
  `2026-11-01T00:00:00+00:00`, not JSON-encoded). The client submits once per
  model per run, not per batch. A fixed `--event-time-start/--event-time-end`
  window sends the same values every run, so an unchanged rerun SKIPs. The
  default moving window runs from `begin` on the first build and from
  `lookback` + 1 batches before the ceiling of now on an incremental run, so
  the first incremental run rebuilds and later runs in the same batch period
  SKIP (captured with client 2.0.11, `explain_incremental_microbatch.jsonl`;
  [explain-catalog.md](explain-catalog.md#microbatch-6)). The microbatch
  submits also carry `event_time`, `lookback` and `on_schema_change`.
- `table_namespace`: from the warehouse connection identifier
  (account/project/host).
- Profile `allow_clones: false`: client 2.0.6 sent it as
  `clone_chain_depth_limit = 0` alone, on both `SubmitEnrichedSQL` and
  `RegisterClone` (`clone_rejected_allow_clones_false.jsonl`). Client 2.0.11
  sends `allow_clones: false` with `clone_chain_depth_limit: 0` on
  `SubmitEnrichedSQL` (`explain_copy_logic_mismatch.jsonl` `cf4`) and
  `SubmitValues` (`explain_seed_copy_disabled.jsonl`), and `allow_clones:
  true` without a limit otherwise.
- Node config `lag_tolerance` becomes `freshness_tolerance_seconds`,
  `require_fresh_data_from` becomes `stale_upstream_policy`, and
  `evaluate_volatile_sql: true` becomes `tolerate_nondeterminism = false`. These
  are set under the nested `state={}` config, which the client reads from
  models only (incrementals and custom materializations included): a snapshot
  always gets the default tolerance (env `RUN_CACHE_FRESHNESS_TOLERANCE`,
  default 45 minutes) and the `ANY` policy (`model_state_for_node`).
- A table whose `last_modified_epoch` the client cannot read is left out of
  `tables`, and the client then skips the State submit (`metadata_complete =
  false`). So the client never sends an upstream with an unknown epoch.

### `ModelExecutionType`

`UNSPECIFIED=0, FULL=1, APPEND=2, MERGE=3, INSERT_OVERWRITE=4, DELETE_INSERT=5,
MICROBATCH=6, SNAPSHOT=7, DBT_DATA_TEST=8, VALUES=9, VIEW=10, DBT_CUSTOM=11`.

### Response `SubmitSQLResponse` (oneof)

- `skip_execution`: `explained_decision`, `execution_results` (the complete
  cached Struct; an empty Struct when none was recorded), `execution_runtime_ms`
  (echo of the reused run), `execution_decision_id`.
- `ready_to_execute`: `request_id`, `explained_decision`,
  `execution_decision_id`. The server always returns
  `transformed_nodes_by_query` empty and `query_hash_metadata_info` unset.
- `ready_to_clone`: a Result copy of an equivalent; see
  [Result copy on submit](#result-copy-on-submit).

`ExplainedDecision` = `{decision, skip_rejection_reason?,
clone_rejection_reason?, is_stale, decision_description}`. Enums are ints on the
wire and in captured JSON.

- `SubmitSQLResultType`: `SKIP_EXECUTION=0, READY_TO_EXECUTE=1,
  READY_TO_CLONE=3, UNKNOWN=4`.
- `RejectionReason`: `TARGET_TABLE_MISMATCH=1`, `EXECUTION_TYPE_MISMATCH=2`,
  `EXECUTION_TYPE_NOT_FULL=3`, `TARGET_TABLE_DOES_NOT_EXIST=4`,
  `FORCED_NOT_ELIGIBLE=5`, `NO_SUITABLE_MATCH_FOUND=6`, `TARGET_TABLE_MATCH=7`,
  `LATEST_QUERY_HASH_NOT_MATCH=8`, `OUTSIDE_TIME_TRAVEL_WINDOW=9`,
  `CLONE_CHAIN_LIMIT_EXCEEDED=10`. The server never sends 9. It sends 2 / 2,
  as hosted does, for a rebuild after an execution type change (#83, #90; see
  [Rejection codes](#rejection-codes)). The server sends
  1 / 3, 1 / 6 or 1 / 2 when it builds a submit that it will not copy because
  its execution type is not FULL although an equivalent exists under another
  name, as hosted 2.0.11 does (#86, #92, #101; see
  [Result copy on submit](#result-copy-on-submit)).

### Decision descriptions

This is the complete set of `decision_description` strings the server sends on
submits (`skip_description`, `execute_description`, `RESULT_COPY_DESCRIPTION`
and `COPY_CHAIN_LIMIT_DESCRIPTION` in `crates/server/src/decision.rs`).
`{noun}` is `snapshot` for `SNAPSHOT`, `data test` for `DBT_DATA_TEST`, `seed`
for `VALUES` and `model` for every other execution type (`node_noun`).
Hosted also says `model` for `DBT_CUSTOM` (`explain_custom_history.jsonl`).

| Decision | `decision_description` | When |
|---|---|---|
| SKIP | `{noun} was a no-op because both its query and its upstream data are up to date` | Lag tolerance is 0, or every upstream epoch equals its recorded epoch; every snapshot SKIP |
| SKIP | `{noun} was a no-op because its query is up to date and its data is within lag tolerance` | Upstream data moved and the build age is under the Lag tolerance (not snapshots) |
| SKIP | `{noun} was a no-op because its query is up to date and at least one upstream dependency is unchanged` | Upstream data moved, the build age is not under the tolerance, and the `ALL` policy tolerates it (not snapshots) |
| SKIP | `seed was a no-op because its data has not changed` | Seed reuse |
| EXECUTE | `{noun} was executed because its table did not exist` | No prior build of the target relation, or the target relation is absent |
| EXECUTE | `{noun} was executed because either its query didn't match or its upstream data is out of date` | Rebuild of an existing target relation (identity or freshness) |
| EXECUTE | `{noun} was executed because the table was modified after the cached execution` | The target relation's epoch is later than its build (External modification) |
| EXECUTE | `model was executed because the view definition is newer than the cached execution` | The same, for a VIEW (`explain_reason_kind_cells.jsonl` `vc0`) |
| EXECUTE | `data test was executed because it has no prior execution or its query changed` | Every data-test execute |
| EXECUTE | `seed was loaded because it did not exist` | Seed first load, or its table is absent; also when its copy is disallowed (`explain_seed_copy_disabled.jsonl` entry 6) |
| EXECUTE | `seed was loaded because its data changed` | Seed reload; also over its table holding other values when its copy is disallowed (entry 12) |
| EXECUTE | `model was executed because cloning is disabled` | An equivalent exists but the submit disallows copies (`allow_clones: false`; client 2.0.11 also sends `clone_chain_depth_limit: 0`) |
| EXECUTE | `model was executed because the maximum number of nested clones has been reached` | An equivalent exists but the submit forbids copies with `clone_chain_depth_limit = 0` alone (client 2.0.6), or the copy chain is at its limit (DuckDB) |
| CLONE | `an equivalent model exists under another name so we cloned that one` | Result copy on submit |
| CLONE | `an equivalent seed exists under another name so we cloned that one` | Seed copy on submit (Snowflake, [below](#seed-copy-on-submit)) |

`RegisterClone` responses carry an empty `decision_description`.

Hosted words a snapshot SKIP as "both … up to date" even when an upstream
epoch moved back or drifted within the Lag tolerance, where a model gets the
"within lag tolerance" string (`explain_snapshot_history.jsonl` entries 28-31,
40). A snapshot reuse past its tolerance under the `ALL` policy is
unreachable on 2.0.11 (the client sends a snapshot the `ANY` policy and the
default tolerance, [What the client computes](#what-the-client-computes))
and keeps the same string. An `ALL`-policy reuse with one of two upstreams
drifted and a build younger than the tolerance gets the "within lag
tolerance" string (`explain_freshness_cells.jsonl` `fa_*`).

### Rejection codes

`skip_rejection_reason` / `clone_rejection_reason` pairs, with `-` for unset.

| Decision | Codes | When | Source |
|---|---|---|---|
| SKIP | - / - | Every reuse | `DecisionRecord::decided` |
| EXECUTE | 6 / 6 (`NO_SUITABLE_MATCH_FOUND`) | Default for every execute | `decision.rs::execute` |
| EXECUTE | 8 / 7 (`LATEST_QUERY_HASH_NOT_MATCH` / `TARGET_TABLE_MATCH`) | A rebuild of an existing target relation whose older build (overwritten since) has the workload's identity and would still be reusable for this submit, also when an equivalent exists under another name (it goes ahead of 1 / 3, 1 / 6 and 1 / 2). Not for first builds, absent target relations or External modifications. | `with_older_match`, `reuse_candidate::{older_build_matches, older_seed_matches}` |
| EXECUTE | 2 / 2 (`EXECUTION_TYPE_MISMATCH`) | The latest build has the workload's identity under another execution type and recorded the workload's genuine upstream names, and the target relation has no build of the workload's type (rule 1 below). Other upstream names are another identity: 6 / 6, or 8 / 7 (`explain_reason_kind_cells.jsonl` `vc`, `cv`). A RegisterClone copy has it when the submit has the logic the copy inherited (`explain_copy_logic_mismatch.jsonl` `kf4`) | `decision.rs::execute`, `NoCandidate::ExecutionTypeChanged` |
| EXECUTE | 4 / 7 (`TARGET_TABLE_DOES_NOT_EXIST` / `TARGET_TABLE_MATCH`) | The target relation is absent, its latest build has the workload's identity and would otherwise be reusable. A stale latest build keeps 6 / 6. | `with_absent_target_match` |
| EXECUTE | 1 / 10 (`TARGET_TABLE_MISMATCH` / `CLONE_CHAIN_LIMIT_EXCEEDED`) | An equivalent exists for an absent or mismatched target relation, but the submit forbids copies; for a Snowflake seed, a load to copy exists but the submit has `allow_clones: false` (`explain_seed_copy_disabled.jsonl`) | `DecisionRecord::copy_disabled` (`allow_clones: false`), `DecisionRecord::copy_chain_limit` (otherwise), `DecisionRecord::with_seed_copy_refused` (seed) |
| EXECUTE | 1 / 3 (`TARGET_TABLE_MISMATCH` / `EXECUTION_TYPE_NOT_FULL`) | A view, snapshot or custom node with no reuse candidate, or whose reuse candidate must rebuild for upstream data drift, whose equivalent exists under another name: it is not copied on submit | `result_copy::plan_submit`, `with_refused_copy` |
| EXECUTE | 1 / 6 (`TARGET_TABLE_MISMATCH` / `NO_SUITABLE_MATCH_FOUND`) | As 1 / 3, for an incremental whose newest equivalent has its own execution type | same |
| EXECUTE | 1 / 2 (`TARGET_TABLE_MISMATCH` / `EXECUTION_TYPE_MISMATCH`) | As 1 / 3, when the newest equivalent has another execution type | same |
| CLONE (submit) | 1 / - (`TARGET_TABLE_MISMATCH`) | Result copy on submit, and a seed's | `DecisionRecord::result_copy`, `DecisionRecord::seed_result_copy` |
| CLONE (`RegisterClone`) | 5 / - (`FORCED_NOT_ELIGIBLE`) | Every `ready_to_clone` from `RegisterClone` | `services.rs::ready_to_clone_response` |
| `unable_to_clone` | - / 10 | `clone_chain_depth_limit = 0` | `services.rs::unable_to_clone_response` |
| `unable_to_clone` | - / 5 | DuckDB only: the pinned incremental contract or a current eligible source is missing | `result_copy::register_lookup`, `plan_register` |

Evidence: `lag_copy_candidates.jsonl` (`ob*`, `dt`), `lag_dropped_targets.jsonl`,
`explain_execution_type_change.jsonl`, `explain_snapshot_history.jsonl` entry 58,
`lag_seed_upstream.jsonl` (arms G, H), `clone_submit_allow_clones_false.jsonl`,
`clone_rejected_allow_clones_false.jsonl`, `explain_custom_copy.jsonl`,
`explain_copy_probes.jsonl`, `explain_incremental_copy_over.jsonl`,
`explain_copy_logic_mismatch.jsonl`, `explain_copy_logic_crafted.jsonl`,
`explain_freshness_cells.jsonl` (8 / 7 ahead of an equivalent elsewhere;
1 / 3 on a data-only rebuild), `explain_rebuild_tolerance_line.jsonl`,
`explain_seed_copy_disabled.jsonl` (a seed's 1 / 10).

### Current decision semantics

The wire-visible rules below are what the server does. Some are conservative
and execute where hosted might reuse; [correctness-handoff.md](correctness-handoff.md)
lists those cases. [architecture.md](architecture.md) describes selection, scope
and fingerprints.

1. **Current state first.** The server reads the latest confirmed build of the
   target relation in its scope (organization, warehouse family,
   `table_namespace`, target relation) before comparing identity. Physical rows
   are ranked by greatest completion epoch, then receipt order for equal epochs.
   A confirmed legacy row with an unknown epoch conservatively blocks reuse and
   copying: even a later delivered valid build cannot prove it replaced that
   unknown result. This floor also applies with
   `ignore_external_modifications=true`. An older
   matching build never resurrects a relation overwritten since; it only
   changes the rejection codes (8 / 7). The identity excludes the execution
   type. When the latest build has the workload's identity under another
   execution type (a snapshot or incremental full refresh, a changed
   materialization or incremental strategy), the submit is judged against the
   latest build of its own type: a target relation modified since rebuilds
   with "the table was modified after the cached execution" (6 / 6), and with
   no such build the rebuild is 2 / 2 with the generic rebuild description.
   That build is never reused; only the latest build is
   ([ADR 0007](adr/0007-lag-tolerance-and-upstream-producers.md#execution-type-change)).
   It is an execution type change only when the latest build also recorded
   the workload's genuine upstream names. A view ↔ custom node switch over an
   upstream (the view reports none, the custom node does) is another
   identity, as on hosted: 6 / 6, or 8 / 7 when an older build of the
   workload's type would qualify (`no_reuse_candidate.upstream_names_changed`,
   `explain_reason_kind_cells.jsonl` `vc`, `cv`, #116).
2. **Identity.** The fingerprint covers the root SQL, semantic extras,
   dialect/catalog/schema and every supplied dependency definition with its
   resolution context. Snowflake SQL is canonicalized
   ([sql-normalization.md](sql-normalization.md)); other dialects use exact SQL
   ([duckdb-identity-and-copy.md](duckdb-identity-and-copy.md) for DuckDB).
   Empty SQL, an empty or duplicated dependency definition, or a changed set of
   physical input names prevents reuse. So do unknown or duplicated epochs for
   an upstream, and a recorded build with no epoch.
3. **`compare_unrendered_code`.** The server records both the rendered identity
   and the template identity (`node_body_hash`), and reuses when either
   matches. A rebuild needs both the rendered SQL and the template to change.
   This applies to models and data tests. Template mode needs a non-empty
   `node_body_hash`. Fixtures `compare_unrendered_code.jsonl`,
   `template_compare_unrendered.jsonl`, `data_test_compare_unrendered.jsonl`;
   tests `template_comparison.rs`, `data_test_template.rs`.
4. **Target relation.** Reuse needs exactly one entry for the target relation
   in `tables` with a known epoch no later than the recorded build. A later
   epoch executes with "the table was modified after the cached execution"
   (for a VIEW, "the view definition is newer than the cached execution").
   Hosted reuses a snapshot or custom node there; this server does not
   (accepted divergence #115). `ignore_external_modifications=true` accepts
   a later epoch but still needs the entry. Data tests have no target
   relation and skip this check.
5. **Freshness**, per [ADR 0007](adr/0007-lag-tolerance-and-upstream-producers.md):
   - An upstream drifted when its current epoch is later than the epoch recorded
     for it at the node's last confirmed build.
   - When the build that produced an upstream's current contents (its Upstream
     producer) differs in logic from the one the node consumed, or the upstream
     was externally modified after that build, the upstream's query changed.
     The node rebuilds regardless of policy and tolerance.
   - `ANY` rebuilds when any direct upstream drifted, `ALL` only when all did.
   - Lag tolerance: a rebuild the policy requires becomes a reuse while the
     build age is under `freshness_tolerance_seconds`. Build age is the server
     clock minus the ConfirmExecution epoch (DuckDB: minus the confirmation's
     receipt time).
   - A seed reloaded with changed values or config is an upstream query change
     for its consumers; a same-values reload is a data change.

   Fixtures `lag_tolerance_matrix.jsonl`, `lag_tolerance_docs_example.jsonl`,
   `lag_external_modification_linked.jsonl`, `lag_seed_upstream.jsonl`; test
   `crates/harness/tests/lag_tolerance.rs`.
6. **`is_stale`.** True on a SKIP whenever upstream drift is present (tolerated
   by `ALL` or within Lag tolerance). On an EXECUTE it is false, except a
   rebuild reporting 8 / 7, where it is set as the older build would be reused.
   On a Result copy it is set as the copied equivalent would be reused.
7. **Data tests.** Matched by project, `table_namespace` and node unique ID,
   plus SQL/config/context identity. Reuse needs complete cached results
   (numeric `failures` and boolean `should_error` and `should_warn`), which
   the SKIP echoes in `execution_results`. Freshness uses the recorded
   upstream epochs. Data tests never get a Result copy. A failing test is
   reused like a passing one and the SKIP echoes the cached failure. A
   `store_failures` test's own audit relation, which the client reports in
   `tables` once it exists, is not an input: the server recognizes it from
   the request (`store_failures: true`, Snowflake, exactly
   `default_catalog`.`default_schema`.`dbt_node_name`, and not mentioned by
   the test's SQL or its query dependencies) and hosted ignores it
   too (#88). Under a configured alias, which no request field carries, the
   server drops the only reported table in the test's schema that the test's
   SQL and query dependencies never mention; two or more such tables keep
   them all (#123, [ADR 0008](adr/0008-store-failures-audit-relation-by-elimination.md)).
   Hosted SKIPs those reruns too (`explain_store_failures_audit_alias.jsonl`).
   Hosted differs in one captured case, a keyed divergence: it reuses a test
   confirmed with incomplete results and echoes them (#87, crafted, an
   [accepted divergence](correctness-handoff.md#accepted-divergences)).
8. **Volatile evaluation.** With `tolerate_nondeterminism=false` the server
   never reuses a model or data test (it does not implement the hosted
   `transformed_nodes_by_query` exchange). On Snowflake the Result copy
   fallback is suppressed only when the root SQL or a dependency definition
   contains a volatile function; a deterministic model is still copied (fixture
   `clone_fallback_volatile.jsonl`, `experiments/clone_eligibility.sh`).

A passing replay establishes only its captured cases.

### Result copy on submit

A FULL submit can answer `ready_to_clone` with an equivalent model built under
another physical name. The equivalent must share `table_namespace`, the copy
logic identity and `execution_type`. On Snowflake that identity abstracts
physical schemas and binds each dependency's schema-abstracted name to its
definition, so swapping two definitions between references is not equivalent
(`clone_fallback_swapped_bindings.jsonl`, `clone_fallback_dep_binding_swap.jsonl`).
A Snowflake submit whose SQL reads a fixed schema other than its own, for
example a dev model reading a prod source with byte-identical SQL, also
matches a build in that schema when the submit, with its Target relation's
schema read as that schema, has the build's logic (the RegisterClone
adoption rule of #93; `explain_post_copy_identity.jsonl` entries 18–19,
`explain_incremental_submit_copy.jsonl` entries 9–10, #107). Before such a
build is copied, its recorded upstream reads must be exactly the submit's,
read in that schema: its Target relation's schema alone does not show where
it resolved its references.
DuckDB computes it only for its checked copy subset
([duckdb-identity-and-copy.md](duckdb-identity-and-copy.md)). Other dialects
have no copy identity and never take this path.

Response: `decision=3`, codes 1 / -, the result-copy description,
`clone_required_last_modified_epoch` = the equivalent's build epoch,
`execution_runtime_ms` = the equivalent's runtime, and `clone_sqls`. Once
confirmed, the copy is the target relation's current build and later matching
submits SKIP.

**FULL only.** Following hosted 2.0.11 (maintainer decision 2026-11-11, #86,
#92, #101), only a FULL submit is copied (`Warehouse::submit_copy_admits`).
DuckDB keeps its own checked subset, which hosted does not engage: FULL, or
delete+insert under the pinned incremental contract. The one exception is a
seed on Snowflake ([Seed copy on submit](#seed-copy-on-submit)). Data tests
never take this path. Any other submit with an equivalent builds, with its
ordinary description and Explain tree, and reports the equivalent in its codes
(`result_copy::plan_submit`):

| Submit | Newest equivalent | Codes | Hosted evidence (client 2.0.11) |
|---|---|---|---|
| view, snapshot, custom | same execution type | 1 / 3 | `explain_copy_probes.jsonl` entries 13–14, `explain_custom_copy.jsonl` entry 9 |
| incremental (any strategy) | same execution type | 1 / 6 | `explain_incremental_copy_over.jsonl` entries 38, 39, 41; `explain_incremental_submit_copy.jsonl` entries 9–10 |
| any of these | another execution type | 1 / 2 | `explain_incremental_copy_over.jsonl` entries 37, 40 |

The equivalent is chosen the way a FULL submit chooses the one it copies,
except that its execution type may differ: newest build first, judged for
freshness when it reads the same upstream relations. In the captured run,
append and insert_overwrite (and merge and delete+insert) had identical SQL
and semantic extras in prod. The append and delete+insert submits' newest
equivalents were the insert_overwrite and merge prod builds, so they got
1 / 2. A view, snapshot or custom node whose newest equivalent has another
type is not captured; it gets 1 / 2 as well. Only submits without a reuse
candidate, or a data-only rebuild for freshness (`upstream_outdated`),
report these codes: captured for a snapshot (`explain_freshness_cells.jsonl`
`fd_sn`, 1 / 3) and crafted for a snapshot and an insert_overwrite model
(`explain_rebuild_tolerance_line.jsonl` `s2`, `i2`, 1 / 3 and 1 / 6). A
rebuild for an Upstream producer change keeps the ordinary codes
(uncaptured). A target whose own older build matches and
would be reused reports that older match instead (8 / 7), as hosted does for
the five incrementals and a snapshot (`explain_freshness_cells.jsonl`
`fr_*`, entries 80–95, #104).

When the fallback applies:

- **No reuse candidate** (first build, absent target relation, or identity
  mismatch), unless the target relation was externally modified. This includes
  a target relation that currently holds other logic
  (`clone_fallback_over_existing_target.jsonl`).
- **The reuse candidate must rebuild for freshness.** Then only an equivalent
  that reads the same upstream relations is copied over the target relation,
  and only if it qualifies for this submit, judged on its own build age.
  Explain shows "but it's old, so we will clone from …".
- An equivalent that reads the same upstream relations is always judged that
  way. A stale one is skipped and the next same-upstream equivalent is tried,
  newest build first, up to `COPY_CANDIDATE_LIMIT` (`submit_decision.rs`). An
  equivalent in another target reads other relations and is copied without
  that judgment (`clone_fallback_stale_source.jsonl`,
  `clone_fallback_zero_tolerance.jsonl`). Fixtures `lag_copy_candidates.jsonl`,
  `lag_dropped_targets.jsonl`.
- Never for data tests, empty SQL, submits without `table_namespace`, or
  `compare_unrendered_code=true` submits (they carry no copy identity), and
  not when volatile SQL blocks it (rule 8 above). Seeds have their own rule
  (below).
- When the submit forbids copies (`clone_chain_depth_limit = 0` or
  `allow_clones = false`) and the target relation has no reuse candidate, the
  server returns the chain-limit EXECUTE instead. A non-FULL submit reports
  its 1 / 3, 1 / 6 or 1 / 2 codes instead (uncaptured).

On 2.0.11, hosted copies incrementals and snapshots only through
`RegisterClone` (below). Hosted 2.0.11 rebuilt every incremental strategy over
an existing dev relation whose own build has other logic, with a prod build of
the same SQL, semantic extras and upstream epoch
(`explain_incremental_copy_over.jsonl`), and append and merge into an absent
dev relation (`explain_incremental_submit_copy.jsonl` entries 9–10). Client
2.0.6 did get submit copies of a MERGE incremental over an existing target
(`clone_failed_fallback.jsonl` entry 2, `clone_submit_equivalent_fallback.jsonl`
entry 1); this server builds there (keyed divergence). Whether 2.0.11 copies an
incremental over a relation with no State history of its own is not captured
(#101 question 3).

### Seed copy on submit

Hosted 2.0.11 answers a `SubmitValues` with `ready_to_clone` when the seed
was loaded with the same values under another name (live:
`explain_copy_probes.jsonl` entry 12, `explain_seed_copy_existing.jsonl`
entries 6 and 14). On Snowflake this server does the same (maintainer
decision 2026-11-11, #91; `Warehouse::submit_copy_admits`,
`result_copy::{seed_copy_target, seed_lookup, plan_seed_submit}`). DuckDB and
every other dialect reload the seed.

- **Identity.** The seed's reuse identity: its `values_hash` and its seed
  logic hash (dialect, `default_catalog` and every `semantic_extras` entry the
  client sends, such as column types), in the same organization, warehouse
  family and `table_namespace`. The source must name the same database and
  object as the target; only the schema may differ.
- **Source.** The latest confirmed row in the source's own Target relation
  history, newest first. A load since replaced with other values is never
  copied. A seed has no upstreams, so the copy is not judged for freshness.
  At submit time an External modification of the source after its load is
  not visible (the same blind spot as a FULL copy).
- **When.** With no reuse candidate, into an absent table (`last_modified_epoch`
  unset), or over an existing table whose latest load in scope is a seed load
  with other values or seed config. Not over a table modified after a load
  with the seed's values: hosted SKIPs that seed (`explain_seed_copy_existing.jsonl`
  entry 13) and this server reloads it (the safety floor, accepted
  divergence #129). Not over an
  existing table without a seed load in scope (uncaptured).
- **Limits.** As for a FULL copy: `allow_clones: false` and
  `clone_chain_depth_limit: 0` do not copy, a requested expiration refuses
  the copy, and `clone_time_travel_limit` is not applied. With
  `allow_clones: false` (client 2.0.11 sends it with
  `clone_chain_depth_limit: 0`) hosted loads the seed with codes 1 / 10 and
  its ordinary description (`seed was loaded because it did not exist` into
  an absent table, `seed was loaded because its data changed` over one
  holding other values), and its Explain tree names the load it could have
  copied (VERIFIED LIVE 2026-11-12, `explain_seed_copy_disabled.jsonl`); the
  server does the same and records `copy_disabled`. With
  `clone_chain_depth_limit: 0` alone (uncaptured) the server sends the
  ordinary load (6 / 6) and records `copy_chain_limit`, whose Explain tree
  is empty.
- **Response.** As for a FULL copy: codes 1 / -, `an equivalent seed exists
  under another name so we cloned that one`, the source's build epoch and
  runtime, and `CREATE OR REPLACE [TRANSIENT] TABLE <target> CLONE <source>
  COPY GRANTS`, transient exactly when the source's confirmed `table_type`
  is (`snowflake::copy_ddl`). The pending row keeps the seed's own identity,
  so the confirmed copy is reused by the next matching submit.

## Speculative submits: `SQL.SubmitEnrichedSQLSpeculative`

The client sends a `SubmitEnrichedSQLRequest` speculatively when its upstream
epoch prefetch has not finished: the request lists every upstream relation,
with the epochs fetched so far and the rest unset (dbt-state 2.62.6
`request_builder.py`). In the client 2.0.11 captures, a model's speculative
request never carries its Target relation's epoch. The answer is a
`SubmitSQLSpeculativeResponse` oneof:
`ready_to_execute_untracked` (1), `skip_execution` (2), `ready_to_clone` (3)
or `undecided` (4). After `undecided` the client waits for the prefetch and
sends the ordinary `SubmitEnrichedSQL`. After `skip_execution` it serves the
SKIP, keeps its `execution_decision_id` for `dbt state explain`, and sends no
ordinary submit and no `ConfirmExecution`.

This server answers `skip_execution` only when the decision needs no table
evidence at all: a data test with no Target relation and no reported tables
that the ordinary submit would SKIP (complete cached results, scoped UID
identity). It decides that submit through the ordinary submit's code
(`submit_decision::speculative_sql`), returns the ordinary SKIP's payload, and
records the decision, reason and node state as the ordinary SKIP does. Every
other speculative call is `undecided` and writes nothing: first runs, logic
changes, incomplete cached results, data tests with upstreams and every model
kind. It never answers `ready_to_execute_untracked` or `ready_to_clone`.

Evidence, client 2.0.11, 314 speculative calls in `golden/fixtures/`: hosted
answers `undecided` to every model, snapshot, view and custom-node call and to
every data test with an upstream. `explain_rebuild_reason_gaps.jsonl` entries
2 and 18 (data test `r40t0` without upstreams: first run, logic change) are
`undecided`; entry 39 (the unchanged rerun) is `skip_execution`, explained at
entry 46, and the client sends no ordinary submit for it.

## State writes: `Execution`

- `ConfirmExecution{request_id, last_modified_epoch?, failed_to_clone,
  table_type?, execution_results, execution_runtime_ms?, labels}` →
  `{success, request_id}`. Confirms the pending row created by a
  `ready_to_execute` or `ready_to_clone`. The first confirmed outcome, including
  results, is immutable. A duplicate returns INTERNAL (13), `Database operation
  failed`, without republishing. An unknown valid UUID returns NOT_FOUND (5),
  `Execution request with ID {submitted_id} not found`; malformed or empty IDs
  return INTERNAL (13), `Database operation failed`. Uppercase, simple and
  braced UUID spellings correlate to the same request and are echoed unchanged.
  A physical target requires `last_modified_epoch`; an absent target requires
  it to be absent. A rejected outcome returns INTERNAL (13), `Database operation
  failed`, leaves the row pending and permits a valid retry.
  A confirm with `failed_to_clone=true` is recorded as a normal build.
- `RecordExecutions{records[]}` → `{records_stored}`. Records outcomes without
  a submit. Client 2.0.11 sends it for an incremental model run under
  `--full-refresh` (see [What the client computes](#what-the-client-computes)). Each `ExecutionRecord` has an `outcome` and either `enriched_sql`
  (`SQLExecution`) or `values` (`ValuesExecution`). The batch is all-or-nothing:
  executions and selector node state are written in one transaction. Missing
  input returns INVALID_ARGUMENT (3), `Record at index N has no input set`.
  Target/epoch presence follows Confirm; an invalid outcome rejects the whole
  batch with INTERNAL (13), `Database operation failed`. Explicit empty SQL
  targets on Snowflake and Databricks return INVALID_ARGUMENT (3), `Failed to
  process SQL`. An empty batch succeeds with zero records.
  `SQLExecution` has no `compare_unrendered_code` flag, so a Record stores only
  the rendered identity. `SQLExecution.from_speculative_submit` is set by the
  client (dbt-state 2.62.6 `run_cache.py`) only for a node it ran after a
  speculative `ready_to_execute_untracked`, with its upstream epochs refetched
  first. This server never answers `ready_to_execute_untracked`, so a client
  never sets it here; a record that does is stored like any other.
- `ResolveDeferredRelations{profile_name, target_name, project_name,
  project_id?, node_unique_ids[]}` → `{fqn_by_unique_id{}}`. Resolves confirmed
  outcomes by organization, exact profile, target and UID. A nonempty project
  ID scopes by ID; otherwise exact project name scopes the lookup, including
  executions originally recorded with an ID. Unknown UIDs are omitted.
  Missing profile/target, no IDs, or missing both project ID and project name
  returns an empty map. A pending or unconfirmed failed attempt cannot replace
  the previous confirmed mapping. `failed_to_clone=true` confirms a successful
  fallback build; the protocol has no general execution-failure flag.
  Fresh hosted Snowflake/Databricks scope histories and actual pinned-client
  Databricks failures establish this behavior (2026-11-13), superseding the
  earlier assumption that selector snapshots made unconfirmed nodes deferrable.
  FQNs retain the submitted warehouse quoting, including Databricks backticks.
  Requests carry no dialect/family/connection namespace, so deferral uses the
  logical scope supplied; physical reuse still requires its own warehouse scope.
  Legacy executions without persisted profile/project/target evidence do not
  resolve speculatively. Fixtures `databricks_deferral{,_scopes}.jsonl` replay
  complete response bodies/statuses through the production router.

## Seeds: `SQL.SubmitValues`

`SubmitValuesRequest{target_table, dialect, default_catalog, values_hash,
semantic_extras{}, last_modified_epoch?, labels, dbt_node_state?,
table_namespace?, …}`. The execution type is implicitly `VALUES=9`. Reuse
matches the seed bytes plus seed configuration and context against the target
relation's latest build, and needs the table's current epoch. Seeds have no Lag
tolerance and always use `ANY`. On Snowflake a seed without a reuse
candidate is copied from its load under another name
([Seed copy on submit](#seed-copy-on-submit), #91); elsewhere it is
reloaded. An externally modified seed table is reloaded (hosted skips it,
accepted divergence #129; see
[correctness-handoff.md](correctness-handoff.md#accepted-divergences)).

## State selectors: `SelectorService.GetStateSelection`

Powers `state:new/old/modified/unmodified` and the `modified.*` sub-selectors
when dbt State is enabled, `project-id` is set and no `--state` manifest is
given. Each node is compared with its own last recorded state in the
`defer_to_target` target. `dbt compile --select state:…` (and build, run, test)
calls `GetStateSelection`; `dbt ls` does not.

Request `SelectorRequest{target, selector_criteria, project_id, nodes[] =
DbtNodeData{node_unique_id, node_hash, node_body_hash?, node_configs_hash?,
node_persisted_descriptions_hash?, node_macros_hash?, node_contract_hash?,
node_database_representation?}}`. `target` is the `defer_to_target` name
(default `prod`), not a schema. The client batches up to 10,000 nodes per
request and unions the responses. Response `SelectorResponse{node_unique_ids[]}`.

`SelectorCriteria`: `SC_UNSPECIFIED=0, MODIFIED=1, NEW=2, UNMODIFIED=3, OLD=4,
BODY=5, CONFIGS=6, RELATION=7, PERSISTED_DESCRIPTIONS=8, MACROS=9, CONTRACT=10`.

The server records the client's `DbtNodeData` hashes on every Submit and
Record (latest write wins) and compares them; it never recomputes. With `rec`
the node's recorded state:

- `new` ⇔ no `rec`; `old` ⇔ `rec` present.
- `modified.<component>` ⇔ no `rec`, or that component hash differs.
- `modified` ⇔ no `rec`, or any of body, configs, persisted_descriptions,
  macros or contract differs. `relation` is excluded.
- `unmodified` ⇔ `old` and not `modified`.

Only models, seeds, snapshots and data tests are recorded. Sources, unit tests,
semantic models, metrics and saved queries are always `new`/`modified`.
`node_body_hash` is a raw file hash, so whitespace-only edits count as
`modified.body`. `SC_UNSPECIFIED` or an empty `project_id` returns
`INVALID_ARGUMENT`. Fixtures `selector_state_selection.jsonl`,
`selector_last_write_wins.jsonl`; tests `selector_conformance.rs`,
`selector_offline.rs`.

## Clone: `Clone.RegisterClone`

The client calls `RegisterClone` for incremental models and snapshots (the
`pre_clone` path). Table materializations get copies through the submit
fallback above. The client requests a copy only when the profile sets
`defer_to_target` and the dev relation is absent.

Request `CloneRequest{target_table, dialect, execution_type,
clone_source_table, clone_source_last_modified_epoch?,
clone_source_table_type?, table_namespace?, clone_chain_depth_limit?, labels,
…}`.

A successful `CloneResponse` sets **both** `ready_to_clone` (field 3) and the
deprecated `ready_to_clone_v1` oneof (field 1) to the same
`ReadyToCloneResponse{request_id, clone_sqls[], clone_source, clone_target,
ExplainedDecision{decision=3, skip_rejection_reason=5,
clone_rejection_reason=null, decision_description=""},
clone_execution_results{fields:{}}, execution_decision_id}`.

Hosted sends an empty `execution_decision_id` in every captured
`RegisterClone` response, so the client never asks to explain the copy
decision; it explains the post-copy submit instead
(`explain_incremental_copy_history.jsonl`, client 2.0.11). This server mints
one, so its copy decision can still be explained
(`clone_lifecycle.rs::register_clone_decision_explains_as_a_clone`). The
replay compares the response without ids.

```sql
CREATE OR REPLACE TRANSIENT TABLE <target>
CLONE <source>
COPY GRANTS
```

Snowflake sources are `TABLE` or `TRANSIENT TABLE`. BigQuery uses
`CREATE TABLE ... CLONE ...`; Databricks/Spark use `CREATE TABLE ... SHALLOW
CLONE ...` only for an explicitly Delta source. These two follow published
syntax and are not verified against hosted or the warehouses. DuckDB is
described in [duckdb-identity-and-copy.md](duckdb-identity-and-copy.md).
`UNIMPLEMENTED` is returned for view sources, Redshift and unknown dialects,
and requests with retention properties (`hours_to_expiration`,
`partition_expiration_days`).

Lifecycle:

- `clone_chain_depth_limit = 0` returns `unable_to_clone{clone_source,
  clone_target, ExplainedDecision{decision=1, clone_rejection_reason=10,
  decision_description=""}}` with the deprecated oneof empty. No history is
  written and the client builds normally.
- Otherwise a pending row is written for the target relation. On Snowflake it
  inherits, from the source's current build, its logic identity, its
  `execution_runtime_ms` and the names of the upstreams it recorded
  (`executions.clone_source_inputs`). After `ConfirmExecution` (whose
  runtime, null from client 2.0.11, does not replace the inherited one), a
  later submit for the target relation SKIPS when its schema-abstracted logic
  matches, directly or with the submit's Target relation schema read as the
  copy source's schema, and executes on a logic change. The SKIP reports the
  source build's runtime, as hosted does (VERIFIED LIVE 2026-11-11,
  `explain_post_copy_identity.jsonl`, `explain_snapshot_register_clone.jsonl`,
  `explain_incremental_copy_history.jsonl`, #93).
- A copy records no inputs. Its freshness is anchored on the copy's own build
  epoch: an upstream newer than the copy drifts, one older than the copy but
  newer than the source's build does not (`clone_chain_source_age.jsonl`,
  `clone_chain_upstream_drift.jsonl`).
- A later submit that cannot adopt the copy (its logic differs from the
  inherited one even read in the source's schema, or none was inherited)
  rebuilds with the same wire answer as an identity mismatch (6 / 6) and the
  Decision reason `no_reuse_candidate.copy_logic_mismatch`, which renders
  SHAPE 7 judged against the copy (VERIFIED LIVE 2026-11-11,
  `explain_copy_logic_mismatch.jsonl`, `explain_copy_logic_crafted.jsonl`,
  #104). A submit of another execution type with the inherited logic (a
  table over an incremental's copy, after a materialization change) is an
  execution type change instead: 2 / 2, the SKIP-shaped tree against the
  copy (`kf4`). The semantic extras are part of the logic, so a view or
  custom node over an incremental's copy differs (6 / 6). When the copy's
  Target relation was dropped as well, every model kind rebuilds with
  6 / 6, "its table did not exist", and the first-build tree (crafted
  `x11`–`x19`, VERIFIED LIVE 2026-11-11).

Client 2.0.11 (VERIFIED LIVE 2026-11-10, `explain_snapshot_register_clone.jsonl`):
the pre-step needs a deferred node whose relation differs from the dev one,
which dbt State auto-deferral (`defer_to_target`) provides. A snapshot with
`target_schema=target.schema` keeps the dev schema in its deferred node, so
the client sends no RegisterClone for it. Hosted answered a snapshot
(execution type 7) copy with `ready_to_clone`, 5 / -, and an empty
`execution_decision_id`, so a RegisterClone copy has no Explain message. This
server mints an id there; the replay compares responses without ids.

## Custom `Struct` type

`struct.proto` defines a `Struct`/`Value`/`ListValue` like
`google.protobuf.Struct` with `int64` support, used for `execution_results` and
telemetry config.

## Explain: `Explain.GetExplainMessages`

The request names execution decision IDs; each tracked ID returns its decision,
description and complete nested Explain tree. Unknown IDs are omitted.
Observed hosted batches of distinct tracked IDs return entries in ascending
decision-ID order, including when the request reverses them (2026-11-12
Databricks baseline and six forward/reverse probes). The shared handler follows
that order. Replay compares every entry's complete text, markers, badges and
children; the new Databricks fixture keeps actual relation names and permits no
missing-history or known-gap exception. See [table/view acceptance](databricks-baseline.md).

## Explain: `Explain.GetUpstreamDependencyChanges` is client-unreachable

The Fusion CLI embeds only `GetExplainMessages` from the `Explain` service, and
no capture has ever called `GetUpstreamDependencyChanges`; the hosted docs place
that view in the dbt platform UI, which the recording proxy does not see. The
server keeps it as an empty stub (`services.rs::get_upstream_dependency_changes`).
Characterize it before implementing it if a client ever calls it.
