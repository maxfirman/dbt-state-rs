# dbt State protocol — research notes

> These are the reverse-engineering notes compiled while building this project.
> They are preserved for provenance and context. For the authoritative,
> maintained protocol reference see [protocol.md](protocol.md); for the capture
> workflow see [harness.md](harness.md).

## What dbt State is
dbt State (GA ~2026, "state-aware orchestration") makes dbt build on *change* instead of
on schedule. On every run, for each node, dbt asks a hosted gRPC decision engine whether to
BUILD / SKIP (NO-OP) / CLONE / DEFER based on whether the node's **logic** (SQL fingerprint)
or **upstream data** (freshness) has changed since a prior recorded build. The decision
engine is the hosted, metered service at `api.state.dbt.com:443`. The pip package / Fusion
binary is only the **client**; with no auth the client disables itself and dbt runs vanilla.

Goal of this project: an open-source Rust + Postgres reimplementation of that server,
validated against the real `api.state.dbt.com` using the Fusion client + jaffle-shop.

## Protocol = "query cache" (internal name), proto package `com.fivetran.query_cache`
The authoritative `.proto` files are OPEN SOURCE and present in our clones:
- Rust/Fusion client: `~/projects/dbt-fusion/crates/dbt-run-cache/proto/query_cache_protobuf/query_cache/`
- Python mirror:       `~/projects/dbt-core/crates/dbt-state/proto/query_cache_protobuf/query_cache/`
- Upstream repo of record: `dbt-labs/dbt-state/proto` (Apache-2.0)

### Services (8 total)
| Service | RPCs | Priority |
|---|---|---|
| `SQL` | `SubmitEnrichedSQL`, `SubmitValues` | **core — the verdict** |
| `Execution` | `ConfirmExecution`, `RecordExecutions` | **core — writes state** |
| `Clone` | `RegisterClone` | phase 2 |
| `ClientValidation` | `ValidateClientVersion` → `{is_supported}` | trivial (return true) |
| `Health` (grpc.health.v1) | `Check`, `Watch` → SERVING | trivial |
| `Explain` | `GetExplainMessages` | optional (human text) |
| `ClientTelemetry` | `RegisterSessionStart`(dep), `SubmitTelemetryBatch` | no-op |
| `SelectorService` | `GetStateSelection` | later (state:modified selectors) |

### Core decision RPC: SQL.SubmitEnrichedSQL
Request `SubmitEnrichedSQLRequest`: target_table?, dialect, default_catalog, execution_type
(enum FULL/APPEND/MERGE/INSERT_OVERWRITE/DELETE_INSERT/MICROBATCH/SNAPSHOT/DBT_DATA_TEST/
VALUES/VIEW/DBT_CUSTOM), sql (RAW compiled SQL — server fingerprints it), tables[]
({name,last_modified_epoch} = input freshness), query_dependencies[] ({name,query,
default_catalog,default_schema}), semantic_extras map, freshness_tolerance_seconds (default 2700),
lenient_dependencies[], tolerate_nondeterminism, labels, clone_time_travel_limit?,
clone_table_properties?, stale_upstream_policy (ANY default / ALL).

Response `SubmitSQLResponse` oneof: ready_to_execute | skip_execution | ready_to_clone.
- ExplainedDecision { decision, skip_rejection_reason?, clone_rejection_reason?, is_stale }
- ReadyToExecute { request_id, explained_decision, transformed_nodes_by_query,
  query_hash_metadata_info{semantic_hash_match,data_hash_match}?, execution_decision_id? }
- SkipExecution (the NO-OP) { explained_decision, execution_results(Struct),
  execution_runtime_ms?, execution_decision_id? }
- ReadyToClone { request_id, clone_sqls[], clone_source, clone_target,
  clone_required_last_modified_epoch?, ... }

### State writes: Execution
- ConfirmExecution(request_id, last_modified_epoch?, failed_to_clone, table_type?,
  execution_results Struct, execution_runtime_ms?, labels) → {success, request_id}.
  Pairs with a prior SubmitEnrichedSQL by request_id.
- RecordExecutions(records[ExecutionRecord{outcome, enriched_sql|values}]) → {records_stored}.
  Bypass path to hydrate history without the Submit/Confirm round-trip. Atomic batch.

### Struct type
Custom `Struct`/`Value`/`ListValue` (like google.protobuf.Struct but with int64 support).

## Client behaviour (from Fusion `dbt-run-cache` crate) — our server must match
- Endpoint default `api.state.dbt.com:443`; TLS when secure (https or :443), else insecure + NO OAuth.
  Config via env `RUN_CACHE_*` (API_URL, API_SECURE, ...) and `DBT_ENGINE_MANAGE_STATE`.
- Auth: OAuth; client_id default `2fd87cd5-69a6-4c5f-9097-747a58f0edf6`,
  token_url `https://auth.state.dbt.com/token`, auth_url `https://auth.state.dbt.com`.
  Sends `authorization: Bearer <id_token>` + `x-organization-id`.
- Per-request gRPC metadata headers: x-request-id, x-session-id, x-submitted-at-epoch,
  x-system-user-id, x-os-name.
- ValidateClientVersion is fail-open (errors → Skipped → run vanilla).
- Client computes: table-name normalization + dependency extraction (sqlglot in py / dbt
  frontend in rust) and md5 of seed file bytes (values_hash for SubmitValues). For models it
  sends RAW SQL. **Server owns the semantic fingerprint, history match, and the decision.**
- execution_type derivation: Test→DBT_DATA_TEST; view→VIEW; custom mat→DBT_CUSTOM;
  snapshot→SNAPSHOT; incremental & !full_refresh→strategy upper (merge w/o unique_key→APPEND);
  else FULL.
- SQL semantic_extras keys: on_schema_change, incremental_predicates, merge_update_columns,
  merge_exclude_columns, severity, limit, where, fail_calc, warn_if, error_if, store_failures,
  store_failures_as, auto_liquid_cluster, databricks_tags. Seed keys: column_types,
  quote_columns, delimiter.

## Local environment
- Fusion dbt v2.0.6 at `dbt`; dbt Cloud CLI 0.40.24 (authed to example-account.us1.dbt.com,
  account example_account, Jaffle Shop project 22222222222222).
- jaffle-shop at `~/projects/jaffle-shop` (DuckDB local + Snowflake targets in profiles.yml).
  Log confirms: "dbt State is enabled (endpoint https://api.state.dbt.com:443, defer_to prod)".
  Response logs captured under `logs/run_cache/responses_*.jsonl` (REAL server responses!).
- Rust 1.98, Postgres client (psql), Docker available. protoc NOT installed (tonic-prost-build
  vendors protoc, so may not be needed).
- Reference clones under ~/projects: dbt-fusion (Rust client, authoritative proto+client),
  dbt-core (Python client + proto mirror), dbt-state-oss (poor ref server, good protocol notes).

## Key leverage
The hosted service's decisions were captured as real request/response pairs via
a recording gRPC proxy (see [harness.md](harness.md)) — not the client-side
`responses_*.jsonl` logs, which only contain per-node decision IDs, not full
wire payloads. Those captured pairs became the golden fixtures that drive the
conformance and differential tests (see [testing.md](testing.md)).
