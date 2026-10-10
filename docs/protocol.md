# Protocol reference

The dbt State protocol is the gRPC **"query cache"** protocol, proto package
`com.fivetran.query_cache` (plus `grpc.health.v1`). The authoritative `.proto`
files are Apache-2.0, published by dbt Labs, and vendored in this repo under
[`proto/query_cache_protobuf/query_cache/`](../proto/query_cache_protobuf/query_cache).

## Transport & auth

- Default endpoint `api.state.dbt.com:443` (TLS). The client picks TLS when the
  URL is https or ends `:443`; otherwise it uses an insecure channel and sends
  **no** OAuth — which is how we point the client at a local server.
- Client config via env `RUN_CACHE_*` (e.g. `RUN_CACHE_API_URL`,
  `RUN_CACHE_API_SECURE`) and `DBT_ENGINE_MANAGE_STATE=true` to enable.
- Hosted auth: OAuth token-exchange of the dbt Cloud credential at
  `https://auth.state.dbt.com/token` (client_id `2fd87cd5-...`), yielding an
  `id_token` sent as `authorization: Bearer <id_token>` plus
  `x-organization-id: <org_id>` (org id parsed from the token scope
  `runcache:scope:org:<ORG_ID>:...`).
- Per-request metadata headers from the real client: `x-request-id`,
  `x-session-id`, `x-submitted-at-epoch`, `x-system-user-id`, `x-os-name`,
  `x-dbt-invocation-id`.

## Services

| Service | RPCs | Role | Our status |
|---|---|---|---|
| `SQL` | `SubmitEnrichedSQL`, `SubmitValues`, `SubmitEnrichedSQLSpeculative` | the verdict | BUILD/SKIP with conservative fallbacks; speculative Undecided |
| `Execution` | `ConfirmExecution`, `RecordExecutions`, `ResolveDeferredRelations` | writes state / deferral | Confirm/Record implemented; deferral UNIMPLEMENTED |
| `Clone` | `RegisterClone` | clone decision + DDL | table DDL; eligibility/provenance incomplete |
| `ClientValidation` | `ValidateClientVersion` | version gate | implemented (true) |
| `Explain` | `GetExplainMessages`, `GetUpstreamDependencyChanges` | human text | empty defaults |
| `SelectorService` | `GetStateSelection` | state:modified selectors | UNIMPLEMENTED |
| `ClientTelemetry` | `SubmitTelemetryBatch`, … | metering (dbt Labs only) | not served |
| `Health` (grpc.health.v1) | `Check`, `Watch` | health | SERVING |

Wire service names must be preserved exactly (the proto service literally named
`Clone` collides with Rust's `std::clone::Clone`; the proto build patches the
generated std-derive impl while keeping the on-wire name — see
[architecture.md](architecture.md#proto-crate)).

## The decision RPC: `SQL.SubmitEnrichedSQL`

### Request `SubmitEnrichedSQLRequest` (observed fields)

`target_table?`, `dialect`, `default_catalog`, `default_schema?`,
`execution_type` (enum below), `sql` (raw compiled SQL),
`tables[]` = `{name, last_modified_epoch?}` (per-input freshness),
`query_dependencies[]`, `semantic_extras{}`, `freshness_tolerance_seconds`,
`lenient_dependencies[]`, `tolerate_nondeterminism`, `labels{}`
(`dbt_node_name`/`dbt_node_fqn`/`dbt_node_unique_id`),
`stale_upstream_policy` (ANY=0 / ALL=1), `table_namespace?`,
`compare_unrendered_code`, `ignore_external_modifications`, `allow_clones?`,
`is_defer_to_profile`, `defer_enabled`, clone-related limits, and
`dbt_node_state` = `{node_unique_id, node_hash, node_body_hash?,
node_configs_hash?, node_contract_hash?, project_id?, …}`.

### `ModelExecutionType` enum

`UNSPECIFIED=0, FULL=1, APPEND=2, MERGE=3, INSERT_OVERWRITE=4, DELETE_INSERT=5,
MICROBATCH=6, SNAPSHOT=7, DBT_DATA_TEST=8, VALUES=9, VIEW=10, DBT_CUSTOM=11`.

### Response `SubmitSQLResponse` (oneof)

- `skip_execution` (the no-op): `ExplainedDecision{decision=0,
  decision_description="model was a no-op because both its query and its
  upstream data are up to date"}`, `execution_results` (complete cached Struct, empty for models when absent),
  `execution_runtime_ms` (echo of the prior run), `execution_decision_id`.
- `ready_to_execute`: `request_id`, `ExplainedDecision{decision=1,
  skip_rejection_reason=6 (NO_SUITABLE_MATCH_FOUND), clone_rejection_reason=6,
  decision_description="model was executed because either its query didn't match
  or its upstream data is out of date"}`, `execution_decision_id`.
- `ready_to_clone`: see Clone below.

`ExplainedDecision` = `{decision (SubmitSQLResultType), skip_rejection_reason?,
clone_rejection_reason?, is_stale, decision_description}`. Enums serialize as
ints on the wire/JSON.

- `SubmitSQLResultType`: `SKIP_EXECUTION=0, READY_TO_EXECUTE=1, READY_TO_CLONE=3,
  UNKNOWN=4`.
- `RejectionReason` (subset): `FORCED_NOT_ELIGIBLE=5, NO_SUITABLE_MATCH_FOUND=6`.

### Current decision semantics

These are offline correctness safeguards, not a complete specification of the
hosted service. [correctness-handoff.md](correctness-handoff.md) records the
remaining policies and compatibility gaps.

1. Select the newest confirmed physical target *before* matching its versioned
   SQL fingerprint, execution type, namespace and dialect. An old matching row
   cannot resurrect a target overwritten by a later build. Data tests use
   scoped project/namespace/node identity (including older label identity).
2. Fingerprint root SQL, semantic extras, dialect/catalog/schema, and supplied
   dependency definitions/context. Snowflake SQL uses a restricted AST
   canonicalization preserving cast kinds, parameters and qualified functions;
   parser failures and other dialects preserve exact SQL. Template comparison
   remains a separate, unresolved mode based on a nonempty body hash.
3. Require a known current target epoch no newer than its recorded confirmation,
   unless external modifications are explicitly ignored. Missing or ambiguous
   target metadata builds. Data tests instead require complete cached results.
4. Compare exact physical upstream names, excluding the own target from upstream
   drift only. Unknown epochs, changed input sets, and duplicate evidence build.
   ANY/ALL and the existing timestamp-distance lag formula remain pending hosted
   characterization. Another physical target builds until provenance is known.
5. Requested volatile evaluation (`tolerate_nondeterminism=false`) builds until
   runtime transformations are implemented. SQL submits do not generate CLONE.

These checks can over-execute compared with hosted behavior. A passing replay
only establishes its reproducible captured cases.

## State writes: `Execution`

- `ConfirmExecution{request_id, last_modified_epoch?, failed_to_clone,
  table_type?, execution_results, execution_runtime_ms?, labels}` →
  `{success, request_id}`. Correlates with the `request_id` from a prior
  `ready_to_execute`/`ready_to_clone`; marks the pending row confirmed so future
  matching submits skip. Idempotent for a repeated `request_id`; the first confirmed outcome, including
  results, is immutable. Complete protobuf results are persisted through both
  ConfirmExecution and RecordExecutions.
- `RecordExecutions{records[]}` → `{records_stored}`. Bypass hydration path:
  each `ExecutionRecord` has an `outcome` and a oneof input of `enriched_sql`
  (`SQLExecution`, hashes via `dbt_node_state`) or `values` (`ValuesExecution`).
  Processed atomically (all-or-nothing).
- `ResolveDeferredRelations{profile_name, target_name, project_name,
  project_id?, node_unique_ids[]}` → `{fqn_by_unique_id{}}`. Observed real
  response: an **empty** map (the clone source came from profile-based
  auto-deferral, not server resolution). Our resolver returns UNIMPLEMENTED until
  authoritative nonempty mapping behavior is implemented.

## Seeds: `SQL.SubmitValues`

`SubmitValuesRequest{target_table, dialect, values_hash (md5 of seed bytes),
last_modified_epoch?, dbt_node_state?, …}`. Decision mirrors
`SubmitEnrichedSQL` but matches on data bytes plus seed configuration/context
and latest physical state, requiring known current-target metadata;
`execution_type` is implicitly `VALUES=9`.

## Clone: `Clone.RegisterClone`

Request `CloneRequest{target_table (dev), dialect, execution_type,
clone_source_table (prod), clone_source_last_modified_epoch?,
clone_source_table_type? (e.g. "TRANSIENT TABLE"), table_namespace?, labels}`.

Response `CloneResponse` populates **both** the `ready_to_clone` field (3) and
the deprecated `ready_to_clone_v1` oneof (1) with the same `ReadyToCloneResponse`:
`{request_id, clone_sqls[], clone_source, clone_target,
ExplainedDecision{decision=3 (READY_TO_CLONE), skip_rejection_reason=5
(FORCED_NOT_ELIGIBLE), clone_rejection_reason=null, decision_description=""},
clone_execution_results{fields:{}}, execution_decision_id}`.

The **server generates `clone_sqls`** (dialect-specific DDL). Observed Snowflake:

```sql
CREATE OR REPLACE TRANSIENT TABLE <target>
CLONE <source>
COPY GRANTS
```

The supported Snowflake kinds are TABLE and TRANSIENT TABLE. View sources
return UNIMPLEMENTED.
Other dialects: BigQuery `CREATE OR REPLACE TABLE ... CLONE ...`, Databricks/Spark
`... SHALLOW CLONE ...`. Unknown dialects return UNIMPLEMENTED. Non-Snowflake variants are our own templates,
not yet verified against the real service.

## Custom `Struct` type

`struct.proto` defines a `Struct`/`Value`/`ListValue` like `google.protobuf.Struct`
but with `int64` support, used for `execution_results` / telemetry config.

## Coverage

See the service table above and the complete remaining feature/testing backlog in
[correctness-handoff.md](correctness-handoff.md). Captured response examples and
experiments are useful evidence for individual scenarios; complete hosted
conformance has not been established. Telemetry, selectors, deferral, automatic
SQL cloning, runtime transformations and several policy rules remain incomplete.
