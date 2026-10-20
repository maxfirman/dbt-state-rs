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
- Every store query is scoped by `x-organization-id` (default tenant `local`
  when absent). Setting `DBT_STATE_REQUIRE_ORG_HEADER=true` rejects requests
  without a non-empty org header (`UNAUTHENTICATED`) for multi-tenant
  deployments. This is org SCOPING presence, not identity authentication —
  verifying the org against a trusted token (OAuth/TLS) is deployment work.

## Services

| Service | RPCs | Role | Our status |
|---|---|---|---|
| `SQL` | `SubmitEnrichedSQL`, `SubmitValues`, `SubmitEnrichedSQLSpeculative` | the verdict | BUILD / SKIP / cross-name CLONE (live-verified); speculative Undecided |
| `Execution` | `ConfirmExecution`, `RecordExecutions`, `ResolveDeferredRelations` | writes state / deferral | Confirm/Record implemented; deferral implemented (live-verified) |
| `Clone` | `RegisterClone` | clone decision + DDL | table DDL + lifecycle + `allow_clones=false` rejection (live-verified); non-Snowflake templates unverified |
| `ClientValidation` | `ValidateClientVersion` | version gate | implemented (true) |
| `Explain` | `GetExplainMessages`, `GetUpstreamDependencyChanges` | human text | empty defaults |
| `SelectorService` | `GetStateSelection` | state:* selectors | implemented (live-verified) |
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

1. Select the newest confirmed physical target within its warehouse family and
   `table_namespace` *before* matching its versioned SQL fingerprint, execution
   type and dialect options. An old matching row
   cannot resurrect a target overwritten by a later build. Data tests use
   scoped project/namespace/node identity (including older label identity).
2. Fingerprint root SQL, semantic extras, dialect/catalog/schema, and supplied
   dependency definitions/context. Snowflake SQL uses a restricted AST
   canonicalization preserving cast kinds, parameters and qualified functions;
   parser failures and other dialects preserve exact SQL. Other dialects do not
   produce a cross-environment equivalence identity until their relation and
   dependency semantics are characterized. Template comparison
   (`compare_unrendered_code=true`) is a DUAL identity: the server records both
   the rendered logic hash and the unrendered-template hash and reuses when
   EITHER matches — a rebuild occurs only when BOTH the rendered SQL and the
   template change (documented contract, verified live; review #2 finding 6).
   This dual identity applies to BOTH models and DATA TESTS
   (`find_confirmed`/`find_confirmed_test`); the data-test lookup honoring the
   template hash was the review #2 finding-6 FOLLOW-UP fix, also live-verified.
3. Require a known current target epoch no newer than its recorded confirmation,
   unless external modifications are explicitly ignored. Missing or ambiguous
   target metadata builds. Data tests instead require complete cached results.
4. Compare exact physical upstream names, excluding the own target from upstream
   drift only. Unknown epochs, changed input sets, and duplicate evidence build.
   An upstream newer than the node's own last-build epoch is drift. `ALL` policy
   (`require_fresh_data_from`) rebuilds only when every direct parent drifted,
   else a tolerated-stale SKIP with `is_stale=true`; every execute reports
   `is_stale=false`. This own-build anchor and the `is_stale` reporting were
   verified live; `freshness_tolerance_seconds` is intentionally not applied
   (though it IS known to gate under some conditions — see
   [correctness-handoff.md §C](correctness-handoff.md)). A data test produces no
   object, so it has no own-build epoch: it anchors drift on the epoch RECORDED
   for each upstream at its last confirmation (per input, strict `>`), rebuilding
   when the tested data changed (review #2 finding 1, verified live).
5. Cross-environment reuse (live-verified): a clone-origin object, or an
   equivalent model under another physical name in the same `table_namespace`
   with matching schema-abstracted logic, is reused via CLONE — a confirmed clone
   target SKIPS a matching later submit, and a submit for an absent target whose
   equivalent exists elsewhere returns `ready_to_clone`. The cross-name clone
   fallback is suppressed only when `tolerate_nondeterminism=false` AND the SQL
   genuinely contains a volatile function (e.g. `current_timestamp()`); a
   deterministic model still clones. Runtime volatile-function VALUES
   (`transformed_nodes_by_query`) are not yet evaluated (a conformance gap).

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
  Processed atomically (all-or-nothing) — the executions batch AND the derived
  selector/deferral `node_states` are written in a SINGLE transaction, so a
  rejected batch or any write failure leaves no partial state (review #2
  finding 5, full cross-store atomicity).
- `ResolveDeferredRelations{profile_name, target_name, project_name,
  project_id?, node_unique_ids[]}` → `{fqn_by_unique_id{}}`. **Implemented
  (live-verified, `experiments/deferral_experiment.sh`).** Resolves each
  requested `node_unique_id` to the physical relation it was last built as in
  the `target_name` (defer-to) env, scoped by `project_id` — i.e. the submit
  `target_table` recorded in `node_states` for that (project, env, node). Nodes
  not built in that env are omitted (an unresolvable scope yields an empty map,
  matching the hosted empty-when-unknown behavior; the first captures we took
  were empty precisely because nothing had been built there yet). The FQN is a
  quoted 3-part name, e.g.
  `"CDO_DEMO_DB"."PROD_STATE_DEMO"."STG_CUSTOMERS"`.

## Seeds: `SQL.SubmitValues`

`SubmitValuesRequest{target_table, dialect, values_hash (md5 of seed bytes),
last_modified_epoch?, dbt_node_state?, …}`. Decision mirrors
`SubmitEnrichedSQL` but matches on data bytes plus seed configuration/context
and latest physical state, requiring known current-target metadata;
`execution_type` is implicitly `VALUES=9`.

## State selectors: `SelectorService.GetStateSelection`

Powers dbt State-powered `state:*` selectors (`state:new/old/modified/
unmodified` and the `modified.*` sub-selectors). Used when dbt State is enabled,
`project-id` is set, and no `--state` manifest is given; each node is compared
against **its own last execution in the `defer_to_target` env** rather than a
single `manifest.json`. Triggered by e.g. `dbt compile --select state:modified`
(and build/run/test); `dbt ls` does **not** engage the service.

Request `SelectorRequest{target, selector_criteria (SelectorCriteria),
project_id, nodes[] = DbtNodeData{node_unique_id, node_hash, node_body_hash?,
node_configs_hash?, node_persisted_descriptions_hash?, node_macros_hash?,
node_contract_hash?, node_database_representation?}}`. `target` is the
`defer_to_target` ENV name (default `prod`), NOT a physical schema. The client
batches ≤10,000 nodes/request and unions the responses. Response
`SelectorResponse{node_unique_ids[]}` — the subset matching the criterion.

`SelectorCriteria`: `SC_UNSPECIFIED=0, MODIFIED=1, NEW=2, UNMODIFIED=3, OLD=4,
BODY=5, CONFIGS=6, RELATION=7, PERSISTED_DESCRIPTIONS=8, MACROS=9, CONTRACT=10`.

### Semantics (live-verified)

Characterized against the real service (`api.state.dbt.com`, jaffle-shop,
Snowflake `prod_demo` baseline, selector queried from `dev_clone`), reproduced
by `dbt-state-rs`; see
[`golden/fixtures/selector_state_selection.jsonl`](../golden/fixtures/selector_state_selection.jsonl),
`experiments/selector_experiment.sh` and `crates/harness/tests/selector_conformance.rs`.

The server does a **pure comparison** of the client-supplied `DbtNodeData`
hashes against the node's recorded state (keyed by org / `project_id` / `target`
env / `node_unique_id`) — it records the hashes verbatim on every
Submit/Record and never recomputes. For a submitted node whose recorded state is
`rec` (or absent → the node is *new*):

- `new` ⇔ `rec` absent; `old` ⇔ `rec` present (`new`/`old` partition the sent set).
- `modified.body/configs/relation/persisted_descriptions/macros/contract` ⇔
  `rec` absent **or** that component hash differs. A *new* node matches every
  `modified.*`.
- `modified` ⇔ `rec` absent **or** any of body/configs/persisted_descriptions/
  macros/contract differs. **`relation` is EXCLUDED from `modified`** (a
  database/schema/alias difference across environments is not a content change).
- `unmodified` ⇔ `old` and not `modified` (`modified`/`unmodified` partition the
  sent set).

Only node types dbt State records (models, seeds, snapshots, data tests) ever
have recorded state; node types it never tracks (sources, unit tests, semantic
models, metrics, saved queries) are always returned as `new`/`modified`.
`node_body_hash` is a raw file hash, so whitespace/comment-only edits DO count as
`modified.body` (the selector is file-hash based, unlike the semantic reuse
decision). An unknown/`SC_UNSPECIFIED` criterion or an empty `project_id` is
rejected with `INVALID_ARGUMENT`.

The server records node state in a dedicated `node_states` table (migration
0009), separate from the `executions` decision ledger, keyed by the logical
(org, project_id, target env, node_unique_id) the selector scopes by.

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
Other dialects: BigQuery `CREATE TABLE ... CLONE ...`; Databricks/Spark
`CREATE TABLE ... SHALLOW CLONE ...` only for an explicitly identified Delta
source. Redshift and unknown dialects return UNIMPLEMENTED. Non-Snowflake
variants follow published syntax but are not verified against the real service
or executed against those warehouses. Requests carrying retention properties
return UNIMPLEMENTED until those properties can be honored.

### Clone eligibility, rejection and lifecycle (live-verified 2026-10-11)

- **Eligibility splits by materialization.** Incremental models and snapshots
  clone via `RegisterClone` (the `pre_clone` path). Table materializations clone
  via `SubmitEnrichedSQL` → `ready_to_clone` (see below). Clones require profile
  `defer_to_target` and the dev object absent.
- **`allow_clones: false`** is encoded by the client as
  `clone_chain_depth_limit = 0`. The service answers `unable_to_clone`
  (`UnableToCloneResponse{clone_source, clone_target, ExplainedDecision{decision=1
  (READY_TO_EXECUTE), clone_rejection_reason=10 (CLONE_CHAIN_LIMIT_EXCEEDED),
  decision_description=""}}`); the deprecated top-level oneof is left empty and no
  history is written. The client then builds normally.
- **Lifecycle.** After `RegisterClone` + `ConfirmExecution`, a later submit for
  the cloned target SKIPS when its schema-abstracted logic matches (and executes
  on a genuine logic change). See `decision_description` wording in the SKIP/
  EXECUTE sections above.

### SQL-submit cross-name clone (`SubmitEnrichedSQL` → `ready_to_clone`)

A `SubmitEnrichedSQL` for a target that does not yet exist, when an equivalent
model (same `table_namespace` + schema-abstracted logic + `execution_type`) was
already built under a DIFFERENT physical name, returns `ready_to_clone` cloning
that equivalent in: `ExplainedDecision{decision=3 (READY_TO_CLONE),
skip_rejection_reason=1 (TARGET_TABLE_MISMATCH), clone_rejection_reason=null,
decision_description="an equivalent model exists under another name so we cloned
that one"}`, with `clone_required_last_modified_epoch` = the source's build epoch
and `execution_runtime_ms` echoing the source's runtime.

When the submit target FORBIDS clones (the client sends
`clone_chain_depth_limit = 0` on the submit — NOT `allow_clones`, which stays
unset), the fallback is suppressed and the service returns `ready_to_execute`
with `ExplainedDecision{decision=1, skip_rejection_reason=1
(TARGET_TABLE_MISMATCH), clone_rejection_reason=10 (CLONE_CHAIN_LIMIT_EXCEEDED),
is_stale=false, decision_description="model was executed because the maximum
number of nested clones has been reached"}` (live-verified,
`experiments/clone_experiment.sh table --no-clones`).

## Custom `Struct` type

`struct.proto` defines a `Struct`/`Value`/`ListValue` like `google.protobuf.Struct`
but with `int64` support, used for `execution_results` / telemetry config.

## Coverage

See the service table above and the complete remaining feature/testing backlog in
[correctness-handoff.md](correctness-handoff.md). Captured response examples and
experiments are useful evidence for individual scenarios; complete hosted
conformance has not been established. Telemetry, automatic SQL cloning, runtime
transformations and several policy rules remain incomplete.
