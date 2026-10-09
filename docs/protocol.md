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
| `SQL` | `SubmitEnrichedSQL`, `SubmitValues`, `SubmitEnrichedSQLSpeculative` | the verdict | implemented |
| `Execution` | `ConfirmExecution`, `RecordExecutions`, `ResolveDeferredRelations` | writes state / deferral | implemented (defer = empty) |
| `Clone` | `RegisterClone` | clone decision + DDL | implemented |
| `ClientValidation` | `ValidateClientVersion` | version gate | implemented (true) |
| `Explain` | `GetExplainMessages`, `GetUpstreamDependencyChanges` | human text | empty defaults |
| `SelectorService` | `GetStateSelection` | state:modified selectors | empty default |
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
  upstream data are up to date"}`, `execution_results{fields:{}}`,
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

### Decision semantics (as reproduced)

Given the request and the latest matching **confirmed** record:

1. **Match key.** Prefer logical cross-environment identity:
   `table_namespace` + `node_body_hash` + `execution_type`, scoped to org. Fall
   back to physical `target_table` when no namespace is present. (The hosted
   service reuses a node built in prod to skip the same logical node in dev.)
2. **No match ⇒ EXECUTE** (`is_stale=false`, hash miss).
3. **Match ⇒ freshness check.** Each genuine upstream input is compared by
   **logical identity** (schema-stripped `catalog..table`) against the recorded
   epoch, within `freshness_tolerance_seconds`. The node's **own** target table
   is excluded (its epoch advancing because we just rebuilt it is not drift).
   - `stale_upstream_policy = ANY` (default): stale if **any** upstream drifted.
   - `stale_upstream_policy = ALL`: stale only if **every** upstream drifted.
   - No upstreams to compare ⇒ not stale (hash match alone skips).
4. **Stale ⇒ EXECUTE** (`is_stale=true`); **fresh ⇒ SKIP**.

## State writes: `Execution`

- `ConfirmExecution{request_id, last_modified_epoch?, failed_to_clone,
  table_type?, execution_results, execution_runtime_ms?, labels}` →
  `{success, request_id}`. Correlates with the `request_id` from a prior
  `ready_to_execute`/`ready_to_clone`; marks the pending row confirmed so future
  matching submits skip. Idempotent for a repeated `request_id`.
- `RecordExecutions{records[]}` → `{records_stored}`. Bypass hydration path:
  each `ExecutionRecord` has an `outcome` and a oneof input of `enriched_sql`
  (`SQLExecution`, hashes via `dbt_node_state`) or `values` (`ValuesExecution`).
  Processed atomically (all-or-nothing).
- `ResolveDeferredRelations{profile_name, target_name, project_name,
  project_id?, node_unique_ids[]}` → `{fqn_by_unique_id{}}`. Observed real
  response: an **empty** map (the clone source came from profile-based
  auto-deferral, not server resolution).

## Seeds: `SQL.SubmitValues`

`SubmitValuesRequest{target_table, dialect, values_hash (md5 of seed bytes),
last_modified_epoch?, dbt_node_state?, …}`. Decision mirrors
`SubmitEnrichedSQL` but matches on `values_hash` instead of `node_body_hash`;
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

(the kind — `TRANSIENT TABLE`/`TABLE`/`VIEW` — follows `clone_source_table_type`).
Other dialects: BigQuery `CREATE OR REPLACE TABLE ... CLONE ...`, Databricks/Spark
`... SHALLOW CLONE ...`, else CTAS. Non-Snowflake variants are our own templates,
not yet verified against the real service.

## Custom `Struct` type

`struct.proto` defines a `Struct`/`Value`/`ListValue` like `google.protobuf.Struct`
but with `int64` support, used for `execution_results` / telemetry config.

## Coverage

See the "Our status" column above. Causally-reproducible skip/execute/clone and
cross-environment reuse are fully reproduced and tested against real traffic;
Explain/Selector/Deferral return the same (empty) responses the hosted service
returned in captured runs.

### Decision inputs: used vs. ignored

The decision engine (`decision::decide`) currently derives its verdict from:
`execution_type`, `node_body_hash`, the upstream `tables[]` freshness,
`freshness_tolerance_seconds`, `target_table` (own-table exclusion), and
`stale_upstream_policy`. Seeds match on `values_hash`. The match key also
includes `table_namespace` (cross-environment reuse) and is org-scoped.

The following request fields are received and (where noted) persisted, but are
**not yet consulted** in the decision. Live differential testing against the
hosted service (see below and [testing.md](testing.md)) has now clarified the
most important case: the hosted service does **not** match on the client-sent
`node_body_hash` at all — it fingerprints the raw `sql` **semantically**
server-side. Our body-hash match is therefore *stricter* than the hosted
service, so the divergence is safe-directional (we over-execute, never serve
stale data).

| Field | Stored? | Observed hosted behavior / risk |
|---|---|---|
| `node_body_hash` | yes (match key) | hosted service ignores it; uses its own SQL semantic fingerprint. We over-execute on semantically-equivalent SQL changes (see C1 below). |
| `node_configs_hash` | yes | changed by a `config()` edit; hosted service still skipped — not a logic gate on its side |
| `node_contract_hash` | yes | stable across the config/logic edits observed |
| `node_macros_hash` | no | not observed to change a verdict |
| `node_persisted_descriptions_hash` | no | not observed |
| `tolerate_nondeterminism` | no | no verdict change observed in fuzzing |
| `ignore_external_modifications` | no | no verdict change observed in fuzzing |
| `compare_unrendered_code` | no | no verdict change observed in fuzzing |
| `lenient_dependencies[]` | no | no verdict change observed in fuzzing |

### C1 (LIVE-VERIFIED) — hosted service fingerprints SQL semantics, not `node_body_hash`

Captured live from `api.state.dbt.com` (jaffle-shop `customers` on Snowflake;
fixture `golden/fixtures/c1_config_vs_logic.jsonl`, characterized by
`crates/harness/tests/c1_probe.rs`). Four real decisions for the same node:

| build | node_body_hash | node_configs_hash | hosted decision |
|---|---|---|---|
| first | `0fbde4f2` | `ff8f1fb7` | execute |
| unchanged rebuild | `0fbde4f2` | `ff8f1fb7` | skip |
| **config-only** (`config(meta=…)`) | `a0a8af93` **(changed)** | `39ba728a` **(changed)** | **skip** |
| **genuine SQL change** (new column) | `22e8207a` **(changed)** | `ff8f1fb7` | **execute** |

`node_body_hash` changed in BOTH the config-only and the real-logic build, yet
the hosted service skipped the former and executed the latter. So its logic
identity is a **server-side semantic fingerprint of the raw `sql`**, not the
client hash. (`table_namespace` is an adapter/connection-level id —
`get_adapter_unique_id()` — shared by all nodes, so it is a coarse scope, not a
per-node key.)

Our server keys the match on `node_body_hash`, which is stricter: we EXECUTE on
a config-only change the hosted service SKIPs (and we agree on genuine logic
changes). Faithfully closing this gap requires server-side SQL semantic
fingerprinting (a SQL engine), which the project deliberately omits (see
[overview.md](overview.md#the-clientserver-split-why-the-server-needs-no-sql-engine)).
The divergence is documented and regression-guarded by `c1_probe.rs`; because it
is safe-directional (over-execute, never stale), body-hash matching is retained
as the conservative behavior pending a decision on whether to add a SQL
fingerprinter.

#### C1b — the hosted service applies a per-config-key skip policy

A follow-up matrix (one config change at a time on the same node; fixture
`golden/fixtures/c1_config_semantics.jsonl`, test `c1_config_semantics.rs`)
shows the hosted service is deliberately selective about *which* config changes
are rebuild-worthy. The SELECT body is semantically identical in every case
(the client renders config out of the compiled SQL into the hashes), yet:

| config change | hosted decision |
|---|---|
| `tags` | skip |
| `meta` | skip |
| `post_hook` (`ALTER TABLE … SET COMMENT`) | **skip** |
| `post_hook` (`GRANT …`) | **skip** |
| `grants` | execute |
| `pre_hook` | execute |
| `persist_docs` | execute |

The decisive observation: a **warehouse-mutating `post_hook` is SKIPPED** while
a `grants` change executes. We verified on Snowflake that when the
`SET COMMENT` post_hook was skipped, the table comment was **not** changed — so
the hosted service silently drops the skipped hook's side effect. It is
therefore NOT conservatively protecting warehouse state; it classifies
`meta`/`tags`/`post_hook` as cosmetic (skippable) and `grants`/`pre_hook`/
`persist_docs` as material (rebuild), on top of its server-side SQL fingerprint.
Our body-hash match executes on all of these (safe-directional over-execute).

### CLONE from the `SubmitEnrichedSQL` path (characterized gap)

The hosted service can answer a plain `SubmitEnrichedSQL` with `ready_to_clone`
("an equivalent model exists under another name so we cloned that one";
`skip_rejection_reason = TARGET_TABLE_MISMATCH = 1`). Our `SQL` service returns
only SKIP/EXECUTE from that path — it never emits CLONE. This is deliberate:
whether a given submit SKIPs, EXECUTEs or CLONEs here depends on **physical
warehouse state** (which tables already exist, whether an equivalently-named
sibling exists to clone from) that the protocol does not carry. The two sibling
fixtures `clone_happy_path.jsonl` and `clone_failed_fallback.jsonl` were captured
in separate sessions and yield SKIP vs CLONE for the SAME logical fingerprint.
The exact real response shape is pinned as a golden contract by
`submit_enriched_sql_clone_fallback_is_characterized` so a future implementation
can be validated against it. `Clone.RegisterClone` (the explicit clone RPC) is
fully implemented and differentially tested.

### Not reproduced (valid empty/default responses)

Rich `Explain` text, `transformed_nodes_by_query` population,
`query_hash_metadata_info`, server-side `ResolveDeferredRelations`, and
non-Snowflake clone DDL verified against the real service. These return the same
empty/default responses observed in captured traffic.
