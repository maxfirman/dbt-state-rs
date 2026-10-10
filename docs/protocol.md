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

1. **Match key (logic identity).** The hosted service's logic identity is the
   **lexically-normalized rendered `sql`** plus an allowlisted subset of
   config carried in **`semantic_extras`** — NOT the client's `node_body_hash`
   (an unrendered template hash the service ignores for reuse). Verified from
   the client source (`run_cache_request.rs`) and controlled live A/B. Scoped by
   org + `execution_type`, keyed logically by `table_namespace` (cross-env) with
   physical `target_table` fallback. Our server computes a single match hash =
   `sha256(normalize_sql(sql) ++ hash(semantic_extras))` and matches on it.
   - **`semantic_extras` allowlist** (the keys that DO force a rebuild when
     changed): `on_schema_change, incremental_predicates, merge_update_columns,
     merge_exclude_columns, constraints, contract, unique_key, grants,
     event_time, sql_header, lookback, table_format`, warehouse-specific keys,
     and `__persisted_docs_hash`. Config NOT in this set — `meta`, `tags`,
     `pre_hook`, `post_hook` — does not appear in `semantic_extras` and does
     **not** force a rebuild (live-verified: `grants` → execute, `meta`/`tags`/
     hooks → skip, whitespace-only SQL edit → skip).
   - **SQL normalization is LEXER-LEVEL, not semantic.** We tested (and
     disproved) the hypothesis that the server compares DataFusion-style logical
     plans: it does NOT. It parses the SQL into a dialect AST and compares a
     canonical re-rendering. It canonicalizes names/syntax — strip
     `--`/`/* */` comments and `/*+ hints */`, collapse whitespace, case-fold
     keywords/unquoted identifiers (PRESERVING string-literal content/case and
     quoted-identifier identity), drop trailing commas/semicolons, make `AS`
     optional, unify operator synonyms (`!=` ≡ `<>`), cast shorthand (`x::t` ≡
     `cast(x as t)`), and type/function synonyms (`varchar` ≡ `text`,
     `coalesce` ≡ `nvl`) — but performs NO semantic simplification:
     `group by 1` ≠ `group by col`, redundant/precedence parens, CTE-vs-inline,
     `not(x is null)` ≠ `x is not null`, and `1` ≠ `1.0` all EXECUTE. We
     reproduce this with `sqlparser` (apache/datafusion-sqlparser-rs, Snowflake
     dialect): parse → canonicalizing AST pass → `Display`, with a lexer-level
     fallback (`normalize_sql_lexer`) when parsing fails. See `crate::sql_norm`
     and [`experiments/SQL_NORMALIZATION.md`](../experiments/SQL_NORMALIZATION.md)
     for the full evidence table and the bounded synonym-catalog gap.
   - **`compare_unrendered_code=true`** switches the SQL side to the UNRENDERED
     template (`node_body_hash`) so non-deterministic rendered values (env_var)
     don't rebuild.
   - **Data-test nodes** (`execution_type = DBT_DATA_TEST = 8`): keyed on
     **`node_unique_id`** (their body hash is identical across tests of the same
     generic type and `target_table` is empty). Via `find_confirmed_by_unique_id`.
   - **`node_body_hash` / `node_contract_hash` are NOT match gates** on their
     own. A column `data_type` change (contract hash moves, but `config.contract`
     and the SQL don't) SKIPs — because it changes neither the normalized SQL nor
     an allowlisted `semantic_extras` key.
2. **No match ⇒ EXECUTE.**
3. **Match ⇒ freshness check.** Each genuine upstream input is compared by
   **logical identity** (schema-stripped `catalog..table`) against the recorded
   epoch, within `freshness_tolerance_seconds`. The node's **own** target table
   is excluded (its epoch advancing because we just rebuilt it is not drift).
   - `stale_upstream_policy = ANY` (default): stale if **any** upstream drifted.
   - `stale_upstream_policy = ALL`: stale only if **every** upstream drifted.
   - No upstreams to compare ⇒ not stale (logic match alone skips).
4. **Stale ⇒ EXECUTE** (`is_stale=true`); **fresh ⇒ SKIP**.

> **Residual non-determinism (honest caveat).** For config NOT in the
> `semantic_extras` allowlist (meta/tags/pre/post_hook), the hosted service was
> observed to be mostly SKIP under a clean-baseline protocol, but a small number
> of captures showed EXECUTE for the same input — i.e. its decision there is not
> a pure function of the request (it also depends on warehouse/server state not
> in the protocol). We implement the reproducible-majority behaviour (treat
> non-allowlisted config as non-rebuilding). This is the one area where exact
> determinism cannot be guaranteed from the protocol alone.

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

The decision engine now derives the logic identity from the
**lexically-normalized rendered `sql`** + the allowlisted **`semantic_extras`**
(plus `execution_type`, `table_namespace`/`target_table`, upstream freshness,
and `stale_upstream_policy`). This was VERIFIED two ways: (1) the client source
(`run_cache_request.rs`) builds `semantic_extras` from a fixed config-key
allowlist, and (2) controlled live A/B against the hosted service.

| Field | Role | Verified hosted behaviour |
|---|---|---|
| `sql` (rendered) | match key (lexically normalized) | comments stripped, case-folded (keywords/unquoted idents), inter-token whitespace collapsed → skip; string-literal/semantic/token-order change → execute |
| `semantic_extras` | match key | changing an allowlisted key (grants, contract, unique_key, persist_docs, …) → execute |
| `node_body_hash` | NOT a reuse gate | changes on cosmetic config, yet hosted still skips; used only as the SQL side under `compare_unrendered_code=true` |
| `node_configs_hash` | NOT a reuse gate | changes on any config edit, incl. meta/hooks that skip |
| `node_contract_hash` | NOT a reuse gate | a column `data_type` change moves it but skips |
| `node_macros_hash`, `node_persisted_descriptions_hash` | informational | the latter surfaces via `__persisted_docs_hash` in semantic_extras |
| `tolerate_nondeterminism`, `ignore_external_modifications`, `lenient_dependencies` | no verdict change observed | — |
| `compare_unrendered_code` | match-key modifier | true ⇒ match the unrendered template instead of rendered SQL |

### C1 (LIVE-VERIFIED) — logic identity is normalized SQL + `semantic_extras`, NOT `node_body_hash`

Captured live from `api.state.dbt.com` (jaffle-shop `customers` on Snowflake;
fixture `golden/fixtures/c1_config_vs_logic.jsonl`, test
`crates/harness/tests/c1_probe.rs`). Four real decisions for the same node:

| build | node_body_hash | hosted decision |
|---|---|---|
| first | `0fbde4f2` | execute |
| unchanged rebuild | `0fbde4f2` | skip |
| **config-only** (`config(meta=…)`) | `a0a8af93` **(changed)** | **skip** |
| **genuine SQL change** (new column) | `22e8207a` **(changed)** | **execute** |

`node_body_hash` changed in BOTH the config-only and the real-logic build, yet
the hosted service skipped the former and executed the latter — proving the
body hash is not the discriminator. The `meta` edit only perturbs whitespace in
the rendered `sql` and adds no allowlisted `semantic_extras` key, so it skips;
the new column changes the normalized SQL, so it executes. **Our server now
reproduces both** (match hash = lexically-normalized SQL + semantic_extras).
This is NOT a deep semantic/AST/logical-plan fingerprint — just lexical
token-stream normalization (comments, case, inter-token whitespace) + a config
allowlist; see §"SQL normalization is LEXER-LEVEL" above.

(`table_namespace` is an adapter/connection-level id — `get_adapter_unique_id()`
— shared by all nodes, so it is a coarse scope, not a per-node key.)

#### C1b — `semantic_extras` allowlist governs config-change rebuilds

A follow-up matrix (one config change at a time, `golden/fixtures/
c1_config_semantics.jsonl`, test `c1_config_semantics.rs`) confirmed the rule:

| config change | in semantic_extras allowlist? | hosted decision | our server |
|---|---|---|---|
| `grants` | yes | execute | execute |
| `persist_docs` (→ `__persisted_docs_hash`) | yes | execute | execute |
| `contract` / `constraints` / `unique_key` | yes | execute | execute |
| `meta` / `tags` | no | skip | skip |
| `post_hook` | no | skip | skip |
| whitespace-only SQL | n/a | skip | skip |

**Correction to an earlier characterization.** An initial single-shot capture
recorded `pre_hook` → execute and framed this as a "per-config-key policy"
including a "server-side semantic SQL fingerprint." Re-testing with a clean
confirmed baseline between each edit showed `pre_hook` reproducibly **skips**
(like `meta`/`post_hook`); the earlier execute was a stateful confound. The one
genuinely non-reproducible observation (the fixture's `pre_hook` entry) is
retained as recorded traffic but is NOT asserted as our target behaviour — see
the residual-non-determinism caveat above. We implement the reproducible rule:
rebuild iff normalized SQL or an allowlisted `semantic_extras` key changed.

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
