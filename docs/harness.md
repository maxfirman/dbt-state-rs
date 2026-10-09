# Test harness — dbt-state-rs

## Goal
Validate our Rust server against the REAL dbt State service (`api.state.dbt.com`)
using the official Fusion dbt client as the traffic generator.

## Components
### 1. Recording proxy (`crates/harness`, bin `record-proxy`) — DONE, WORKING
A local INSECURE tonic server implementing every query_cache service. The Fusion
client is pointed at it; the proxy forwards each call to the real service over TLS
with a minted Bearer token, and appends decoded request/response JSON to a golden
`.jsonl` file.

- `src/auth.rs` — parses `~/.dbt/dbt_cloud.yml` (active host + project token), does
  the dbt platform token-exchange at `https://auth.state.dbt.com/token`
  (grant_type=urn:ietf:params:oauth:grant-type:token-exchange, subject_token_type=dbt,
  subject_token=<cloud token>, dbt_hostname=<host>, client_id=2fd87cd5-...),
  caches the id_token, and extracts org_id from scope `runcache:scope:org:<ORG_ID>:...`.
  Token TTL ~900s; minter refreshes with 60s slack.
- `src/golden.rs` — append-only JSONL writer; one `GoldenEntry` per call
  {service, method, recorded_at, metadata[], request, response}.
- `src/bin/record_proxy.rs` — tonic server impls for SQL, Execution, Clone,
  ClientValidation, Explain (newtype `ProxyService(Arc<Proxy>)` to satisfy the
  orphan rule; derives Clone for tonic). Each handler clones the decoded message,
  forwards upstream with auth metadata, logs, returns the real response verbatim.

### How to run a capture
    # terminal: start proxy
    cd ~/projects/dbt-state-rs && cargo build -p dbt-state-harness
    RUST_LOG=info GOLDEN_DIR=./golden PROXY_LISTEN=127.0.0.1:50099 ./target/debug/record-proxy
    # then run Fusion dbt against it (Snowflake target — DuckDB does not trigger State):
    cd ~/projects/jaffle-shop
    DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL=127.0.0.1:50099 RUN_CACHE_API_SECURE=false \
      dbt build --profile snowflake --target analytics_dev --select <model> --skip-semantic-manifest-validation
    # To force EXECUTE: make a *semantic* SQL change (add a column). Comments alone are
    # normalized away by the server fingerprint and still SKIP. --full-refresh does NOT
    # force execute when logic+data unchanged.

### 2. Differential tester (NEXT) — replays golden requests against OUR server
Read golden `.jsonl`, feed each request into our server via an in-process tonic
client, diff the response against the recorded one. Normalize nondeterministic
fields before diff: request_id, execution_decision_id, timestamps/epochs,
execution_runtime_ms. These are the TDD assertions.

## Golden corpus (golden/fixtures/)
First capture (golden_20261008T222744...jsonl), 22 entries:
- ClientValidation.ValidateClientVersion x3  -> {is_supported: true}
- SQL.SubmitEnrichedSQL x12: 8 skip_execution + 4 ready_to_execute
- SQL.SubmitEnrichedSQLSpeculative x3
- Execution.ConfirmExecution x4 -> {success:true, request_id}

## Key protocol observations from REAL traffic
- The REAL SubmitEnrichedSQLRequest has MANY more fields than the Fusion crate proto
  (the dbt-core mirror we copied matches better). Observed request keys:
  allow_clones, clone_chain_depth_limit, clone_table_properties, clone_time_travel_limit,
  compare_unrendered_code, dbt_node_state, default_catalog, default_schema, defer_enabled,
  dialect, execution_type, freshness_tolerance_seconds, ignore_external_modifications,
  is_defer_to_profile, labels, lenient_dependencies, query_dependencies, semantic_extras,
  sql, stale_upstream_policy, table_namespace, tables, target_table, tolerate_nondeterminism.
- ExplainedDecision has a `decision_description` string field (not in the Fusion proto).
- SKIP response: response.skip_execution { explained_decision{decision:0,
  skip_rejection_reason:null, clone_rejection_reason:null, is_stale:false,
  decision_description:"model was a no-op because both its query and its upstream data are up to date"},
  transformed_nodes_by_query:{}, execution_results:{fields:{}}, execution_runtime_ms:<prev run ms>,
  execution_decision_id:<uuidv7> }
- EXECUTE response: response.ready_to_execute { request_id:<uuidv7>, last_modified_query:"",
  explained_decision{decision:1, skip_rejection_reason:6 (NO_SUITABLE_MATCH_FOUND),
  clone_rejection_reason:6, is_stale:false, decision_description:"model was executed because
  either its query didn't match or its upstream data is out of date"},
  transformed_nodes_by_query:{}, query_hash_metadata_info:null, execution_decision_id:<uuidv7> }
- ConfirmExecutionRequest { request_id (== the ready_to_execute request_id),
  last_modified_epoch, failed_to_clone:false, table_type:null, execution_results:null,
  execution_runtime_ms, labels{dbt_node_name,dbt_node_fqn,dbt_node_unique_id} }
  -> ConfirmExecutionResponse { success:true, request_id }
- request_id correlates Submit(ready_to_execute) with the later ConfirmExecution.
- execution_decision_id is a separate uuidv7 for Explain lookups.
- Enums are serialized as ints in JSON: decision 0=SKIP_EXECUTION,1=READY_TO_EXECUTE,
  3=READY_TO_CLONE; RejectionReason 6=NO_SUITABLE_MATCH_FOUND; execution_type 10=VIEW.
- Metadata headers from the real client: x-request-id, x-session-id, x-submitted-at-epoch,
  x-system-user-id, x-os-name, x-dbt-invocation-id, te, content-type, user-agent.
  (NOTE: x-dbt-invocation-id is additional vs the Fusion crate's list.)

## CLONE + deferral capture (task 10-12) — DONE
Triggered a REAL clone decision WITHOUT a dbt Cloud prod job (the managed git
repo dbt-cloud-managed-repo is inaccessible and the prod job 404s).
Instead used PROFILE-based auto-deferral: dbt State synthesizes defer nodes from
the profile `defer_to_target` (run_cache_defer.rs), so:
  1. Added an INCREMENTAL model (orders_incremental, merge strategy) — dev clone
     only fires for incremental models + snapshots (run_cache_dev_clone.rs).
  2. Set `defer_to_target: prod_demo` on the dev_clone profile target in profiles.yml.
  3. Built in prod_demo (clone source), then built in dev_clone via the proxy.
Captured golden_20261009T100637...jsonl: ValidateClientVersion, ResolveDeferredRelations,
RegisterClone -> ready_to_clone, ConfirmExecution x2, SubmitEnrichedSQL.

RegisterClone request: {target_table(dev), dialect, execution_type:3(MERGE),
clone_source_table(prod), clone_source_last_modified_epoch, clone_source_table_type:
"TRANSIENT TABLE", table_namespace}.
ready_to_clone response: {request_id, clone_sqls:[
  "\n    CREATE OR REPLACE TRANSIENT TABLE <target>\n    CLONE <source>\n    COPY GRANTS"],
  clone_source, clone_target, explained_decision{decision:3(READY_TO_CLONE),
  skip_rejection_reason:5(FORCED_NOT_ELIGIBLE), clone_rejection_reason:null,
  decision_description:""}, clone_execution_results{fields:{}}, execution_decision_id}.
CloneResponse populates BOTH ready_to_clone (field 3) AND ready_to_clone_v1 (deprecated
oneof 1) with the same payload.
=> SERVER generates the clone DDL (dialect-specific). Implemented in crates/server/src/clone.rs
   (snowflake TRANSIENT/TABLE/VIEW, bigquery CLONE, databricks SHALLOW CLONE, else CTAS).

ResolveDeferredRelations request: {profile_name, target_name(defer-to), project_name,
project_id, node_unique_ids[]} -> {fqn_by_unique_id: {}} (real returned EMPTY map; the
clone source came from profile auto-deferral, not server resolution). Our stub matches.

Differential tests (crates/harness/tests/clone.rs): register_clone_matches_real_shape
(asserts clone_sqls byte-match the real DDL, decision=3, source/target, oneof mirror) and
resolve_deferred_relations_matches_real_shape. Both green.

NOTE on live clone: forcing the Fusion client to clone against OUR server live is
state-dependent (clone-if-missing only fires when the dev table is absent). The
differential replay against real golden traffic is the authoritative proof of parity.
