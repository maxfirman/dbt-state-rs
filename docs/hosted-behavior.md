# Hosted behavior matrix

Behaviors characterized against the hosted service (`api.state.dbt.com`, dbt
Fusion client, Snowflake demo warehouse) through the recording proxy, and
whether this server reproduces them. Evidence is in
[golden/README.md](../golden/README.md), which maps each fixture to its test.
Open questions and deliberate divergences live in
[correctness-handoff.md](correctness-handoff.md); wire details in
[protocol.md](protocol.md).

## Node-type reusability

| Node type | Reusable | Match key |
|---|---|---|
| SQL model (view, table, incremental) | yes | Logic identity: normalized rendered SQL, all `semantic_extras` the client sends, and dependency definitions |
| Snapshot (et=7) | yes | Same as models (`snapshot_reuse.jsonl`) |
| Seed (`SubmitValues`) | yes | `values_hash` plus seed config (`seed_values_hash.jsonl`) |
| Data test (et=8) | yes | Scoped `node_unique_id` (generic tests share one body hash) (`c1_test_node_identity.jsonl`). A failing test is reused like a passing one. Hosted also reuses a test confirmed with incomplete or no cached results and echoes them (crafted, `explain_incomplete_test_results.jsonl`); this server requires complete results (accepted divergence #87, see [correctness-handoff.md](correctness-handoff.md#accepted-divergences)) |
| Custom materialization (et=11) | yes | Same as models; the public docs say it is never reused, but hosted skips an unchanged one (`custom_materialization_reuse.jsonl`) |
| Python model | no | Always builds |

## Reuse or rebuild

| Scenario | et | Hosted | Reproduced |
|---|---|---|---|
| Table, view, incremental, snapshot, seed or test unchanged | — | skip | ✅ |
| No prior execution | — | build | ✅ |
| View, logic unchanged, upstream advanced | 10 | skip (the client omits upstreams from `tables[]`) | ✅ |
| Model definition change (added column) | — | build | ✅ |
| Incremental `unique_key` change | 3 | build | ✅ |
| Incremental `--full-refresh` | — | no submit; client 2.0.11 sends `RecordExecutions` as FULL (`explain_incremental_append_merge_history.jsonl`) | ✅ |
| Incremental resubmitted after a `--full-refresh` run | 2, 3 | build ("the table was modified after the cached execution", judged against the last incremental build) | ✅ |
| Same SQL, other execution type (snapshot under `--full-refresh`, table ↔ append, merge ↔ delete+insert, an incremental or snapshot first built under `--full-refresh`, view ↔ custom node reading no relation), no build of the new type | — | build, 2 / 2 (`EXECUTION_TYPE_MISMATCH`), the node-query-unchanged Explain tree against the latest build (`explain_snapshot_history.jsonl`, `explain_execution_type_change.jsonl`, `explain_reason_kind_cells.jsonl`) | ✅ |
| Same SQL, view ↔ custom node over an upstream (the view reports no upstreams, the custom node does) | 10, 11 | build, 6 / 6; back to the custom node, 8 / 7 (another identity) | ❌ 2 / 2, then 6 / 6 "modified after the cached execution" (#116) |
| Custom node with `on_schema_change` set ↔ table, same SQL | 1, 11 | build, 6 / 6 or 8 / 7: the client sends only `lookback` for a custom node, so it never shares a table's identity (`explain_reason_kind_cells_custom.jsonl`) | ✅ |
| Same, back to an execution type with an older build | — | build ("the table was modified after the cached execution", judged against that older build) | ✅ |
| Same SQL, table → view → table | 1 | build, 8 / 7 (a view sends other `semantic_extras`, so its build has another identity; the older table build matches) | ✅ |
| Incremental fully refreshed (recorded FULL), then materialized as a table with the same SQL and extras | 1 | skip (the recorded row is a build) | ✅ |
| Seed content change | VALUES | build (new `values_hash`) | ✅ |
| Data test added or changed | 8 | build (new `node_unique_id`) | ✅ |
| `store_failures` data test unchanged, its audit relation (table or view, passing or failing) now in `tables` | 8 | skip; the tree lists only the tested source (`explain_data_test_history.jsonl`, `explain_store_failures_audit.jsonl`), also with the audit relation in a separate audit schema (`explain_store_failures_audit_schema.jsonl`) or under a configured alias (`explain_store_failures_audit_alias.jsonl`) | ✅ when the request names the audit relation as `default_catalog`.`default_schema`.`dbt_node_name` on Snowflake (#88; `default_schema` is the test's own schema, #106); builds under a configured alias, which no request field carries (#123) |
| Upstream newer than the build (ANY) | — | build, `is_stale=false` | ✅ |
| `require_fresh_data_from: all`, some parents unchanged | — | skip, `is_stale=true` (also for the five incrementals and a custom node, "within lag tolerance", `explain_freshness_cells.jsonl`; a snapshot cannot set the policy) | ✅ |
| `require_fresh_data_from: all`, all parents changed | — | build | ✅ |
| Upstream rebuilt by dbt with the same logic, or source changed, Build age under `lag_tolerance` | — | skip "within lag tolerance", `is_stale=true` | ✅ |
| Same, Build age over `lag_tolerance` | — | build (also insert_overwrite, delete+insert, microbatch at `0s` and a snapshot past the 45 minute default, `explain_freshness_cells.jsonl`); a non-FULL node with a current equivalent elsewhere reports it (1 / 3, 1 / 6) and its tree ends with the equivalent's Lag tolerance line (`fd_sn`; crafted, `explain_rebuild_tolerance_line.jsonl`) | ✅ |
| Back to an older build's logic while an upstream drifted, older build within `lag_tolerance` | — | build, 8 / 7, `is_stale=true`, SHAPE 7 with the Lag tolerance line of the older build's age; 8 / 7 also when an equivalent exists under another name (`explain_freshness_cells.jsonl` `fr_*`, every kind but view) | ✅ |
| Upstream reported with an unknown epoch (`null`; crafted, the client never sends it) | 1 | skip within `lag_tolerance` (`explain_unknown_epoch.jsonl`) | ❌ deliberate: the server rebuilds, unknown epochs never prove freshness (accepted divergence #113) |
| Upstream externally modified after the build the model consumed | — | build ("an upstream model query has changed") under any policy or tolerance | ✅ |
| Target relation modified after its build | — | build ("the table was modified after the cached execution"; for a view, "the view definition is newer than the cached execution") | ✅ |
| Snapshot or custom node whose Target relation was modified after its build (External modification, a later `--full-refresh`, or another materialization's build in between) | 7, 11 | skip (`explain_reason_kind_cells.jsonl` `sn_c`, `sn_cz`, `sn_e`, `sn_ez`, `vc`, `cv0`) | ❌ the server rebuilds (reuse invariant 3, #115) |
| Seed's own table externally modified, values unchanged | VALUES | skip | ❌ deliberate: the server reloads (keyed safety floor, `lag_seed_upstream.jsonl`) |

The Lag tolerance rule is recorded in
[ADR 0007](adr/0007-lag-tolerance-and-upstream-producers.md) and pinned by
`crates/harness/tests/lag_tolerance.rs`.

## SQL and config equivalence

The server's logic identity hashes normalized rendered SQL, all
`semantic_extras` the client sends (sorted), the default catalog and schema,
and each dependency's name and definition. It ignores `node_body_hash`. SQL
normalization depends on the warehouse (`crates/server/src/sql_identity.rs`
`normalized_sql`):

- **Snowflake**: AST canonicalization in `crates/server/src/sql_norm.rs`
  ([sql-normalization.md](sql-normalization.md)). A schema-abstracted variant
  keys cross-target result copy.
- **DuckDB**: a validated formatting subset, otherwise exact SQL
  ([duckdb-identity-and-copy.md](duckdb-identity-and-copy.md)).
- **Other dialects**: exact SQL, and no automatic cross-target reuse.

The effective config allowlist is therefore the client's choice of keys to put
in `semantic_extras`. Config the client sends (such as `grants`, contract
enforcement, `unique_key`, persisted docs) rebuilds; config it does not send
(`meta`, `tags`, `pre_hook`, `post_hook`, contract `data_type`) does not.

| Change | Hosted | Reproduced |
|---|---|---|
| Comments, whitespace, case, optional `AS`, `!=`/`<>`, `::`/`CAST`, type and function synonyms | skip | ✅ (Snowflake) |
| Config `meta`, `tags`, `pre_hook`, `post_hook` | skip | ✅, except `c1_config_vs_logic.jsonl`, where the dependency definition also changes from SELECT text to view DDL and the server rebuilds (keyed gap) |
| Contract `data_type` (contract hash only) | skip | ✅ |
| Config the client sends in `semantic_extras` | build | ✅ |
| `env_var` or `--vars` change that alters rendered SQL | build | ✅ |
| `compare_unrendered_code=true` with only the template or only the rendered SQL changed | skip | ✅ |

Hosted decisions for config the client does not send are not a pure function of
the request: `c1_config_semantics.jsonl` recorded a `pre_hook` rebuild that a
clean-baseline retest did not reproduce. The server implements the
reproducible skip.

## Result copy (`CLONE`)

| Scenario | Hosted | Reproduced |
|---|---|---|
| Incremental | copy via `RegisterClone` | ✅ |
| Incremental, equivalent under another name, on submit (client 2.0.11, `explain_incremental_copy_over.jsonl`, `explain_incremental_submit_copy.jsonl`) | build, srr=1 / crr=6, or crr=2 when the newest equivalent has another execution type | ✅ (FULL-only submit copy, #101); into an absent dev relation srr=6 ❌ (the server finds no equivalent, #107) |
| Incremental (MERGE) over an existing target, on submit (client 2.0.6, `clone_failed_fallback.jsonl`, `clone_submit_equivalent_fallback.jsonl`) | `ready_to_clone` | ❌ deliberate: the server follows 2.0.11 and builds (#101 question 3) |
| Snapshot, dev target missing, under auto-deferral (client 2.0.11, `explain_snapshot_register_clone.jsonl`) | copy via `RegisterClone` (execution type 7, 5 / -) | ✅ copy and the post-copy SKIP (#93) |
| Post-copy submit after the dev-clone pre-step: the source build's SQL over a fixed prod source, a ref'd table or a ref'd view resolved per target (client 2.0.11, `explain_post_copy_identity.jsonl`) | skip, with the source build's `execution_runtime_ms` | ✅ (the submit is read in the copy source's schema, #93) |
| Table or view whose SQL reads a fixed prod-schema source, dev target missing, prod equivalent exists (client 2.0.11, `explain_post_copy_identity.jsonl` entries 18–19) | table: `ready_to_clone` (srr=1); view: build 1 / 3 | ❌ no equivalent found: build 6 / 6 (#107) |
| Snapshot with `target_schema=target.schema` (client 2.0.11, `explain_copy_probes.jsonl`) | no `RegisterClone` (the deferred relation is the dev one); submit answered build, srr=1 / crr=3 EXECUTION_TYPE_NOT_FULL | build ✅; codes 1 / 3 when the server finds the equivalent (#92), 6 / 6 here ❌ (it finds none, #107) |
| View, equivalent exists under another name (client 2.0.11, `explain_copy_probes.jsonl`) | build, srr=1 / crr=3 EXECUTION_TYPE_NOT_FULL; never copied | build ✅; codes 1 / 3 when the server finds the equivalent (#92), 6 / 6 here ❌ (it finds none, #107) |
| Seed, same values under another name (client 2.0.11, `explain_copy_probes.jsonl`) | `SubmitValues` → `ready_to_clone` (srr=1), "an equivalent seed exists under another name so we cloned that one" | ❌ the server loads it (#91) |
| Table | copy via `SubmitEnrichedSQL` → `ready_to_clone` | ✅ |
| Custom materialization (et=11), equivalent exists under another name | build, codes 1 / 3 (`EXECUTION_TYPE_NOT_FULL`) (`explain_custom_copy.jsonl`) | ✅ (#86) |
| Register, confirm, resubmit the copied target relation with the same logic | skip | ✅ |
| Resubmit the copy with changed logic | build | ✅ |
| Post-copy submit after the dev-clone pre-step with other logic: the five strategies and a snapshot (client 2.0.11, `explain_copy_logic_mismatch.jsonl`) | build, 6 / 6, SHAPE 7 against the copy | ✅ (#104) |
| A later table, view or custom node over an incremental's RegisterClone copy (materialization changed; client 2.0.11, `explain_copy_logic_mismatch.jsonl` `kf4`, `kv4`, `kc4`) | build: 2 / 2 and the SKIP-shaped tree when the logic and semantic extras match (the table); otherwise 6 / 6 and SHAPE 7 against the copy | ✅ (#104) |
| A submit with other logic over a RegisterClone copy whose Target relation was dropped, every model kind (crafted, `explain_copy_logic_crafted.jsonl` `x11`–`x19`) | build, 6 / 6, "its table did not exist", first-build tree | ✅ (#104) |
| `allow_clones: false` on a table submit, Target relation absent, equivalent exists (client 2.0.11 sends `allow_clones: false` and `clone_chain_depth_limit: 0`; `explain_copy_logic_mismatch.jsonl` `cf4`) | build, 1 / 10, "model was executed because cloning is disabled", tree names the equivalent | ✅ (#104) |
| `allow_clones: false` (`clone_chain_depth_limit=0`) on `RegisterClone` | `unable_to_clone` (crr=10, decision READY_TO_EXECUTE) | ✅ |
| FULL SQL submit, equivalent exists under another name | `ready_to_clone` (srr=1 TARGET_TABLE_MISMATCH) | ✅ |
| Same, with `clone_chain_depth_limit=0` | build (srr=1, crr=10) | ✅ |
| Same, source older than the current upstream, any tolerance | copy | ✅ |
| Same, volatile SQL with `tolerate_nondeterminism=false` | build | ✅ conservative gate only (see below) |
| Swapped dependency bindings | build | ✅ |
| Snowflake copy DDL (TABLE, TRANSIENT TABLE) | `CREATE OR REPLACE … CLONE … COPY GRANTS` | ✅ byte match |
| View source, or a dialect without copy DDL | — | rejected; no guessed DDL |

## Known gaps

- **Volatile evaluation**: `result_copy.rs::submit_lookup` suppresses the copy
  when volatile evaluation is requested and the SQL contains a volatile
  function. The server does not evaluate volatile values, so hosted
  `transformed_nodes_by_query` is a field-scoped gap
  (`clone_fallback_volatile.jsonl` in `golden/replay_gaps.json`).
- **Result copy and upstream producers**: hosted did not copy an equivalent
  whose consumed upstream had since been externally modified, then later copied
  it with `is_stale=true`; the server copies on the first submit. This
  interaction is open (see the `CHARACTERIZATION_ONLY` comments in
  `crates/harness/tests/conformance.rs`).
- **Submit-time equivalent of a model over a fixed prod-schema source**
  (#107): the Result copy fallback looks equivalents up by the submit's
  request-relative schema-abstracted logic, which differs from the prod
  build's when the fixed schema is the prod default schema. Hosted finds the
  equivalent (`explain_post_copy_identity.jsonl` entries 18–19,
  `explain_incremental_submit_copy.jsonl` entries 9–10). RegisterClone
  adoption does not have this gap (#93).
- **Incremental self-reference after a copy**: hosted skips the final resubmit
  in `clone_chain_source_age.jsonl`; the server rebuilds (pinned by
  `clone_chain_provenance.rs::registerclone_source_age_chain_exact_replay`).
  Not the #93 cause: that submit's schema already matched the copy, but it
  adds an `is_incremental()` predicate and its Target relation's epoch is
  newer than the copy's confirmation.
- **Copies seen by the #76 probes** (seed copy, and the view and snapshot
  codes where the server finds no equivalent): #91, #107, keyed in
  `golden/replay_gaps.json`.
- **Non-Snowflake copy DDL** (BigQuery, Databricks) is this server's own and
  unverified against a warehouse or hosted responses.
- **Malformed requests** (crafted only, #114): hosted rejects empty SQL, or a
  query dependency with an empty name or query, with `INVALID_ARGUMENT`, and
  SKIPs a resubmit whose query dependency is listed twice. The server builds
  each of them and rebuilds every resubmit
  (`no_reuse_candidate.failed_reuse_qualification`), keyed in
  `golden/replay_gaps.json` (`explain_malformed_requests.jsonl`).
- **Unit test reuse** is not characterized.

Deferral (`ResolveDeferredRelations`) is implemented in
`services.rs::resolve_deferred_relations` and pinned by
`crates/harness/tests/deferral.rs`.

## State selectors (`state:*`)

`SelectorService.GetStateSelection` powers dbt State `state:*` selectors. The
baseline was built in `prod_demo` and the selector queried from `dev_clone`
(`defer_to_target: prod_demo`, so `target=prod_demo`) with
`experiments/selector_experiment.sh`. The server records the client-supplied
`DbtNodeData` hashes verbatim, keyed by organization, `project_id`, `target`
and `node_unique_id`, and compares them without recomputing
(`crates/server/src/selector.rs::classify`). Pinned by
`selector_conformance.rs` and `selector_offline.rs`.

Rule per submitted node against its recorded state `rec` (absent means *new*):

| Criterion (wire value) | Matches when |
|---|---|
| `new` (2) | `rec` absent |
| `old` (4) | `rec` present |
| `modified.body` (5) | `rec` absent or `node_body_hash` differs |
| `modified.configs` (6) | `rec` absent or `node_configs_hash` differs |
| `modified.relation` (7) | `rec` absent or `node_database_representation` differs |
| `modified.persisted_descriptions` (8) | `rec` absent or that hash differs |
| `modified.macros` (9) | `rec` absent or `node_macros_hash` differs |
| `modified.contract` (10) | `rec` absent or `node_contract_hash` differs |
| `modified` (1) | `rec` absent, or body, configs, persisted descriptions, macros or contract differ (relation excluded) |
| `unmodified` (3) | `old` and not `modified` |

`new`/`old` and `modified`/`unmodified` each partition the submitted set; a new
node matches every `modified.*`. Node types dbt State never records (source,
unit test, semantic model, metric, saved query) are always `new` and
`modified`. `node_body_hash` is a raw file hash, so whitespace or comment edits
count as `modified.body`. Unknown or `SC_UNSPECIFIED` criteria and an empty
`project_id` are rejected with `INVALID_ARGUMENT`.
