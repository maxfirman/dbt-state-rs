# dbt State conformance matrix (live-verified)

Behaviors characterized against the real service (`api.state.dbt.com`, dbt Fusion
client, Snowflake demo warehouse) via the recording proxy, and reproduced by
`dbt-state-rs`. This is the current state, not a to-do list; open questions live
in [docs/correctness-handoff.md](../docs/correctness-handoff.md).

## Node-type reusability

| node type | reusable | match key |
|---|---|---|
| SQL model (view/table/incremental) | yes | rendered SQL + `semantic_extras` (all keys the client sends) |
| snapshot (et=7) | yes | same as models |
| seed (VALUES) | yes | `values_hash` + seed config |
| data test (et=8) | yes | `node_unique_id` (shared body hash per generic type) + cached result |
| custom materialization (et=11) | yes | treated like any model (docs claim "never reuse"; the real service SKIPs an unchanged one — docs are wrong) |
| Python model | no | not Snowflake-SQL; always builds |

## Reuse / rebuild decision

| scenario | et | hosted | reproduced |
|---|---|---|---|
| table/view/incremental/snapshot/seed/test unchanged | — | skip | ✅ |
| no prior execution | — | execute | ✅ |
| view, logic unchanged, upstream advanced | 10 | skip (client omits upstreams from `tables[]`) | ✅ |
| view/model definition change (added column) | — | execute | ✅ |
| incremental `unique_key` change | 3 | execute | ✅ |
| incremental `--full-refresh` | — | client bypasses state (no submit) | ✅ |
| seed content change | VALUES | execute (new `values_hash`) | ✅ |
| data test add/changed | 8 | execute (new `node_unique_id`) | ✅ |
| upstream newer than own build (ANY) | — | execute, `is_stale=false` | ✅ |
| `require_fresh_data_from: all`, some parents fresh | — | skip, `is_stale=true` (tolerated-stale) | ✅ |
| `require_fresh_data_from: all`, all parents drifted | — | execute | ✅ |

## SQL / config equivalence (match key)

The server fingerprints whitespace-normalized **rendered SQL** (schema-abstracted
for cross-environment reuse) plus a hash of **all `semantic_extras` the client
sends** (sorted). It ignores `node_body_hash` for reuse. The effective "config
allowlist" is therefore the CLIENT's choice of which config keys to place in
`semantic_extras`: config that changes a key the client sends (e.g. `grants`,
contract enforce, `unique_key`, `pre_hook`) rebuilds; config the client does not
send (`meta`, `tags`, `post_hook`, contract `data_type`) does not. See
[SQL_NORMALIZATION.md](SQL_NORMALIZATION.md) for the SQL canonicalization.

| change | hosted | reproduced |
|---|---|---|
| comments / whitespace / case / optional `AS` / `!=`↔`<>` / `::`↔`cast` / type+function synonyms | skip | ✅ |
| config `meta` / `tags` / `post_hook` (client omits from `semantic_extras`) | skip | ✅ |
| contract `data_type` (contract hash only; not in `semantic_extras`) | skip | ✅ |
| config in `semantic_extras` (grants, contract enforce, unique_key, pre_hook, …) | execute | ✅ |
| env_var / `--vars` change (rendered SQL changes) | execute | ✅ |
| `compare_unrendered_code=true` + env_var (template match) | skip | ✅ |

Caveat: the hosted decision for config the client does NOT send is not a pure
function of the request (rare non-reproducible EXECUTEs were observed). We
implement the reproducible-majority (skip) behavior.

## Clone (live-verified, see docs/correctness-handoff.md §A)

| scenario | hosted | reproduced |
|---|---|---|
| eligibility: incremental/snapshot | clone via `RegisterClone` | ✅ |
| eligibility: table | clone via `SubmitEnrichedSQL` → `ready_to_clone` | ✅ |
| register → confirm → re-submit cloned target (same logic) | skip | ✅ |
| re-submit cloned target with changed logic | execute (`LATEST_QUERY_HASH_NOT_MATCH`) | ✅ |
| `allow_clones: false` (`clone_chain_depth_limit=0`) | `unable_to_clone` (crr=10, decision=READY_TO_EXECUTE) | ✅ |
| SQL submit, equivalent exists under another name | `ready_to_clone` (srr=1 TARGET_TABLE_MISMATCH) | ✅ |
| clone DDL (Snowflake TABLE / TRANSIENT TABLE) | `CREATE OR REPLACE … CLONE … COPY GRANTS` | ✅ byte-match |
| clone source is a view / unknown dialect | — | UNIMPLEMENTED (rejected, no guessed DDL) |

## Known gaps / open

- `lag_tolerance` CAN gate a rebuild under ANY policy (observed), but the exact
  rule is not isolated; the engine keeps the safe own-build anchor. Handoff §C.
- Non-Snowflake clone templates (BigQuery/Databricks/Spark) are our own, unverified.
- Volatile-function evaluation, state selectors and state-backed deferral: UNIMPLEMENTED.
- `unit test` reuse: not characterized (state-explain shows "details unavailable").
