# Snowflake Volatile evaluation: the uncaptured cells — 2026-11-19

Capture batch for #202 (parent #201). It follows the
[#191 note](snowflake-volatile-evaluation-2026-11-17.md) and covers the cells
#191 left uncaptured. One live capture went through the pinned client and a
recording proxy to hosted dbt State on Snowflake `CDO_DEMO_DB.PROD_STATE_DEMO`
(and `DEV_CLONE_DEMO` for the copy). After every build the driver read back
what the Warehouse wrote. Each cell separates **Observed** facts (read from
the capture or the Warehouse) from **Inferred** rules (the likeliest hosted
policy, not proven). Findings that imply an identity or Upstream producer
change, or relax a reuse refusal, are marked **Check-in** and wait for the
maintainer (#201 check-in rule). This ticket changes no server code.

## Provenance

| | |
|---|---|
| Scenario | `experiments/capture_scenarios/volatile_cells.py` |
| Run id | `1431544d67b8` |
| Window (UTC) | 2026-11-19 14:32:04–14:35:23 |
| Fixture | `golden/fixtures/explain_volatile_cells.jsonl` (210 RPCs; 11 telemetry batches dropped) |
| dbt invocations | 3 build, 1 run-operation, 48 `dbt show`; 60 `state-probe` Explains |
| Metered submits | 60 `SubmitEnrichedSQL`; the client also sent 36 `SubmitEnrichedSQLSpeculative`, all answered `undecided` |

- Client (`client.json`): `dbt 2.0.11` at `~/.local/bin/dbt`, SHA-256
  `f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31`; every
  `ValidateClientVersion` sent `dbt_run_cache_version: "2.0.11"`. Evaluation
  on is sent as `tolerate_nondeterminism = false`.
- The capture certified complete (`capture_integrity.verify_capture`); every
  Explain step passed the driver oracle.
- Writes: per-run models, one snapshot and one stored-failures audit table in
  `CDO_DEMO_DB.PROD_STATE_DEMO`, one copy in `CDO_DEMO_DB.DEV_CLONE_DEMO`, and
  one `insert` into the per-run table `rx`. Reads used `dbt show` with dbt
  State disabled. Views were not read: a view stores no value.
- Explains are `state-probe` `GetExplainMessages` calls for the node's
  current decision, sent right after the build that decided it (the same RPC
  `dbt state explain` sends).
- Raw window, `reads.json`, `client.json`, `steps.json` and logs are out of the
  tree at `~/projects/worktrees/dbt-state-rs/notes-201/202-evidence/live1/`.
- Session time zone is UTC. 44 of 44 clock values lie between the submit's
  `x-submitted-at-epoch` and the proxy's `recorded_at` for the response.

Rounds: 1 = first build of all 30 nodes; 2 = rerun of 29 nodes after `rq`,
`ro` and `rt` changed and a row was inserted into `rx`; 3 = `pc` built in
`dev_clone`. Wire names as in the #191 note (decision 0 SKIP, 1 EXECUTE,
3 CLONE; reasons 6 = `NO_SUITABLE_MATCH_FOUND`; marker 1/2/3 =
SUCCESS/FAIL/INFO; badges 1 FRESH, 4 NODE_QUERY_CHANGED, 5
NODE_QUERY_UNCHANGED, 7 UPSTREAM_QUERY_UNCHANGED, 8 TARGET_TABLE_EXISTS).

## Entry index

S = `SubmitEnrichedSQL`, C = `ConfirmExecution`, E = `GetExplainMessages`
(round 1 / round 2). Eval = evaluation requested.

| Node | What | Eval | S | C | E |
|---|---|---|---|---|---|
| vb | deterministic table, upstream of the tests and eu/xi/xs/edu | off | 30 / 130 | 51 | 79 / 176 |
| fct | `current_time` (parenless) | on | 23 / 139 | 40 / 156 | 80 / 177 |
| flt | `localtime` (parenless) | on | 28 / 138 | 46 / 155 | 81 / 178 |
| fts | `current_timestamp(3)` | on | 21 / 135 | 37 / 152 | 82 / 179 |
| fra | `uniform(1, 10, random())`, `randstr(5, 1234)`, `normal(5, 2, 42)` | on | 33 / 134 | 50 / 151 | 83 / 180 |
| frb | `uniform(1, 100, 1234)`, `randstr(8, random())`, `normal(0, 1, random())` | on | 36 / 131 | 56 / 150 | 84 / 181 |
| fcu | `current_user()` | on | 25 / 129 | 43 | 85 / 182 |
| fcx | `current_role()`, `current_warehouse()`, `current_database()` | on | 31 / 136 | 48 | 86 / 183 |
| fcq | `current_session()`, `current_statement()`, `last_query_id()` | on | 20 / 128 | 45 | 87 / 184 |
| vw | view calling `current_timestamp()` (the volatile dependency) | off | 22 / 127 | 39 | 88 / 185 |
| cmb | table over `ref(vw)` calling `uuid_string()` itself | on | 59 / 159 | 68 / 164 | 89 / 186 |
| ctv | data test over `ref(vw)`, no function of its own | on | 52 / 144 | 57 / 157 | 105 / 201 |
| cto | data test calling `current_timestamp()` | off | 60 / 146 | 63 | 106 / 202 |
| cdo | table over `ref(vw)` | off | 58 / 158 | 66 | 90 / 187 |
| cvo | view calling `current_timestamp()` | on | 19 / 141 | 38 / 153 | 91 / 188 |
| xi | append incremental (type 2) over `ref(vb)`, `current_timestamp()` | on | 71 / 167 | 73 / 173 | 92 / 189 |
| xs | snapshot (type 7) of `ref(vb)` with a `current_timestamp()` column | on | 69 / 165 | 76 / 175 | 93 / 190 |
| xc | custom materialization (type 11), `current_timestamp()` | on | 24 / 143 | 41 / 160 | 94 / 191 |
| xt | `store_failures` data test (warn, 1 failing row), `current_timestamp()` | on | 61 / 145 | 67 / 163 | 107 / 203 |
| rq | table, `current_timestamp()`; query changes in round 2 | on | 27 / 142 | 44 / 161 | 95 / 192 |
| rx | table, `current_timestamp()`; row inserted before round 2 | on | 26 / 137 | 42 / 154 | 96 / 193 |
| ro | table, `current_timestamp()`; query changes in round 2 | off | 34 / 133 | 55 / 149 | 97 / 194 |
| rt | data test, `current_timestamp()`; logic changes in round 2 | on | 62 / 148 | 65 / 162 | 108 / 204 |
| eu | table over `ref(vb)`, `current_timestamp()` | on | 70 / 166 | 75 / 171 | 98 / 195 |
| vwd | deterministic view | off | 32 / 132 | 49 | 99 / 196 |
| eq | table over `ref(vwd)`, `current_timestamp()` | on | 53 / 147 | 64 / 169 | 100 / 197 |
| ecu | table, `current_timestamp()`, `compare_unrendered_code: true` | on | 35 / 140 | 54 | 101 / 198 |
| edu | table over `ref(vw)` joined to `ref(vb)`, no function of its own | on | 72 / 168 | 74 / 170 | 102 / 199 |
| pc | table, `current_timestamp()`; prod, then dev (round 3) | off | 29 / 207 | 47 / 208 | 103 / 209 |
| lz | table over `ref(eu)`, Lag tolerance 0s | off | 77 / 172 | 78 / 174 | 104 / 200 |

**Observed, every first build** (all 30): `ready_to_execute`, decision 1,
reasons 6/6, `is_stale: false`, "model was executed because its table did not
exist" ("snapshot was executed …" for xs; data tests "data test was executed
because it has no prior execution or its query changed"). Model and snapshot
first-build Explains print only the two-line table analysis and no volatile
lines (#191 cell 8 holds for views, incrementals, snapshots and custom nodes).
The map is filled on the first build in every cell where a function renders.

## Call forms and functions outside the detector set (#203)

**Observed.**

| SQL | Key | Value (round 1 / round 2) | Rerun |
|---|---|---|---|
| `current_time` (parenless) | `CURRENT_TIME` | `14:32:10.945853` / `14:33:51.043509` | EXECUTE 6/6 |
| `localtime` (parenless) | `CURRENT_TIME` | `14:32:11.039290` / `14:33:51.034169` | EXECUTE 6/6 |
| `current_timestamp(3)` | `CURRENT_TIMESTAMP(3)` | `2026-11-19 14:32:10.951024` (6 fractional digits, no offset) | EXECUTE 6/6 |
| `uniform(1, 10, random())` | `UNIFORM(1, 10, RANDOM())` plus `RANDOM()` | `0.3241189184511579` / `0.32851968989725855` | EXECUTE 6/6 |
| `randstr(5, 1234)` | `RANDSTR(5, 1234)` | `9XCha` both runs | (same model) |
| `normal(5, 2, 42)` | `NORMAL(5, 2, 42)` | `4.711819340844143` both runs | (same model) |
| `uniform(1, 100, 1234)` | `UNIFORM(1, 100, 1234)` | `96` both runs (an integer) | EXECUTE 6/6 |
| `randstr(8, random())` | `RANDSTR(8, RANDOM())` plus `RANDOM()` | `0.62736052987072` / `0.7806433267383432` (a decimal) | (same model) |
| `normal(0, 1, random())` | `NORMAL(0, 1, RANDOM())` plus `RANDOM()` | `0.5431403180470619` / `0.6677621253055257` | (same model) |
| `current_user()` | none (`{}`) | — | **SKIP**, "model was a no-op because both its query and its upstream data are up to date", runtime 9404 |
| `current_role()`, `current_warehouse()`, `current_database()` | none | — | SKIP, runtime 10828 |
| `current_session()`, `current_statement()`, `last_query_id()` | none | — | SKIP, runtime 9773 |

- frb holds one `RANDOM()` key for its two nested `random()` calls.
- Rerun Explains (177–181) have the #191 cell 2 shape: "non-deterministic
  functions detected in query" [INFO] with one value line per key, "the model
  query is not deterministic and this is not tolerated by config" [INFO], "the
  model query has changed" [FAIL] <4>. Value lines are in SQL pre-order, an
  outer call before its nested call, each key once: fra prints `UNIFORM(1, 10,
  RANDOM())`, `RANDOM()`, `RANDSTR(5, 1234)`, `NORMAL(5, 2, 42)`; frb prints
  `UNIFORM(1, 100, 1234)`, `RANDSTR(8, RANDOM())`, `RANDOM()`, `NORMAL(0, 1,
  RANDOM())`. The map's JSON key order differs between the two runs, so it
  carries no meaning.
- The SKIP Explains (182–184) have no query-analysis volatile lines: "the
  model query has not changed" [SUCCESS] <5>.
- Warehouse: fct wrote `14:32:12.482`, fts `14:32:11.507`, fra `U = 5, R =
  E5tav, N = 6.192819091228834`, frb `U = 66, R = 9jzuNgk8, N =
  -0.334577336275027`. The SKIPs kept their round-1 values (`fcq.A` the same
  session id both reads).

**Inferred.**

- Hosted prints a function's arguments as written, in upper case, and folds
  only the aliases #191 found. `current_timestamp(n)` keeps its argument in
  the key but renders the same microsecond value as `CURRENT_TIMESTAMP()`.
- A call whose generator is `random()` renders as a fresh decimal in [0, 1),
  whatever the outer function (`RANDSTR`, `NORMAL`, `UNIFORM`). A constant
  generator renders a stable value: `RANDSTR(5, 1234)` is the prefix of #191's
  `RANDSTR(8, 1234)` = `9XChaf68` (Snowflake's own `E5tav` is likewise the
  prefix of its `E5tavqyk`), and `UNIFORM(1, 100, 1234)` renders an integer.
  Values are hosted's own, never Snowflake's.
- Context functions outside the detector set render nothing and do not block
  reuse under evaluation. Whether `uniform(1, 100, 1234)` alone blocks reuse is
  not isolated (frb also calls `random()`).

## Combinations (#204)

**Observed.**

- **Own SQL and a dependency** (cmb, evaluation on): the map has two outer
  keys, `{"<VW relation>": {"CURRENT_TIMESTAMP()"}, "<CMB relation>":
  {"UUID_STRING()"}}`, on the first build (59) and the rerun (159, EXECUTE
  6/6, "model was executed because either its query didn't match or its
  upstream data is out of date"). The rerun Explain (186) prints only the own
  block (`UUID_STRING()`), "not tolerated by config" [INFO], "the model query
  has changed" [FAIL] <4>, then "upstream model queries have not changed"
  [SUCCESS], and data freshness "upstream data is up to date" [SUCCESS]. The
  dependency's value is in the map and not in the tree.
- **Data test over a volatile view** (ctv, evaluation on): the client sends
  the view as `query_dependencies[0]` and no `tables`. The map holds only the
  dependency key (52, 144). Both executions are EXECUTE 6/6, "data test was
  executed because it has no prior execution or its query changed". Both
  Explains (105, 201) are one bare "data freshness analysis" line with no
  children.
- **Data test with evaluation off** (cto): the map holds
  `__target_query__ -> CURRENT_TIMESTAMP()` on the first execution (60) and on
  the rerun **SKIP** (146, "data test was a no-op because both its query and
  its upstream data are up to date", `execution_results` failures 0). First
  Explain (106): "query analysis" holds only the volatile block (no
  "tolerated" line, no query line), then the upstream listing. SKIP Explain
  (202): the volatile block, "the test query is not deterministic but this is
  tolerated by config" [INFO], "the test query has not changed" [SUCCESS] <5>,
  "upstream model queries have not changed" [SUCCESS], vb <FRESH> "no updates
  since test last executed".
- **Query dependency with evaluation off** (cdo): the dependency key is
  rendered on the first build (58) and on the rerun SKIP (158, runtime 1651).
  SKIP Explain (187): "the model query has not changed" [SUCCESS] <5>,
  "upstream dependencies" -> the vw relation -> "non-deterministic functions
  detected in query" [INFO] -> the value line (no "a change in rendered value
  will result in a diff" line), then "an upstream model query is not
  deterministic but this is tolerated by config" [INFO], "upstream model
  queries have not changed" [SUCCESS]; data freshness "upstream data is up to
  date" [SUCCESS]. The table kept `TS = 14:32:18.840`.
- **View with evaluation on** (cvo): the own key is rendered on the first
  build (19) and the rerun (141), and the rerun is EXECUTE 6/6 with the #191
  cell 2 tree (188).

**Inferred.**

- A Volatile function in the own SQL decides the query analysis: hosted then
  reports the node's own query as changed and does not look at the
  dependency's values. Only a node without own values gets the #191 cell 5
  dependency block.
- With evaluation off, a volatile query dependency and a volatile data test
  are reused, as #191 found for a model's own SQL.

**Check-in (cto, Observed).** Hosted's SKIP of cto echoes
`execution_runtime_ms: 2136`, the confirmed runtime of the other data test
`rt` (entry 65), not cto's own 1701 (entry 63). The two tests had identical
SQL in round 1 and differ in unique id and in `tolerate_nondeterminism`.
**Inferred:** hosted reused the latest execution of the same SQL across data
test unique ids and evaluation settings. This server scopes data-test reuse
by UID and echoes 1701. The same class of difference is documented for
Databricks (#138). It is an identity finding; keyed as a field-scoped gap on
`execution_runtime_ms` (entry 146), not adopted.

## Execution types (#205)

**Observed.**

- **Incremental** (xi, APPEND): own key rendered on the first build (71) and
  the incremental run (167, EXECUTE 6/6). The run's tree (189): the own block,
  "not tolerated by config" [INFO], "the model query has changed" [FAIL] <4>,
  "upstream model queries have not changed" [SUCCESS]; data freshness lists vb
  <FRESH> "no updates since … last executed". The Warehouse appended a second
  row (`TS 14:32:21.210` and `14:33:57.501`).
- **Snapshot** (xs): own key on both submits (69, 165). The rerun is EXECUTE
  6/6 "snapshot was executed because either its query didn't match or its
  upstream data is out of date", tree as xi with "snapshot" wording ("the
  snapshot query is not deterministic and this is not tolerated by config",
  "the snapshot query has changed"). The check strategy on `v` wrote no new
  row: the table still held the round-1 row (`TS 14:32:21.749`).
- **Custom materialization** (xc, type 11): own key on both submits (24, 143),
  rerun EXECUTE 6/6 with the #191 cell 2 tree (191).
- **Data test with stored failures** (xt): the client sends
  `target_table: null` for a `store_failures` test, and from round 2 lists its
  audit relation in `tables` (145). The map key is `__target_query__` on both
  executions, both EXECUTE 6/6, trees as #191 cell 6 (107, 203). The audit
  table held the failing row (`TS 14:32:19.363`, then `14:33:54.783`).

**Inferred.** Every execution type follows the FULL model rule: own values
rendered on every build, a rebuild on every rerun under evaluation, and the
cell 2 tree with the node's kind word.

**Limit.** A data test **with a Target relation** cannot be reached through
the pinned client on Snowflake: it sends `target_table: null` even for a
`store_failures` test (as in `explain_store_failures_audit.jsonl`). The only
captured data-test submit with a `target_table` is a crafted Databricks probe
(`databricks_existing_target_without_history.jsonl` entry 10). The cell stays
uncaptured; a crafted `state-probe` submit would not show client behavior.

## Rebuild reasons and Explain variants (#206)

**Observed.**

- **Query-changed FULL rebuild, evaluation on** (rq, 142/192) and **External
  modification, evaluation on** (rx, 137/193; the round-2 request reports the
  table at epoch 1795098826150, after the round-1 confirm at 1795098734723):
  both EXECUTE 6/6, own key rendered, and the tree is exactly the unchanged
  rerun tree. Neither names the changed query nor the modification.
- **Evaluation-off rebuild** (ro, query changed, 133/194): EXECUTE 6/6, own
  key rendered; the tree is the volatile block, then directly "the model query
  has changed" [FAIL] <4>, with no "tolerated by config" line.
- **Volatile data test logic change** (rt, 148/204): EXECUTE 6/6 with the same
  tree as an unchanged rerun (#191 cell 6).
- **Model with an upstream table** (eu, 195): the cell 2 tree plus "upstream
  model queries have not changed" [SUCCESS] and vb <FRESH>.
- **Model with a deterministic query dependency** (eq, 197): the cell 2 tree
  plus "upstream model queries have not changed" [SUCCESS] and "upstream data
  is up to date" [SUCCESS]. The dependency is not listed.
- **View, incremental**: cvo (188), xi (189) above.
- **Volatile dependency plus an upstream table** (edu, 199): "the model query
  has not changed" [SUCCESS] <5>; "upstream dependencies" lists the vw relation
  with the #191 cell 5 block (value line, then "a change in rendered value will
  result in a diff") and the vb relation with "query or upstream query
  unchanged" <7>; then "an upstream model query has changed" [FAIL]; data
  freshness lists vb <FRESH>.
- **Values of its own plus a dependency**: cmb (186) above.
- **`compare_unrendered_code`** (ecu): the rerun (140) is a **SKIP**, decision
  0, runtime 12089 (its round-1 confirm), "model was a no-op because both its
  query and its upstream data are up to date", with the map still filled
  (`CURRENT_TIMESTAMP()`). Explain (198): "the model table exists already"
  [SUCCESS] <8>; "the model was compared to state based on its unrendered code"
  [INFO], "the unrendered model code has not changed" [SUCCESS]; "no upstream
  dependencies" [SUCCESS]. No volatile lines. The table kept `TS =
  14:32:17.115`.

**Inferred.** Under evaluation hosted short-circuits the query analysis of a
volatile node: the volatile verdict replaces the query and modification
checks. Under `compare_unrendered_code` hosted compares the unrendered code
instead and never applies the volatile verdict.

**Check-in (ecu).** Hosted reuses volatile SQL with evaluation requested when
`compare_unrendered_code` is on. This server refuses it
(`reuse_candidate.rs::sql_qualifies`) and rebuilds. Matching hosted relaxes a
reuse refusal. Keyed as a whole-response gap (entry 140) and an `explain` gap
(198).

## Copy and zero Lag tolerance (#207)

**Observed.**

- **Copy** (pc, evaluation off): built in prod (29, confirm 47, runtime
  10382), then in `dev_clone` (207): `ready_to_clone`, decision 3,
  `skip_rejection_reason: 1`, `clone_rejection_reason: null`, `is_stale:
  false`, "an equivalent model exists under another name so we cloned that
  one", `execution_runtime_ms: 10382`, the zero-copy `CREATE OR REPLACE
  TRANSIENT TABLE … CLONE … COPY GRANTS`. **`ready_to_clone` carries a map**,
  keyed by the dev Target relation:
  `{"\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"PC_1431544D67B8\"": {"node_to_func":
  {"CURRENT_TIMESTAMP()": "2026-11-19 14:35:19.687966"}}}`. The client
  confirmed the copy (208). Explain (209): "the model table does not exist"
  [FAIL], "the model table could be cloned from <prod relation>" [SUCCESS];
  "query analysis (vs clone)": the volatile block, "the model query is not
  deterministic but this is tolerated by config" [INFO], "the model query has
  not changed" [SUCCESS] <5>; "data freshness analysis (vs clone)": "no
  upstream dependencies" [SUCCESS]. The dev table holds the prod value
  `TS = 14:32:15.299`; the dev render was never written.
- **Zero Lag tolerance** (lz, over eu): the rerun (172) is EXECUTE 6/6 "model
  was executed because either its query didn't match or its upstream data is
  out of date", empty map. Explain (200): "the model query has not changed"
  [SUCCESS] <5>, "upstream model queries have not changed" [SUCCESS]; eu
  <OUTDATED> "updated 2 minutes after … last executed"; "upstream data is
  outdated" [FAIL]; no Lag tolerance line. lz wrote eu's round-2 value. The
  local server replays both entries exactly.

**Inferred.** With evaluation off, hosted copies volatile SQL as any other
SQL and renders the values for the copy's own Target relation; they describe
nothing the Warehouse holds. A volatile upstream's rebuild is ordinary data
drift, so zero Lag tolerance rebuilds the consumer (ADR 0007), as #196
assumed offline.

## Replay against the current server

`cargo test -p dbt-state-harness --test conformance conformance_replay_all_corpora`
passes with 50 entries keyed in `golden/replay_gaps.json` (every other entry
replays exactly). Reasons name the ticket each gap is for.

| Kind | Entries | What differs |
|---|---|---|
| `ignore_fields: [transformed_nodes_by_query]`, `ready_to_execute` | 19, 21, 23, 24, 28, 33, 36, 52, 58, 59, 60, 69, 71, 131, 134, 135, 138, 139, 141, 143, 144, 159, 165, 167 | Hosted's map; local `{}` (no rule or uncaptured cell), or only `RANDOM()` (33, 36, 131, 134) |
| same, `skip_execution` | 158 | Hosted's dependency map on the SKIP |
| `ignore_fields: [transformed_nodes_by_query, execution_runtime_ms]`, `skip_execution` | 146 | Map, and the runtime echo of `rt` (**Check-in**, cto) |
| same, `ready_to_clone` | 207 | Hosted's map on the copy (needs the conformance clone arm to accept a field-scoped gap, added here) |
| whole response, local `ready_to_execute` | 140 | **Check-in** ecu: hosted SKIP, local failed-reuse-qualification rebuild |
| `empty_tree` | 177, 178, 179, 186, 188, 189, 190, 191, 195, 197, 199, 201 | Local rebuild renders no tree |
| `ignore_explain_lines` | 106, 180, 181, 194, 202, 204 | Only the named hosted lines are missing locally |
| `explain` | 105, 187, 198, 209 | Local tree differs beyond missing lines |

Entries replaying exactly include the stored-failures test (61, 145, 107,
203), rq/rx/ro submits and the rq/rx trees (192, 193), eu/eq/edu submits,
the deterministic and context-function SKIPs, and lz (172, 200).

## Check-in findings

1. **ecu** (#206): hosted SKIPs volatile SQL under evaluation when
   `compare_unrendered_code` is on. Matching relaxes a reuse refusal.
2. **cto** (#204): hosted's data-test SKIP echoes another data test's runtime
   (same SQL, other unique id and evaluation setting). Matching would widen
   data-test reuse identity beyond the scoped UID.

No other cell implies an identity or Upstream producer change: every other
decision, code and description equals this server's, and the differences are
the map and Explain text.
