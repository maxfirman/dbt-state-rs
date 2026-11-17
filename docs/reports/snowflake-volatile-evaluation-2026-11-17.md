# Snowflake Volatile evaluation — 2026-11-17

Characterization batch for #191 (spec #67). Two live captures went through
the pinned client and a recording proxy to hosted dbt State on Snowflake
`CDO_DEMO_DB.PROD_STATE_DEMO`. After every build the driver read back what
the Warehouse wrote. Each cell below separates **Observed** facts (read
directly from the capture or the Warehouse) from **Inferred** rules (the most
likely hosted policy, not proven). Cells that would change identity, Upstream
producer semantics or a reuse refusal are marked **Check-in** and wait for
the maintainer (#67 check-in rule). This ticket changes no server code.

## Provenance

| | Run 1 | Run 2 |
|---|---|---|
| Scenario | `experiments/capture_scenarios/volatile_evaluation.py` | `experiments/capture_scenarios/volatile_evaluation_functions.py` |
| Run id | `164840c243b8` | `12054155e979` |
| Window (UTC) | 16:48:43–16:49:53 | 17:05:43–17:06:15 |
| Fixture | `golden/fixtures/explain_volatile_evaluation.jsonl` (52 RPCs) | `golden/fixtures/explain_volatile_evaluation_functions.jsonl` (49 RPCs) |
| dbt invocations | 3 build, 15 `dbt state explain`, 11 `dbt show` | 1 build, 12 `dbt show`, 12 `state-probe` explains |
| Decisions (metered submits) | 15 | 12 |

- Client (both runs, `client.json`): `dbt 2.0.11` at `~/.local/bin/dbt`,
  SHA-256 `f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31`;
  every `ValidateClientVersion` sent `dbt_run_cache_version: "2.0.11"`. This
  client sends `evaluate_volatile_sql: true` as `tolerate_nondeterminism = false`
  (observed on every submit below; an older dbt-fusion checkout passes it
  uninverted).
- Both proxy captures certified complete (`capture_integrity.verify_capture`).
- Writes: only `CREATE TABLE`/`VIEW` of the per-run models in
  `CDO_DEMO_DB.PROD_STATE_DEMO`. Reads used `dbt show` with dbt State disabled.
- Raw windows, `reads.json`, `client.json`, `steps.json` and logs are kept
  out of the tree (golden/README.md), at
  `~/projects/worktrees/dbt-state-rs/notes-67/191-evidence/run{1,2}/`.
- Session time zone is UTC (the read-backs end in `Z`), so timestamps compare
  directly.

Wire names: decision 0 = SKIP, 1 = EXECUTE. Rejection reason 6 =
`NO_SUITABLE_MATCH_FOUND`. Marker 1/2/3 = SUCCESS/FAIL/INFO. Badge 2 =
WITHIN_TOLERANCE, 3 = OUTDATED, 4 = NODE_QUERY_CHANGED, 5 =
NODE_QUERY_UNCHANGED, 8 = TARGET_TABLE_EXISTS.

## Map shape and value formats (all cells)

**Observed.**

- `transformed_nodes_by_query` is `{<outer key>: {"node_to_func": {<function
  text>: <value string>}}}`.
- Outer key: the request's `target_table` exactly as sent for a model
  (`"CDO_DEMO_DB"."PROD_STATE_DEMO"."VM_164840C243B8"`); the literal
  `__target_query__` for a data test (no Target relation); the dependency's
  `query_dependencies[].name` for a Volatile function found in a dependency
  definition (cell 5). A model with no Volatile function of its own and one in
  a dependency has only the dependency's key.
- One key per distinct function text. `current_timestamp()` written twice gives
  one `CURRENT_TIMESTAMP()` key and one value.
- The map is filled with evaluation on **and off**, on `ready_to_execute`
  **and** `skip_execution`. It is empty for SQL with no rendered function
  (cell 3, cell 7, `seq4()`, names only in text).
- Explain prints exactly the map's values for the same decision (7 of 7
  Explains with volatile lines), in the same order every time.
- Every clock value lies between the client's `x-submitted-at-epoch` and
  the proxy's `recorded_at` for that response (28 of 28 values in 18
  submits). Inside one response the clock functions carry different
  microseconds in Explain order (`CURRENT_TIMESTAMP()` …782781, `SYSDATE()`
  …782884, `CURRENT_TIME` …782956): separate clock reads.

Function text (map key) per SQL form. Run 2 has one form per model; run 1
has all forms in one model.

| SQL form | Key | Value format (observed example) | Evidence |
|---|---|---|---|
| `current_timestamp()` | `CURRENT_TIMESTAMP()` | `2026-11-17 16:48:51.782781` (UTC, 6 fractional digits, no offset) | run 1 entry 6 |
| `current_timestamp` (parenless) | `CURRENT_TIMESTAMP()` | same | run 2 entry 24 |
| `localtimestamp()` | `CURRENT_TIMESTAMP()` | same | run 2 entry 23 |
| `localtimestamp` (parenless) | `CURRENT_TIMESTAMP` (no parentheses) | same | run 2 entry 14 |
| `systimestamp()` | `CURRENT_TIMESTAMP()` | same | run 2 entry 19 |
| `getdate()` | `CURRENT_TIMESTAMP()` | same | run 2 entry 13 |
| `sysdate()` | `SYSDATE()` | `2026-11-17 16:48:51.782884+00:00` | run 1 entry 6 |
| `current_time()`, `localtime()` | `CURRENT_TIME` | `16:48:51.782956` | run 2 entries 20, 17 |
| `current_date`, `current_date()` | `CURRENT_DATE` | `2026-11-17` | run 2 entry 15; run 1 entry 6 |
| `random()` | `RANDOM()` | `0.2496468898543448` (decimal in [0, 1)) | run 1 entry 6 |
| `uniform(1, 100, random())` | `UNIFORM(1, 100, RANDOM())` **and** `RANDOM()` | `0.31728658843764224` (decimal in [0, 1), bounds ignored) | run 2 entry 18 |
| `zipf(1, 10, random())` | `RANDOM()` only | `0.46891902463269197` | run 2 entry 16 |
| `randstr(8, 1234)` | `RANDSTR(8, 1234)` | `9XChaf68` (same in all 5 renders) | run 1 entries 6, 8, 33, 34, 50 |
| `normal(0, 1, 1234)` | `NORMAL(0, 1, 1234)` | `1.0542196419272387` (same in all renders) | same |
| `uuid_string()` | `UUID_STRING()` | `8f162c59-9bda-412e-b5de-d0d22af57bf9` (lowercase, version 4) | run 1 entry 6 |
| `zipf(1, 10, 1234)` | none | — | run 1 entry 6 |
| `seq1()`, `seq2()`, `seq4()`, `seq8()` | none | — | run 2 entry 21 (`{}`); run 1 entry 6 |
| names only in a string literal, a quoted identifier (`"CURRENT_DATE"`) and a block comment | none (`{}`) | — | run 2 entry 22 |

Run 1 entry 6 (all forms) returns exactly 9 keys: `CURRENT_TIMESTAMP()`,
`SYSDATE()`, `CURRENT_TIME`, `CURRENT_DATE`, `RANDOM()`, `RANDSTR(8, 1234)`,
`UUID_STRING()`, `NORMAL(0, 1, 1234)`, `UNIFORM(1, 100, RANDOM())`.

**Inferred.**

- Hosted parses the SQL and prints each function from a canonical form:
  timestamp aliases fold into `CURRENT_TIMESTAMP()`, `LOCALTIME()` into
  `CURRENT_TIME`, and the context functions print without parentheses except
  `CURRENT_TIMESTAMP()`. The parenless `localtimestamp` printing as
  `CURRENT_TIMESTAMP` while parenless `current_timestamp` prints as
  `CURRENT_TIMESTAMP()` looks like a generator quirk; take each key from this
  table, not from a rule.
- Values are rendered by hosted itself, not by Snowflake: `RANDOM()` and
  `UNIFORM(...)` are decimals in [0, 1) while Snowflake returns a 64-bit
  integer and an integer in [1, 100]; `RANDSTR`/`NORMAL` with a constant
  generator are stable across renders but differ from Snowflake's stable
  values. The clock is UTC with microseconds.
- Explain line order is the order of first occurrence in the SQL. Observed
  only for the run 1 model, where it is consistent with that rule.
- Hosted's set of rendered functions is smaller than this server's detector:
  `ZIPF` and `SEQ1/2/4/8` render nothing (the server's detector flags them).

## Cell 1 — every Volatile function, evaluation on (`vm`, first build)

Run 1 entry 6 (submit), 12 (confirm), 19 (Explain).

**Observed.** `ready_to_execute`, decision 1, rejection reasons 6/6,
`is_stale: false`, description "model was executed because its table did not
exist", `last_modified_query: ""`, `query_hash_metadata_info: null`. The map
has the 9 keys above under the Target relation. Explain: only "table analysis"
with "the model table does not exist" [FAIL] and "there is no other table that
the model could be cloned from" [FAIL]; **no query analysis section and no
volatile lines**. Warehouse wrote `F_CURRENT_TIMESTAMP = 16:48:53.264`
(rendered 16:48:51.782781), `F_RANDOM = -6608009171017764379` (rendered
`0.2496468898543448`), `F_RANDSTR = E5tavqyk` (rendered `9XChaf68`),
`F_NORMAL = -0.6604156716424611` (rendered `1.0542196419272387`),
`F_UNIFORM = 71`, `F_ZIPF = 4`, `F_SEQn = 0`, a different UUID.

**Inferred.** The Warehouse executed the original SQL: no written value equals
a rendered one, and the written timestamps come after the render.

## Cell 2 — rerun over the existing Target relation (`vm`, evaluation on)

Run 1 entry 33 (submit), 36 (confirm), 41 (Explain). The request is identical
to entry 6 apart from `tables` now holding the model's own table.

**Observed.** `ready_to_execute`, decision 1, reasons 6/6, `is_stale: false`,
description "model was executed because either its query didn't match or its
upstream data is out of date". The map has the same 9 keys with new clock,
`RANDOM()`, `UUID_STRING()` and `UNIFORM` values; `RANDSTR`/`NORMAL` unchanged.
Explain:

```
- table analysis ("CDO_DEMO_DB"."PROD_STATE_DEMO"."VM_164840C243B8")
  - the model table exists already                         [SUCCESS] <TARGET_TABLE_EXISTS>
- query analysis
  - non-deterministic functions detected in query          [INFO]
    - CURRENT_TIMESTAMP() -> rendered with '2026-11-17 16:49:23.041056'
    - SYSDATE() -> rendered with '2026-11-17 16:49:23.041146+00:00'
    - CURRENT_TIME -> rendered with '16:49:23.041205'
    - CURRENT_DATE -> rendered with '2026-11-17'
    - RANDOM() -> rendered with '0.7961268049160215'
    - RANDSTR(8, 1234) -> rendered with '9XChaf68'
    - UUID_STRING() -> rendered with '3eef6695-a096-4e6d-a6dd-1caf00175e8b'
    - NORMAL(0, 1, 1234) -> rendered with '1.0542196419272387'
    - UNIFORM(1, 100, RANDOM()) -> rendered with '0.6569315178135932'
  - the model query is not deterministic and this is not tolerated by config   [INFO]
  - the model query has changed                            [FAIL] <NODE_QUERY_CHANGED>
- data freshness analysis
  - no upstream dependencies                               [SUCCESS]
```

The value lines have no marker and no badge. Warehouse wrote
`F_CURRENT_TIMESTAMP = 16:49:23.604` (rendered 16:49:23.041056).

**Inferred.** **Check-in.** Hosted reports "the model query has changed" for
SQL that did not change. The likeliest rule is that, when evaluation is
requested, hosted compares a query identity that includes the rendered values,
which differ on every run. The response codes and description equal this
server's failed-reuse-qualification rebuild; only the Explain text implies the
identity rule. Spec #67 keeps identity on the original SQL, so printing this
line as captured is response-only, but the maintainer should confirm that the
line may be printed without adopting the identity rule.

## Cell 3 — deterministic control, evaluation on (`vc`)

Run 1 entries 5 (first build), 21 (Explain); 31 (rerun), 43 (Explain).

**Observed.** First build as cell 1 with an empty map and the same two-line
table analysis. **The rerun is a SKIP**: `skip_execution`, decision 0,
reasons null, `is_stale: false`, description "model was a no-op because both
its query and its upstream data are up to date", `execution_runtime_ms: 5874`
(the first build's confirmed runtime), empty map. Explain: "the model table
exists already" [SUCCESS] <TARGET_TABLE_EXISTS>; query analysis "the model
query has not changed" [SUCCESS] <NODE_QUERY_UNCHANGED>; "no upstream
dependencies" [SUCCESS]. No volatile or "tolerated" lines. The table kept
`V = vc_164840c243b8`.

**Inferred.** Hosted refuses reuse only when the SQL actually renders a
Volatile function; `evaluate_volatile_sql: true` alone does not block reuse.

**Check-in.** This server rebuilds the rerun (`reuse_candidate.rs::sql_qualifies`
refuses every submit with `tolerate_nondeterminism = false`). Matching hosted
relaxes that reuse refusal. Keyed as a whole-response gap (entry 31) and an
`empty_tree` Explain gap (entry 43).

## Cell 4 — the volatile model with evaluation off (`vo`, and `vm` switched off)

`vo`: run 1 entries 8 (first build), 20 (Explain), 34 (rerun), 42 (Explain).
`vm` switched off over its evaluation-on build: entries 50, 51.

**Observed.**

- `vo` first build: as cell 1, with the same 9 keys rendered although
  evaluation is off.
- `vo` rerun: `skip_execution`, decision 0, description "model was a no-op
  because both its query and its upstream data are up to date", runtime 4971,
  **map still filled** (9 keys, fresh values). Explain: same volatile block as
  cell 2, then "the model query is not deterministic but this is tolerated by
  config" [INFO] and "the model query has not changed" [SUCCESS]
  <NODE_QUERY_UNCHANGED>; "no upstream dependencies" [SUCCESS]. The table kept
  its first-build values (`F_CURRENT_TIMESTAMP = 16:48:52.523`).
- `vm` switched off (entry 50): only `tolerate_nondeterminism` and the node
  hashes changed, not the SQL. Hosted SKIPs (runtime 5097, the round-2
  rebuild's), with the same Explain shape as the `vo` rerun and a filled map.
  The table kept the round-2 values.

**Inferred.** With evaluation off hosted keeps today's reuse rules and still
renders values, so the map is independent of the flag. A build made with
evaluation on qualifies for reuse once evaluation is off, so the flag is not
part of identity.

**Check-in (falls under the spec's example, no change implied).** Hosted
SKIPs volatile SQL with evaluation off. This server already SKIPs both cases
(entries 34 and 50 differ only in the map), so nothing is relaxed; recorded
because the check-in rule names this case.

## Cell 5 — a model whose view dependency calls a Volatile function (`vv`, `vd`)

`vv` (view, evaluation off): entries 7, 22, 32, 44. `vd` (table over
`ref(vv)`, evaluation on): entries 10, 23, 35, 45.

**Observed.**

- `vd` sends the view as `query_dependencies[0]` (`name`
  `"CDO_DEMO_DB"."PROD_STATE_DEMO"."VV_164840C243B8"`, the view SELECT as
  `query`) and has no `tables`.
- `vd` first build and rerun maps hold only
  `{"<VV relation>": {"node_to_func": {"CURRENT_TIMESTAMP()": ...}}}`.
- `vd` rerun: `ready_to_execute`, reasons 6/6, description "model was executed
  because either its query didn't match or its upstream data is out of date".
  Explain:

  ```
  - table analysis (...VD...)
    - the model table exists already                       [SUCCESS] <TARGET_TABLE_EXISTS>
  - query analysis
    - the model query has not changed                      [SUCCESS] <NODE_QUERY_UNCHANGED>
    - upstream dependencies
      - "CDO_DEMO_DB"."PROD_STATE_DEMO"."VV_164840C243B8"
        - non-deterministic functions detected in query    [INFO]
          - CURRENT_TIMESTAMP() -> rendered with '2026-11-17 16:49:23.229574'
          - a change in rendered value will result in a diff
    - an upstream model query has changed                  [FAIL]
  - data freshness analysis
    - upstream data is up to date                          [SUCCESS]
  ```

  No "tolerated by config" line. "an upstream model query has changed" has no
  badge.
- `vv` (view, evaluation off) first build renders `CURRENT_TIMESTAMP()` under
  its own relation; its rerun SKIPs with the map filled and the cell 4
  "tolerated by config" Explain shape.
- Warehouse: `vd.TS = 16:48:53.489` (dependency rendered 16:48:53.284372),
  then `16:49:24.527` after the rerun (rendered 16:49:23.229574).

**Inferred.** **Check-in.** Hosted treats a dependency whose definition
renders a Volatile function as a changed Upstream producer when the consumer
requests evaluation: the rendered value enters the dependency's query
identity ("a change in rendered value will result in a diff"). The decision
equals this server's (rebuild, same codes and description), but adopting the
reason changes Upstream producer semantics. Keyed as an `empty_tree` gap
(entry 45).

## Cell 6 — a data test with evaluation on (`vt`)

Entries 16 (first execution), 17 (confirm, `failures: 0`), 24 (Explain);
37, 38, 46 (rerun).

**Observed.** Both executions: `ready_to_execute`, reasons 6/6, description
"data test was executed because it has no prior execution or its query
changed", map `{"__target_query__": {"node_to_func": {"CURRENT_TIMESTAMP()":
...}}}`. The rerun request is identical to the first. Explain, the same shape
both times (first execution included):

```
- query analysis
  - non-deterministic functions detected in query          [INFO]
    - CURRENT_TIMESTAMP() -> rendered with '2026-11-17 16:48:56.015041'
  - the test query is not deterministic and this is not tolerated by config    [INFO]
  - the test query has changed                             [FAIL] <NODE_QUERY_CHANGED>
- data freshness analysis
  - upstream dependencies
    - "CDO_DEMO_DB"."PROD_STATE_DEMO"."VC_164840C243B8"
      - last updated: a second ago, 2026-11-17 16:48:54 UTC
```

The test has no relation; it passed both times.

**Inferred.** Data tests follow the model rule with "test" wording and the
`__target_query__` key. Unlike a model's first build, a data test's first
execution already prints the query analysis. The "test query has changed"
line carries the cell 2 check-in.

## Cell 7 — a table downstream of the volatile model, run twice (`vk`)

Entries 15, 25 (first build), 39, 47 (rerun). `vk` has a Lag tolerance of
3600 s and evaluation off; `vm` is its upstream in `tables`.

**Observed.** First build as cell 1, empty map. Rerun, after `vm` rebuilt:
`skip_execution`, decision 0, `is_stale: true`, description "model was a no-op
because its query is up to date and its data is within lag tolerance", empty
map. Explain: "the model query has not changed" [SUCCESS]
<NODE_QUERY_UNCHANGED>, "upstream model queries have not changed" [SUCCESS];
the `vm` upstream <OUTDATED> "updated 30 seconds after … last executed",
"upstream data is outdated" [FAIL], "the model was last built 28 seconds ago,
within lag tolerance of an hour" [SUCCESS] <WITHIN_TOLERANCE>. `vk` kept
`UPSTREAM_TS = 16:48:53.264`, the first `vm` value. The local server replays
this cell exactly.

**Inferred.** A volatile upstream that rebuilds is an ordinary data change for
its consumer: the Upstream producer is unchanged and Lag tolerance applies.
No change to identity or Upstream producer semantics.

## Cell 8 — `dbt state explain`

Run 1 has one `dbt state explain --verbose` per node after each build
(entries 19–25, 41–47, 51); run 2 has one `state-probe` GetExplainMessages per
model (entries 37–48). The trees are quoted in the cells above.

**Observed.** Volatile lines appear only under "query analysis": never on a
model first build (run 1 entries 19, 20, 22, 23; all 12 run 2 Explains), and
always when the submit's map is non-empty on a rebuild, a SKIP or any data
test execution. The block is "non-deterministic functions detected in query"
[INFO] with one unmarked "`<key>` -> rendered with '`<value>`'" child per map
key, followed by "the {model|test} query is not deterministic and this is not
tolerated by config" [INFO] (evaluation on) or "… but this is tolerated by
config" [INFO] (evaluation off). The dependency form (cell 5) nests the block
under the dependency name and ends it with "a change in rendered value will
result in a diff".

## Replay gaps keyed by this batch

`golden/replay_gaps.json`; every other entry of both fixtures replays exactly.

| Fixture entries | Kind | What differs |
|---|---|---|
| run 1: 6, 7, 8, 10, 16, 32, 33, 34, 35, 37, 50; run 2: 13–20, 23, 24 | `ignore_fields: ["transformed_nodes_by_query"]` | Hosted's map; local `{}`. The rest of each payload matches. |
| run 1: 31 | whole response, local `ready_to_execute` | **Check-in** cell 3: hosted SKIP, local failed-reuse-qualification rebuild. |
| run 1: 42, 44, 51 | `ignore_explain_lines` (new, `conformance.rs`) | Only the volatile block and the "tolerated by config" line are missing locally; the rest of the tree matches. |
| run 1: 24 | `explain` | Hosted's data-test query analysis section; local has only data freshness. |
| run 1: 41, 43, 45, 46 | `empty_tree` | Local failed-reuse-qualification rebuilds render no tree (43 is the cell 3 SKIP). |

`ignore_explain_lines` removes the named hosted lines (with their children)
and then requires the decision, description and remaining tree to match
exactly, and fails if no named line was present (stale exemption).

## Not captured

- A model with Volatile functions both in its own SQL and in a dependency
  (one key or two).
- `ready_to_clone` with a filled map, and Result copy of volatile SQL with
  evaluation off.
- A data test with evaluation off, and a data test over a volatile view.
- Whether hosted renders functions this server's detector does not know
  (for example `CURRENT_USER` or `SEQ` variants with arguments).
- Lag tolerance zero for the cell 7 consumer (expected to rebuild on upstream
  drift; not run).
