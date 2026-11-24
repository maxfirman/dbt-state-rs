# Snowflake Volatile evaluation: constant generator and MERGE over drift — 2026-11-24

First capture batch for #211 (parent #201). It follows the
[#202 note](snowflake-volatile-cells-2026-11-19.md) and answers two questions:

1. Does a constant-generator call alone prevent reuse?
2. How does Volatile evaluation behave on a MERGE incremental when upstream
   data changes?

One live capture went through the pinned client and a recording proxy to
hosted dbt State on Snowflake `CDO_DEMO_DB.PROD_STATE_DEMO`. After every
build the driver read back what the Warehouse wrote. Each section separates
**Observed** facts (read from the capture or the Warehouse) from **Inferred**
rules (the likeliest hosted policy, not proven). A finding that relaxes a
Reuse refusal is marked **Check-in** and waits for the maintainer (#201
check-in rule).

## Provenance

| | |
|---|---|
| Scenario | `experiments/capture_scenarios/volatile_generator_merge.py` |
| Run id | `000054c3e38d` |
| Window (UTC) | 2026-11-24 00:01:10–00:02:28 |
| Fixture | `golden/fixtures/explain_volatile_generator_merge.jsonl` (47 RPCs; 9 telemetry batches dropped from the 56-RPC window) |
| dbt invocations | 3 build, 2 run-operation, 14 `dbt show`; 12 `state-probe` Explains |
| Metered submits | 12 `SubmitEnrichedSQL`; the client also sent 11 `SubmitEnrichedSQLSpeculative`, all answered `undecided` |

- Client (`client.json`): `dbt 2.0.11` at `~/.local/bin/dbt`, SHA-256
  `f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31`; every
  `ValidateClientVersion` sent `dbt_run_cache_version: "2.0.11"`.
- All five models set `state: {evaluate_volatile_sql: true}`, and every
  ordinary submit carried `tolerate_nondeterminism: false`.
- The capture certified complete (`capture_integrity.verify_capture`, 56
  RPCs); every Explain step passed the driver oracle.
- Writes: five per-run models and the per-run source table `ms` in
  `CDO_DEMO_DB.PROD_STATE_DEMO`, created by `create or replace table` and
  changed by one `update`. Reads used `dbt show` with dbt State disabled.
- Explains are `state-probe` `GetExplainMessages` calls for the node's
  current decision, sent right after the build that decided it.
- Raw window, `reads.json`, `client.json`, `steps.json` and logs are out of
  the tree at `~/projects/worktrees/dbt-state-rs/notes-211/live1/`.
- Session time zone is UTC. The three `CURRENT_TIMESTAMP()` values lie
  between the submit's `x-submitted-at-epoch` and the proxy's `recorded_at`
  for the response.

Wire names as in the #202 note: decision 0 SKIP, 1 EXECUTE; reasons 6 =
`NO_SUITABLE_MATCH_FOUND`; marker 1/2/3 = SUCCESS/FAIL/INFO; badges 1 FRESH,
3 OUTDATED, 4 NODE_QUERY_CHANGED, 5 NODE_QUERY_UNCHANGED, 8
TARGET_TABLE_EXISTS.

## Design

| Node | Kind | SQL | Config |
|---|---|---|---|
| `gl` | FULL table | `select 1 as id, 'gl_<run>' as v` | evaluation on |
| `gu` | FULL table | `select 1 as id, uniform(1, 100, 1234) as u` | evaluation on |
| `gr` | FULL table | `select 1 as id, random() as r` | evaluation on |
| `mv` | MERGE incremental, `unique_key='id'` | `select id, v, current_timestamp() as ts from source(ms)` | evaluation on, `lag_tolerance: 0s` |
| `mc` | MERGE incremental, `unique_key='id'` | `select id, v, '2026-01-01 00:00:00'::timestamp_ltz as ts from source(ms)` | evaluation on, `lag_tolerance: 0s` |

`ms` is a source table the run creates (`select 1 as id, 'a' as v`), so it
has no Upstream producer: changing it is upstream data drift, not an
External modification of a dbt-built relation. Both MERGE models have zero
Lag tolerance so the control rebuilds on drift instead of reusing within the
45-minute default. No model's SQL or config changes between runs.

Rounds: 1 = first build of all five; 2 = unchanged rerun of all five; 3 =
`update ms set v = 'b' where id = 1`, then a rerun of `mv` and `mc`.

## Rows

S = `SubmitEnrichedSQL`, C = `ConfirmExecution`, E = `GetExplainMessages`
(fixture entries). "Map" is `transformed_nodes_by_query`, keyed by the
node's Target relation. "Read back" is the row `dbt show` read after the
round. The `ms` epoch is the `last_modified_epoch` the submit reported for
the source.

| Node | Round | S | C | E | Decision, codes | Description | Map | Read back | `ms` epoch |
|---|---|---|---|---|---|---|---|---|---|
| gl | 1 | 6 | 12 | 16 | EXECUTE, 6 / 6 | "model was executed because its table did not exist" | `{}` | `V = gl_000054c3e38d` | — |
| gl | 2 | 30 | — | 34 | SKIP, – / –, runtime 5287 (C 12) | "model was a no-op because both its query and its upstream data are up to date" | `{}` | `V = gl_000054c3e38d` (kept) | — |
| gu | 1 | 9 | 11 | 17 | EXECUTE, 6 / 6 | "… its table did not exist" | `UNIFORM(1, 100, 1234) -> 96` | `U = 66` | — |
| gu | 2 | 28 | — | 35 | **SKIP**, – / –, runtime 4826 (C 11) | "… both its query and its upstream data are up to date" | `UNIFORM(1, 100, 1234) -> 96` | `U = 66` (kept) | — |
| gr | 1 | 10 | 15 | 18 | EXECUTE, 6 / 6 | "… its table did not exist" | `RANDOM() -> 0.6111945338547795` | `R = 6337935761198095612` | — |
| gr | 2 | 27 | 32 | 36 | EXECUTE, 6 / 6 | "model was executed because either its query didn't match or its upstream data is out of date" | `RANDOM() -> 0.3976081167068446` | `R = -3399785292037573726` | — |
| mv | 1 | 7 | 14 | 19 | EXECUTE, 6 / 6 | "… its table did not exist" | `CURRENT_TIMESTAMP() -> 2026-11-24 00:01:15.584078` | `V = a`, `TS = 00:01:16.995` | 1795478465632 (00:01:05.632) |
| mv | 2 | 31 | 33 | 37 | EXECUTE, 6 / 6 | "… either its query didn't match or its upstream data is out of date" | `CURRENT_TIMESTAMP() -> 2026-11-24 00:01:42.090251` | `V = a`, `TS = 00:01:44.935` | 1795478465632 |
| mv | 3 | 41 | 43 | 45 | EXECUTE, 6 / 6 | same | `CURRENT_TIMESTAMP() -> 2026-11-24 00:02:16.869513` | `V = b`, `TS = 00:02:19.734` | 1795478527299 (00:02:07.299) |
| mc | 1 | 8 | 13 | 20 | EXECUTE, 6 / 6 | "… its table did not exist" | `{}` | `V = a`, `TS = 2026-01-01 00:00:00` | 1795478465632 |
| mc | 2 | 29 | — | 38 | SKIP, – / –, runtime 5403 (C 13) | "… both its query and its upstream data are up to date" | `{}` | `V = a`, `TS = 2026-01-01 00:00:00` (kept) | 1795478465632 |
| mc | 3 | 42 | 44 | 46 | EXECUTE, 6 / 6 | "… either its query didn't match or its upstream data is out of date" | `{}` | `V = b`, `TS = 2026-01-01 00:00:00` | 1795478527299 |

Every response has `is_stale: false`. The source read back `V = a` before
round 1 and `V = b` after the update.

## Explain trees

**Observed.**

- Every first-build Explain (16–20) is the two-line table analysis ("the
  model table does not exist" [FAIL], "there is no other table that the
  model could be cloned from" [FAIL]) with no volatile lines.
- `gl` rerun (34): the ordinary SKIP tree, "no upstream dependencies".
- `gu` rerun (35):
  ```
  - table analysis: the model table exists already            [SUCCESS] <8>
  - query analysis
    - non-deterministic functions detected in query           [INFO]
      - UNIFORM(1, 100, 1234) -> rendered with '96'
    - the model query has not changed                         [SUCCESS] <5>
  - data freshness analysis: no upstream dependencies         [SUCCESS]
  ```
  There is no "tolerated by config" line.
- `gr` rerun (36): the #191 cell 2 tree, with `RANDOM()`, "the model query
  is not deterministic and this is not tolerated by config" [INFO] and "the
  model query has changed" [FAIL] <4>, then "no upstream dependencies".
- `mv` unchanged rerun (37): the volatile block, "not tolerated by config"
  [INFO], "the model query has changed" [FAIL] <4>, "upstream model queries
  have not changed" [SUCCESS]; data freshness lists `ms` <FRESH> "no updates
  since … last executed", then "upstream data is up to date" [SUCCESS].
- `mv` after drift (45): the same query analysis; data freshness lists `ms`
  <OUTDATED> "updated a minute after … last executed", then "upstream data
  is outdated" [FAIL]. No Lag tolerance line.
- `mc` unchanged rerun (38): the ordinary SKIP tree with `ms` <FRESH> and
  "upstream data is up to date". `mc` after drift (46): "the model query has
  not changed" [SUCCESS] <5>, "upstream model queries have not changed",
  `ms` <OUTDATED>, "upstream data is outdated" [FAIL].

## Constant-generator call alone (question 1)

**Observed.** The literal control `gl` is reused and `random()` alone (`gr`)
is rebuilt with a fresh `RANDOM()` value on each build. `uniform(1, 100,
1234)` alone (`gu`) is **reused** with evaluation requested: SKIP with the
first build's runtime, the map still filled with `96`, and the Explain
reports the query unchanged with no tolerance line. Snowflake's own value
is `66`, not `96` (as `frb` in #202).

**Inferred.**

- Hosted treats a call whose generator argument is a constant as
  deterministic for reuse, while still reporting it as a Volatile function
  with a stable rendered value. #202's `frb` rebuild came from its
  `random()` calls, not from `uniform(1, 100, 1234)`.
- The Explain form is the one Databricks shows for the checked `rand(42)`
  reuse (`databricks_volatile_priority.jsonl` entries 56, 58).
- Not established: whether `randstr(5, 1234)` or `normal(5, 2, 42)` alone,
  or other constant arguments, are reused the same way; whether the rule
  holds over upstreams or for other node kinds.

**Check-in (`gu`).** This server refuses `gu` at Reuse qualification
(`reuse_candidate::sql_qualifies`, the Volatile function detector) and
rebuilds it with the same map. Matching hosted relaxes a Reuse refusal, so
the change waits for the maintainer. Entries 28 (whole response) and 35
(Explain) are keyed in `golden/replay_gaps.json`, and
`volatile_evaluation.rs::generator_merge::snowflake_constant_generator_alone_is_still_refused_pending_check_in`
pins the current local answer.

## MERGE over upstream data drift (question 2)

**Observed.** `mv` is rebuilt on every run with codes 6 / 6, and every
build's map holds its own `CURRENT_TIMESTAMP()` value, including the first
build. The rerun after drift has the same description, codes and query
analysis as the unchanged rerun; only the freshness section differs. The
control `mc` is reused on the unchanged rerun and rebuilt after drift with
an empty map. Each submit reports the `ms` epoch the Warehouse holds:
`1795478465632` in rounds 1 and 2, `1795478527299` after the update. Both
MERGE models wrote `V = b` in round 3. Every rendered timestamp differs from
the `TS` the Warehouse wrote (the Warehouse executes the original SQL).

**Inferred.**

- A MERGE incremental follows the FULL rule under evaluation: its own
  values are rendered on every build, and its volatile SQL is refused for
  reuse whatever its upstreams did.
- Upstream drift adds no reason of its own to the decision or the query
  analysis. Explain still judges the upstream against the refused build,
  as for the FULL, APPEND and snapshot reruns of #202.

## Replay and implementation

`cargo test -p dbt-state-harness --test conformance conformance_replay_all_corpora`
replays every entry exactly except the two keyed Check-in gaps (28, 35).
Before this change the replay differed in seven entries:

| Entries | What differed | Now |
|---|---|---|
| 7, 31, 41 | `mv`'s map: hosted filled, local `{}` (MERGE was not a Snowflake cell) | rendered: `cell(Merge, true, FIRST_BUILD \| REFUSED_VOLATILE_SQL, Own)` in `warehouse/snowflake.rs` |
| 37, 45 | `mv`'s rerun trees: local empty | `explain.rs::volatile_rebuild_lines` prints the MERGE rerun tree with zero Lag tolerance over one source table, fresh or after its data drifted (`snowflake::VOLATILE_EXPLAIN_FORMS`) |
| 28, 35 | `gu`'s reuse | keyed Check-in gaps |

These are response-only changes: no decision, identity or Upstream producer
rule changed. Uncaptured neighbours keep the conservative behavior: a MERGE
rerun after drift within a Lag tolerance, over an upstream of unknown epoch,
or over a dbt-built upstream rebuilt since the refused build, is refused and
rendered but explained with no tree
(`snowflake_merge_rerun_over_uncaptured_upstream_evidence_prints_no_tree`).
The other #211 cells stay open in
[correctness-handoff.md](../correctness-handoff.md#open-hosted-questions).
