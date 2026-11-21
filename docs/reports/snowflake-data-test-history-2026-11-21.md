# Snowflake data-test history per Target — 2026-11-21

Live characterization for #200: which recorded execution a hosted data-test
SKIP reuses when the same tests run alternately on `prod_demo` and
`dev_clone`, and which request fields separate two histories. Each fact is
marked **Observed** (read from the capture) or **Inferred** (the likeliest
hosted rule, not proven). This note changes no server code; any change to
the data-test history key waits for a maintainer decision under reuse
invariant 1 (AGENTS.md, ADR 0007).

> **Outcome (2026-11-21).** The maintainer chose K1 (option A on #200). The
> server implements it ([ADR 0007 "Data-test history key"](../adr/0007-lag-tolerance-and-upstream-producers.md#data-test-history-key)),
> and both fixtures replay without gaps. The "Current server" columns and
> the "Replay" section below describe the server before that change.

## Provenance

| | Real-client run | Crafted one-factor probes | Crafted same-key probes |
|---|---|---|---|
| Driver | `experiments/explain_capture.py data_test_history` (`experiments/capture_scenarios/data_test_history.py`) | `experiments/data_test_history.py probes` | `experiments/data_test_history.py probes-order` |
| Run id | `1947007f67a3` | `1951045ff8d5` | `195426daf362` |
| Window (UTC) | 19:47:09–19:48:35 | 19:51:04–19:52:02 | 19:54:26–19:54:41 |
| Fixture | `golden/fixtures/data_test_history.jsonl` (111 rows; 25 telemetry batches dropped) | `golden/fixtures/data_test_history_probes.jsonl` entries 0–59 | same file, entries 60–75 |
| Metered submits (`SubmitEnrichedSQL`) | 35 | 30 | 8 |
| Other calls | 17 `SubmitEnrichedSQLSpeculative` (all `undecided`), 13 confirms, 31 `state-probe` Explains, 4 `ResolveDeferredRelations` | 17 confirms, 13 Explains | 4 confirms, 4 Explains |

- Client: `dbt 2.0.11` at `~/.local/bin/dbt`, SHA-256
  `f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31`; every
  `ValidateClientVersion` sent `dbt_run_cache_version: "2.0.11"`
  (`client.json`). The crafted requests copy the shape 2.0.11 sends for a
  singular data test (`state-probe`, no warehouse).
- Total: 73 metered submits, all on 2026-11-21. Both captures certified
  complete (`capture_integrity.verify_capture`); every real-client Explain
  step passed the driver oracle.
- Writes: per-run tables `u`, `uf` and `sh` in `CDO_DEMO_DB.PROD_STATE_DEMO`,
  copies of `u` and `uf` in `CDO_DEMO_DB.DEV_CLONE_DEMO`, and one `insert`
  into the dev `uf`. The crafted probes name never-created per-run relations
  in the two demo schemas with a made-up epoch.
- Rehearsals against this server (same scenario and probes, other run ids,
  `PROXY_LOCAL_UPSTREAM`, Postgres database `i200_rehearsal`) give the
  current server's end-to-end behaviour below. Raw windows, logs and
  rehearsal outputs are out of the tree at
  `~/projects/worktrees/dbt-state-rs/notes-200/200-evidence/`.
- `experiments/data_test_history.py table <capture>` prints, for every
  data-test submit, the decision and the confirmed execution(s) whose
  runtime a SKIP echoes. Every confirmed runtime in each capture is distinct
  (real runs by chance, crafted runs by plan), so each echo names one run.

Abbreviations: **EXEC** = `ready_to_execute`, decision 1, reasons 6/6,
"data test was executed because it has no prior execution or its query
changed". **SKIP** = `skip_execution`, decision 0, reasons null/null, "data
test was a no-op because both its query and its upstream data are up to
date". Every SKIP below has that description; `is_stale` is false throughout.
F*n* = `data_test_history.jsonl` entry *n*, P*n* = `data_test_history_probes.jsonl`
entry *n*. Explain trees:

- **T-exec**: the EXECUTE tree (data freshness listing only).
- **T-skip**: the ordinary SKIP tree: "the test query has not changed"
  <NODE_QUERY_UNCHANGED>, "upstream model queries have not changed", the
  upstream with <FRESH> "no updates since test last executed".
- **T-copy**: T-skip **without** <FRESH> and "no updates since test last
  executed"; the dev upstream's "last updated" is the prod build time
  (19:47:15 UTC), not the copy's.
- **T-cuc**: query analysis "the model was compared to state based on its
  unrendered code" [INFO], "the unrendered model code has not changed",
  "upstream model queries have not changed" (model wording, as on Databricks).

## Nodes (real-client run)

Every test has its own SQL except the deliberate pairs. `u` and `uf` are
tables built on prod (F4, F5, EXEC "did not exist") and then on dev, where
hosted **copied** them from prod (F27, F28: `ready_to_clone`, "an equivalent
model exists under another name so we cloned that one",
`clone_required_last_modified_epoch` = the prod build epoch).

| Test | Question | SQL (rendered on prod) |
|---|---|---|
| ta | Q1 | `select id, v from CDO_DEMO_DB.prod_state_demo.u_… where id < 0` |
| tf | Q3 | the same over `uf_…` |
| tr | Q3 | over `u_…`: S1 `where id < -1`, S2 `where id > 0` (fails, 1 row) |
| tc | Q4 | over `u_…` `where id < -2`, `compare_unrendered_code: true` |
| ts | Q5 | over the source `sh` (`schema: prod_state_demo`), identical on both Targets |
| xa / xb | Q6 | identical SQL over `sh`, two unique ids |
| ya / yb | Q6 | identical SQL over `sh`, evaluation off (ya) / on (yb) |

**Observed request differences** between a prod and a dev submit of one test
(F11 vs F34, F10 vs F31, F13 vs F35): `dbt_node_state.target_name`,
`default_schema` (`prod_state_demo` / `dev_clone_demo`), the relation in
`sql` and `tables`. ts (F6 vs F29) differs only in `target_name` and
`default_schema`. `project_id` (`70437463667433`), `table_namespace`
(`fc38a5ba…`), `node_hash`, `node_body_hash` and `node_configs_hash` are
the same on both Targets.

## Q1: prod, dev, prod, dev with unchanged inputs (ta)

| Step | Entry | Hosted | Echo → whose run | Explain | Current server |
|---|---|---|---|---|---|
| R1 prod | F11 | EXEC, confirmed 969 ms (F15) | — | T-exec (F18) | same (replay exact) |
| R2 dev | F34 | SKIP | 969 → prod R1 (F11) | T-copy (F36) | EXEC 6/6 (UID's latest row has the prod SQL) |
| R3 prod | F59 | SKIP | 969 → prod R1 | T-skip (F64) | EXEC 8/7 in the rehearsal (latest row is the dev run) |
| R4 dev | F76 | SKIP | 969 → prod R1 | T-copy (F81) | EXEC 8/7 |

**Observed.** Every SKIP echoes the one prod execution. The dev test never
executed: its first dev submit reused the prod run, read through the dev
copy of `u`. tf and tr (before their changes) and tc behave the same at R2
(F31 → F10, F33 → F12, F35 → F13).

**Inferred.** Each Target does not keep its own last run: hosted resolves
the dev relation to the prod relation it copied from (unchanged since the
copy), so the dev SQL is the prod SQL, and one execution serves both. This
reproduces data-test reuse across Targets (#68, closed as not reproduced);
#68 most likely failed to reproduce because its dev upstream was built, not
copied. The old 2.0.6 evidence (`golden_20261009T083754.534Z.jsonl`
entries 13–16) fits: dev `customers` was not a copy of the newly rebuilt
prod table, so hosted reused earlier dev runs of the dev SQL, as the #190
gap reasons already say.

## Q2: what separates two histories (crafted, one factor per cell)

Each cell: base submit, confirm with a runtime no other run uses, variant
submit; if the variant executes, confirm it with its own runtime and submit
the base again (cells marked ↺ then submit the variant again).

| Factor changed in the variant | Entries | Variant | Base again | Separates? | Current server (rehearsal) |
|---|---|---|---|---|---|
| `dbt_node_state.target_name` prod_demo → dev_clone | P0–P3 | SKIP, echo base 41011 | — | no (Observed) | SKIP (same) |
| `default_schema` → dev_clone_demo (SQL fully qualified) | P4–P7 | SKIP, echo base 41111 | — | no (Observed) | EXEC; base again EXEC 8/7 |
| unique id (labels, `node_unique_id`) | P8–P11 | SKIP, echo the **other** UID's 41211 | — | no (Observed) | EXEC first build ↺ each UID then SKIPs its own |
| `project_id` → `99999999999999` | P12–P15 | SKIP, echo the other project's 41311 | — | no (Observed) | EXEC; base again SKIP |
| `table_namespace` → another 32-hex | P16–P21 | EXEC (41422) | SKIP, echo base 41411 | yes (Observed) | same |
| `tolerate_nondeterminism` true → false (deterministic SQL) | P22–P25 | SKIP, echo 41511 | — | no (Observed) | SKIP (same) |
| `semantic_extras.severity` ERROR → WARN | P26–P31 | EXEC (41622) | SKIP, echo base 41611 | yes (Observed) | EXEC; base again EXEC 8/7 |
| `node_hash`, `node_body_hash`, `node_configs_hash` | P32–P35 | SKIP, echo 41711 | — | no (Observed) | SKIP (same) |
| SQL whitespace (leading newline, trailing spaces) | P36–P39 | SKIP, echo 41811 | — | no (Observed) | SKIP (same) |
| input relation: SQL and `tables` read the dev relation (failing variant) ↺ | P40–P47 | EXEC (41922, failures 1) | SKIP, echo base 41911 | yes; both histories stay reusable (P46: variant again SKIP, echo 41922, failures 1) (Observed) | EXEC; base again EXEC 8/7; variant again EXEC 8/7 |
| dev Target: relation, `default_schema`, `target_name` (relation never built or copied) | P48–P53 | EXEC (42022) | SKIP, echo base 42011 | yes (Observed) | EXEC; base again EXEC 8/7 |
| the same under `compare_unrendered_code` (same client hashes) | P54–P59 | EXEC (42122) | SKIP, echo base 42111 | yes (Observed) | EXEC; base again EXEC 8/7 |

Every crafted SKIP Explain (P3 … P53, P65–P75) is T-skip, except P59, which
is T-cuc with <FRESH>.

**Inferred.** The history key holds the test's normalized SQL with its input
relations, the test configuration that sets the result (`severity`; the other
`semantic_extras` are untested), and `table_namespace`. It does not hold the
unique id, `project_id`, `target_name`, `default_schema` (when the SQL is
fully qualified), the evaluation flag or the client node hashes. A dev
relation of the same name is another input unless hosted recorded it as a
copy (Q1 against P48–P53: the crafted dev relation had the base's epoch and
still separated).

## Q3: a failing newer run (tf, tr)

**tf** (dev `uf` gains a failing row before the dev run):

| Step | Entry | Hosted | Echo → whose run | Explain | Current server |
|---|---|---|---|---|---|
| R1 prod | F10 | EXEC, 1014 ms, failures 0 | — | T-exec | same |
| R2 dev | F31 | SKIP | 1014 → prod R1 (through the copy) | T-copy (F37) | EXEC 6/6 |
| dev after the insert | F44 | EXEC, 2220 ms, **failures 1** (F45) | — | T-exec (F46) | EXEC (same decision) |
| R3 prod | F62 | SKIP, failures 0 | 1014 → prod R1 | T-skip (F65) | EXEC 8/7 (latest row is the failing dev run) |
| R4 dev | F79 | SKIP, failures 1 | 2220 → the failing dev run | T-skip (F82) | EXEC 8/7 in the rehearsal (latest row is its R3 prod rebuild); the replay SKIPs, echo 2220, because hosted's R3 SKIP left no local prod confirm |

**tr** (dev logic S1 → S2 → S1):

| Step | Entry | Hosted | Echo → whose run | Explain | Current server |
|---|---|---|---|---|---|
| R1 prod S1 | F12 | EXEC, 1060 ms | — | T-exec | same |
| R2 dev S1 | F33 | SKIP | 1060 → prod R1 | T-copy (F38) | EXEC 6/6 |
| dev S2 | F50 | EXEC, 2137 ms, failures 1 | — | T-exec (F52) | EXEC (same decision) |
| dev S1 again | F56 | SKIP | 1060 → prod R1 | T-copy (F57) | EXEC 8/7 (latest row is S2) |
| R3 prod S1 | F63 | SKIP | 1060 → prod R1 | T-skip (F66) | EXEC 8/7 |
| R4 dev S1 | F78 | SKIP | 1060 → prod R1 | T-copy (F83) | EXEC 8/7 |

**Observed.** Prod keeps SKIPping from its own pass after a newer failing dev
run (F62). Dev then SKIPs from its own failing run (F79): the older pass,
read through the copy before the insert, is not reused, and the failure is
served. When the failing run had other SQL (tr S2), the next S1 run reuses
the older S1 pass (F56).

**Inferred.** No resurrection happens within one key: the external `insert`
broke the copy (the dev `uf` is now its own input), so tf's dev runs since
have their own key, whose latest run is the failure. Across keys, an older
run of the same unique id stays reusable after a newer run with another key
(tr S1 after S2; P30, P44, P52, P58).

## Q4: `compare_unrendered_code` (tc)

**Observed.** tc follows ta exactly: R1 prod EXEC 1150 ms (F13); R2 dev SKIP
echoing 1150 (F35), R3 prod SKIP (F61), R4 dev SKIP (F80), all echoing the
prod run. The trees are T-cuc: with T-copy freshness on dev (F39, F84),
with <FRESH> on prod (F67). Hosted prints the template comparison although
the rendered SQL also matches on prod. The crafted cell (P54–P59) separates
two Targets' relation names under `compare_unrendered_code` with the same
client hashes.

**Inferred.** `compare_unrendered_code` does not change the history key for a
data test; the dev SKIP comes from the copy, not from the template identity.

**Current server.** R2 and R4 dev EXEC; R3 prod EXEC 8/7 in the rehearsal.
Its template identity includes `default_schema`, so dev never matches prod.
On F61 (replayed SKIP) it prints the rendered-identity lines instead of
T-cuc (keyed `explain` gap, F67).

## Q5: SQL identical on both Targets (ts, over a shared source)

| Step | Entry | Hosted | Echo → whose run | Explain | Current server |
|---|---|---|---|---|---|
| R1 prod | F6 | EXEC, 1847 ms | — | T-exec (F22) | same |
| R2 dev | F29 | SKIP | 1847 → prod R1 | T-skip (F40) | EXEC 6/6 (`default_schema` is in its identity) |
| R3 prod | F60 | SKIP | 1847 → prod R1 | T-skip (F68) | EXEC 8/7 |
| R4 dev | F77 | SKIP | 1847 → prod R1 | T-skip (F85) | EXEC 8/7 |

**Observed.** One history for both Targets. **Inferred.** `target_name` and
`default_schema` do not separate it (P0–P7 agree).

## Q6: identical SQL under two unique ids

| Step | Entry | Hosted | Echo → whose run | Explain | Current server |
|---|---|---|---|---|---|
| xa first | F87 | EXEC, 2251 ms | — | T-exec (F91) | same |
| ya first (evaluation off) | F88 | EXEC, 2341 ms | — | T-exec (F92) | same |
| xb first | F95 | SKIP | 2251 → **xa** | T-skip (F97) | EXEC, first build |
| yb first (evaluation on) | F96 | SKIP | 2341 → **ya** | T-skip (F98) | EXEC, first build |
| xa again | F102 | SKIP | 2251 → xa | T-skip (F103) | SKIP (same) |
| ya again | F101 | SKIP | 2341 → ya | T-skip (F104) | SKIP (same) |
| xb again | F108 | SKIP | 2251 → xa | T-skip (F109) | SKIP of its own run |
| yb again | F107 | SKIP | 2341 → ya | T-skip (F110) | SKIP of its own run |

Same-key order cells (crafted; two unique ids, one SQL, both submitted before
either confirmed, confirmed second-first, then each submitted again):

| Cell | Runs (submit order) | Confirm order | First UID again | Second UID again | Current server |
|---|---|---|---|---|---|
| `fail_confirmed_last` | t 43011 failing, t2 43022 passing | t2, then t | SKIP, echo 43011, failures 1 (P64) | SKIP, echo **43011**, failures 1 (P66) | each echoes its own run |
| `fail_submitted_last` | t 43111 passing, t2 43122 failing | t2, then t | SKIP, echo 43111, failures 0 (P72) | SKIP, echo **43111**, failures 0 (P74) | each echoes its own run |

**Observed.** A new unique id's first submit SKIPs from another unique id's
run of the same SQL, also when the evaluation flag differs (deterministic
SQL). With two runs in one key, every SKIP echoes the later-confirmed run,
its runtime and its failures, whatever the unique id and the submit order:
t2's own failing run is not served in `fail_submitted_last`. This is the
mechanism behind `explain_volatile_cells.jsonl` entry 146 (`cto` echoing
`rt`, whose confirm came later).

**Inferred.** Within a key only the latest confirmed execution is reusable;
the unique id plays no part.

## What the current server does

`Store::latest_data_test` (`crates/server/src/store/history.rs`) returns the
one latest confirmed data-test row for (organization, test unique id,
`project_id`, `table_namespace`, warehouse family), ordered by
`confirmed_at`. `reuse_candidate::for_data_test`
(`crates/server/src/reuse_candidate.rs`) reuses it only when its rendered
identity (`node_sql_hash`: dialect, catalog, `default_schema`, normalized
SQL, `semantic_extras`, query dependencies) or, under
`compare_unrendered_code`, its template identity (which also includes
`default_schema`) equals the submit's. So alternating Targets rebuild every
time, an older run is never reused after a newer run of the same unique id,
and a unique id never reuses another's run. These are the conservative
directions only: the server never SKIPs where hosted rebuilds in these
captures. Every SKIP it makes here is one hosted makes too; it echoes its
own unique id's run where hosted echoes another's (P66, P74). The
"Current server" columns are the rehearsal's end-to-end decisions, where
every local execution is confirmed; the replay of the hosted fixture can
differ from them because a hosted SKIP leaves no local confirm.

## Replay

`cargo test -p dbt-state-harness --test conformance` passes (corpus 147
fixtures). The new divergences are keyed narrowly in `golden/replay_gaps.json`:

| Kind | Entries | What differs |
|---|---|---|
| whole response, local `ready_to_execute` | F29, F31, F33, F34, F35, F56, F62, F63, F76, F77, F78, F80; P6, P30, P44, P52, P58 | hosted SKIP from another Target's run, a copy, an older run in another key, or across `default_schema` |
| `ignore_fields: [execution_runtime_ms, execution_results]`, `skip_execution` | P66, P74 | the echo of the other unique id's later-confirmed run |
| `explain` | F36–F40, F57, F65, F66, F81, F83–F85, F97, F98, F109, F110; P7, P11, P15, P31, P45, P53, P59 | the local tree explains its own rebuild |
| `explain` (wording) | F67 | T-cuc against the local rendered-identity lines |

Hosted SKIPs the replay counts as unknown baselines (no confirmed local
history for the unique id): F95, F96, F107, F108, P10, P14. In the replay,
R3 prod F59–F61 and R4 dev F79 match only because hosted's SKIPs leave no
local confirm of the runs this server would have made; the rehearsal, where
those runs are confirmed, rebuilds them.

## Candidate history keys

Consistent with every observation:

**K1 — (organization, `table_namespace`, normalized SQL with each input
relation resolved through hosted's recorded copies, result-setting test
config), latest confirmed execution per key, then the ordinary freshness
check against that execution's recorded upstream epochs.**

- In: Q1, Q4 (dev reuses prod through the copy), Q5 (one history), Q2
  (namespace and severity separate; unique id, project, `target_name`,
  `default_schema`, evaluation flag, hashes do not), Q3 (tf's insert breaks
  the copy and starts a key; tr S1 reuses its own key's latest run after S2),
  Q6 and the order cells (latest confirm per key, any unique id), the 2.0.6
  dev SKIPs.
- Not distinguished by these captures: whether the key holds the upstream
  epochs themselves (a content address) or the latest run is chosen first
  and then judged fresh (identical under monotonic epochs; they differ under
  Lag tolerance); whether copy provenance or "same producer node at the same
  build" resolves the dev relation; whether `fail_calc`, `warn_if`,
  `error_if`, `store_failures` or query dependencies are in the key; the
  organization boundary (one organization only).
- Invariant 1: holds within a key (only its latest confirmed run is reusable,
  O: P66, P74, F79), but the unit of history becomes the key, not the test.
  A data test has no Target relation a later run overwrites, so an older run
  of the same unique id under another key still describes its inputs; that
  is the argument an ADR amendment would need. Freshness must stay exact per
  physical input (invariant 2), and a copy may stand for its source only
  while the copy is unchanged since the copy (invariant 3; tf after the
  insert).
- Isolation: keeps organization and namespace scoping. It drops the project
  and unique-id scoping, and through copies it crosses Targets. The server
  could keep `project_id` (and the organization) as a stricter scope and only
  lose reuse hosted makes across projects (P12–P15).

Ruled out:

- **UID + rendered SQL identity, latest row per UID (the current server).**
  F34, F29 (dev SKIPs from prod), F56 and P30/P44/P52/P58 (an older run reused
  after a newer run of the UID), F95/F96 and P10 (another UID's run), P6
  (`default_schema`).
- **UID + rendered SQL identity, latest row per (UID, SQL).** F95, F96, P10,
  P66, P74 (the echo is another UID's run); F34 (other relation names);
  P6 if `default_schema` is part of the identity.
- **UID + `target_name`.** P2 (`target_name` alone does not separate),
  F29/F34 (dev reuses the prod run), F95.
- **Rendered SQL alone (exact text, relation names as written).** F31, F33,
  F34, F35 (the dev SQL names `dev_clone_demo`, yet reuses the prod run),
  P28–P30 (same SQL, other severity, separate), P18–P20 (same SQL, other
  namespace, separate).
- **Schema-abstracted SQL (relation schemas abstracted, as the Snowflake
  Result copy logic identity).** P50 and P56 (the crafted dev relation with
  the base's epoch is a separate history); only a hosted copy merges them.

## Surprises

- Data-test reuse across Targets happens on Snowflake when the dev upstream
  is a hosted copy of the prod upstream (Q1, Q3, Q4). #68 was closed as not
  reproduced; this capture reproduces it under copy provenance.
- Reuse crosses `project_id` (P14) and unique ids, and a unique id can be
  served another's result although its own different result exists (P74).
- The copy-mediated SKIP's Explain drops <FRESH> and "no updates since test
  last executed", and dates the dev upstream at the prod build (T-copy).
- On Snowflake, hosted prints the template comparison lines for a data test
  under `compare_unrendered_code` even when the rendered SQL matches (F67).

## Open questions

Recorded in [correctness-handoff.md](../correctness-handoff.md#open-hosted-questions):
the K1 details left open above; whether a test of a dev relation built
(not copied) on dev at an older time reuses an older dev run while a newer
prod run exists (the 2.0.6 shape, with the dev runs captured); Databricks
and DuckDB (not captured).
