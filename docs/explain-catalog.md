# `dbt state explain` line & badge catalog

The authoritative per-node-type catalog of the structured explain tree
(`ExplainMessageEntry` → `ExplainLine` / `ExplainMarker` / `ExplainBadge`) that
`ExplainService.GetExplainMessages` must return. Pinned from live captures
against `api.state.dbt.com` (Snowflake), **not** inferred. It is a living
document: it grows as unobserved node kinds (snapshot/incremental) are
captured.

Source fixtures (wayfinder map #19):
- `golden/fixtures/lag_seed_upstream.jsonl` (2026-11-10) — seed SKIP/reload/first-load trees.
- `golden/fixtures/lag_copy_candidates.jsonl` (2026-11-10) — CLONE over an existing target, anchored CLONE listing, absent-target rebuilds.
- `golden/fixtures/lag_dropped_targets.jsonl` (2026-11-10) — stale CLONE into a dropped target, dropped-target rebuilds with a stale build.
- `golden/fixtures/explain_happy_history.jsonl` (#34) — HISTORY-BEARING model/view/data-test SKIP batch (supersedes the empty-store `explain_snowflake_happy.jsonl` from #20).
- `golden/fixtures/explain_clone_history.jsonl` (#34) — HISTORY-BEARING CLONE tree with the clone source built in-window (supersedes the empty-store `explain_clone_deferral.jsonl` from #23).
- `golden/fixtures/explain_tolerance_history.jsonl` (#34) — HISTORY-BEARING within-lag-tolerance SKIP + upstream-OUTDATED EXECUTE, asserted directly in the main gate.
- `golden/fixtures/explain_tolerance.jsonl` (#24) — retained as the seeded-oracle source for `explain_skip_tree_matches_hosted_oracle_*` / `explain_outdated_execute_tree_matches_hosted_oracle_*` (excluded from the generic replay; see `conformance.rs` CHARACTERIZATION_ONLY).

Related decisions: the tree is **re-derived on demand** per
[ADR 0005](adr/0005-re-derive-explain-tree-on-demand.md); the
`decision_description` string selection is owned by issue #22 (see
[§ decision_description](#decision_description-separate-output)).

## How to read this catalog

- **Tree vs string are separate outputs.** The explain generator produces (a) the
  `ExplainLine` tree below, and (b) the `decision_description`. The three model
  SKIP trees are STRUCTURALLY IDENTICAL; only the description differs. Never
  derive the string from the tree — select it per #22.
- **Runtime slots.** `«FQN»` = a fully-qualified `"DB"."SCHEMA"."OBJECT"`.
  `«rel_time»` = humanized wall-clock ("a month ago, 2026-10-11 20:55:48 UTC").
  `«gap»` = "updated N seconds after «FQN» last executed". These are
  runtime-substituted; the catalog pins the surrounding template only. Replay
  (#26) MUST normalize these slots.
- **Markers / badges** shown as `[MARKER] <BADGE>`. Omitted = none on that line.

## Execution type (node kind) coverage

The `execution_type` integer (dbt `ModelExecutionType`, `shared.proto`) selects
which node kind a decision is for. The explain generator is a strict positive
**allowlist** over `(execution_type × decision class)`: a CHARACTERIZED pair
renders its cataloged tree; everything else renders NOTHING (unobserved — never
invent). This table is the single lookup for "what does integer N mean, and do
we render it?" — enforced by `explain.rs::is_characterized` and the exhaustive
`fence_matrix_is_exhaustive_and_matches_is_characterized` test (they must agree,
so a kind cannot graduate in one place and not the other).

| `execution_type` | Node kind | Explain coverage |
|---|---|---|
| 1 | FULL (table / model) | ✅ characterized — SKIP, EXECUTE, CLONE |
| 2 | APPEND (incremental) | ❌ unobserved → empty tree |
| 3 | MERGE (incremental) | ❌ unobserved → empty tree |
| 4 | INSERT_OVERWRITE (incremental) | ❌ unobserved → empty tree |
| 5 | DELETE_INSERT (incremental) | ❌ unobserved → empty tree |
| 6 | MICROBATCH (incremental) | ❌ unobserved → empty tree |
| 7 | SNAPSHOT | ❌ unobserved → empty tree |
| 8 | DBT_DATA_TEST | ⚠️ SKIP only characterized; EXECUTE/CLONE unobserved → empty tree |
| 9 | VALUES (seed) | ✅ characterized — SKIP, EXECUTE (2026-11-10); CLONE unobserved → empty tree |
| 10 | VIEW | ✅ characterized — SKIP, EXECUTE, CLONE |
| 11 | DBT_CUSTOM | ❌ unobserved → empty tree |
| 0 / other | UNSPECIFIED / unknown | ❌ unobserved → empty tree |

Source of truth for the integer→kind mapping: `shared.proto` (`ModelExecutionType`)
and [`docs/protocol.md`](protocol.md). Clone ADOPTION (a later submit reusing a
clone-origin row) is execution-type-agnostic — it is reachable at type 1 (model),
so its freshness-anchored explain subtree is characterized there even though the
original clone was captured via a MERGE (type 3) RegisterClone. The unobserved
kinds are elaborated in [§ unobserved](#unobserved--do-not-implement-until-captured).

## Observed vocabulary (complete across #20/#23/#24)

| Enum | Value | Observed? |
|---|---|---|
| ExplainMarker | SUCCESS (1) | ✅ |
| ExplainMarker | FAIL (2) | ✅ |
| ExplainMarker | INFO (3) | ❌ never |
| ExplainMarker | EM_UNSPECIFIED (0) | ❌ never |
| ExplainBadge | TARGET_TABLE_EXISTS (8) | ✅ |
| ExplainBadge | NODE_QUERY_CHANGED (4) | ✅ |
| ExplainBadge | NODE_QUERY_UNCHANGED (5) | ✅ |
| ExplainBadge | UPSTREAM_QUERY_CHANGED (6) | ✅ |
| ExplainBadge | FRESH (1) | ✅ |
| ExplainBadge | OUTDATED (3) | ✅ |
| ExplainBadge | UPSTREAM_QUERY_UNCHANGED (7) | ✅ (SHAPE 9 per-upstream query listing; the "upstream model queries have not changed" line carries NO badge) |
| ExplainBadge | WITHIN_TOLERANCE (2) | ✅ (lag tolerance line) |
| ExplainBadge | EB_UNSPECIFIED (0) | ❌ never |

## Section structure

Top-level sections appear in this order:

1. `table analysis ("«FQN»")` — the node's own target object. **Omitted for data
   tests** (they produce no object).
2. `query analysis` — node + upstream query/logic identity.
3. `data freshness analysis` — upstream data recency.

For a **CLONE** decision the latter two are suffixed **`(vs clone)`**
(`query analysis (vs clone)`, `data freshness analysis (vs clone)`), because the
comparison is against the clone source, not a prior build of the node itself.

## MODEL

### SKIP — tree (identical for all three description variants #1/#2/#3)

```
- table analysis ("«FQN»")
  - the model table exists already                    [SUCCESS] <TARGET_TABLE_EXISTS>
- query analysis
  - the model query has not changed                   [SUCCESS] <NODE_QUERY_UNCHANGED>
  - upstream model queries have not changed           [SUCCESS]        (no badge)
- data freshness analysis
  <ONE of the freshness bodies below>
```

Freshness-analysis body, by upstream situation (all observed):

- **No genuine upstreams:** `- no upstream dependencies  [SUCCESS]`
- **Upstream data fresh, no per-upstream listing** (single simple upstream):
  `- upstream data is up to date  [SUCCESS]`
- **Per-upstream listing** (one child per genuine upstream; the `orders`/
  `customers`-class and the lag-tolerance model):
  ```
  - upstream dependencies
    - "«FQN»"  <FRESH>
      - no updates since "«FQN»" last executed     (model anchor)
      - last updated: «rel_time»
  ```
  (for a data test the anchor line reads `- no updates since test last executed`)

  An anchored model SKIP follows the listing with `- upstream data is up to
  date [SUCCESS]` only when every upstream's current epoch equals the epoch
  recorded for it at the build (VERIFIED LIVE 2026-11-10: lag_seed_upstream
  entry 32; absent in explain_tolerance entry 1 and `dsdec_young`, whose
  upstream is older than recorded). With drifted upstream data it is followed
  by the lines in [§ Lag tolerance lines](#lag-tolerance-lines-observed-2026-11-10).

The `upstream model queries have not changed [SUCCESS]` line carries **no badge**
(the `UPSTREAM_QUERY_UNCHANGED` enum exists but is unused here).

### EXECUTE — two observed sub-shapes

**(a) node query changed** (fixture #20/#23):
```
- table analysis ("«FQN»")
  - the model table exists already                    [SUCCESS] <TARGET_TABLE_EXISTS>
- query analysis
  - the model query has changed                       [FAIL]    <NODE_QUERY_CHANGED>
  - upstream model queries have not changed           [SUCCESS]
- data freshness analysis
  - upstream data is up to date                       [SUCCESS]
```

**(b) upstream drift / OUTDATED** (fixture #24):

> **Scope (2026-11-09):** every capture of this shape came from an EXTERNAL
> MODIFICATION of a dbt-managed upstream (`create or replace` outside dbt).
> The `UPSTREAM_QUERY_CHANGED` / "an upstream model query has changed" [FAIL]
> lines appeared although the upstream's SQL never changed, so this shape may
> combine two causes: hosted reading an epoch it never recorded as an
> upstream query change, and data drift. Do not assume upstream data drift
> alone renders the query-analysis FAIL; see correctness-handoff.md §C.
```
- table analysis ("«FQN»")
  - the model table exists already                    [SUCCESS] <TARGET_TABLE_EXISTS>
- query analysis
  - the model query has not changed                   [SUCCESS] <NODE_QUERY_UNCHANGED>
  - upstream dependencies
    - "«FQN»"
      - query or upstream query changed               <UPSTREAM_QUERY_CHANGED>
  - an upstream model query has changed               [FAIL]
- data freshness analysis
  - upstream dependencies
    - "«FQN»"  <OUTDATED>
      - «gap»                                          ("updated N seconds after «FQN» last executed")
      - last updated: «rel_time»
  - upstream data is outdated                         [FAIL]
```

**First build** (no prior; fixture #24 baseline) — table-analysis only:
```
- table analysis ("«FQN»")
  - the model table does not exist                    [FAIL]
  - there is no other table that the model could be cloned from  [FAIL]
```

## DATA TEST

No `table analysis` section (tests produce no object).

### SKIP
```
- query analysis
  - the test query has not changed                    [SUCCESS] <NODE_QUERY_UNCHANGED>
  - upstream model queries have not changed           [SUCCESS]
- data freshness analysis
  - upstream dependencies
    - "«FQN»"  <FRESH>
      - no updates since test last executed
      - last updated: «rel_time»
```
(one `upstream dependencies` child per tested upstream; EXECUTE for data tests
was not captured — see unobserved.)

## CLONE (model)

```
- table analysis ("«FQN»")
  - the model table does not exist                    [FAIL]
  - the model table could be cloned from "«FQN»"      [SUCCESS]   (clone source)
- query analysis (vs clone)
  - the model query has not changed                   [SUCCESS] <NODE_QUERY_UNCHANGED>
  - upstream model queries have not changed           [SUCCESS]
- data freshness analysis (vs clone)
  - upstream dependencies
    - "«FQN»"                                          (no badge; clone has no own-build anchor)
      - last updated: «rel_time»
  - upstream data is up to date                       [SUCCESS]
```

The CLONE `upstream dependencies` listing differs from the SKIP one: a clone has
NO own-build anchor (ADR 0005 — a CLONE records no `matched_execution_id`), so
hosted emits **no `<FRESH>`/`<OUTDATED>` badge and no `no updates since … last
executed` anchor line** — just the humanized `last updated: …` child per
upstream. VERIFIED LIVE by the history-bearing `explain_clone_history.jsonl`
(#34); the earlier catalog draft (and generator) wrongly reused the anchored
SKIP listing here. Rendered by `explain.rs::clone_upstream_dependencies`.
When the copy keeps the target's own (rebuilt-for-freshness or dropped) build
as its anchor, the listing is the anchored SKIP one instead (VERIFIED LIVE
2026-11-10, lag_copy_candidates `de`). A copy of a stale equivalent ends with
`upstream data is outdated` [FAIL] and the lag tolerance line in place of
`upstream data is up to date` (lag_dropped_targets `dset600`).

### CLONE over an existing target ("but it's old")

VERIFIED LIVE 2026-11-10 (`lag_copy_candidates.jsonl` arms `fe`, `st`, `wba`).
The target exists, but its own build had to rebuild for freshness, so an
equivalent that qualifies for this submit is copied over it. The query and
freshness sections analyse the target against its OWN build (the anchor);
the summary says how fresh the copied equivalent is.
```
- table analysis ("«FQN»")
  - the model table exists already                    [SUCCESS] <TARGET_TABLE_EXISTS>
  - but it's old, so we will clone from "«FQN»" to recreate it   [FAIL]
- query analysis (vs clone)
  <the SHAPE 9 query analysis against the target's own build>
- data freshness analysis (vs clone)
  - upstream dependencies                              (anchored listing)
  - upstream data is up to date                       [SUCCESS]   (equivalent fresh)
    OR
  - upstream data is outdated                         [FAIL]      (equivalent stale)
  - the model was last built «age» ago, within lag tolerance of «T»   [SUCCESS] <WITHIN_TOLERANCE>
```
The age is the equivalent's build age (`wba`). Rendered by
`explain.rs::copy_over_tree`.

## SEED

VERIFIED LIVE 2026-11-10 (`lag_seed_upstream.jsonl`). A seed has no upstreams,
so there is no freshness section.

**SKIP**, and a reload whose data did not change since an older load (arm H,
rejection codes 8/7):
```
- table analysis ("«FQN»")
  - the seed file exists already                      [SUCCESS] <TARGET_TABLE_EXISTS>
- query analysis
  - the seed data has not changed                     [SUCCESS] <NODE_QUERY_UNCHANGED>
```
**Reload with changed values or config**: as above, with
`- the seed data has changed [FAIL]` (no badge).

**Load into a missing table** (first load, or a dropped table, arm G):
```
- table analysis ("«FQN»")
  - the seed table does not exist                     [FAIL]
  - there is no other table that the seed could be cloned from  [FAIL]
```
The engine's reload of a seed whose own table was modified externally is a
deliberate divergence (hosted skips it; correctness-handoff.md §C) and renders
no tree.

## UNKNOWN (untracked kinds, e.g. unit tests)

Client-side only (resolved in #20): State never tracks these nodes, they never
hit the Explain RPC, and the server returns **nothing** for them. The client
prints `UNKNOWN <uid> - dbt State explain details unavailable`. No server work.

## Lag tolerance lines (observed 2026-11-10)

Captured in `lag_tolerance_matrix.jsonl` and `lag_upstream_query_listing.jsonl`;
rendered by `explain.rs` from the tolerance and build age persisted with the
decision (migration 0015).

- **SKIP with drifted upstream data** adds, under `data freshness analysis`
  after the `upstream dependencies` listing:
  ```
  - upstream data is outdated                                   [FAIL]
  - the model was last built «age» ago, within lag tolerance of «T»   [SUCCESS] <WITHIN_TOLERANCE>
  ```
  or, when the build is older than the tolerance (an ALL-policy reuse),
  `… exceeds lag tolerance of «T»` [FAIL] <OUTDATED>. With tolerance 0 only the
  `upstream data is outdated` line appears. Durations read like "a moment",
  "7 seconds", "a minute", "10 minutes".
- **EXECUTE (SHAPE 9)**: the query analysis lists every upstream with
  `query or upstream query changed` <UPSTREAM_QUERY_CHANGED> or
  `query or upstream query unchanged` <UPSTREAM_QUERY_UNCHANGED>, then
  `an upstream model query has changed` [FAIL], only when an upstream's
  producing build changed (ADR 0007). A data-only rebuild instead shows
  `upstream model queries have not changed` [SUCCESS]. No tolerance line.
- The `«gap»` line `updated N seconds after «FQN» last executed` is the
  upstream's current epoch minus the epoch recorded for it at the build.
- **Own target modified** (no upstreams): `table exists already`, `the model
  query has not changed`, `no upstream dependencies`.

## decision_description (separate output)

Selected independently of the tree (issue #22). The three model-SKIP strings are
distinct coexisting reasons on one hosted version (confirmed #24):

| # | string | situation |
|---|---|---|
| 1 | `model was a no-op because both its query and its upstream data are up to date` | tolerance 0, or every upstream epoch equals its recorded epoch |
| 2 | `model was a no-op because its query is up to date and its data is within lag tolerance` | otherwise, while build age < tolerance |
| 3 | `model was a no-op because its query is up to date and at least one upstream dependency is unchanged` | otherwise |

Characterized live 2026-11-10 (`lag_tolerance_matrix.jsonl`, `dsdec_*`, `pao_m`,
`dsall_m`), replacing the earlier situation guesses. The noun follows the node
kind ("snapshot was a no-op …").

Other pinned strings (match our engine's current pins; see `decision.rs`):
- model EXECUTE: `model was executed because either its query didn't match or its upstream data is out of date`
- model first build, or a rebuild of an absent target: `model was executed because its table did not exist`
- own target modified after the build: `model was executed because the table was modified after the cached execution`
- data test SKIP: `data test was a no-op because both its query and its upstream data are up to date`
- seed SKIP: `seed was a no-op because its data has not changed`
- seed reload: `seed was loaded because its data changed`; into a missing table: `seed was loaded because it did not exist`
- CLONE: `an equivalent model exists under another name so we cloned that one`

## Re-derivation binding (ADR 0005)

Each tree signal maps to a re-derivable source (see ADR 0005):

| Line / badge | Re-derivation source |
|---|---|
| `table analysis` existence + `<TARGET_TABLE_EXISTS>` | `node_decisions` decision class + persisted `decision_reason` + whether the decision-time `input_tables` report the own target (ADR 0006; NO live physical-existence probe — see [§ ambiguous own-target known limitation](#known-limitation--ambiguous-own-target-rebuild)) |
| `<NODE_QUERY_CHANGED>` / `<NODE_QUERY_UNCHANGED>` | node SQL/template fingerprint vs matched row |
| per-upstream `<FRESH>` / `<OUTDATED>` + `«gap»` | decision-time `input_tables` (node_decisions) vs matched `executions.input_tables` + `executions.last_modified_epoch` (own-build anchor), via `matched_execution_id` |
| `<UPSTREAM_QUERY_CHANGED>` + "an upstream model query has changed" | per-upstream name-set / logic diff (same two snapshots) |
| `«rel_time»` ("last updated …") | humanized upstream `last_modified_epoch` |

The per-upstream FRESH/OUTDATED verdict is the signal `evaluate_freshness`
computes and discards; ADR 0005 re-derives it from the two `input_tables`
snapshots. The catalog's per-upstream lines are the render target for that.

## Unobserved — do not implement until captured

Not present in any capture; mark as unimplemented rather than invent (each is a
future-capture fog item on map #19):

- **Node kinds:** snapshot (7), incremental model, seed CLONE.
  Their explain trees (esp. whether incremental/snapshot add lines) are unknown.
- **Badges:** `EB_UNSPECIFIED` (0). (`WITHIN_TOLERANCE` (2) and
  `UPSTREAM_QUERY_UNCHANGED` (7) are now observed; see "Lag tolerance lines".)
- **Markers:** `INFO` (3), `EM_UNSPECIFIED` (0).
- **Data test EXECUTE** tree (only SKIP captured).
- **`Explain.GetUpstreamDependencyChanges`** — CLIENT-UNREACHABLE, not merely
  undiscovered. The dbt Fusion CLI never calls this RPC: a raw byte scan of the
  client binary (v2.0.6 AND v2.0.11) finds zero occurrences of the method name,
  and the only `Explain/` method it embeds is `GetExplainMessages`. The
  upstream-dependency-changes view is a dbt-platform ("Explain tab") surface that
  does not pass through the recording proxy, so it cannot be captured with this
  harness. Server keeps it an empty, non-asserted stub; do NOT invent the
  `UpstreamDependencyChanges` line tree before a real capture exists. See
  [protocol.md](protocol.md#explain-explaingetupstreamdependencychanges-is-client-unreachable).
- **Dependency-set change** (`decision::FreshnessEvidence::DependencySetChanged`):
  UNREACHABLE on the characterized hosted version, like `WITHIN_TOLERANCE` above.
  #32 drove three live Snowflake scenarios through the recording proxy to make a
  node's GENUINE upstream NAME set change (added/removed/ambiguous) while keeping
  its own fingerprint matchable, and NONE produced a distinct dependency-set
  subtree:
  - **ref swap / add / remove** (`explain_dep_set_variants.sh` variant S): the
    upstream physical FQN is embedded in the consumer's RENDERED SQL, so changing
    which upstream a `ref()` resolves to necessarily changes `node_sql_hash`.
    Hosted then rebuilds via the node/upstream-query-changed path, never a pure
    name-set diff. An added/removed upstream with a MATCHABLE own fingerprint is
    therefore structurally impossible through a real dbt client.
  - **view indirection** (`explain_dependency_set_changed.sh`): the consumer's
    submitted `tables` set tracks only its DIRECT upstream (it is not the
    transitive closure through a view). Repointing a view's own upstream surfaces
    on the consumer as an UPSTREAM_QUERY_CHANGED EXECUTE whose explain tree is
    `query analysis → upstream dependencies → "«view FQN»" → "view ignored"` then
    `"an upstream model query has changed" [FAIL]` — the upstream is named by its
    physical FQN; NO added/removed/ambiguous dependency NAME is surfaced.
  - **duplicate ref** (variant D): the client DEDUPES two `ref()`s to the same
    model, so the submitted `tables` multiset carries a single occurrence — the
    `ambiguous` set is never put on the wire.
  - **comment ref** (`explain_dep_set_comment_ref.sh`): a `ref()` inside a SQL
    comment registers NO dependency (and the comment is normalized out of the
    fingerprint), so it cannot add a genuine upstream either.
  `Explain.GetUpstreamDependencyChanges` never fired in any of these flows.
  RESOLUTION (#33): there is no hosted dependency-set-change subtree to build,
  and a name-set change with a MATCHABLE own fingerprint (the only case that
  would need a distinct subtree) is not client-reachable, so the explain
  generator stays on its generic path for `DependencySetChanged`. The
  per-name diff that `upstream_freshness` used to retain was unused by any
  faithful output and has been DROPPED (the `DependencySetChanged` decision
  variant itself stays — it is still produced by the set-change short-circuit
  and consumed by the SKIP/EXECUTE fold). See `decision.rs::upstream_freshness`.
  Drivers: `experiments/explain_dependency_set_changed.sh`,
  `experiments/explain_dep_set_variants.sh`,
  `experiments/explain_dep_set_comment_ref.sh`.

## Shape selection (ADR 0006)

Explain selects a tree shape from the persisted decision class, the persisted
decision reason (`node_decisions.decision_reason`) and the matched execution
anchor (`explain.rs::shape`), after the node-kind fence. It never reads the
`decision_description` wording.

| class | decision reason | shape |
|---|---|---|
| SKIP | `reused.fresh`, `reused.tolerated_stale`, `reused.within_lag_tolerance` | SKIP (SHAPE 0/1/2/4 by kind and upstreams) |
| EXECUTE | `first_build`, `no_reuse_candidate.own_target_missing` | first build (`table analysis` only) |
| EXECUTE | `upstream_outdated`, anchor re-derives an OUTDATED upstream; `upstream_query_changed` with an anchor | SHAPE 9 |
| EXECUTE | `no_reuse_candidate.own_target_modified`, with an anchor or no upstreams | SKIP-shaped tree ("table exists already", "query has not changed") |
| EXECUTE | any other reason when the decision-time inputs lack the own target | first build (VERIFIED LIVE 2026-11-10, `dt_m`) |
| EXECUTE | `upstream_outdated`/`upstream_query_changed` without that anchor; `no_reuse_candidate.identity_mismatch`, `.failed_reuse_qualification`, `.incomplete_cached_test_results`; `dependency_set_changed`; `copy_chain_limit` | SHAPE 7 |
| EXECUTE | `no_reuse_candidate.own_target_changed_or_missing` (ambiguous own-target evidence), `no_build_anchor`, `own_target_modified` without anchor | SHAPE 7, **awaiting hosted capture (#40)** |
| CLONE | `result_copy` with an anchor and the own target present | CLONE over an existing target |
| CLONE | `result_copy` otherwise (Submit Result copy and RegisterClone) | CLONE |
| seed SKIP | `reused.*` | SEED, data unchanged |
| seed EXECUTE | `no_reuse_candidate.identity_mismatch` | SEED, data changed (unchanged when skip rejection is 8) |
| seed EXECUTE | `first_build`, `no_reuse_candidate.own_target_missing` | first build |
| any | NULL (row written before migration 0014), unknown, or contradicting the class | empty tree |

## Known limitation — ambiguous own-target rebuild

A dropped own target is now its own reason
(`no_reuse_candidate.own_target_missing`, VERIFIED LIVE 2026-11-10,
`lag_copy_candidates` `dt`) and renders the first-build tree, as hosted does.
What remains **awaiting hosted capture (#40)**: a rebuild whose own-target
evidence is ambiguous (duplicate entries, no epoch, or a target that does not
match the build's) persists `no_reuse_candidate.own_target_changed_or_missing`,
and a rebuild with no build anchor persists `no_build_anchor`. Explain has NO
own-target physical-existence signal and runs NO live warehouse probe, so both
keep the SHAPE 7 rendering ("the model table exists already" + "the model
query has changed"), which may be wrong on both counts. Their hosted shapes
are unobserved; per the do-not-invent rule above, we keep today's rendering
rather than ship a guessed tree. Once a capture exists, fixing them only
changes their arm in `explain.rs::shape`.
