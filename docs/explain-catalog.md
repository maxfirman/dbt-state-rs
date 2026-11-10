# `dbt state explain` line & badge catalog

The catalog of record for the structured Explain tree
(`ExplainMessageEntry` → `ExplainLine` / `ExplainMarker` / `ExplainBadge`) that
`ExplainService.GetExplainMessages` returns. Every tree is pinned from live
captures against `api.state.dbt.com` (Snowflake), never inferred. Unobserved
node kinds render nothing until captured.

The tree is re-derived on demand ([ADR 0005](adr/0005-re-derive-explain-tree-on-demand.md))
and its shape is selected from the persisted Decision reason
([ADR 0006](adr/0006-persist-the-decision-reason.md)). The generator is
`crates/server/src/explain.rs`. The `decision_description` strings are a
separate output, listed in [protocol.md](protocol.md).

Source fixtures (`golden/fixtures/`):
- `explain_happy_history.jsonl`: history-bearing model/view/data-test SKIPs.
- `explain_clone_history.jsonl`: CLONE with the copy source built in-window.
- `explain_tolerance_history.jsonl`: within-tolerance SKIP and upstream-OUTDATED
  EXECUTE, asserted in the main replay.
- `explain_tolerance.jsonl`: seeded-oracle source for
  `explain_skip_tree_matches_hosted_oracle_when_history_seeded` and
  `explain_outdated_execute_tree_matches_hosted_oracle_when_history_seeded`
  (excluded from the generic replay, `CHARACTERIZATION_ONLY` in
  `crates/harness/tests/conformance.rs`).
- `lag_seed_upstream.jsonl`: seed SKIP, reload and first-load trees.
- `lag_copy_candidates.jsonl`: CLONE over an existing Target relation, anchored
  CLONE listing, absent-target rebuilds.
- `lag_dropped_targets.jsonl`: stale CLONE into a dropped Target relation.
- `lag_tolerance_matrix.jsonl`, `lag_upstream_query_listing.jsonl`: Lag
  tolerance lines and the per-upstream query listing.

## How to read this catalog

- **Tree vs string.** The tree and the `decision_description` are separate
  outputs. The model SKIP tree is the same for all three SKIP descriptions;
  never derive the string from the tree.
- **Runtime slots.** `«FQN»` = a fully-qualified `"DB"."SCHEMA"."OBJECT"`.
  `«rel_time»` = humanized wall-clock ("a month ago, 2026-10-11 20:55:48 UTC").
  `«gap»` = "updated N seconds after «FQN» last executed". The catalog pins the
  surrounding template; replay masks these slots on both sides
  (`normalize_explain_lines` in `conformance.rs`).
- **Markers / badges** shown as `[MARKER] <BADGE>`. Omitted = none on that line.

## SHAPE numbers

Code comments and tests cite shapes by number. The numbers are labels from the
original characterization capture; their meaning:

| SHAPE | Tree |
|---|---|
| 0 | FULL model SKIP with no genuine upstreams: no "upstream model queries" line; freshness body `no upstream dependencies`. |
| 1, 2, 4 | SKIPs that carry `upstream model queries have not changed` (a view, a data test, or any node with a genuine upstream). |
| 7 | Node-query-changed EXECUTE (also the fallback for several rebuild reasons; see [Shape selection](#shape-selection)). |
| 8 | SKIP with the anchored per-upstream `<FRESH>` listing (`explain_tolerance.jsonl` entry 1). |
| 9 | Upstream EXECUTE rendered against the matched build: per-upstream query and freshness listings. |

## Execution type (node kind) coverage

The `execution_type` integer (dbt `ModelExecutionType`, `shared.proto`) selects
the node kind. The generator is a strict allowlist over
`(execution_type × decision class)`: a characterized pair renders its tree,
everything else renders nothing. Enforced by `explain.rs::is_characterized` and
the `fence_matrix_is_exhaustive_and_matches_is_characterized` test.

| `execution_type` | Node kind | Explain coverage |
|---|---|---|
| 1 | FULL (table / model) | ✅ SKIP, EXECUTE, CLONE |
| 2 | APPEND (incremental) | ❌ unobserved → empty tree |
| 3 | MERGE (incremental) | ❌ unobserved → empty tree |
| 4 | INSERT_OVERWRITE (incremental) | ❌ unobserved → empty tree |
| 5 | DELETE_INSERT (incremental) | ❌ unobserved → empty tree |
| 6 | MICROBATCH (incremental) | ❌ unobserved → empty tree |
| 7 | SNAPSHOT | ❌ unobserved → empty tree |
| 8 | DBT_DATA_TEST | ⚠️ SKIP only; EXECUTE/CLONE → empty tree |
| 9 | VALUES (seed) | ✅ SKIP, EXECUTE; CLONE → empty tree |
| 10 | VIEW | ✅ SKIP, EXECUTE, CLONE |
| 11 | DBT_CUSTOM | ❌ unobserved → empty tree |
| 0 / other | UNSPECIFIED / unknown | ❌ unobserved → empty tree |

Adoption of a copy-origin row by a later submit does not depend on the
execution type, so its anchored subtree renders for a FULL model even when the
copy was registered for another kind.

## Observed vocabulary

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
| ExplainBadge | UPSTREAM_QUERY_UNCHANGED (7) | ✅ (SHAPE 9 per-upstream query listing only) |
| ExplainBadge | FRESH (1) | ✅ |
| ExplainBadge | OUTDATED (3) | ✅ |
| ExplainBadge | WITHIN_TOLERANCE (2) | ✅ (Lag tolerance line) |
| ExplainBadge | EB_UNSPECIFIED (0) | ❌ never |

## Section structure

Top-level sections appear in this order:

1. `table analysis ("«FQN»")`: the node's Target relation. **Omitted for data
   tests** (they produce no relation).
2. `query analysis`: node and upstream query identity.
3. `data freshness analysis`: upstream data recency.

For a **CLONE** decision the latter two are suffixed **`(vs clone)`**.

## MODEL

### SKIP (SHAPEs 0, 1, 2, 4, 8)

```
- table analysis ("«FQN»")
  - the model table exists already                    [SUCCESS] <TARGET_TABLE_EXISTS>
- query analysis
  - the model query has not changed                   [SUCCESS] <NODE_QUERY_UNCHANGED>
  - upstream model queries have not changed           [SUCCESS]        (no badge; see below)
- data freshness analysis
  <freshness body>
```

The `upstream model queries have not changed` line is omitted for a FULL model
with no genuine upstreams (SHAPE 0) and present otherwise
(`explain.rs::has_upstream_query_line`).

Freshness body (`explain.rs::data_freshness_analysis_fresh`):

- **No genuine upstreams:** a FULL model renders
  `- no upstream dependencies [SUCCESS]`; any other kind (e.g. a view) renders
  `- upstream data is up to date [SUCCESS]`.
- **Genuine upstreams:** one child per upstream, then a summary:
  ```
  - upstream dependencies
    - "«FQN»"  <FRESH>
      - no updates since "«FQN»" last executed     (data test: "no updates since test last executed")
      - last updated: «rel_time»
  ```
  The summary is `- upstream data is up to date [SUCCESS]` only when every
  upstream's current epoch equals the epoch recorded for it at the build
  (`lag_seed_upstream` entry 32). With drifted upstream data it is the
  [Lag tolerance lines](#lag-tolerance-lines). When an epoch only moved back,
  there is no summary line.

### EXECUTE: node query changed (SHAPE 7)

```
- table analysis ("«FQN»")
  - the model table exists already                    [SUCCESS] <TARGET_TABLE_EXISTS>
- query analysis
  - the model query has changed                       [FAIL]    <NODE_QUERY_CHANGED>
  - upstream model queries have not changed           [SUCCESS]
- data freshness analysis
  - upstream data is up to date                       [SUCCESS]
```

### EXECUTE: upstream, against the matched build (SHAPE 9)

Rendered by `explain.rs::execute_outdated_tree`, from per-upstream verdicts
re-derived against the matched build.

```
- table analysis ("«FQN»")
  - the model table exists already                    [SUCCESS] <TARGET_TABLE_EXISTS>
- query analysis
  - the model query has not changed                   [SUCCESS] <NODE_QUERY_UNCHANGED>
  <query body>
- data freshness analysis
  - upstream dependencies
    - "«FQN»"  <OUTDATED> or <FRESH>                   (one per genuine upstream)
      - «gap» (OUTDATED) or no updates since "«FQN»" last executed (FRESH)
      - last updated: «rel_time»
  <freshness summary>
```

- **Query body.** When at least one upstream's Upstream producer changed
  (ADR 0007), every genuine upstream is listed, then the FAIL summary:
  ```
  - upstream dependencies
    - "«FQN»"
      - query or upstream query changed               <UPSTREAM_QUERY_CHANGED>
        (or: query or upstream query unchanged        <UPSTREAM_QUERY_UNCHANGED>)
  - an upstream model query has changed               [FAIL]
  ```
  Otherwise (a data-only rebuild) the body is
  `- upstream model queries have not changed [SUCCESS]`.
- **Freshness summary.** If an upstream drifted:
  `- upstream data is outdated [FAIL]`, followed by the Lag tolerance line when
  the tolerance is non-zero and the decision is stale (a rebuild reported
  against an older qualifying build). If every upstream epoch equals its
  recorded epoch: `- upstream data is up to date [SUCCESS]`. Otherwise none.

### EXECUTE: Target relation modified after the build

Reason `no_reuse_candidate.own_target_modified` with a matched build or no
genuine upstreams renders the SKIP tree: `the model table exists already`,
`the model query has not changed`, and for a node without upstreams
`no upstream dependencies` (`lag_tolerance_matrix` `lfs_u0`).

### First build

Also a rebuild whose decision-time inputs lack the Target relation
(`lag_copy_candidates` `dt`, `dt_m`):
```
- table analysis ("«FQN»")
  - the model table does not exist                    [FAIL]
  - there is no other table that the model could be cloned from  [FAIL]
```

## DATA TEST

No `table analysis` section.

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
One `upstream dependencies` child per tested upstream. Data test EXECUTE is
unobserved.

## CLONE (model)

```
- table analysis ("«FQN»")
  - the model table does not exist                    [FAIL]
  - the model table could be cloned from "«FQN»"      [SUCCESS]   (copy source)
- query analysis (vs clone)
  - the model query has not changed                   [SUCCESS] <NODE_QUERY_UNCHANGED>
  - upstream model queries have not changed           [SUCCESS]
- data freshness analysis (vs clone)
  - upstream dependencies
    - "«FQN»"                                          (no badge, no anchor line)
      - last updated: «rel_time»
  - upstream data is up to date                       [SUCCESS]
```

Without a matched build the listing has no `<FRESH>`/`<OUTDATED>` badge and no
`no updates since … last executed` line (`explain_clone_history.jsonl`,
`explain.rs::clone_upstream_dependencies`). When the copy keeps the Target
relation's own build as its anchor, the listing is the anchored SKIP one
(`lag_copy_candidates` `de`). A copy of a stale equivalent ends with
`upstream data is outdated` [FAIL] and, with a non-zero tolerance, the Lag
tolerance line instead of `upstream data is up to date`
(`lag_dropped_targets` `dset600`).

### CLONE over an existing Target relation ("but it's old")

`lag_copy_candidates.jsonl` arms `fe`, `st`, `wba`. The Target relation exists,
but its own build had to rebuild for freshness, so a qualifying equivalent is
copied over it. Query and freshness are analysed against the Target relation's
own build; the summary says how fresh the copied equivalent is.
```
- table analysis ("«FQN»")
  - the model table exists already                    [SUCCESS] <TARGET_TABLE_EXISTS>
  - but it's old, so we will clone from "«FQN»" to recreate it   [FAIL]
- query analysis (vs clone)
  <the SHAPE 9 query analysis against the Target relation's own build>
- data freshness analysis (vs clone)
  - upstream dependencies                              (anchored listing)
  - upstream data is up to date                       [SUCCESS]   (equivalent fresh)
    OR
  - upstream data is outdated                         [FAIL]      (equivalent stale)
  - the model was last built «age» ago, within lag tolerance of «T»   [SUCCESS] <WITHIN_TOLERANCE>
```
The age is the equivalent's Build age (`wba`). Rendered by
`explain.rs::copy_over_tree`.

## SEED

`lag_seed_upstream.jsonl`. A seed has no upstreams, so there is no freshness
section.

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
The engine's reload of a seed whose table was modified externally is a known
divergence (hosted skips it; see [correctness-handoff.md](correctness-handoff.md))
and renders no tree.

## UNKNOWN (untracked kinds, e.g. unit tests)

Client-side only: State never tracks these nodes, they never reach the Explain
RPC, and the server returns nothing for them. The client prints
`UNKNOWN <uid> - dbt State explain details unavailable`.

## Lag tolerance lines

`lag_tolerance_matrix.jsonl`, `lag_upstream_query_listing.jsonl`. Rendered from
the tolerance and Build age persisted with the decision
(`node_decisions.freshness_tolerance_ms`, `build_age_ms`).

- **SKIP with drifted upstream data** adds, after the `upstream dependencies`
  listing:
  ```
  - upstream data is outdated                                   [FAIL]
  - the model was last built «age» ago, within lag tolerance of «T»   [SUCCESS] <WITHIN_TOLERANCE>
  ```
  or, when the build is older than the tolerance (an ALL-policy reuse),
  `… exceeds lag tolerance of «T»` [FAIL] <OUTDATED>. With tolerance 0 only
  `upstream data is outdated` appears. Durations read like "a moment",
  "7 seconds", "a minute", "10 minutes" (`explain.rs::humanize_duration`).
- The `«gap»` line `updated N seconds after «FQN» last executed` is the
  upstream's current epoch minus the epoch recorded for it at the build.

## Re-derivation binding (ADR 0005)

| Line / badge | Re-derivation source |
|---|---|
| `table analysis` existence + `<TARGET_TABLE_EXISTS>` | persisted decision class and `decision_reason`, plus whether the decision-time `input_tables` report the Target relation (no live warehouse probe) |
| `<NODE_QUERY_CHANGED>` / `<NODE_QUERY_UNCHANGED>` | persisted decision reason |
| per-upstream `<FRESH>` / `<OUTDATED>` + `«gap»` | decision-time `node_decisions.input_tables` vs the matched execution's recorded inputs (`matched_execution_id`) |
| `<UPSTREAM_QUERY_CHANGED>` / `<UPSTREAM_QUERY_UNCHANGED>` | per-upstream Upstream producer comparison, same two snapshots |
| `«rel_time»` ("last updated …") | the upstream's decision-time `last_modified_epoch` |

Explain computes the per-upstream verdicts with the same
`decision::upstream_freshness` helper the decision uses, against the matched
execution as returned by `store::ExplainDecisionRow::anchor` (which applies the
decision's copy-adoption input rule, `reuse_candidate::adopt_current_inputs`).

## Unobserved — do not implement until captured

Not present in any capture; render nothing rather than invent:

- **Node kinds:** snapshot (7), incremental models, seed CLONE.
- **Badges:** `EB_UNSPECIFIED` (0).
- **Markers:** `INFO` (3), `EM_UNSPECIFIED` (0).
- **Data test EXECUTE** tree.
- **`Explain.GetUpstreamDependencyChanges`**: the dbt Fusion client never calls
  it, so it cannot be captured through the recording proxy. The server keeps an
  empty stub. See [protocol.md](protocol.md).
- **Dependency-set change** (`dependency_set_changed`): hosted shows no distinct
  subtree. Through a real client, changing a node's genuine upstream names
  also changes its rendered SQL, repeated refs are deduplicated, and a ref in a
  comment registers no dependency, so hosted rebuilds on the query-changed
  path. Explain renders SHAPE 7 for this reason. Drivers:
  `experiments/explain_dependency_set_changed.sh`,
  `experiments/explain_dep_set_variants.sh`,
  `experiments/explain_dep_set_comment_ref.sh`.

## Shape selection

Explain selects a shape from the persisted decision class, Decision reason
(`node_decisions.decision_reason`) and matched execution anchor
(`explain.rs::shape`), after the node-kind fence. It never reads the
`decision_description` wording. Arms are tried in this order:

| class | Decision reason | shape |
|---|---|---|
| seed SKIP | `reused.*` | SEED, data unchanged |
| seed EXECUTE | `first_build`, `no_reuse_candidate.own_target_missing` | first build |
| seed EXECUTE | `no_reuse_candidate.identity_mismatch` | SEED, data changed (unchanged when the skip rejection code is 8) |
| seed, other | any other reason | empty tree |
| SKIP | `reused.fresh`, `reused.tolerated_stale`, `reused.within_lag_tolerance` | SKIP (SHAPE 0/1/2/4/8 by kind and upstreams) |
| EXECUTE | `first_build`, `no_reuse_candidate.own_target_missing` | first build |
| EXECUTE | `upstream_outdated` when the anchor re-derives an OUTDATED upstream; `upstream_query_changed` with an anchor | SHAPE 9 |
| EXECUTE | `no_reuse_candidate.own_target_modified` with an anchor or no genuine upstreams | SKIP-shaped tree |
| EXECUTE | any other reason when the decision-time inputs lack the Target relation | first build (`lag_copy_candidates` `dt_m`) |
| EXECUTE | `upstream_outdated` / `upstream_query_changed` otherwise; `no_reuse_candidate.identity_mismatch`, `.failed_reuse_qualification`, `.incomplete_cached_test_results`; `dependency_set_changed`; `copy_chain_limit` | SHAPE 7 |
| EXECUTE | `no_reuse_candidate.own_target_changed_or_missing`; `no_reuse_candidate.own_target_modified` otherwise; `no_build_anchor` | SHAPE 7, **awaiting hosted capture** (below) |
| CLONE | `result_copy` with an anchor and the Target relation present | CLONE over an existing Target relation |
| CLONE | `result_copy` otherwise (submit Result copy and RegisterClone) | CLONE |
| any | NULL (row written before migration 0014), unknown, or contradicting the class | empty tree |

## Known limitation — ambiguous own-target rebuild

Three rebuilds keep the SHAPE 7 rendering ("the model table exists already" +
"the model query has changed"), which may be wrong on both counts, because
their hosted shapes are unobserved:

- `no_reuse_candidate.own_target_changed_or_missing`: the Target relation is
  reported ambiguously (duplicate entries, no epoch, or a name that does not
  match the build's);
- `no_reuse_candidate.own_target_modified` with genuine upstreams but no
  matched build;
- `no_build_anchor`: the build has upstreams but no epoch to compare them with.

Explain runs no live warehouse probe. Once a capture exists, fixing these only
changes their arm in `explain.rs::shape`.
