# `dbt state explain` line & badge catalog

The authoritative per-node-type catalog of the structured explain tree
(`ExplainMessageEntry` → `ExplainLine` / `ExplainMarker` / `ExplainBadge`) that
`ExplainService.GetExplainMessages` must return. Pinned from live captures
against `api.state.dbt.com` (Snowflake), **not** inferred. It is a living
document: it grows as unobserved node kinds (seed/snapshot/incremental) are
captured.

Source fixtures (wayfinder map #19):
- `golden/fixtures/explain_snowflake_happy.jsonl` (#20) — model SKIP/EXECUTE, data test SKIP, UNKNOWN unit tests.
- `golden/fixtures/explain_clone_deferral.jsonl` (#23) — CLONE tree, deferral.
- `golden/fixtures/explain_tolerance.jsonl` (#24) — within-lag-tolerance SKIP, upstream-OUTDATED EXECUTE.

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
| ExplainBadge | UPSTREAM_QUERY_UNCHANGED (7) | ❌ never (the "upstream model queries have not changed" line carries NO badge) |
| ExplainBadge | WITHIN_TOLERANCE (2) | ❌ never — see [§ unobserved](#unobserved--do-not-implement-until-captured) |
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
    - "«FQN»"  <FRESH>
      - no updates since "«FQN»" last executed         (clone-target anchor)
      - last updated: «rel_time»
  - upstream data is up to date                       [SUCCESS]
```

## UNKNOWN (untracked kinds, e.g. unit tests)

Client-side only (resolved in #20): State never tracks these nodes, they never
hit the Explain RPC, and the server returns **nothing** for them. The client
prints `UNKNOWN <uid> - dbt State explain details unavailable`. No server work.

## decision_description (separate output)

Selected independently of the tree (issue #22). The three model-SKIP strings are
distinct coexisting reasons on one hosted version (confirmed #24):

| # | string | situation |
|---|---|---|
| 1 | `model was a no-op because both its query and its upstream data are up to date` | model, no `lag_tolerance`, simple fresh |
| 2 | `model was a no-op because its query is up to date and its data is within lag tolerance` | model that CARRIES a `lag_tolerance` config, upstream fresh |
| 3 | `model was a no-op because its query is up to date and at least one upstream dependency is unchanged` | model, target exists, ≥1 genuine upstream unchanged (multi-upstream) |

Other pinned strings (match our engine's current pins; see `decision.rs`):
- model EXECUTE: `model was executed because either its query didn't match or its upstream data is out of date`
- model first build: `model was executed because its table did not exist`
- data test SKIP: `data test was a no-op because both its query and its upstream data are up to date`
- CLONE: `an equivalent model exists under another name so we cloned that one`

## Re-derivation binding (ADR 0005)

Each tree signal maps to a re-derivable source (see ADR 0005):

| Line / badge | Re-derivation source |
|---|---|
| `table analysis` existence + `<TARGET_TABLE_EXISTS>` | `node_decisions` decision class + persisted `decision_description` (NO live physical-existence probe — see [§ dropped-target known limitation](#known-limitation--dropped-own-target-rebuild-mislabel)) |
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

- **Node kinds:** seed (`execution_type` 9), snapshot (7), incremental model.
  Their explain trees (esp. whether incremental/snapshot add lines) are unknown.
- **Badges:** `WITHIN_TOLERANCE` — never produced even for a `lag_tolerance`
  model (#24: a fresh upstream shows `<FRESH>`, a newer one shows `<OUTDATED>` +
  EXECUTE; tolerance did not absorb an 11s gap under a 240h setting). May be
  unreachable on this hosted version; needs a dedicated probe before implementing.
  `UPSTREAM_QUERY_UNCHANGED` (7) — the corresponding line carries no badge, so
  this enum may be vestigial. `EB_UNSPECIFIED` (0).
- **Markers:** `INFO` (3), `EM_UNSPECIFIED` (0).
- **Data test EXECUTE** tree (only SKIP captured).

## Known limitation — dropped-own-target rebuild mislabel

Explain has NO own-target physical-existence signal and runs NO live warehouse
probe: the `table analysis` existence line (`<TARGET_TABLE_EXISTS>` vs "table
does not exist") is inferred from the persisted decision class + the
`decision_description` wording, not from a catalog lookup. (The
"physical-existence probe" phrasing in earlier drafts of the re-derivation
binding above was aspirational — no such probe exists in the code.)

CONSEQUENCE (PR #31 round-3 F6): if a confirmed model's own target is later
DROPPED and then rebuilt with UNCHANGED SQL, the decision persists the GENERIC
rebuild description ("model was executed because either its query didn't match
or its upstream data is out of date") — the SAME string a node-query-changed
rebuild uses. With no anchor and no "did not exist" wording, the generator falls
to SHAPE 7 and renders "the model table exists already" `<TARGET_TABLE_EXISTS>`
+ "the model query has changed" `<NODE_QUERY_CHANGED>` + "upstream data is up to
date" — wrong on both counts (the table is gone; the query did not change).

This is NOT fixed, deliberately. The dropped-target EXECUTE is INDISTINGUISHABLE
by any persisted signal from the hosted-characterized SHAPE-7 node-query-changed
EXECUTE (both anchor-less, both carrying that one generic rebuild description —
see `decision.rs::decision_descriptions_match_hosted_wording` and the SHAPE-7
conformance fixture). Any guard that suppressed the dropped-target tree would
also regress the captured SHAPE-7 fixture. The correct hosted shape for a
dropped-then-rebuilt target is UNOBSERVED (no Snowflake capture; DuckDB does not
trigger dbt State explain). Closing it needs a capture PLUS a persisted
own-target existence signal on `ExplainDecisionRow` so `execute_tree` can select
a "table does not exist" branch (follow-up). Until then, per the do-not-invent
fencing rule above, we accept the known mislabel rather than ship a guard that
breaks a hosted-faithful common case.
