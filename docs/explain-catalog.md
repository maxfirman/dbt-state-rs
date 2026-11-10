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
- `explain_snapshot_history.jsonl` (client 2.0.11): snapshot first build,
  SKIPs, source drift within Lag tolerance, upstream query change and logic
  change; FULL and VIEW logic-change rebuilds (SHAPE 7); a snapshot under
  `--full-refresh`. Captured by `experiments/explain_capture.py
  snapshot_explain`.
- `explain_incremental_append_merge_history.jsonl`,
  `explain_incremental_copy_history.jsonl`,
  `explain_incremental_submit_copy.jsonl` (client 2.0.11): APPEND and MERGE
  incrementals, their dev-clone copy and a submit-time copy probe
  ([INCREMENTAL](#incremental)). Captured by `experiments/explain_capture.py`
  `incremental_append_merge`, `incremental_copy` and
  `incremental_submit_copy`.
- `explain_ambiguous_rebuilds.jsonl`: crafted probes d1–d4 for the
  [ambiguous rebuild reasons](#ambiguous-rebuild-reasons). Hosted's SKIP and
  first-build trees there are keyed divergences: the server decides
  differently and renders nothing.

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

Each node kind × decision cell has one status (spec #40):

| Status | Meaning |
|---|---|
| characterized (live) | captured from hosted through the real client; the server renders the exact tree |
| characterized (crafted only) | captured from hosted for a `state-probe` request; the server renders the exact tree |
| unreachable on 2.0.11 | client-source evidence or a live negative result shows the cell cannot occur |
| local-only, empty tree | a decision this server makes that hosted never makes; the server renders nothing |
| out of scope → #N | owned by another issue |
| pending → #N | not yet settled; the named #40 child ticket sets the final status |

A cell renders a tree only if `is_characterized` opens it. "Fence" says
whether it is open. The evidence column gives the census tree counts by origin
(see [Explain tree census](#explain-tree-census)). The real-client trees in the
tracked fixtures were recorded with `dbt_run_cache_version` 2.0.6, except the
9 in `explain_snapshot_history.jsonl` and the 21 in the
`explain_incremental_*` append/merge fixtures, which come from 2.0.11.

| `execution_type` | Node kind | SKIP | EXECUTE | CLONE | Fence | Evidence (tracked trees, 2026-11-10) |
|---|---|---|---|---|---|---|
| 1 | FULL (table / model) | characterized (live) | characterized (live)¹ | characterized (live) | S E C | SKIP 19 (4 client, 15 crafted), EXECUTE 35 (4 client, 31 crafted), CLONE 8 (1 client, 7 crafted) |
| 2 | APPEND (incremental) | characterized (live)⁶ | characterized (live)⁷ | out of scope → #97⁸ | S E | SKIP 5, EXECUTE 6, all client 2.0.11 (`explain_incremental_*`) |
| 3 | MERGE (incremental) | characterized (live)⁶ | characterized (live)⁷ | out of scope → #97⁸ | S E | SKIP 4, EXECUTE 6, all client 2.0.11 (`explain_incremental_*`) |
| 4 | INSERT_OVERWRITE (incremental) | pending → #73 | pending → #73 | pending → #73 | — | none |
| 5 | DELETE_INSERT (incremental) | pending → #73 | pending → #73 | pending → #73 | — | none |
| 6 | MICROBATCH (incremental) | pending → #73 | pending → #73 | pending → #73 | — | none |
| 7 | SNAPSHOT | characterized (live) | characterized (live) | pending → #76 | S E | SKIP 3, EXECUTE 3, all client 2.0.11 (`explain_snapshot_history.jsonl`) |
| 8 | DBT_DATA_TEST | characterized (live) | pending → #75 | out of scope → #68 | S | SKIP 1 (client) |
| 9 | VALUES (seed) | characterized (crafted only) | characterized (crafted only)² | pending → #76 | S E | SKIP 1, EXECUTE 5, all crafted (`lag_seed_upstream.jsonl`) |
| 10 | VIEW | characterized (live) | characterized (live)³ | pending → #78⁴ | S E C | SKIP 1 (client 2.0.6); EXECUTE 1 (client 2.0.11); CLONE 0 |
| 11 | DBT_CUSTOM | pending → #74 | pending → #74 | pending → #74 | — | none |
| 0 / other | UNSPECIFIED / unknown | unreachable on 2.0.11⁵ | unreachable on 2.0.11⁵ | unreachable on 2.0.11⁵ | — | none |

1. Except the three rebuild reasons hosted never makes, which are
   local-only, empty tree ([Ambiguous rebuild reasons](#ambiguous-rebuild-reasons)),
   the rebuild after an execution-type change (a snapshot under
   `--full-refresh`): hosted decides it differently, so it renders no tree
   (local-only, empty tree; divergence → #83), and the rebuild over a Result
   copy this server does not adopt (`no_reuse_candidate.copy_logic_mismatch`,
   divergence → #95), which renders no tree for any kind.
2. Except the reload of an externally modified seed table, which is
   local-only, empty tree: hosted SKIPs it
   ([correctness-handoff.md](correctness-handoff.md), "Seed table externally
   modified").
3. The SHAPE 7 tree (`the model query has changed` <NODE_QUERY_CHANGED>) is
   captured over State history for a FULL model and a view in
   `explain_snapshot_history.jsonl` (client 2.0.11) and asserted by the
   generic replay (#78 item 2). The 3 VIEW EXECUTE trees in the removed
   `explain_snowflake_happy.jsonl` (client 2.0.6, commit `8dff472`) had the
   same shape.
4. The cell has been open since `bc8d80e`, together with FULL × CLONE, but no
   capture of a view copy exists (#78 item 1). The live attempt is part of
   #76.
5. The client's `execution_type_from_input` always yields a concrete kind
   ([protocol.md](protocol.md#what-the-client-computes)). The server keeps
   these values fenced regardless.
6. Except the post-copy SKIP after the dev-clone pre-step, which this server
   rebuilds (divergence → #95): local-only, empty tree.
7. Except the rebuild after a `--full-refresh` run (divergence → #90) and
   the rebuild over an unadopted copy (#95): both local-only, empty tree.
8. Hosted answers the dev-clone pre-step's `RegisterClone` with an empty
   `execution_decision_id`, so the client never explains that copy. The one
   submit-time copy probe was rebuilt, not copied; client 2.0.6 did get an
   incremental submit copy (`clone_failed_fallback.jsonl`, no Explain). See
   [INCREMENTAL](#incremental).

Adoption of a copy-origin row by a later submit does not depend on the
execution type, so its anchored subtree renders for a FULL model even when the
copy was registered for another kind.

## Observed vocabulary

Line counts from the [census](#explain-tree-census) over the 98 trees in the
tracked fixtures (2026-11-10). A line without a marker or badge is "none"
(705 lines without a marker, 768 without a badge). Only an explicit `0` is
counted as unspecified.

| Enum | Value | Observed? |
|---|---|---|
| ExplainMarker | SUCCESS (1) | ✅ 260 lines |
| ExplainMarker | FAIL (2) | ✅ 110 lines |
| ExplainMarker | INFO (3) | not observed across 98 trees (2026-11-10; clients 2.0.6 and 2.0.11, crafted probes)¹ |
| ExplainMarker | EM_UNSPECIFIED (0) | not observed across 98 trees (2026-11-10; clients 2.0.6 and 2.0.11, crafted probes)¹ |
| ExplainBadge | TARGET_TABLE_EXISTS (8) | ✅ 83 lines |
| ExplainBadge | NODE_QUERY_CHANGED (4) | ✅ 7 lines (SHAPE 7, `explain_snapshot_history.jsonl`, `explain_incremental_append_merge_history.jsonl`) |
| ExplainBadge | NODE_QUERY_UNCHANGED (5) | ✅ 77 lines |
| ExplainBadge | UPSTREAM_QUERY_CHANGED (6) | ✅ 24 lines |
| ExplainBadge | UPSTREAM_QUERY_UNCHANGED (7) | ✅ 10 lines (SHAPE 9 per-upstream query listing only) |
| ExplainBadge | FRESH (1) | ✅ 42 lines |
| ExplainBadge | OUTDATED (3) | ✅ 48 lines |
| ExplainBadge | WITHIN_TOLERANCE (2) | ✅ 16 lines (Lag tolerance line) |
| ExplainBadge | EB_UNSPECIFIED (0) | not observed across 98 trees (2026-11-10; clients 2.0.6 and 2.0.11, crafted probes)¹ |

1. The 98 trees are 9 real-client trees from client 2.0.6 (2 of them with
   the version unrecorded), 30 from client 2.0.11 and 59 for crafted
   `state-probe` requests. The removed `explain_snowflake_happy.jsonl` and
   `explain_clone_deferral.jsonl` captures (client 2.0.6, 179 trees) have none
   either.

Client rendering (`a0fc53e`): the client renders `INFO` as `[INFO]` and
renders `EM_UNSPECIFIED` and `EB_UNSPECIFIED` as nothing. No client code path
treats `INFO` specially. A hosted `INFO` line or unspecified value would
therefore print like any other line, and the census names the fixture and
entry of any it finds.

## Explain tree census

`experiments/explain_census.py` counts, for each node kind × decision class,
the trees, the empty trees, their origin, and every marker and badge over all
tree lines. A tree's node kind comes from the submit that minted its
`execution_decision_id` (SubmitEnrichedSQL and RegisterClone by
`execution_type`, SubmitValues as VALUES). Its origin is `crafted` when that
submit came from `state-probe`, otherwise the client version. The census
names the fixture and entry of every `INFO`, unspecified or unmapped value.
How to rerun it: [testing.md](testing.md#explain-tree-census).

Tracked fixtures, 2026-11-10 (98 trees, none empty, no findings):

| Node kind | Decision | Trees | Origin |
|---|---|---|---|
| FULL | SKIP | 19 | 15 crafted, 3 client 2.0.6, 1 client (version unrecorded) |
| FULL | EXECUTE | 35 | 31 crafted, 2 client 2.0.11, 1 client 2.0.6, 1 client (version unrecorded) |
| FULL | CLONE | 8 | 7 crafted, 1 client 2.0.6 |
| APPEND | SKIP | 5 | client 2.0.11 |
| APPEND | EXECUTE | 6 | client 2.0.11 |
| MERGE | SKIP | 4 | client 2.0.11 |
| MERGE | EXECUTE | 6 | client 2.0.11 |
| SNAPSHOT | SKIP | 3 | client 2.0.11 |
| SNAPSHOT | EXECUTE | 3 | client 2.0.11 |
| DBT_DATA_TEST | SKIP | 1 | client 2.0.6 |
| VALUES | SKIP | 1 | crafted |
| VALUES | EXECUTE | 5 | crafted |
| VIEW | SKIP | 1 | client 2.0.6 |
| VIEW | EXECUTE | 1 | client 2.0.11 |

The "client (version unrecorded)" trees are in `explain_tolerance.jsonl`,
which was trimmed without its ValidateClientVersion row.

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

`explain_snapshot_history.jsonl` (`s72fl`, `s72vl`, `s72sl`),
`explain_incremental_append_merge_history.jsonl` (`a73b`, `m73b`, `a73t`,
`m73t`).
```
- table analysis ("«FQN»")
  - the model table exists already                    [SUCCESS] <TARGET_TABLE_EXISTS>
- query analysis
  - the model query has changed                       [FAIL]    <NODE_QUERY_CHANGED>
  - upstream model queries have not changed           [SUCCESS]
- data freshness analysis
  <freshness body>
```

The freshness body is the SKIP one (above), rendered by
`explain.rs::data_freshness_analysis_fresh`. A view (whose submit reports no
upstreams) renders `upstream data is up to date`. A model lists its upstream
with `<FRESH>` and `no updates since "«FQN»" last executed`. An
identity-mismatch rebuild has no matched build, so the listing renders
`<FRESH>` for every upstream, and the summary is judged against the latest
build in scope, whose identity did not match:
`upstream data is up to date` when every upstream epoch equals the one
recorded at that build (the incremental logic changes), none when one moved
back (`s72fl`, `s72sl`). The decision persists that fact
(`node_decisions.unmatched_upstreams_unchanged`, migration 0017) because
that build is not the Explain anchor. A drifted upstream under a query
change is unobserved.

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

## SNAPSHOT

`explain_snapshot_history.jsonl` (client 2.0.11). A snapshot (execution type
7) renders the MODEL trees above with `snapshot` as the noun in every line that
names the node: `the snapshot table exists already`, `the snapshot query has
(not) changed`, `the snapshot table does not exist`, `there is no other table
that the snapshot could be cloned from` and `the snapshot was last built «age»
ago, within lag tolerance of «T»`. `upstream model queries have not changed`
and `an upstream model query has changed` keep `model`.

| Scenario (node) | Decision | Tree |
|---|---|---|
| first execution (`s72sd`) | EXECUTE | [First build](#first-build) |
| unchanged rerun, source upstream (`s72sd`) | SKIP | SKIP, `<FRESH>` listing, `upstream data is up to date` |
| unchanged rerun, model upstream (`s72su`) | SKIP | SKIP, `<FRESH>` listing, no summary (epoch moved back) |
| its source gained rows (`s72sd`) | SKIP (`is_stale`) | SKIP, `<OUTDATED>` listing, `upstream data is outdated`, Lag tolerance line |
| upstream query change (`s72su`) | EXECUTE | SHAPE 9 with the per-upstream query listing |
| its own logic changed (`s72sl`) | EXECUTE | SHAPE 7 |
| run under `--full-refresh` (`s72sf`) | EXECUTE | none (see below) |

The client's default Lag tolerance for these snapshots was 45 minutes, so the
source drift was a reuse within tolerance. A snapshot SKIP's
`decision_description` is always the "both … up to date" one
([protocol.md](protocol.md#decision-descriptions)).

**`--full-refresh`.** The client submits the snapshot as FULL (execution type
1, so the tree would use `model`) over its SNAPSHOT State history. Hosted
rebuilds with rejection codes 2/2 (EXECUTION_TYPE_MISMATCH) and renders the
FULL node-query-unchanged tree with the anchored `<FRESH>` listing. This
server rebuilds with codes 6/6 and the Decision reason
`no_reuse_candidate.execution_type_changed`, and renders no tree (decision
divergence → #83).

Every captured snapshot has an upstream. A snapshot without one is
unobserved; it renders like a view (`upstream model queries have not changed`
and `upstream data is up to date`, `explain.rs::has_upstream_query_line`).

Snapshot CLONE is pending → #76.

## INCREMENTAL

Each incremental strategy is its own node kind (execution types 2–6,
[protocol.md](protocol.md#what-the-client-computes)). Two strategies share
rendering only where their captured trees are identical.

### APPEND (2) and MERGE (3)

`explain_incremental_append_merge_history.jsonl` (client 2.0.11). The append
and merge trees are identical to each other and to the [MODEL](#model) trees,
with `model` as the noun, and so are their `decision_description` strings.
Every node reads a source table created by the run.

| Scenario (append / merge node) | Decision | Tree |
|---|---|---|
| first build (`a73b` / `m73b`) | EXECUTE | [First build](#first-build) |
| unchanged rerun (`a73b` / `m73b`) | SKIP | SKIP, `<FRESH>` listing, `upstream data is up to date` |
| its own logic changed (`a73b` / `m73b`) | EXECUTE | [SHAPE 7](#execute-node-query-changed-shape-7), with `upstream data is up to date` |
| source gained a row, default Lag tolerance 45 minutes (`a73d` / `m73d`) | SKIP (`is_stale`) | SKIP, `<OUTDATED>` listing, `upstream data is outdated`, Lag tolerance line |
| source gained a row, `lag_tolerance: 0s` (`a73z` / `m73z`) | EXECUTE | SHAPE 9 data-only rebuild: `upstream model queries have not changed`, `<OUTDATED>` listing, `upstream data is outdated` |
| first incremental run, reading `{{ this }}` (`a73t` / `m73t`) | EXECUTE | SHAPE 7: the compiled SQL gained the `is_incremental()` filter |
| second incremental run, reading `{{ this }}` (`a73t` / `m73t`) | SKIP | SKIP, `<FRESH>` listing of the source only |
| ordinary run after a `--full-refresh` run (`a73b` / `m73b`) | EXECUTE | none (see below) |
| unset strategy with a `unique_key` (`u73`) | SKIP | sent as APPEND; the APPEND SKIP tree |

**Self-read.** The client merges a `{{ this }}` read into the Target
relation's single entry in `tables`, so the self-read is never listed as an
upstream and never shows as upstream drift.

**Unset strategy.** On Snowflake, client 2.0.11 sends an incremental without
`incremental_strategy` as APPEND (2), the request builder's default, even
with a `unique_key` (the Snowflake adapter's default strategy is merge).

**`--full-refresh`.** The client sends no Submit for an incremental under
`--full-refresh`. It records the build with a write-only `RecordExecutions`
whose request is built as FULL (execution type 1), and hosted stores it
(entries 69–70). The next ordinary run submits APPEND/MERGE again. Hosted
rebuilds it with "the table was modified after the cached execution" and the
SKIP-shaped [own-target-modified tree](#execute-target-relation-modified-after-the-build).
This server takes the recorded FULL execution as the latest row, rebuilds with
`no_reuse_candidate.execution_type_changed` and renders no tree (decision
divergence → #90).

**Result copy.** `explain_incremental_copy_history.jsonl`: with the
`dev_clone` target (auto-defer through the profile's `defer_to_target`) and
`state.pre_clone` unset (IfTableMissing), the client copies an absent dev
relation with `RegisterClone`, confirms the copy, then submits the model.
Hosted answers `RegisterClone` with an empty `execution_decision_id`, so that
CLONE decision is never explained. Hosted SKIPs the post-copy submit with the
SKIP tree, its listing anchored on the dev relation (`no updates since
"«dev FQN»" last executed`). This server does not adopt the copy and rebuilds
(`no_reuse_candidate.copy_logic_mismatch`), so it renders no tree (decision
divergence → #95). `explain_incremental_submit_copy.jsonl` (one probe,
`state.pre_clone: never`): an absent dev relation whose prod build exists is
built, not copied; hosted reports skip code 1, this server 6 (#95); both
render the first-build tree. A submit-time incremental CLONE tree is not
captured (→ #97).

### INSERT_OVERWRITE (4)

Pending → #73 (insert_overwrite, delete+insert and microbatch are captured
separately).

### DELETE_INSERT (5)

Pending → #73.

### MICROBATCH (6)

Pending → #73.

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
divergence (hosted skips it; see [correctness-handoff.md](correctness-handoff.md)).
Its status is local-only, empty tree.

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

- **Node kinds:** INSERT_OVERWRITE, DELETE_INSERT and MICROBATCH
  incrementals (#73), APPEND and MERGE CLONE (#97), snapshot CLONE (#76), seed
  CLONE, view CLONE (fence open without a capture, #78).
- **Badges:** `EB_UNSPECIFIED` (0); census counts in
  [Observed vocabulary](#observed-vocabulary).
- **Markers:** `INFO` (3), `EM_UNSPECIFIED` (0); same.
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
| EXECUTE | `no_reuse_candidate.execution_type_changed` | empty tree (decision divergence, #83, #90) |
| EXECUTE | `no_reuse_candidate.copy_logic_mismatch` | empty tree (decision divergence, #95) |
| EXECUTE | `no_reuse_candidate.own_target_changed_or_missing`; `no_reuse_candidate.own_target_modified` otherwise; `no_build_anchor` | empty tree, local-only ([below](#ambiguous-rebuild-reasons)) |
| EXECUTE | any other reason when the decision-time inputs lack the Target relation | first build (`lag_copy_candidates` `dt_m`) |
| EXECUTE | `upstream_outdated` / `upstream_query_changed` otherwise; `no_reuse_candidate.identity_mismatch`, `.failed_reuse_qualification`, `.incomplete_cached_test_results`; `dependency_set_changed`; `copy_chain_limit` | SHAPE 7 |
| CLONE | `result_copy` with an anchor and the Target relation present | CLONE over an existing Target relation |
| CLONE | `result_copy` otherwise (submit Result copy and RegisterClone) | CLONE |
| any | NULL (row written before migration 0014), unknown, or contradicting the class | empty tree |

## Ambiguous rebuild reasons

Three model rebuild reasons used to fall back to the SHAPE 7 tree. Crafted
probes show hosted never makes any of them, so each one is local-only and
renders nothing (`explain.rs::shape`). Client 2.0.11 sends none of the
requests that lead to them. Probes: `experiments/ambiguous_rebuilds.py`
(2026-11-10, fixture `explain_ambiguous_rebuilds.jsonl`, crafted only). Each
probe builds and confirms a FULL model over one source, then resubmits it with
the same identity two seconds later.

| Decision reason | When this server records it | Hosted (crafted probe) | Status |
|---|---|---|---|
| `no_reuse_candidate.own_target_changed_or_missing` (model) | The Target relation is reported twice or with no epoch, or the submit has no target and a build with no target exists | d1, reported twice with the build's epoch: SKIP, ordinary SKIP tree (entries 2–3). d2, no epoch: rebuild as a missing Target relation, codes 4 / 7, first-build tree (entries 6–7). d3 / d3e, target unset / `""`: `Internal` / `InvalidArgument` error, not recorded | local-only, empty tree. Divergences #80 (d1, d2) and #81 (d3: this server answers a Result copy of the node's own build) |
| `no_build_anchor` | The reuse candidate's build was confirmed without an epoch and the node has genuine upstreams | d4: the ConfirmExecution without an epoch fails (`Internal`, "Database operation failed"), so hosted holds no such build. The decisive submit was not sent | local-only, empty tree. Divergence #82 |
| `no_reuse_candidate.own_target_modified` with genuine upstreams and no matched build | The anchor's `executions` row disappeared after the decision (`reuse_candidate::Selection::of` keeps the rejected row otherwise) | not probed: no request produces it | local-only, empty tree |

A seed's `no_reuse_candidate.own_target_changed_or_missing` (its table was
modified after the load) is the seed safety floor in [SEED](#seed), also
local-only, empty tree.

Why client 2.0.11 cannot produce the first two (`a0fc53e`):

- `collect_table_modified_infos` keys the relations it reports by name, so
  `tables` holds one entry per relation
  (`crates/dbt-tasks-core/src/run_cache/run_cache_service.rs:3718-3722`,
  `:3807-3815`).
- It drops the Target relation when its epoch is unknown (`:3809`). This
  server then sees a missing Target relation (`own_target_missing`).
- It always sets the target for non-test nodes
  (`crates/dbt-tasks-core/src/run_cache/run_cache_request.rs:543`); only data
  tests clear it (`:322`).
- It sends no ConfirmExecution for a non-test build without an epoch
  (`run_cache_service.rs:2162-2172`).

The hosted d1 and d2 trees use existing shapes (the anchored SKIP tree and the
first-build tree). If #80 aligns the decisions, the server renders them
through the ordinary SKIP and first-build arms, and the stale-exemption guard
fails the four keyed gaps until they are removed.
