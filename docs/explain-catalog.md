# `dbt state explain` line & badge catalog

The catalog of record for the structured Explain tree
(`ExplainMessageEntry` → `ExplainLine` / `ExplainMarker` / `ExplainBadge`) that
`ExplainService.GetExplainMessages` returns. Every tree is pinned from live
captures against `api.state.dbt.com` (Snowflake), never inferred. A cell
without a capture renders nothing and has a final status
([Decision reason coverage](#decision-reason-coverage)).

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
- `explain_custom_history.jsonl` (client 2.0.11): custom materialization
  (DBT_CUSTOM) first build, SKIPs, source drift under the default and a `0s`
  Lag tolerance, upstream query change, logic change and a `--full-refresh`
  rerun. Captured by `experiments/explain_capture.py custom_explain`.
- `explain_custom_copy.jsonl` (client 2.0.11): a custom node and a FULL
  control built in `dev_clone` over equivalent prod builds. Hosted copies only
  the FULL one and rebuilds the custom node (1 / 3, #86).
  Captured by `experiments/explain_capture.py custom_copy`.
- `explain_incremental_append_merge_history.jsonl`,
  `explain_incremental_copy_history.jsonl`,
  `explain_incremental_submit_copy.jsonl` (client 2.0.11): APPEND and MERGE
  incrementals, their dev-clone copy and a submit-time copy probe
  ([INCREMENTAL](#incremental)). Captured by `experiments/explain_capture.py`
  `incremental_append_merge`, `incremental_copy` and
  `incremental_submit_copy`.
- `explain_incremental_insert_overwrite.jsonl`,
  `explain_incremental_delete_insert.jsonl`,
  `explain_incremental_microbatch.jsonl` (client 2.0.11): per-strategy
  [INCREMENTAL](#incremental) trees, captured by `experiments/explain_capture.py
  incremental_insert_overwrite`, `incremental_delete_insert` and
  `incremental_microbatch`.
- `explain_incremental_copy_over.jsonl` (client 2.0.11): one model per
  incremental strategy, built in `dev_clone` with the prod logic over an
  existing dev relation while the prod build exists. Hosted rebuilds all five
  (1 / 6 or 1 / 2, #101). Captured by
  `experiments/explain_capture.py incremental_copy_over`.
- `explain_execution_type_change.jsonl` (client 2.0.11): the same SQL under
  another execution type, and which build hosted judges it against
  ([EXECUTE: execution type change](#execute-execution-type-change)). Two
  windows concatenated: `experiments/explain_capture.py execution_type_change`,
  then `execution_type_change_append --run-id` of that run.
- `explain_ambiguous_rebuilds.jsonl`: crafted probes d1–d4 for the
  [ambiguous rebuild reasons](#ambiguous-rebuild-reasons). Hosted's SKIP and
  first-build trees there are keyed divergences: the server decides
  differently and renders nothing.
- `explain_copy_probes.jsonl` (client 2.0.11): a seed, a snapshot and a view
  built in `prod_demo`, then in `dev_clone` (one copy attempt per path, #76,
  #78). Hosted copies the seed (VALUES CLONE tree, fenced → #91) and builds
  the view and the snapshot. Captured by `experiments/explain_capture.py
  copy_probes`.
- `explain_snapshot_register_clone.jsonl` (client 2.0.11): a snapshot copied
  into `dev_clone` by the client's RegisterClone pre-step, then its post-copy
  SKIP. Captured by `experiments/explain_capture.py
  snapshot_register_clone`.
- `explain_post_copy_identity.jsonl` (client 2.0.11, #93): post-copy SKIPs
  after the RegisterClone pre-step for three snapshots (over a fixed prod
  source, a ref'd table and a ref'd view, each resolved per target) and an
  append incremental over the ref'd table. Captured by
  `experiments/explain_capture.py post_copy_identity`.
- `explain_data_test_history.jsonl` (client 2.0.11): data-test first
  execution, unchanged rerun, logic change and upstream data drift for a
  passing, a failing and a `store_failures` test. Captured by
  `experiments/explain_capture.py data_test_execute`.
- `explain_store_failures_audit.jsonl` (client 2.0.11, #88): a failing
  `store_failures` test (audit table) and a `store_failures_as: view` test,
  first execution and unchanged rerun. Captured by
  `experiments/explain_capture.py store_failures_audit`.
- `explain_store_failures_audit_schema.jsonl` and
  `explain_store_failures_audit_alias.jsonl` (client 2.0.11, #106):
  `store_failures` tests (audit table, audit view) whose audit relation is in
  a separate audit schema or under a configured alias, first execution and
  unchanged rerun. Captured by `experiments/explain_capture.py
  store_failures_audit_schema` / `store_failures_audit_alias`.
- `explain_rebuild_reason_gaps.jsonl` (client 2.0.11, #40 code review): a
  FULL model whose logic changed while its upstream drifted (SHAPE 7 with an
  OUTDATED listing), and data tests over a model whose logic changed, with
  no upstreams and over two upstreams. Captured by
  `experiments/explain_capture.py rebuild_reason_gaps`.
- `explain_reason_kind_cells.jsonl` (client 2.0.11, #104 group b): every
  model kind but FULL without upstreams (SKIP and logic change), its Target
  relation modified after the build (an External modification or a later
  FULL full refresh), an execution type change over a FULL full refresh or a
  VIEW / DBT_CUSTOM switch, an upstream query change, and a logic change over
  a dropped Target relation. Two windows concatenated:
  `experiments/explain_capture.py reason_kind_cells`, then
  `reason_kind_cells_more --run-id … --prior-window …`. All but one explain
  were sent by `state-probe` for the real client's decisions; the one sent by
  `dbt state explain` (entry 350, `di_d`) got the same answer as its probe
  (entry 360).
- `explain_reason_kind_cells_custom.jsonl` (client 2.0.11, #104 group b): a
  custom node with `on_schema_change` set and a FULL table with the same SQL
  over an upstream switching materializations. The custom node still sends
  only `lookback`, so these are logic-identity rebuilds (SHAPE 7). Captured by
  `experiments/explain_capture.py reason_kind_cells_custom`.
- `explain_incomplete_test_results.jsonl`: crafted probes, a data test
  confirmed without complete cached results, then resubmitted
  (`experiments/incomplete_test_results.py`). Hosted SKIPs; keyed
  accepted divergence #87.
- `explain_freshness_cells.jsonl` (client 2.0.11, #104): two windows
  concatenated. Run 1 (`experiments/explain_capture.py freshness_cells`)
  covers three groups. `fa_*`: ALL-policy reuse with one of two upstreams
  drifted. `fz_*`: drift past a `0s` Lag tolerance. `fr_*`: logic A, then
  B, then back to A while the upstream drifts within the tolerance (a stale
  older match). Run 2 (`freshness_cells_snapshot --run-id` of run 1, after
  the 45 minute default tolerance) rebuilds the snapshot `fd_sn` past it.
- `explain_unknown_epoch.jsonl`: crafted probes, a FULL model whose
  upstream epoch is unknown (`null`) at the build, at the resubmit or both
  (`experiments/unknown_epoch_probes.py`). Hosted SKIPs within the Lag
  tolerance; keyed accepted divergence #113.
- `explain_rebuild_tolerance_line.jsonl`: crafted probes, data-only
  rebuilds past a 20 s Lag tolerance for FULL (`f0`), SNAPSHOT (`s1`, `s2`)
  and INSERT_OVERWRITE (`i1`, `i2`), the `2` probes with a current
  equivalent elsewhere (`experiments/rebuild_tolerance_line_probes.py`).
  Only those two end with a Lag tolerance line.
- `explain_copy_logic_mismatch.jsonl` (client 2.0.11, #104): rebuilds over
  a dev-clone pre-step copy. Five incrementals and a snapshot change their
  logic before the copy. Three append incrementals are copied unchanged,
  then become a table, a view and a custom node. A table with a prod
  equivalent is built under `allow_clones: false`. Captured by
  `experiments/explain_capture.py copy_logic_mismatch`.
- `explain_copy_logic_crafted.jsonl` (#104): crafted probe `x1`, a table
  whose kind and logic both changed over an append incremental's
  RegisterClone copy, then every model kind over a copy whose source build
  read no upstream (`x2`–`x10`) and over a copy whose Target relation was
  dropped (`x11`–`x19`) (`experiments/copy_logic_probes.py`).
- `explain_malformed_requests.jsonl` (#104): crafted probe `e4`, a model
  whose query dependency is listed twice (`experiments/malformed_requests.py`).
  Hosted SKIPs the resubmit; keyed accepted divergence #114.
- `explain_view_reads_no_relation.jsonl` (client 2.0.11, #117): the SKIP and
  logic-change rebuild of four views that read no relation (no FROM, a CTE
  without FROM, VALUES, FLATTEN) and of a view over a `ref`. Captured by
  `experiments/explain_capture.py view_reads_no_relation`; the first SKIP
  explained by `dbt state explain`, the rest by `state-probe`.

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
| 0 | SKIP with no genuine upstreams (FULL, an incremental, a snapshot, a custom node, a data test, a view that reads no relation): no "upstream model queries" line; freshness body `no upstream dependencies`. |
| 1, 2, 4 | SKIPs that carry `upstream model queries have not changed` (a view over a `ref`, a data test, or any node with a genuine upstream). |
| 7 | Node-query-changed EXECUTE, for `no_reuse_candidate.identity_mismatch` and `no_reuse_candidate.copy_logic_mismatch` only ([Shape selection](#shape-selection)). |
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
| out of scope → #N | owned by another issue; the server renders nothing |

A cell renders a tree only if `is_characterized` opens it. "Fence" says
whether it is open. Inside an open cell, each Decision reason renders only
where a tracked capture backs it for that node kind; the status of every
other reason is in [Decision reason coverage](#decision-reason-coverage). The evidence column gives the census tree counts by origin
(see [Explain tree census](#explain-tree-census)). The real-client trees in the
tracked fixtures were recorded with `dbt_run_cache_version` 2.0.6, except the
9 in `explain_snapshot_history.jsonl`, the 4 in `explain_copy_probes.jsonl`
and `explain_snapshot_register_clone.jsonl`, the 11 in
`explain_data_test_history.jsonl`, the 2 in
`explain_store_failures_audit.jsonl`, the 4 in
`explain_store_failures_audit_schema.jsonl` and
`explain_store_failures_audit_alias.jsonl`, the 7 in
`explain_rebuild_reason_gaps.jsonl`, the 10 in `explain_custom_history.jsonl`
and `explain_custom_copy.jsonl`, the 21 in the `explain_incremental_*`
append/merge fixtures, the 20 in the three insert_overwrite,
delete+insert and microbatch fixtures, the 5 in
`explain_incremental_copy_over.jsonl`, the 5 in
`explain_execution_type_change.jsonl`, the 4 in
`explain_post_copy_identity.jsonl`, the 18 in
`explain_freshness_cells.jsonl`, the 10 in
`explain_copy_logic_mismatch.jsonl`, the 71 in
`explain_reason_kind_cells.jsonl` and `explain_reason_kind_cells_custom.jsonl`
and the 10 in `explain_view_reads_no_relation.jsonl`, which come from 2.0.11.

| `execution_type` | Node kind | SKIP | EXECUTE | CLONE | Fence | Evidence (tracked trees, 2026-11-11) |
|---|---|---|---|---|---|---|
| 1 | FULL (table / model) | characterized (live) | characterized (live)¹ ⁶ | characterized (live) | S E C | SKIP 25 (5 client, 20 crafted), EXECUTE 48 (13 client, 35 crafted), CLONE 8 (1 client, 7 crafted) |
| 2 | APPEND (incremental) | characterized (live)¹¹ | characterized (live)¹² | unreachable on 2.0.11¹³ | S E | SKIP 9 (8 client 2.0.11, 1 crafted), EXECUTE 18 (16 client 2.0.11, 2 crafted) (`explain_incremental_*`, `explain_execution_type_change.jsonl`, `explain_post_copy_identity.jsonl`, `explain_freshness_cells.jsonl`, `explain_copy_logic_*.jsonl`, `explain_malformed_requests.jsonl`, `explain_reason_kind_cells.jsonl`) |
| 3 | MERGE (incremental) | characterized (live)¹¹ | characterized (live)¹² | unreachable on 2.0.11¹³ | S E | SKIP 6, EXECUTE 20 (18 client 2.0.11, 2 crafted) (`explain_incremental_*`, `explain_execution_type_change.jsonl`, `explain_freshness_cells.jsonl`, `explain_copy_logic_*.jsonl`, `explain_reason_kind_cells.jsonl`) |
| 4 | INSERT_OVERWRITE (incremental) | characterized (live) | characterized (live)⁶ | unreachable on 2.0.11¹³ | S E | SKIP 6 (5 client 2.0.11, 1 crafted), EXECUTE 20 (16 client 2.0.11, 4 crafted) (`explain_incremental_insert_overwrite.jsonl`, `explain_incremental_copy_over.jsonl`, `explain_freshness_cells.jsonl`, `explain_rebuild_tolerance_line.jsonl`, `explain_copy_logic_*.jsonl`, `explain_malformed_requests.jsonl`, `explain_reason_kind_cells.jsonl`) |
| 5 | DELETE_INSERT (incremental) | characterized (live) | characterized (live)⁶ | unreachable on 2.0.11¹³ | S E | SKIP 5, EXECUTE 19 (17 client 2.0.11, 2 crafted) (`explain_incremental_delete_insert.jsonl`, `explain_incremental_copy_over.jsonl`, `explain_freshness_cells.jsonl`, `explain_copy_logic_*.jsonl`, `explain_reason_kind_cells.jsonl`) |
| 6 | MICROBATCH (incremental) | characterized (live) | characterized (live)⁶ | unreachable on 2.0.11¹³ | S E | SKIP 6, EXECUTE 19 (17 client 2.0.11, 2 crafted) (`explain_incremental_microbatch.jsonl`, `explain_incremental_copy_over.jsonl`, `explain_freshness_cells.jsonl`, `explain_copy_logic_*.jsonl`, `explain_reason_kind_cells.jsonl`) |
| 7 | SNAPSHOT | characterized (live)¹¹ | characterized (live)⁶ | unreachable on 2.0.11⁷ | S E | SKIP 13 (12 client 2.0.11, 1 crafted), EXECUTE 15 (11 client 2.0.11, 4 crafted) (`explain_snapshot_history.jsonl`, `explain_copy_probes.jsonl`, `explain_snapshot_register_clone.jsonl`, `explain_post_copy_identity.jsonl`, `explain_freshness_cells.jsonl`, `explain_rebuild_tolerance_line.jsonl`, `explain_copy_logic_*.jsonl`, `explain_malformed_requests.jsonl`, `explain_reason_kind_cells.jsonl`) |
| 8 | DBT_DATA_TEST | characterized (live) | characterized (live)⁹ | out of scope → #68 | S E | SKIP 15 (11 client 2.0.11, 1 client 2.0.6, 3 crafted), EXECUTE 12 (client 2.0.11) (`explain_data_test_history.jsonl`, `explain_rebuild_reason_gaps.jsonl`, `explain_store_failures_audit*.jsonl`, `explain_malformed_requests.jsonl`) |
| 9 | VALUES (seed) | characterized (crafted only) | characterized (crafted only)² | out of scope → #91⁸ | S E | SKIP 1, EXECUTE 5, all crafted (`lag_seed_upstream.jsonl`); CLONE 1 (client 2.0.11, `explain_copy_probes.jsonl`, fenced) |
| 10 | VIEW | characterized (live)¹⁴ | characterized (live)³ ¹⁴ | unreachable on 2.0.11⁴ | S E | SKIP 7 (5 client 2.0.11, 1 client 2.0.6, 1 crafted); EXECUTE 14 (12 client 2.0.11, 2 crafted) (`explain_view_reads_no_relation.jsonl`, `explain_reason_kind_cells.jsonl`, `explain_snapshot_history.jsonl`; `explain_copy_logic_crafted.jsonl` for the crafted ones); CLONE 0 |
| 11 | DBT_CUSTOM | characterized (live) | characterized (live) | unreachable on 2.0.11¹⁰ | S E | SKIP 9 (8 client 2.0.11, 1 crafted), EXECUTE 15 (13 client 2.0.11, 2 crafted) (`explain_custom_history.jsonl`, `explain_custom_copy.jsonl`, `explain_freshness_cells.jsonl`, `explain_copy_logic_*.jsonl`, `explain_malformed_requests.jsonl`, `explain_reason_kind_cells*.jsonl`) |
| 0 / other | UNSPECIFIED / unknown | unreachable on 2.0.11⁵ | unreachable on 2.0.11⁵ | unreachable on 2.0.11⁵ | — | none |

1. Per Decision reason in [Decision reason coverage](#decision-reason-coverage).
   The three rebuild reasons hosted never makes are local-only, empty tree
   ([Ambiguous rebuild reasons](#ambiguous-rebuild-reasons)). The rebuild
   over a RegisterClone copy whose logic differs from the copy's source
   build, even read in the source's schema
   (`no_reuse_candidate.copy_logic_mismatch`), renders SHAPE 7 judged
   against the copy ([Rebuild over a RegisterClone copy](#execute-rebuild-over-a-registerclone-copy)).
   The rebuild after a node kind change renders the
   [execution type change](#execute-execution-type-change) tree, also over a
   copy.
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
4. Closed by #76 (#78 item 1), live negative 2026-11-10
   (`explain_copy_probes.jsonl` entry 13). A view built in `prod_demo`, then
   in `dev_clone` with its target missing, is answered `ready_to_execute`
   with codes 1 / 3 (TARGET_TABLE_MISMATCH / EXECUTION_TYPE_NOT_FULL): hosted
   finds the equivalent but does not copy a non-FULL node on submit. The
   client sends RegisterClone only for incrementals and snapshots
   (`run_cache_dev_clone.rs::dev_clone_candidate_for_node`). The cell had been
   open since `bc8d80e` without a capture; it is now fenced. This server
   sends 1 / 3 too (FULL-only submit copy, #92). The dev SQL reads the prod
   source under another default schema; the server finds the prod build by
   reading the submit in that schema (#107).
5. The client's `execution_type_from_input` always yields a concrete kind
   ([protocol.md](protocol.md#what-the-client-computes)). The server keeps
   these values fenced regardless.
6. Including the rebuild over a RegisterClone copy that the submit cannot
   adopt (`no_reuse_candidate.copy_logic_mismatch`, live 2026-11-11, #104,
   `explain_copy_logic_mismatch.jsonl`): SHAPE 7 judged against the copy
   ([Rebuild over a RegisterClone copy](#execute-rebuild-over-a-registerclone-copy)).
   Hosted reuses a copy whose post-copy submit has the source build's logic
   read in the source's schema (#93, `explain_snapshot_register_clone.jsonl`,
   `explain_post_copy_identity.jsonl`), and so does this server.
7. Live, 2026-11-10 (#76). On submit, hosted answers a dev snapshot whose
   equivalent exists in prod with 1 / 3, as for a view
   (`explain_copy_probes.jsonl` entry 14). The copy through the client's
   RegisterClone pre-step is granted (`explain_snapshot_register_clone.jsonl`
   entry 5), but its response carries an empty `execution_decision_id`, so
   no Explain message exists for it. The decision that follows is the
   post-copy submit, a SKIP.
8. Hosted copies a seed on submit (`explain_copy_probes.jsonl` entry 12,
   `an equivalent seed exists under another name so we cloned that one`).
   This server never copies seeds, so the decision diverges (#91) and the
   cell stays fenced until #91 lands. The captured tree is in [SEED](#seed).
9. For a first execution, a logic change, upstream data drift and an
   upstream whose logic changed, with or without upstreams
   ([DATA TEST](#data-test)). One rebuild is local-only, empty tree,
   because hosted SKIPs the test instead: cached results that are
   incomplete (`no_reuse_candidate.incomplete_cached_test_results`, crafted
   only, accepted divergence #87). A `store_failures` test whose audit relation the
   request does not name (a configured alias) rebuilds for
   `dependency_set_changed` where hosted SKIPs (divergence #123).
   A test failing reuse qualification is out of scope → #67
   ([Decision reason coverage](#decision-reason-coverage)).
10. Live, 2026-11-10 (#74, #86). Hosted does not copy a custom node: built
    in `dev_clone` over an equivalent prod build, it rebuilds with codes 1 / 3
    (EXECUTION_TYPE_NOT_FULL) and the first-build tree, where it copies a FULL
    control (`explain_custom_copy.jsonl`, one attempt). The client sends
    `RegisterClone` only for incrementals and snapshots, so no custom CLONE
    decision has an Explain message. This server copies only FULL on submit
    and makes the same decision, with the same tree (#86).
11. Including the post-copy SKIP after the dev-clone pre-step (#93,
    `explain_incremental_copy_history.jsonl`,
    `explain_snapshot_register_clone.jsonl`,
    `explain_post_copy_identity.jsonl`). Its listing gives a FRESH verdict
    only to an upstream the copy's source build recorded; an upstream
    resolved per target under another name is listed with its `last updated`
    line only ([SNAPSHOT](#snapshot)).
12. Including the rebuild over an unadopted copy (live 2026-11-11, #104):
    [Rebuild over a RegisterClone copy](#execute-rebuild-over-a-registerclone-copy).
    The rebuild after a `--full-refresh` run and other execution type changes:
    [EXECUTE: execution type change](#execute-execution-type-change).
13. Live 2026-11-11 (#73 73c, #101), one attempt for all five strategies
    (`explain_incremental_copy_over.jsonl`). Hosted 2.0.11 has no
    explainable incremental CLONE decision. Its copy through the dev-clone
    pre-step (`RegisterClone`) has an empty `execution_decision_id`, and on
    submit it rebuilds instead of copying: over an existing dev relation
    with codes 1 / 6 (merge, insert_overwrite, microbatch) or 1 / 2 (append,
    delete+insert), and into an absent one with 1 / 6
    (`explain_incremental_submit_copy.jsonl`, append and merge). This server
    copies only FULL on submit and makes the same rebuilds with the same
    SHAPE 7 trees (#101). Client 2.0.6 did get MERGE submit copies over an
    existing target (`clone_failed_fallback.jsonl`,
    `clone_submit_equivalent_fallback.jsonl`, no Explain); this server
    builds there (keyed divergence). DuckDB's checked delete+insert submit
    copy stays local-only and renders nothing, as hosted does not engage
    DuckDB. See [INCREMENTAL](#incremental).
14. Client 2.0.11 reports no upstreams for a view, so the decision records
    whether the view's SQL reads a relation (`node_decisions.reads_relation`,
    migration 0019, #117). A view that reads none says `no upstream
    dependencies`; one over a `ref` says `upstream data is up to date`
    (`explain_view_reads_no_relation.jsonl`). A view whose SQL does not
    parse as one query renders nothing without reported upstreams.

Adoption of a copy-origin row by a later submit does not depend on the
execution type, so its anchored subtree renders for a FULL model even when the
copy was registered for another kind.

## Observed vocabulary

Line counts from the [census](#explain-tree-census) over the 316 trees in the
tracked fixtures (2026-11-11). A line without a marker or badge is "none"
(1824 lines without a marker, 2102 without a badge). Only an explicit `0` is
counted as unspecified.

| Enum | Value | Observed? |
|---|---|---|
| ExplainMarker | SUCCESS (1) | ✅ 820 lines |
| ExplainMarker | FAIL (2) | ✅ 264 lines |
| ExplainMarker | INFO (3) | not observed across 316 trees (2026-11-11; clients 2.0.6 and 2.0.11, crafted probes)¹ |
| ExplainMarker | EM_UNSPECIFIED (0) | not observed across 316 trees (2026-11-11; clients 2.0.6 and 2.0.11, crafted probes)¹ |
| ExplainBadge | TARGET_TABLE_EXISTS (8) | ✅ 248 lines |
| ExplainBadge | NODE_QUERY_CHANGED (4) | ✅ 62 lines (SHAPE 7, `explain_snapshot_history.jsonl`, `explain_custom_history.jsonl`, `explain_incremental_*.jsonl`, `explain_rebuild_reason_gaps.jsonl`, `explain_freshness_cells.jsonl`, `explain_copy_logic_*.jsonl`, `explain_reason_kind_cells*.jsonl`, `explain_view_reads_no_relation.jsonl`) |
| ExplainBadge | NODE_QUERY_UNCHANGED (5) | ✅ 202 lines |
| ExplainBadge | UPSTREAM_QUERY_CHANGED (6) | ✅ 30 lines |
| ExplainBadge | UPSTREAM_QUERY_UNCHANGED (7) | ✅ 10 lines (SHAPE 9 per-upstream query listing only) |
| ExplainBadge | FRESH (1) | ✅ 128 lines |
| ExplainBadge | OUTDATED (3) | ✅ 87 lines |
| ExplainBadge | WITHIN_TOLERANCE (2) | ✅ 39 lines (Lag tolerance line) |
| ExplainBadge | EB_UNSPECIFIED (0) | not observed across 316 trees (2026-11-11; clients 2.0.6 and 2.0.11, crafted probes)¹ |

1. The 316 trees are 9 real-client trees from client 2.0.6 (2 of them with
   the version unrecorded), 211 from client 2.0.11 and 96 for crafted
   `state-probe` requests. 80 of the client 2.0.11 trees explain a
   real-client decision but were requested with `state-probe`
   (`explain_reason_kind_cells*.jsonl`, `explain_store_failures_audit_schema.jsonl`,
   `explain_store_failures_audit_alias.jsonl`, `explain_view_reads_no_relation.jsonl`, `via="probe"` in
   [harness.md](harness.md#shared-live-capture-driver-experimentsexplain_capturepy)). The removed `explain_snowflake_happy.jsonl` and
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
`execution_type`, SubmitValues as VALUES; a speculative submit when hosted
decided on it, #103). Its origin is `crafted` when that
submit came from `state-probe`, otherwise the client version. The census
names the fixture and entry of every `INFO`, unspecified or unmapped value.
How to rerun it: [testing.md](testing.md#explain-tree-census).

Tracked fixtures, 2026-11-11 (316 trees, none empty, no findings):

| Node kind | Decision | Trees | Origin |
|---|---|---|---|
| FULL | SKIP | 25 | 20 crafted, 3 client 2.0.6, 1 client 2.0.11, 1 client (version unrecorded) |
| FULL | EXECUTE | 48 | 35 crafted, 11 client 2.0.11, 1 client 2.0.6, 1 client (version unrecorded) |
| FULL | CLONE | 8 | 7 crafted, 1 client 2.0.6 |
| APPEND | SKIP | 9 | 8 client 2.0.11, 1 crafted |
| APPEND | EXECUTE | 18 | 16 client 2.0.11, 2 crafted |
| MERGE | SKIP | 6 | client 2.0.11 |
| MERGE | EXECUTE | 20 | 18 client 2.0.11, 2 crafted |
| INSERT_OVERWRITE | SKIP | 6 | 5 client 2.0.11, 1 crafted |
| INSERT_OVERWRITE | EXECUTE | 20 | 16 client 2.0.11, 4 crafted |
| DELETE_INSERT | SKIP | 5 | client 2.0.11 |
| DELETE_INSERT | EXECUTE | 19 | 17 client 2.0.11, 2 crafted |
| MICROBATCH | SKIP | 6 | client 2.0.11 |
| MICROBATCH | EXECUTE | 19 | 17 client 2.0.11, 2 crafted |
| SNAPSHOT | SKIP | 13 | 12 client 2.0.11, 1 crafted |
| SNAPSHOT | EXECUTE | 15 | 11 client 2.0.11, 4 crafted |
| DBT_DATA_TEST | SKIP | 15 | 11 client 2.0.11, 3 crafted, 1 client 2.0.6 |
| DBT_DATA_TEST | EXECUTE | 12 | client 2.0.11 |
| VALUES | SKIP | 1 | crafted |
| VALUES | EXECUTE | 5 | crafted |
| VALUES | CLONE | 1 | client 2.0.11 (fenced, #91) |
| VIEW | SKIP | 7 | 5 client 2.0.11, 1 client 2.0.6, 1 crafted |
| VIEW | EXECUTE | 14 | 12 client 2.0.11, 2 crafted |
| DBT_CUSTOM | SKIP | 9 | 8 client 2.0.11, 1 crafted |
| DBT_CUSTOM | EXECUTE | 15 | 13 client 2.0.11, 2 crafted |

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

The `upstream model queries have not changed` line is omitted for a FULL model,
custom node, data test or a view that reads no relation, with no genuine
upstreams (SHAPE 0), and present otherwise (`explain.rs::has_upstream_query_line`).

Freshness body (`explain.rs::data_freshness_analysis_fresh`):

- **No genuine upstreams:** a FULL model, custom node or data test renders
  `- no upstream dependencies [SUCCESS]`. Client 2.0.11 reports no upstreams
  for a view, so a view's wording follows its SQL, which the decision records
  (`node_decisions.reads_relation`, migration 0019, #117): a view that reads
  no relation renders `- no upstream dependencies [SUCCESS]` (no FROM, a CTE
  without FROM, VALUES, a table function such as FLATTEN;
  `explain_view_reads_no_relation.jsonl`), a view over a `ref` renders
  `- upstream data is up to date [SUCCESS]` (`explain_happy_history.jsonl`,
  `vrr`). A view whose SQL was not judged (it does not parse as one query,
  or a CTE names itself or a later sibling without RECURSIVE) renders
  nothing. The other kinds were captured only with an upstream
  ([Decision reason coverage](#decision-reason-coverage)).
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

Decision reasons `no_reuse_candidate.identity_mismatch` and, over a
RegisterClone copy, `no_reuse_candidate.copy_logic_mismatch`
([below](#execute-rebuild-over-a-registerclone-copy)).
`explain_snapshot_history.jsonl` (`s72fl`, `s72vl`, `s72sl`),
`explain_incremental_append_merge_history.jsonl` (`a73b`, `m73b`, `a73t`,
`m73t`), `explain_rebuild_reason_gaps.jsonl` (`r40q`),
`explain_reason_kind_cells.jsonl` (`*_z`, entries 351–356: every incremental
strategy and a snapshot without upstreams),
`explain_view_reads_no_relation.jsonl` (entries 51–55: views that read no
relation and one over a `ref`).
```
- table analysis ("«FQN»")
  - the model table exists already                    [SUCCESS] <TARGET_TABLE_EXISTS>
- query analysis
  - the model query has changed                       [FAIL]    <NODE_QUERY_CHANGED>
  - upstream model queries have not changed           [SUCCESS] (not for SHAPE 0 kinds without upstreams)
- data freshness analysis
  <freshness body>
```

The freshness body is the SKIP one (above), judged against the latest build
in scope, whose identity did not match (an identity-mismatch rebuild has no
matched build). A view (whose submit reports no upstreams) renders `upstream
data is up to date` over a `ref` and `no upstream dependencies`, without the
`upstream model queries` line, when it reads no relation. For each upstream
of a model, against the epoch recorded for it at that build:

| Upstream epoch | Listing | Summary | Capture |
|---|---|---|---|
| unchanged | `<FRESH>`, `no updates since "«FQN»" last executed` | `upstream data is up to date` | the append and merge logic changes; the moving-window microbatch rebuild (`explain_incremental_microbatch.jsonl` entry 40) |
| moved back | `<FRESH>`, `no updates since "«FQN»" last executed` | none | `s72fl`, `s72sl`; the insert_overwrite, delete+insert and microbatch logic changes |
| drifted | `<OUTDATED>`, `«gap»` | `upstream data is outdated` [FAIL] | `r40q` (entry 33) |
| not recorded by that build | no badge, only `last updated: «rel_time»` | none | a table rebuilt over its view build (`explain_execution_type_change.jsonl` `tv`, entry 39) |

The decision persists that build's id (`node_decisions.unmatched_execution_id`,
migration 0017), and Explain re-derives the listing from it with the shared
freshness helper, as for a matched build. A rebuild recorded before migration
0017 has no such build: its listing keeps the default `<FRESH>` badges and
the summary line is omitted, because unknown evidence never proves freshness
(ADR 0007). A stale rebuild (an older matching build would have been reused
stale, so `is_stale` is set, codes 8 / 7) with a non-zero Lag tolerance and
an upstream drifted against that build ends with the Lag tolerance line
after `upstream data is outdated`:
`the model was last built «age» ago, within lag tolerance of «T»` [SUCCESS]
<WITHIN_TOLERANCE>. Captured for FULL, the five incrementals, SNAPSHOT
(`snapshot` noun) and DBT_CUSTOM (`explain_freshness_cells.jsonl` `fr_*`,
entries 114–121). The age is the older matching build's: in the
capture, the FULL decision came 32 seconds after the older build's
confirmation and 13 seconds after the latest one's, and hosted said "32
seconds". The decision records that age (`DecisionRecord::with_older_match`).
A rebuild recorded before #104 has none and renders nothing rather than an
unknown age. In this capture the upstream's epoch recorded by both builds
was the same, so the listing reads the same against either one.

### EXECUTE: upstream, against the matched build (SHAPE 9)

Rendered by `explain.rs::execute_outdated_tree`, from per-upstream verdicts
re-derived against the matched build. A data-only rebuild
(`upstream_outdated`) selects it only when an upstream's drift is proven
(`explain.rs::has_provably_outdated_upstream`: both epochs known and the
current one later). A rebuild for an upstream with an unknown epoch is
local-only and renders nothing: hosted reuses it within the Lag tolerance
(`explain_unknown_epoch.jsonl`, crafted, accepted divergence #113). Captured
for FULL, the five incrementals, SNAPSHOT and DBT_CUSTOM
([Decision reason coverage](#decision-reason-coverage)).

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
  against an older qualifying build), or the rebuild reports an equivalent
  under another name that it did not copy (codes 1 / 3, 1 / 6 or 1 / 2). The
  line then carries the equivalent's build age: "within" for a young one
  (`explain_rebuild_tolerance_line.jsonl` `s2`, `i2`), and
  `the snapshot was last built «age» ago, exceeds lag tolerance of «T»`
  [FAIL] <OUTDATED> for one older than the tolerance (`fd_sn`). A rebuild
  past the tolerance with no equivalent has no line (`f0`, `s1`, `i1`;
  `lag_tolerance_matrix.jsonl`). If every upstream epoch equals its
  recorded epoch: `- upstream data is up to date [SUCCESS]`. Otherwise none.

### EXECUTE: Target relation modified after the build

Reason `no_reuse_candidate.own_target_modified` with a matched build or no
genuine upstreams renders the SKIP tree: `the model table exists already`,
`the model query has not changed`, and for a node without upstreams
`no upstream dependencies` (`lag_tolerance_matrix` `lfs_u0`). With an
upstream, the listing and summary are judged against the matched build. That
build can be older than the latest one: after an execution type change the
submit is judged against the latest build of its own type
([below](#execute-execution-type-change)). Captured that way for FULL
(`explain_execution_type_change.jsonl` `ta`), APPEND and MERGE
(`explain_incremental_append_merge_history.jsonl` entries 78–79, `mi`), and
for all five incremental strategies with and without an upstream, after a
`--full-refresh` (`explain_reason_kind_cells.jsonl` `*_c`, `*_cz`, entries
222–230) and after an External modification (`*_e`, `*_ez`, entries
365–374).

Hosted does not make this rebuild for every kind (same fixture):

| Kind | Target relation changed by | Hosted | This server |
|---|---|---|---|
| SNAPSHOT (`sn_c`, `sn_cz`; `sn_e`, `sn_ez`) | a later `--full-refresh` (FULL) build; an External modification | SKIP, the ordinary SKIP tree | rebuilds 6 / 6, renders nothing (accepted divergence #115) |
| DBT_CUSTOM (`vc`; `cv0`) | an External modification; a view build in between | SKIP | the same |
| VIEW (`vc0`, history VIEW, DBT_CUSTOM, VIEW) | the custom node's build | EXECUTE 6 / 6, "model was executed because the view definition is newer than the cached execution", SKIP tree with `no upstream dependencies` | the same decision, description and tree (entry 377) |

A view whose SQL reads no relation says `no upstream dependencies`, one over
a `ref` says `upstream data is up to date`; client 2.0.11 reports no
upstreams for either, so the decision records which it is
(`node_decisions.reads_relation`, #117). This rebuild is captured only for a
view that reads no relation; for a view over a `ref` it renders nothing.

### EXECUTE: execution type change

`explain_snapshot_history.jsonl` entries 58–60, `explain_execution_type_change.jsonl`
(client 2.0.11). The SQL identity excludes the execution type. When the latest
build has the submit's identity under another execution type (a snapshot
under `--full-refresh` is sent as FULL; a materialization or incremental
strategy change), hosted judges the submit against the latest build of its own
type ([ADR 0007](adr/0007-lag-tolerance-and-upstream-producers.md#execution-type-change)):

| History (newest first) | Submit | Decision, codes | Tree |
|---|---|---|---|
| SNAPSHOT builds only (`s72sf`) | FULL | EXECUTE, 2 / 2, "either its query didn't match …" | SKIP tree against the latest build, no summary (epoch moved back) |
| FULL, VIEW, FULL (`tv`, run 2) | APPEND | EXECUTE, 2 / 2, same | SKIP tree against the latest build, `upstream data is up to date` |
| FULL (`ta`), MERGE (`mi`) | APPEND, DELETE_INSERT | EXECUTE, 2 / 2, same | not explained in the capture² |
| APPEND, FULL (`ta`); DELETE_INSERT, MERGE (`mi`) | FULL; MERGE | EXECUTE, 6 / 6, "the table was modified after the cached execution" | [Target relation modified](#execute-target-relation-modified-after-the-build), against the older build |
| FULL (RecordExecutions full refresh), APPEND/MERGE (`a73b`, `m73b`) | APPEND, MERGE | same | same |
| FULL (RecordExecutions full refresh), APPEND (`rf`) | FULL | SKIP | SKIP tree |
| VIEW, FULL (`tv`) | FULL | EXECUTE, 8 / 7 (a view sends other semantic extras, so its build has another identity) | SHAPE 7, its upstream not recorded by the view build |
| an APPEND RegisterClone copy (`explain_copy_logic_mismatch.jsonl` `kf4`) | FULL, same SQL and semantic extras | EXECUTE, 2 / 2, "either its query didn't match …" | SKIP tree against the copy, `upstream data is up to date` |
| FULL (`--full-refresh`, RecordExecutions or a snapshot's FULL submit) only (`explain_reason_kind_cells.jsonl` `*_e`, `*_ez`) | MERGE, INSERT_OVERWRITE, DELETE_INSERT, MICROBATCH, SNAPSHOT, with and without an upstream; APPEND without | EXECUTE, 2 / 2 | SKIP tree against the latest build (entries 232–242) |
| SNAPSHOT only (`sn_c`, `sn_cz`, round 2) | FULL (`--full-refresh`), with and without an upstream | EXECUTE, 2 / 2 | the same (entry 127 without an upstream) |
| VIEW (`vc0`) / DBT_CUSTOM (`cv0`), the SQL reads no relation | DBT_CUSTOM / VIEW | EXECUTE, 2 / 2 | the same, with `no upstream dependencies` (entries 244, 246) |
| VIEW (`vc`) / DBT_CUSTOM (`cv`), over an upstream | DBT_CUSTOM / VIEW | EXECUTE, 6 / 6: the view reports no upstreams, the custom node does, so hosted sees another identity; then 8 / 7 for the custom node over VIEW, DBT_CUSTOM (`cv`, entry 322) | `the model query has not changed`; the view lists the custom build's upstream as `view ignored` under `query analysis`, then `an upstream model query has changed [FAIL]` (entry 245). This server answers 2 / 2 and 6 / 6 and renders nothing (divergence #116) |
| FULL (`cf`) / DBT_CUSTOM (`fc`) with `on_schema_change` set (`explain_reason_kind_cells_custom.jsonl`) | DBT_CUSTOM / FULL | EXECUTE, 6 / 6, then 8 / 7: a custom node sends only `lookback` even with `on_schema_change` set, so it never shares a FULL table's identity | SHAPE 7 |

The tree for 2 / 2 is the SKIP tree (`the model table exists already`, `the
model query has not changed`, `upstream model queries have not changed`, the
listing and summary; SHAPE 0 without upstreams) judged against the latest
build, whose id the decision persists as its matched build (ADR 0005). A
RegisterClone copy has the submit's identity when the submit has the logic
the copy inherited from its source build (the [post-copy identity](#snapshot),
#93), whatever their execution types: a FULL submit over an append copy is
this rebuild (`kf4`). It renders for FULL, the five incremental strategies
and SNAPSHOT with and without an upstream, and for DBT_CUSTOM without one,
over a latest build that recorded the node's genuine upstream names. A
DBT_CUSTOM execution type change over an upstream is unreachable on 2.0.11:
no other kind sends a custom node's semantic extras with its upstreams (`cf`,
`vc`). For VIEW it renders when the view reads no relation (`cv0`, #117); a
view over a `ref` is not captured and renders nothing.

2. The scenario explained those nodes after a later build of another node,
   and `dbt state explain` explained nothing for them.

### EXECUTE: rebuild over a RegisterClone copy

`explain_copy_logic_mismatch.jsonl` (client 2.0.11, #104),
`explain_copy_logic_crafted.jsonl` (crafted `x1`). The client copies a node
with `RegisterClone` only in its dev-clone pre-step, which handles incremental
models and snapshots only
(`run_cache_dev_clone.rs::dev_clone_candidate_for_node`). The post-copy
submit of the same run has the copy's execution type. A submit of another
kind lands on the copy only in a later run, after the node's materialization
changed.

| Submit over the copy | Decision, codes | Reason | Tree |
|---|---|---|---|
| same kind, other logic: the five strategies (`la4`–`lb4`), a snapshot (`ls4`) | EXECUTE, 6 / 6, "either its query didn't match …" | `no_reuse_candidate.copy_logic_mismatch` | SHAPE 7 against the copy: upstream `<FRESH>`, `no updates since "«FQN»" last executed` (the copy), `upstream data is up to date` |
| a table (FULL) over an append copy, same SQL and semantic extras (`kf4`) | EXECUTE, 2 / 2 | `no_reuse_candidate.execution_type_changed` | [execution type change](#execute-execution-type-change) |
| a table (FULL) over an append copy, other logic (crafted `x1`) | EXECUTE, 6 / 6 | `no_reuse_candidate.copy_logic_mismatch` | SHAPE 7 against the copy, as above |
| a custom node over an append copy, same SQL (`kc4`) | EXECUTE, 6 / 6 (a custom node sends other semantic extras) | `no_reuse_candidate.copy_logic_mismatch` | SHAPE 7 against the copy, as above |
| a view over an append copy, same SQL (`kv4`) | EXECUTE, 6 / 6 (a view sends other semantic extras) | `no_reuse_candidate.copy_logic_mismatch` | SHAPE 7 with an empty `data freshness analysis` section: the view reports no upstreams, but the copy's source build read one |

The copy records no inputs of its own. As for a reused copy, the submit's
inputs stand in, each at the copy's epoch or earlier
(`reuse_candidate::adopt_current_inputs`, applied to the unmatched build in
`ExplainDecisionRow::unmatched`).

Crafted probes (`explain_copy_logic_crafted.jsonl`, `x2`–`x19`) cover the
cells the client run did not:

- **No genuine upstreams.** For FULL, all five strategies, a snapshot, a view
  and a custom node over a copy whose source build read none (`x2`–`x10`):
  6 / 6 and
  ```
  - table analysis ("«FQN»")
    - the model table exists already                  [SUCCESS] <TARGET_TABLE_EXISTS>
  - query analysis
    - the model query has changed                     [FAIL]    <NODE_QUERY_CHANGED>
  - data freshness analysis
    - no upstream dependencies                        [SUCCESS]
  ```
  (`snapshot` as the noun for a snapshot). Over a copy whose source build
  read an upstream, a view keeps `upstream model queries have not changed`
  and an empty freshness section (`kv4`). The copy's source build decides,
  not the node kind (`explain.rs::copy_logic_changed_tree`).
- **The copy's Target relation dropped**: 6 / 6, "its table did not
  exist" ("snapshot was executed …" for a snapshot), and the
  [first-build](#first-build) tree, for every model kind: an append (`x11`)
  and a table (`x12`) over an append copy, merge, insert_overwrite,
  delete+insert and microbatch over a copy of their own strategy
  (`x13`–`x16`), a snapshot over a snapshot copy (`x17`), and a view and a
  custom node over an append copy whose source build read no upstream
  (`x18`, `x19`). Each copy's source build and the dropped-copy submit had
  other logic. Hosted decides as this server does (2026-11-11).

This server renders these trees for every model kind except a view with an
upstream over a copy that exists, which client 2.0.11 never reports
([Decision reason coverage](#decision-reason-coverage) ᵇ).

### EXECUTE: copy disabled (`allow_clones: false`)

`explain_copy_logic_mismatch.jsonl` `cf4` (client 2.0.11). A table whose
Target relation is absent and whose equivalent exists in prod, built under a
profile with `allow_clones: false`. Client 2.0.11 sends `allow_clones: false`
with `clone_chain_depth_limit: 0`. Hosted builds it with codes 1 / 10
(TARGET_TABLE_MISMATCH / CLONE_CHAIN_LIMIT_EXCEEDED) and "model was executed
because cloning is disabled" (Decision reason `copy_disabled`):
```
- table analysis ("«FQN»")
  - the model table does not exist                    [FAIL]
  - the model table could have been cloned from "«FQN»", but cloning is disabled  [FAIL]
```
The second `«FQN»` is the equivalent, persisted as the decision's copy
source. Client 2.0.6 sent `clone_chain_depth_limit: 0` alone, and hosted
answered the same codes with "model was executed because the maximum number
of nested clones has been reached" (`clone_submit_allow_clones_false.jsonl`,
no Explain; Decision reason `copy_chain_limit`). That reason renders nothing.

### First build

Also a rebuild whose decision-time inputs lack the Target relation
(`lag_copy_candidates` `dt`, `dt_m`; for every model kind, a logic change
over a dropped Target relation, `explain_reason_kind_cells.jsonl` `*_d`,
entries 350–364, with the kind's noun):
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
| back to an older build's logic while its source gained a row (`fr_sn`, `explain_freshness_cells.jsonl`) | EXECUTE (8 / 7, `is_stale`) | SHAPE 7, `<OUTDATED>` listing, `upstream data is outdated`, Lag tolerance line |
| its source gained a row, build older than the 45 minute default tolerance (`fd_sn`, run 2) | EXECUTE (1 / 3: `fr_sn` has the same logic and is current) | SHAPE 9, data only: `upstream model queries have not changed`, `<OUTDATED>` listing, `upstream data is outdated`, then `the snapshot was last built «age» ago, exceeds lag tolerance of «T»` [FAIL] <OUTDATED>, the age being `fr_sn`'s ([SHAPE 9](#execute-upstream-against-the-matched-build-shape-9)) |
| run under `--full-refresh` (`s72sf`) | EXECUTE (2 / 2) | SKIP-shaped, `model` noun (see below) |
| `dev_clone` build, target missing, prod equivalent (`c76snap`, `explain_copy_probes.jsonl`) | EXECUTE (1 / 3: the prod build is the equivalent, #92, #107) | [First build](#first-build) |
| post-copy submit after the RegisterClone pre-step, over a fixed prod source (`c76rsnap`, `explain_snapshot_register_clone.jsonl`; `c93sa`, `explain_post_copy_identity.jsonl`) | SKIP | SKIP, `<FRESH>` listing with `no updates since "«dev FQN»" last executed`, `upstream data is up to date` |
| post-copy submit over a ref'd view resolved per target, whose definition reads the fixed prod source (`c93sv`) | SKIP | as above: the listing names the view's base source |
| post-copy submit over a ref'd table resolved per target (`c93st`; the append `c93it` alike) | SKIP | SKIP; the dev upstream is listed with its `last updated` line only (no badge, no `no updates since` line), then `upstream data is up to date` |
| post-copy submit with other logic (`ls4`, `explain_copy_logic_mismatch.jsonl`) | EXECUTE (6 / 6) | SHAPE 7 against the copy ([Rebuild over a RegisterClone copy](#execute-rebuild-over-a-registerclone-copy)) |

The client's default Lag tolerance for these snapshots was 45 minutes, so the
source drift was a reuse within tolerance. A snapshot cannot set its own
`lag_tolerance` or `require_fresh_data_from`: client 2.0.11 reads the `state`
config from models only (`model_state_for_node` downcasts to `DbtModel`,
dbt-fusion `run_cache_service.rs`), so a snapshot always gets the default
tolerance (`RUN_CACHE_FRESHNESS_TOLERANCE`, 45 minutes) and the ANY policy.
Its drift rebuilds only once its build is older than that (`fd_sn`), and an
ALL-policy reuse (`reused.tolerated_stale`) is unreachable for it. A snapshot SKIP's
`decision_description` is always the "both … up to date" one
([protocol.md](protocol.md#decision-descriptions)).

**`--full-refresh`.** The client submits the snapshot as FULL (execution type
1, so the tree uses `model`) over its SNAPSHOT State history. Hosted and this
server rebuild with rejection codes 2 / 2 (EXECUTION_TYPE_MISMATCH) and
render the FULL node-query-unchanged tree, its `<FRESH>` listing anchored on
the latest snapshot build, with no summary here (the upstream epoch moved
back) ([EXECUTE: execution type change](#execute-execution-type-change)).

A snapshot without upstreams renders SHAPE 0 like a FULL model (`no upstream
dependencies`, no upstream query line): its SKIP, logic change, `--full-refresh`
rebuild and execution type change (`explain_reason_kind_cells.jsonl` `sn_z`,
`sn_cz`, `sn_ez`). Hosted reuses a snapshot whose Target relation changed after
its build, by an External modification or a later `--full-refresh`; this
server rebuilds it and renders nothing (accepted divergence #115,
[Target relation modified](#execute-target-relation-modified-after-the-build)).

**Copies (#76).** Snapshot CLONE is unreachable on 2.0.11. On submit, hosted
answers a dev snapshot whose prod equivalent exists with 1 / 3
(TARGET_TABLE_MISMATCH / EXECUTION_TYPE_NOT_FULL) and the first-build tree.
Under auto-deferral (`defer_to_target`) the client's dev-clone pre-step
copies a snapshot through RegisterClone (execution type 7, `state.pre_clone`
default IfTableMissing). Hosted grants it (5 / unset, empty
`execution_decision_id`, so it has no Explain message). The pre-step needs a
deferred relation that differs from the dev one: a snapshot with
`target_schema=target.schema` keeps the dev schema in its deferred node, and
the client logs "source and target relations are identical" and sends no
RegisterClone (`c76snap`). After the copy, hosted SKIPs the snapshot's own
submit, reporting the prod build's `execution_runtime_ms`, and so does this
server (#93): it adopts the copy when the submit, read in the copy source's
schema, has the logic the copy inherited from its source build
(`reuse_candidate::rehomed_logic_matches`), and the copy keeps the source
build's runtime. In the SKIP tree only an upstream the source build recorded
(by physical name) is FRESH with the `no updates since` line; the copy keeps
those names (`executions.clone_source_inputs`, migration 0018) and Explain
lists any other upstream with its `last updated` line only. A rebuild over a
copy whose logic differs even so (`no_reuse_candidate.copy_logic_mismatch`,
`ls4`, crafted `x5`) renders SHAPE 7 judged against the copy
([Rebuild over a RegisterClone copy](#execute-rebuild-over-a-registerclone-copy)).

## DBT_CUSTOM (custom materialization)

`explain_custom_history.jsonl`, `explain_custom_copy.jsonl` (client 2.0.11).
The client sends execution type 11 for a materialization outside the built-in
list (the scenarios use a per-run project macro that runs
`create or replace table … as (sql)`), also under `--full-refresh`. A custom
node renders the MODEL trees above exactly, with `model` as the noun, and
with no genuine upstreams it renders SHAPE 0 like a FULL model
(`no upstream dependencies`, no `upstream model queries` line). Its
`decision_description` strings are the model ones.

| Scenario (node) | Decision | Tree |
|---|---|---|
| first execution (`c74cd`) | EXECUTE | [First build](#first-build) |
| unchanged rerun, no upstream (`c74c0`) | SKIP | SKIP, SHAPE 0 |
| unchanged rerun, source upstream (`c74cd`) | SKIP | SKIP, `<FRESH>` listing, `upstream data is up to date` |
| unchanged rerun, model upstream (`c74cl`) | SKIP | SKIP, `<FRESH>` listing, no summary (epoch moved back) |
| its source gained rows, default 45 minute tolerance (`c74cd`) | SKIP (`is_stale`) | SKIP, `<OUTDATED>` listing, `upstream data is outdated`, Lag tolerance line |
| its source gained rows, `lag_tolerance: 0s` (`c74cz`) | EXECUTE | SHAPE 9, data only: `upstream model queries have not changed`, `<OUTDATED>` listing, `upstream data is outdated` |
| upstream query change (`c74cu`) | EXECUTE | SHAPE 9 with the per-upstream query listing |
| its own logic changed (`c74cl`) | EXECUTE | SHAPE 7 |
| ALL policy, one of its two sources gained a row (`fa_cu`, `explain_freshness_cells.jsonl`) | SKIP (`is_stale`) | SKIP, one `<OUTDATED>` and one `<FRESH>` upstream, `upstream data is outdated`, Lag tolerance line |
| back to an older build's logic while its source gained a row (`fr_cu`) | EXECUTE (8 / 7, `is_stale`) | SHAPE 7, `<OUTDATED>` listing, `upstream data is outdated`, Lag tolerance line |
| unchanged, run under `--full-refresh` (`c74cf`) | SKIP | SKIP, `<FRESH>` listing, no summary |
| built in `dev_clone` over an equivalent prod build (`c74k`) | EXECUTE, codes 1 / 3 | [First build](#first-build) (#86) |
| its Target relation modified externally (`vc`), or rewritten by a view build in between (`cv0`) (`explain_reason_kind_cells.jsonl`) | SKIP (this server: EXECUTE 6 / 6, accepted divergence #115) | SKIP; this server renders nothing |
| first ordinary run over a view build with the same SQL, no upstream (`vc0`) | EXECUTE 2 / 2 | SKIP-shaped, SHAPE 0 ([execution type change](#execute-execution-type-change)) |
| logic change over a dropped Target relation (`cu_d`) | EXECUTE | [First build](#first-build) |

Unlike a snapshot, a custom node under `--full-refresh` is still submitted as
DBT_CUSTOM, and hosted reuses it. A custom node sends only `lookback` in its
semantic extras even with `on_schema_change` set
(`explain_reason_kind_cells_custom.jsonl`), so only a view shares its
identity, and only when the SQL reads no relation (a view reports no
upstreams, #116).

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
| ALL policy, one of two sources gained a row (`fa_ap` / `fa_mg`, `explain_freshness_cells.jsonl`) | SKIP (`is_stale`) | SKIP, one `<OUTDATED>` and one `<FRESH>` upstream, `upstream data is outdated`, Lag tolerance line |
| back to an older build's logic while its source gained a row (`fr_ap` / `fr_mg`) | EXECUTE (8 / 7, `is_stale`) | SHAPE 7, `<OUTDATED>` listing, `upstream data is outdated`, Lag tolerance line |
| first incremental run, reading `{{ this }}` (`a73t` / `m73t`) | EXECUTE | SHAPE 7: the compiled SQL gained the `is_incremental()` filter |
| second incremental run, reading `{{ this }}` (`a73t` / `m73t`) | SKIP | SKIP, `<FRESH>` listing of the source only |
| ordinary run after a `--full-refresh` run (`a73b` / `m73b`) | EXECUTE | SKIP-shaped, against the last incremental build (see below) |
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
and this server rebuild it with "the table was modified after the cached
execution" and the SKIP-shaped
[own-target-modified tree](#execute-target-relation-modified-after-the-build):
the recorded FULL execution has the submit's identity under another execution
type, so the submit is judged against the last APPEND/MERGE build, whose
Target relation the full refresh rewrote
([EXECUTE: execution type change](#execute-execution-type-change)).

**Result copy.** `explain_incremental_copy_history.jsonl`: with the
`dev_clone` target (auto-defer through the profile's `defer_to_target`) and
`state.pre_clone` unset (IfTableMissing), the client copies an absent dev
relation with `RegisterClone`, confirms the copy, then submits the model.
Hosted answers `RegisterClone` with an empty `execution_decision_id`, so that
CLONE decision is never explained. Hosted SKIPs the post-copy submit with the
SKIP tree, its listing anchored on the dev relation (`no updates since
"«dev FQN»" last executed`), and so does this server (#93, as for a
[SNAPSHOT](#snapshot)); `explain_post_copy_identity.jsonl` `c93it` is the
append over a ref'd table resolved per target. `explain_incremental_submit_copy.jsonl` (one probe,
`state.pre_clone: never`): an absent dev relation whose prod build exists is
built, not copied; hosted and this server report skip code 1 (the submit,
read in the fixed prod schema it reads, has the prod build's logic, #107) and
render the first-build tree. Over an existing dev relation hosted rebuilds
too (see [Result copy, all strategies](#result-copy-all-strategies)).

### INSERT_OVERWRITE (4), DELETE_INSERT (5)

`explain_incremental_insert_overwrite.jsonl`, `explain_incremental_delete_insert.jsonl`
(client 2.0.11). The two strategies render identical trees, the MODEL trees
above:

| Scenario (node) | Decision | Tree |
|---|---|---|
| first build (`iod`, `did`) | EXECUTE | [First build](#first-build) |
| unchanged rerun (`iod`, `did`) | SKIP | SKIP, `<FRESH>` listing, `upstream data is up to date` |
| its source gained a row (`iod`, `did`) | SKIP (`is_stale`) | SKIP, `<OUTDATED>` listing, `upstream data is outdated`, Lag tolerance line |
| its own logic changed (`iol`, `dil`) | EXECUTE | SHAPE 7, no summary (epoch moved back) |
| `{{ this }}` self-read, first incremental run (`ios`, `dis`) | EXECUTE | SHAPE 7, no summary (epoch moved back) |
| `{{ this }}` self-read, unchanged rerun (`ios`, `dis`) | SKIP | SKIP, `<FRESH>` listing of its one genuine upstream, `upstream data is up to date` |
| ALL policy, one of two sources gained a row (`fa_io`, `fa_di`, `explain_freshness_cells.jsonl`) | SKIP (`is_stale`) | SKIP, one `<OUTDATED>` and one `<FRESH>` upstream, `upstream data is outdated`, Lag tolerance line |
| its source gained a row, `lag_tolerance: 0s` (`fz_io`, `fz_di`) | EXECUTE | SHAPE 9, data only |
| back to an older build's logic while its source gained a row (`fr_io`, `fr_di`) | EXECUTE (8 / 7, `is_stale`) | SHAPE 7, `<OUTDATED>` listing, `upstream data is outdated`, Lag tolerance line |

The client's default Lag tolerance for these models was 45 minutes, so the
source drift was a reuse within tolerance, worded "its query is up to date and
its data is within lag tolerance". With `lag_tolerance: 0s` the drift
rebuilds (`explain_freshness_cells.jsonl` `fz_io`, `fz_di`, `fz_mb`, entries
111–113): SHAPE 9 data only, as for append and merge. CLONE: see
[Result copy, all strategies](#result-copy-all-strategies).

### MICROBATCH (6)

`explain_incremental_microbatch.jsonl` (client 2.0.11). Built with a fixed
`--event-time-start 2026-11-01 --event-time-end 2026-11-03` window, the six
scenarios above (`mbd`, `mbl`, `mbs`) render the same trees as
insert_overwrite and delete+insert, and so do the #104 cells with the same
window (`fa_mb`, `fz_mb`, `fr_mb`, `explain_freshness_cells.jsonl`).

**Batch window.** The client submits once per model per run, not once per
batch (10 batches, one submit). It folds the run's resolved window into the
semantic extras as `__microbatch_event_time_start` and
`__microbatch_event_time_end`, raw ISO-8601 strings such as
`2026-11-01T00:00:00+00:00`. A different window is therefore an
identity mismatch (codes 6 / 6, SHAPE 7, `the model query has changed`) even
though the SQL did not change.

**Default moving window** (`mbm`, `begin='2026-11-01'`, `batch_size='day'`,
`lookback` 1, three builds on 2026-11-10):

| Build | Window sent | Decision | Tree |
|---|---|---|---|
| 1 (table absent, not incremental) | `2026-11-01` → `2026-11-11` (`begin` → ceiling of now) | EXECUTE | [First build](#first-build) (not explained in the capture) |
| 2 (first incremental run) | `2026-11-09` → `2026-11-11` (`lookback` + 1 batches before the ceiling of now) | EXECUTE | SHAPE 7, ending `upstream data is up to date`¹ |
| 3 (same UTC day) | `2026-11-09` → `2026-11-11` | SKIP | SKIP, `<FRESH>` listing, `upstream data is up to date` |

So a moving-window microbatch model SKIPs an unchanged rerun once its window
is stable: from the second incremental run on, within the same batch period.
By the client's window rule (`MicrobatchBuilder::build_end_time` takes the
ceiling of now to the batch boundary, `a0fc53e`) it rebuilds when that
ceiling crosses into the next batch; that rollover was not captured.

1. The upstream epoch equals the one recorded at the previous build (the
   first, whose window differed), so the listing judged against that build
   (`unmatched_execution_id`) ends with `upstream data is up to date`
   ([SHAPE 7](#execute-node-query-changed-shape-7), #89).

CLONE: see [Result copy, all strategies](#result-copy-all-strategies).

### All strategies: no upstreams, Target relation, upstream query change

`explain_reason_kind_cells.jsonl` (client 2.0.11, #104 group b), one node per
strategy and situation (nodes `<strategy>_<role>`); the five strategies
render the same trees, the MODEL ones:

| Scenario (role) | Decision | Tree |
|---|---|---|
| no upstream: unchanged rerun (`z`) | SKIP | SKIP, SHAPE 0 |
| no upstream: its own logic changed (`z`) | EXECUTE | SHAPE 7, SHAPE 0 freshness body, no upstream query line |
| first ordinary run over only a `--full-refresh` build, with / without an upstream (`e`, `ez`; APPEND without only) | EXECUTE 2 / 2 | [execution type change](#execute-execution-type-change) tree |
| ordinary run after a `--full-refresh` over its own build, with / without an upstream (`c`, `cz`) | EXECUTE 6 / 6, "… modified after the cached execution" | [own-target-modified tree](#execute-target-relation-modified-after-the-build) |
| unchanged rerun after an External modification (`e`, `ez`, round 4) | EXECUTE 6 / 6, same | same |
| an upstream table model's logic changed (`q`) | EXECUTE | SHAPE 9 with the per-upstream query listing (`query or upstream query changed <UPSTREAM_QUERY_CHANGED>`, `<OUTDATED>`, `upstream data is outdated`) |
| its own logic changed while its Target relation was dropped (`d`) | EXECUTE, "… its table did not exist" | [First build](#first-build) |

### Result copy, all strategies

`explain_incremental_copy_over.jsonl` (client 2.0.11, one attempt). One model
per strategy (`ka73` append, `km73` merge, `ki73` insert_overwrite, `kd73`
delete+insert, `kb73` microbatch), all with `state.pre_clone: never` and the
same fixed microbatch window, reads `CDO_DEMO_DB.RAW.RAW_ORDERS`. That
relation is outside both default schemas, so the prod and dev SQL is
byte-identical. Each model is built in `prod_demo`, then in `dev_clone` with
other logic (`where id is not null`), then in `dev_clone` with the prod logic
again. At that last submit the dev relation exists, its own build has other
logic, and the prod build has the same SQL, extras and upstream epoch.

The scenario follows the 2.0.6 copies, which were MERGE submits over an
existing target, and the 2.0.11 probe into an absent dev relation, which was
rebuilt. Hosted rebuilds every strategy here too:

| Strategy (entry) | Hosted response | Codes | Tree |
|---|---|---|---|
| APPEND (40) | `ready_to_execute`, "either its query didn't match …" | 1 / 2 | SHAPE 7, `<FRESH>` listing anchored on the dev relation, `upstream data is up to date` |
| MERGE (38) | same | 1 / 6 | same |
| INSERT_OVERWRITE (39) | same | 1 / 6 | same |
| DELETE_INSERT (37) | same | 1 / 2 | same |
| MICROBATCH (41) | same | 1 / 6 | same |

Code 2 (EXECUTION_TYPE_MISMATCH) is a confound of this run. Append and
insert_overwrite had identical SQL and semantic extras in prod, and so had
merge and delete+insert, so for append and delete+insert hosted's equivalent
was the other strategy's prod build. The strategies whose equivalent had
their own execution type got 1 / 6. Hosted sends no 1 / 3
(EXECUTION_TYPE_NOT_FULL) for incrementals, unlike views, snapshots and
custom nodes.

This server copies only FULL on submit (#101, `Warehouse::submit_copy_admits`).
It finds the same newest equivalent for each strategy (newest prod build
first, of any execution type), so it makes the same five rebuilds with the
same codes, and the replay asserts the five SHAPE 7 trees (entries 47–51).
Every incremental CLONE cell is unreachable on 2.0.11. On DuckDB, which
hosted does not engage, this server still copies delete+insert under its
pinned contract; that CLONE is local-only and renders nothing.

**An older match first.** When the target's own older build matches the
submit and would be reused, hosted reports that build (8 / 7, `is_stale` as
it would be reused) even though an equivalent under another name exists
(`explain_freshness_cells.jsonl` `fr_*`, entries 80–95). There, append,
merge, insert_overwrite, delete+insert, microbatch and snapshot nodes went
back to an older build's logic. Other nodes in the run had the same SQL,
and this server took them as equivalents (1 / 2, 1 / 6, 1 / 3) before #104.
This server now reports the older match ahead of a refused copy
(`submit_decision::older_match`).

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
One `upstream dependencies` child per tested upstream, in the order of the
decision-time inputs (two upstreams: `explain_rebuild_reason_gaps.jsonl`
`r40t2`). A failing test SKIPs like a passing one and is served its cached
failure (`explain_data_test_history.jsonl`).

A test with no upstreams (`r40t0`, a singular test without `ref` or
`source`) omits the upstream-queries line, like a FULL model (SHAPE 0):
```
- query analysis
  - the test query has not changed                    [SUCCESS] <NODE_QUERY_UNCHANGED>
- data freshness analysis
  - no upstream dependencies                          [SUCCESS]
```
Hosted decided that SKIP on the speculative submit, which this server
answers `undecided` (divergence #103); the replay drives the ordinary submit
the client then sends, and this server renders the same tree for its SKIP.

### EXECUTE
```
- data freshness analysis
  - upstream dependencies
    - "«FQN»"                                          (no badge)
      - last updated: «rel_time»
```
VERIFIED LIVE 2026-11-10 and 2026-11-11 (`explain_data_test_history.jsonl`,
`explain_rebuild_reason_gaps.jsonl`, client 2.0.11). No query analysis, no
badges and no markers. The same tree for a first execution, a logic change
(`the test query has changed` never appears), upstream data drift (no
`<OUTDATED>`, no `«gap»`) and an upstream whose logic changed (`r40tm`, no
`<UPSTREAM_QUERY_CHANGED>`), although the last three have a matched build.
A test with no upstreams says `- no upstream dependencies [SUCCESS]` under
`data freshness analysis` instead (`r40t0`, first execution and logic
change). Each upstream is listed from the decision-time `tables`, so
after drift `last updated` is the new epoch. The listing is the
[CLONE](#clone-model) one without an anchor
(`explain.rs::clone_upstream_dependencies`).

The tree does not depend on the test's outcome: the passing and the failing
test render the same tree in every round, and the outcome (`failures`,
`should_warn`, `should_error`) only arrives later in the ConfirmExecution of
that execution (`crates/harness/tests/data_test_explain.rs`).

**`store_failures: true`.** Once the test has written its audit relation
(named after the test, in its database and schema), the client reports that
relation in `tables` too, whether it is a table or a `store_failures_as:
view` view and whether the test passed or failed. Hosted neither rebuilds for
it nor lists it: an unchanged rerun SKIPs with the SKIP tree above, serving a
failing test its cached failure, and after drift the EXECUTE tree lists only
the source (`explain_data_test_history.jsonl` `t75s`,
`explain_store_failures_audit.jsonl`). This server drops the audit relation
from the test's inputs, so it decides and renders the same (#88,
`execution.rs::is_own_audit_relation`). It recognizes the relation only as
`default_catalog`.`default_schema`.`dbt_node_name` on Snowflake, and never
when the test's SQL or a query dependency mentions that name (then it is a
read, not the test's output). `default_schema` is the test's own schema, so an
audit relation in a separate audit schema is recognized too
(`explain_store_failures_audit_schema.jsonl`, #106). Under a configured alias,
which no request field carries, hosted still SKIPs
(`explain_store_failures_audit_alias.jsonl`) but this server rebuilds for
`dependency_set_changed`, which renders nothing (divergence #123).

Rendered for `first_build` and `no_reuse_candidate.identity_mismatch`, and
for `upstream_outdated` and `upstream_query_changed` (which need an
upstream). Nothing else:

- **Incomplete cached results** (crafted only). A test confirmed with
  `execution_results: null`, or with only `failures`, is SKIPped by hosted,
  which serves what was confirmed (`{"fields": {}}` or `failures` alone) with
  the ordinary SKIP tree. This server rebuilds for
  `no_reuse_candidate.incomplete_cached_test_results`, which renders nothing
  (accepted divergence #87, keyed gaps `explain_incomplete_test_results.jsonl`
  2, 3, 6, 7). Client 2.0.11 always confirms all three result fields.
- `no_reuse_candidate.failed_reuse_qualification`: out of scope → #67 for
  the cause the client sends (`evaluate_volatile_sql: true`). From a
  malformed request, local-only, empty tree (crafted, accepted divergence
  #114): hosted
  rejects empty SQL and SKIPs a test whose query dependency is listed twice
  (`explain_malformed_requests.jsonl` `e4-8`).

### CLONE

Out of scope → #68.

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

**Copy (CLONE), hosted only** (`explain_copy_probes.jsonl` entry 18, client
2.0.11). A seed loaded in `prod_demo` and then built in `dev_clone` with the
same values is copied by hosted on submit (`an equivalent seed exists under
another name so we cloned that one`):
```
- table analysis ("«FQN»")
  - the seed table does not exist                     [FAIL]
  - the seed table could be cloned from "«FQN»"       [SUCCESS]   (copy source)
- query analysis (vs clone)
  - the seed data has not changed                     [SUCCESS] <NODE_QUERY_UNCHANGED>
```
This server never copies seeds; it loads the seed and renders the first-load
tree for that decision. VALUES × CLONE stays fenced until the decision lands
(#91).

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
  An ALL-policy reuse with one of two upstreams drifted
  (`reused.tolerated_stale`) reads exactly like a reuse within the Lag
  tolerance: the drifted upstream `<OUTDATED>`, the other `<FRESH>`, then
  both lines above, with `is_stale` and the "within lag tolerance"
  description (`explain_freshness_cells.jsonl` `fa_*`: append, merge,
  insert_overwrite, delete+insert, microbatch, custom; entries 105–110).
- **A stale SHAPE 7 rebuild** ends with the same two lines, the age being
  the older matching build's
  ([SHAPE 7](#execute-node-query-changed-shape-7)).
- The `«gap»` line `updated N seconds after «FQN» last executed` is the
  upstream's current epoch minus the epoch recorded for it at the build.

## Re-derivation binding (ADR 0005)

| Line / badge | Re-derivation source |
|---|---|
| `table analysis` existence + `<TARGET_TABLE_EXISTS>` | persisted decision class and `decision_reason`, plus whether the decision-time `input_tables` report the Target relation (no live warehouse probe) |
| `<NODE_QUERY_CHANGED>` / `<NODE_QUERY_UNCHANGED>` | persisted decision reason |
| per-upstream `<FRESH>` / `<OUTDATED>` + `«gap»` | decision-time `node_decisions.input_tables` vs the matched execution's recorded inputs (`matched_execution_id`: the reuse candidate, a build rejected for its own target, or for an execution type change the latest build or the older build of the submit's type); for SHAPE 7, the unmatched latest build's (`unmatched_execution_id`, migration 0017). An upstream that build did not record has no badge; over a RegisterClone copy, neither has an upstream its source build did not record (`executions.clone_source_inputs`, migration 0018, #93) |
| `<UPSTREAM_QUERY_CHANGED>` / `<UPSTREAM_QUERY_UNCHANGED>` | per-upstream Upstream producer comparison, same two snapshots |
| `«rel_time»` ("last updated …") | the upstream's decision-time `last_modified_epoch` |
| a view's `no upstream dependencies` / `upstream data is up to date` (no reported upstreams) | whether the view's SQL reads a relation, judged at decision time (`node_decisions.reads_relation`, migration 0019, #117) |

Explain computes the per-upstream verdicts with the same
`decision::upstream_freshness` helper the decision uses, against the matched
execution as returned by `store::ExplainDecisionRow::anchor` (which applies the
decision's copy-adoption input rule, `reuse_candidate::adopt_current_inputs`).

## Not rendered

What this server never renders, each with its final status:

- **CLONE cells:** seed CLONE is captured but out of scope → #91 (its
  decision diverges); snapshot and view CLONE are unreachable on 2.0.11
  (#76); custom and incremental CLONE are unreachable on 2.0.11 too (#86,
  #101): this server copies only FULL on submit, and a RegisterClone copy
  has no Explain message. DuckDB's checked delete+insert copy is local-only.
  Data-test CLONE is out of scope → #68.
- **Badges and markers** `EB_UNSPECIFIED` (0), `INFO` (3) and
  `EM_UNSPECIFIED` (0): never emitted; not observed across the census trees
  ([Observed vocabulary](#observed-vocabulary)).
- **`Explain.GetUpstreamDependencyChanges`**: unreachable on 2.0.11, the dbt
  Fusion client never calls it, so it cannot be captured through the
  recording proxy. The server keeps an empty stub. See [protocol.md](protocol.md).
- **Dependency-set change** (`dependency_set_changed`): renders nothing. For
  a model it is unreachable on 2.0.11: through a real client, changing a
  node's genuine upstream names also changes its rendered SQL, repeated refs
  are deduplicated, and a ref in a comment registers no dependency, so the
  rebuild is an identity mismatch (SHAPE 7). Drivers:
  `experiments/explain_dependency_set_changed.sh`,
  `experiments/explain_dep_set_variants.sh`,
  `experiments/explain_dep_set_comment_ref.sh`. Switching the same SQL
  between a view (which reports no upstreams) and a custom node changes the
  reported upstreams without changing the SQL, but also the execution type:
  hosted rebuilds it as another identity, this server as an execution type
  change, and renders nothing (#116). For a data test it arises
  only for a `store_failures` audit relation the request does not name (a
  configured alias): local-only, hosted SKIPs (divergence #123,
  [DATA TEST](#data-test)).
- **Every other Decision reason × node kind without a capture:**
  [Decision reason coverage](#decision-reason-coverage).

## Decision reason coverage

Each Decision reason renders a tree only for the node kinds (and upstream
situations) a tracked capture backs (`explain.rs::shape`); every other cell
renders nothing. Statuses as in the
[coverage table](#execution-type-node-kind-coverage), abbreviated: **L**
characterized (live), **C** characterized (crafted only), **U** unreachable
on 2.0.11, **LO** local-only, empty tree, **→ #N** out of scope → #N (empty
tree). "—" means this server never records the reason for that kind, so
there is no cell.

| Decision reason | FULL | APPEND, MERGE | INSERT_OVERWRITE, DELETE_INSERT, MICROBATCH | SNAPSHOT | VIEW | DBT_CUSTOM | DBT_DATA_TEST | VALUES |
|---|---|---|---|---|---|---|---|---|
| `reused.fresh` | L | L | L | L | L ᵇ ᵒ | L | L | C |
| `reused.within_lag_tolerance` | L | L ᵃ | L ᵃ | L ᵃ | U ᵇ | L | U ᶜ | — |
| `reused.tolerated_stale` | C | L ᵃ | L ᵃ | U ᵐ | U ᵇ | L | U ᶜ | — |
| `first_build`, `no_reuse_candidate.own_target_missing` ᵈ | C | L | L | L | L | L | L | C |
| `no_reuse_candidate.identity_mismatch` (SHAPE 7) | L ᵉ | L ᵉ | L ᵉ | L ᵉ | L ᵉ ᵒ | L ᵉ | L | C |
| `upstream_outdated` (SHAPE 9, data only) ᶠ | C | L | L | L ᵐ | U ᵇ | L | L | — |
| `upstream_query_changed` (SHAPE 9, query listing) ᶠ | L | L | L | L | U ᵇ | L | L | — |
| `no_reuse_candidate.own_target_modified` ᵍ | L | L | L | LO (#115) | L ᵒ | LO (#115) | — | — |
| `no_reuse_candidate.execution_type_changed` ᵏ | L | L | L | L | L ᵒ | L ⁿ | — | — |
| `no_reuse_candidate.copy_logic_mismatch` ˡ | C | L | L | L | L | L | — | — |
| `no_reuse_candidate.own_target_changed_or_missing`, `no_build_anchor` | LO (#80, #82) | LO | LO | LO | LO | LO | — | LO ʰ |
| `no_reuse_candidate.failed_reuse_qualification` ⁱ | → #67, LO | → #67, LO | → #67, LO | → #67, LO | → #67, LO | → #67, LO | → #67, LO | U ʲ |
| `no_reuse_candidate.incomplete_cached_test_results` | — | — | — | — | — | — | LO (#87) | — |
| `dependency_set_changed` | U | U | U | U | U | U | LO (#123) | — |
| `copy_chain_limit` ᵐ | U | — | — ᵏ | — | — | — | — | — |
| `copy_disabled` ᵐ | L | — | — ᵏ | — | — | — | — | — |
| `result_copy` (CLONE) | L | U (#101) | U (#101) ᵏ | U | U | U (#86) | → #68 | → #91 |

Evidence for each L and C cell is in the per-kind sections above and the
conformance replay. No cell is marked → #104 any more; #104 records how the
last ones were settled.

- ᵃ With an upstream. Without one there is no drift to tolerate, so the reuse is
  `reused.fresh` and there is no cell.
- ᵇ Client 2.0.11 reports no upstreams for a view (`explain_snapshot_history.jsonl`
  `s72vl`, a view over a `ref`), so it never drifts.
- ᶜ The client sends Lag tolerance 0 for a data test, and a data test
  rebuilds on any upstream drift whatever the policy.
- ᵈ One tree (lag_copy_candidates `dt`: hosted's tree for a dropped Target
  relation is the first-build tree). FULL is crafted only (`dt`, `ddc`,
  `lag_dropped_targets.jsonl`); VALUES is the seed load into a missing table
  (`lag_seed_upstream` G). A data test has no Target relation, so only
  `first_build` applies.
- ᵉ Over an absent Target relation the rebuild renders the first-build tree
  (lag_copy_candidates `dt_m`, crafted, FULL; `explain_reason_kind_cells.jsonl`
  `*_d`, every other model kind). Without genuine upstreams SHAPE 7 has
  the SHAPE 0 freshness body (`*_z`). A stale rebuild (older match) under
  drift with a non-zero Lag tolerance: L, with the Lag tolerance line, for
  every kind but VIEW, whose tolerance is 0 (`explain_freshness_cells.jsonl`
  `fr_*`).
- ᶠ Needs the matched build. Without it (its execution row disappeared): LO.
  An `upstream_outdated` whose matched build re-derives no provably drifted
  upstream (an unknown epoch): LO for every kind, accepted divergence #113.
  Hosted reuses it within the Lag tolerance (`explain_unknown_epoch.jsonl`,
  crafted). Client 2.0.11 never sends an unknown epoch: it leaves the table
  out of `tables` and skips the State submit.
- ᵍ With a matched build, or with no genuine upstreams (lag_copy_candidates
  `ome`, lag_tolerance_matrix `lfs_u0`). With upstreams and no matched build
  (the anchor's execution row disappeared): LO. Live for FULL, APPEND and
  MERGE judged against an older build of the submit's type after an
  execution type change (`explain_execution_type_change.jsonl`,
  `explain_incremental_append_merge_history.jsonl` 78–79), and for the five
  incremental strategies with and without an upstream after a
  `--full-refresh` or an External modification
  (`explain_reason_kind_cells.jsonl`). The incrementals need the matched
  build. Hosted reuses a snapshot or a custom node over a modified Target
  relation (accepted divergence #115), so this server's rebuild is
  local-only. VIEW: a view that reads no relation (`vc0`, entry 377) ᵒ.
- ʰ The seed safety floor ([SEED](#seed)).
- ᵐ A snapshot gets the default Lag tolerance (45 minutes) and the ANY
  policy: client 2.0.11 reads `state` config from models only
  ([SNAPSHOT](#snapshot)). So its ALL-policy reuse is unreachable, and its
  drift rebuilds only past the default tolerance (`fd_sn`, run 2 of
  `explain_freshness_cells.jsonl`).
- ᵏ Needs the latest build as the matched build, with the node's genuine
  upstream names ([EXECUTE: execution type change](#execute-execution-type-change)).
  Captured with and without an upstream for FULL
  (`explain_snapshot_history.jsonl` entry 60, `explain_reason_kind_cells.jsonl`
  `sn_cz`), APPEND (`explain_execution_type_change.jsonl` `tv`, `a_ez`), the
  other incrementals and SNAPSHOT (`*_e`, `*_ez`), and VIEW for a view that
  reads no relation (`cv0`, entry 246) ᵒ. Over an upstream,
  a VIEW <-> DBT_CUSTOM switch is another identity on hosted (#116).
- ⁿ Without an upstream (`vc0`). With one: unreachable on 2.0.11, as no other
  kind sends a custom node's semantic extras (only `lookback`, even with
  `on_schema_change` set, `explain_reason_kind_cells_custom.jsonl`) with its
  upstreams: a view reports none.
- ⁱ The client sends it for `evaluate_volatile_sql: true`
  (`tolerate_nondeterminism = false`): → #67. From a malformed request it is
  local-only, empty tree (crafted `experiments/malformed_requests.py`,
  accepted divergence #114). For a model of every kind and a data test,
  hosted rejects empty SQL with `INVALID_ARGUMENT` ("Failed to process SQL"), and
  also a model with an empty dependency name or query. A query dependency
  listed twice is built, then SKIPped with the ordinary SKIP tree
  (`explain_malformed_requests.jsonl`, keyed gaps).
- ʲ The client always sends a `values_hash` (the md5 of the seed file).
- ᵏ This server copies only FULL on submit (#86, #92, #101), so it never
  records these reasons for another kind on Snowflake.
- ˡ [Rebuild over a RegisterClone copy](#execute-rebuild-over-a-registerclone-copy).
  Client 2.0.11 copies with RegisterClone only incrementals and snapshots
  (the dev-clone pre-step). A table (FULL), view or custom node lands on such
  a copy only after its materialization changed, in a later run. That rebuild
  is this reason only when the logic, including the semantic extras, also
  differs. Same logic is an execution type change (`kf4`). Live: the five
  strategies and SNAPSHOT (logic change, `la4`–`ls4`), VIEW and DBT_CUSTOM
  (kind change, their semantic extras differ, `kv4`, `kc4`). FULL is crafted
  only (`x1`, `x2`). Each kind without an upstream (`x2`–`x10`), and each
  kind over a dropped copy (the first-build tree, `x11`–`x19`), are crafted.
  A view with an upstream over a copy that exists: U ᵇ.
- ᵒ A view's tree depends on whether its SQL reads a relation, which the
  decision records (`node_decisions.reads_relation`, #117). SKIP and SHAPE 7:
  live both ways (`explain_view_reads_no_relation.jsonl`; a view over a
  `ref` also `explain_happy_history.jsonl`, `explain_snapshot_history.jsonl`
  `s72vl`). `own_target_modified` and `execution_type_changed`: live for a
  view that reads no relation only (`vc0`, `cv0`); for a view over a `ref`
  they are not captured and render nothing. A view whose SQL was not judged
  (it does not parse as one query, or a CTE names itself or a later sibling
  without RECURSIVE) renders nothing for all four.
- ᵐ An equivalent exists for an absent Target relation, but the submit
  forbids copies (1 / 10). Client 2.0.11 sends `allow_clones: false` with
  `clone_chain_depth_limit: 0`, which is `copy_disabled` (`cf4`,
  [copy disabled](#execute-copy-disabled-allow_clones-false)). `copy_chain_limit`
  is the limit alone: client 2.0.6 sent `clone_chain_depth_limit: 0` without
  `allow_clones` (`clone_submit_allow_clones_false.jsonl`, no Explain). On
  Snowflake this server tracks no copy chain depth, and the only setting
  found in the 2.0.11 binary that forbids copies is the profile's
  `allow_clones`, which sends both fields (the local client source predates
  them). So no 2.0.11 path to `copy_chain_limit` is known. Its tree is
  uncaptured and it renders nothing. DuckDB can record it at its copy depth
  limit (local-only). DuckDB, which hosted
  does not engage, still copies delete+insert under its pinned contract and
  can record both reasons for it; they render nothing (local-only).

## Shape selection

Explain selects a shape from the persisted decision class, Decision reason
(`node_decisions.decision_reason`), node kind and matched execution anchor
(`explain.rs::shape`), after the node-kind fence. It never reads the
`decision_description` wording. Arms are tried in this order:

| class | Decision reason | shape |
|---|---|---|
| seed SKIP | `reused.fresh` | SEED, data unchanged |
| seed EXECUTE | `first_build`, `no_reuse_candidate.own_target_missing` | first build |
| seed EXECUTE | `no_reuse_candidate.identity_mismatch` | SEED, data changed (unchanged when the skip rejection code is 8) |
| seed, other | any other reason | empty tree |
| data test SKIP | `reused.fresh` | data-test SKIP |
| data test EXECUTE | `first_build`, `no_reuse_candidate.identity_mismatch`; `upstream_outdated`, `upstream_query_changed` with at least one upstream | data-test EXECUTE |
| data test, other | any other reason (including `incomplete_cached_test_results`, #87, and `dependency_set_changed`, #123) | empty tree |
| SKIP | `reused.fresh`, `reused.within_lag_tolerance` where the kind's listing is captured¹; `reused.tolerated_stale` likewise, except SNAPSHOT | SKIP (SHAPE 0/1/2/4/8 by kind and upstreams) |
| EXECUTE | `first_build`, `no_reuse_candidate.own_target_missing` | first build |
| EXECUTE | `upstream_outdated` when the anchor re-derives an upstream whose drift is proven (FULL, the five incrementals, SNAPSHOT, DBT_CUSTOM); `upstream_query_changed` with an anchor (FULL, the incrementals, SNAPSHOT, DBT_CUSTOM) | SHAPE 9 |
| EXECUTE | `no_reuse_candidate.own_target_modified` for FULL, with an anchor or no genuine upstreams; for the incrementals, with an anchor; for a VIEW that reads no relation (not SNAPSHOT or DBT_CUSTOM, #115; not a view over a `ref`) | SKIP-shaped tree |
| EXECUTE | `no_reuse_candidate.execution_type_changed` with an anchor that recorded the node's genuine upstream names: FULL, the incrementals and SNAPSHOT; DBT_CUSTOM without upstreams; a VIEW that reads no relation | SKIP-shaped tree, against the latest build |
| EXECUTE | `no_reuse_candidate.identity_mismatch` when the decision-time inputs lack the Target relation | first build (`lag_copy_candidates` `dt_m`, `explain_reason_kind_cells.jsonl` `*_d`) |
| EXECUTE | `no_reuse_candidate.identity_mismatch` where the kind's listing is captured¹, with the Lag tolerance line when stale with a non-zero tolerance and drift against the unmatched build, unless the decision recorded no build age for it (a row written before #104) | SHAPE 7, judged against the unmatched latest build |
| EXECUTE | `no_reuse_candidate.copy_logic_mismatch` when the decision-time inputs lack the Target relation | first build, every model kind (crafted `x11`–`x19`) |
| EXECUTE | `no_reuse_candidate.copy_logic_mismatch` for every model kind but a view with an upstream, unless stale with a non-zero tolerance and drift against the copy (uncaptured, even with a recorded build age) | SHAPE 7 against the copy ([Rebuild over a RegisterClone copy](#execute-rebuild-over-a-registerclone-copy)) |
| CLONE | `result_copy` with an anchor and the Target relation present | CLONE over an existing Target relation |
| CLONE | `result_copy` otherwise (submit Result copy and RegisterClone) | CLONE |
| EXECUTE | `copy_disabled` for FULL, with the Target relation absent and the equivalent persisted | [copy disabled](#execute-copy-disabled-allow_clones-false) |
| any | every other reason (local-only reasons, `copy_chain_limit`, uncaptured cells), NULL (row written before migration 0014), unknown, or contradicting the class | empty tree |

1. Every model kind with or without upstreams, VIEW without, when the
   decision recorded whether its SQL reads a relation
   (`explain.rs::freshness_listing_captured`).

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
| `no_reuse_candidate.own_target_changed_or_missing` (model) | The Target relation is reported twice or with no epoch | d1, reported twice with the build's epoch: SKIP, ordinary SKIP tree (entries 2–3). d2, no epoch: rebuild as a missing Target relation, codes 4 / 7, first-build tree (entries 6–7). d3 / d3e, target unset / `""`: `Internal` / `InvalidArgument` error, not recorded | local-only, empty tree. Accepted divergence #80 (d1, d2). d3 / d3e: this server rejects a non-test submit with no target with `InvalidArgument` before deciding (#81) |
| `no_build_anchor` | The reuse candidate's build was confirmed without an epoch and the node has genuine upstreams | d4: the ConfirmExecution without an epoch fails (`Internal`, "Database operation failed"), so hosted holds no such build. The decisive submit was not sent | local-only, empty tree. Accepted divergence #82 |
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
first-build tree). #80, #82 and #87 are accepted divergences
([correctness-handoff.md](correctness-handoff.md#accepted-divergences)): the
server keeps its conservative rebuilds, so their keyed gaps stay.
