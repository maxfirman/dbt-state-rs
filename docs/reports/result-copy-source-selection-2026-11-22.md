# Hosted Result copy source selection on Snowflake and Databricks

On both warehouses, hosted State copied an absent FULL dev downstream that
read prod's physical upstream and built when dev read an independently built
upstream, even with equal rows. The decisions differed when dev read a
confirmed copy of the prod upstream. In crafted histories hosted chose a fresh
same-input source ahead of stale or different-input competitors. Databricks
chose the later completion when two fresh sources competed. The Snowflake
histories had at most one source that passed its isolation control. This is
the evidence handoff from [#242](https://github.com/maxfirman/dbt-state-rs/issues/242)
to the behavior-preserving refactor in [#243](https://github.com/maxfirman/dbt-state-rs/issues/243).

## Native upstream origin

Both machines used the same dbt 2.0.11 binary hash. Each arm had one confirmed
prod downstream build and an absent dev downstream Target relation. The dev
downstream used `pre_clone=never`, FULL, zero Lag tolerance and an ordinary
`SubmitEnrichedSQL`. The first dev and next unchanged submits permitted copies
(`allow_clones=true` on both warehouses). Snowflake sent a null chain limit and
Databricks sent 1. Snowflake also permitted copies on prod submits. Databricks
prod used `allow_clones=false`, chain limit 0, and dev deferred to prod. The
Snowflake runs used a merge-incremental upstream (run 1) and a table upstream
(run 2). Databricks used managed Delta tables.
[Snowflake native report](snowflake-upstream-origin-2026-11-22.md),
[Databricks native report](databricks-upstream-origin-2026-11-21.md).

CLONE names the selected source. BUILD was `ready_to_execute` with rejection
codes 6/6 and no source. The rows are shown as prod / first dev / repeat dev /
State-disabled Reference. Snowflake reports `(id, v)` with `id=1` in every row.
Databricks reports one `marker` column. The shortened source names identify the
per-run or UUID-prefixed prod downstream recorded in each linked case.

| Case | Snowflake upstream read by dev | Snowflake downstream, both runs | Databricks upstream read by dev | Databricks downstream |
|---|---|---|---|---|
| A | Same physical prod upstream, fixed source | CLONE `PROD_STATE_DEMO.OA_DN_<run>` → SKIP. Rows `prod / prod / prod / prod`. [A](evidence/snowflake-upstream-origin/A.json) | Same physical prod upstream, resolved by `ref` | CLONE prod `<prefix>_downstream` → SKIP. Rows `A_prod / A_prod / A_prod / A_prod`. [A](evidence/databricks-upstream-origin/A.json) |
| B | Dev upstream copied from prod and confirmed. Run 1 used `RegisterClone`, run 2 used submit | BUILD → SKIP. Rows `prod / prod / prod / prod`. [B](evidence/snowflake-upstream-origin/B.json) | Dev upstream copied from prod on submit and confirmed, with a physical Delta CLONE | CLONE prod `<prefix>_downstream` → SKIP. Rows `B_prod / B_prod / B_prod / B_prod`. [B](evidence/databricks-upstream-origin/B.json) |
| C | Independent dev build from a dev-schema input with the same row as prod | BUILD → SKIP. Rows `same / same / same / same`. [C](evidence/snowflake-upstream-origin/C.json) | Independent dev build with the same SQL, input and row as prod | BUILD → SKIP. Rows `C_prod / C_prod / C_prod / C_prod`. [C](evidence/databricks-upstream-origin/C.json) |
| D | Independent dev build from a dev-schema input with the dev marker | BUILD → SKIP. Rows `prod / dev / dev / dev`. [D](evidence/snowflake-upstream-origin/D.json) | Independent dev build with the same SQL after the shared raw input changed | BUILD → SKIP. Rows `D_prod / D_dev / D_dev / D_dev`. [D](evidence/databricks-upstream-origin/D.json) |

Case B's best comparison is Snowflake run 2 and Databricks. The dev upstream
was a confirmed ordinary submit copy of a prod table in both. Snowflake's
different result already held when `RegisterClone` made the upstream copy in
run 1. Chain limit, prod permissions, defer setup, SQL/configuration and
warehouse differed across machines. These records establish the decision
difference for the captured setups. They leave the deciding condition open.
Older Snowflake captures granted downstream copies over confirmed dev upstream
copies, including a submit copy, so B cannot support a blanket prohibition.
[Snowflake B](evidence/snowflake-upstream-origin/B.json),
[Databricks B](evidence/databricks-upstream-origin/B.json),
[older Snowflake grant, entry 12](../../golden/fixtures/explain_clone_history.jsonl),
[older Snowflake grant, entry 13](../../golden/fixtures/clone_chain_upstream_history.jsonl).

For C and D, Snowflake run 1 disabled the upstream pre-copy and still sent
`allow_clones=true` with a null chain limit. Run 2 and Databricks disabled
copying for the upstream build (`false`, 0). Those controls left the dev
downstream free to copy. Snowflake varied the physical raw input by schema.
Databricks kept that raw input fixed and changed its row after the prod builds
in D. The Snowflake Reference used a later full refresh of the same nodes
with `--no-manage-state`. Databricks ran the dev downstream SELECT with
`dbt show --no-manage-state` against its intended upstream. Each verified
Reference recorded zero State RPCs. Snowflake's original driver Reference
lacked that check. [Snowflake native report](snowflake-upstream-origin-2026-11-22.md),
[Databricks native report](databricks-upstream-origin-2026-11-21.md).

## Crafted completion and confirmation orders

Both drivers submitted all source builds before any confirmations and submitted
the absent FULL destination after all confirmations succeeded. Both used zero
Lag tolerance and ANY policy. Snowflake enabled copies on all submits with a
null chain limit. Databricks disabled copies on sources (limit 0) and enabled
them on the destination (limit 1). The `old` source read the destination's
current input in both setups. The `stale` source (Snowflake `new`) read that
input before it changed. The `middle` source (Snowflake `mid`) read a different
relation of the same object name. Databricks middle reported the same epoch as
the destination's input, as did Snowflake's `mid_current_alone` control.
Snowflake's other middle sources reported an earlier epoch. Databricks `peer`
read the same current input as `old`. These requests test the RPC decision and
the source named by a grant. They execute no warehouse SQL and end before
destination confirmation or resubmit.
[Snowflake crafted report](snowflake-copy-source-choice-2026-11-21.md),
[Databricks crafted report](databricks-copy-candidates-2026-11-21.md).

Completion lists are ordered by the physical epoch reported in
`ConfirmExecution`. Confirmation lists are ordered by arrival of those RPCs.
Each cell gives completion / confirmation → hosted decision and source, with
the zero-based destination submit entry in the fixture. Source roles denote
the relation in that history's `CSC_OLD`, `CSC_MID`, `CSC_NEW`, `cc_old` or
`cc_peer` schema. The exact names are in the fixtures.
[Snowflake fixture](../../golden/fixtures/copy_source_choice.jsonl),
[Databricks fixture](../../golden/fixtures/databricks_copy_candidates.jsonl).

| History | Snowflake | Databricks |
|---|---|---|
| Fresh alone | `old` / `old` → CLONE `old` (2, 6) | `old` / `old` → CLONE `old` (2) |
| Different input alone, earlier epoch | `middle` / `middle` → BUILD (10) | — |
| Different input alone, same epoch | `middle` / `middle` → BUILD (78) | `middle` / `middle` → BUILD (5) |
| Stale alone | `stale` / `stale` → BUILD (14) | `stale` / `stale` → BUILD (8) |
| Fresh and different input | `old < middle` / `old, middle` → CLONE `old` (20) | — |
| Fresh and different input, reverse confirms | `old < middle` / `middle, old` → CLONE `old` (26) | — |
| Fresh and different input, fresh completes last | `middle < old` / `middle, old` → CLONE `old` (60) | — |
| Fresh and stale | `old < stale` / `old, stale` → CLONE `old` (32) | `old < stale` / `old, stale` → CLONE `old` (13) |
| Fresh and stale, reverse confirms | `old < stale` / `stale, old` → CLONE `old` (38) | — |
| Fresh and stale, fresh completes last | `stale < old` / `stale, old` → CLONE `old` (66) | — |
| Fresh, different input and stale | `old < middle < stale` / `old, middle, stale` → CLONE `old` (46) | `old < middle < stale` / `old, middle, stale` → CLONE `old` (20) |
| Fresh, different input and stale, reverse confirms | `old < middle < stale` / `stale, middle, old` → CLONE `old` (54) | `old < middle < stale` / `stale, middle, old` → CLONE `old` (27) |
| Fresh, different input and stale, fresh completes last | `stale < middle < old` / `stale, middle, old` → CLONE `old` (74) | — |
| Two fresh same-input builds | — | `old < peer` / `old, peer` → CLONE `peer` (32) |
| Two fresh same-input builds, reverse confirms | — | `old < peer` / `peer, old` → CLONE `peer` (37) |
| Two fresh same-input builds, exchange epochs | — | `peer < old` / `old, peer` → CLONE `old` (42) |

The isolation controls and competitions support a shared observable rule for
these histories. Each service could still select the fresh same-input result
when competitors were confirmed and completed later. Reported epoch equality
alone did not make a differently bound input sufficient. Snowflake's control
cannot distinguish narrower Logical copy identity from a separate physical
input check. Its different-input schemas were fixed third schemas, unlike
the Target-relative reads in native B–D. Databricks' reverse-confirm and
exchange-epoch controls establish the later-completion preference for the
tested pair of fresh sources. Snowflake leaves that preference open. The
captures also leave internal traversal, equal-epoch ties, and how hosted
handles a pending write on a newer source open.
[Snowflake crafted report](snowflake-copy-source-choice-2026-11-21.md),
[Databricks crafted report](databricks-copy-candidates-2026-11-21.md).

## Physical correctness finding for policy review

The Snowflake Rust rehearsal `002752a5958d` ran the native table scenario.
In D the dev upstream was independently built and held `(1, 'dev')`. Rust
granted a copy of `CDO_DEMO_DB.PROD_STATE_DEMO.OD_DN_002752A5958D`, which had
read the prod upstream. The first dev downstream read returned `(1, 'prod')`
after a successful copy confirmation without fallback. The next unchanged
submit returned SKIP and the downstream still held `(1, 'prod')`. The verified
State-disabled Reference returned `(1, 'dev')` with zero State RPCs. Hosted
Snowflake and hosted Databricks both built D and matched their respective
References. The Snowflake crafted `mid_alone` and `mid_current_alone` local
grants likewise name a source with a different recorded input. Those histories
give protocol evidence of the mismatched bindings. The wrong rows were measured
in the native Snowflake rehearsal. The safety of accepting a different upstream
binding is tracked separately from the #243 refactor.
[Snowflake D including the rehearsal](evidence/snowflake-upstream-origin/D.json),
[Databricks D](evidence/databricks-upstream-origin/D.json),
[local replay gaps](../../golden/replay_gaps.json),
[policy review #264](https://github.com/maxfirman/dbt-state-rs/issues/264).

## Rust behavior to preserve in #243

- **Ordering and scope.** The scoped candidate query takes current confirmed
  rows, newest completion epoch first with Target relation name as the SQL
  tie-breaker, up to 16 before the lookup's post-query filtering. Preserve
  Organization, warehouse family and connection namespace boundaries, the
  exclusion of the destination, and the versioned Logical copy identities.
  [State history query](../../crates/server/src/store/history.rs).
- **Snowflake and DuckDB.** Preserve their existing Warehouse qualification,
  second physical-state read and distinction between continuing and stopping.
  A rejected same-input candidate allows the search to continue. The generic
  path can select a differently bound candidate without judging freshness when
  the destination has no reuse candidate and the search has had no freshness
  rejection. Encountering one after a freshness rejection stops the search,
  as does losing the source's current state on the second read. Snowflake's
  crafted replay pins `mid_alone` and `mid_current_alone` to `mid`, `old_mid`
  and `old_mid_rev` to `mid`, and `old_mid_new` and `old_mid_new_rev` to BUILD.
  The completion-reversed `_late` histories copy `old`. These are local
  refactor baselines. [Snowflake crafted report](snowflake-copy-source-choice-2026-11-21.md),
  [DuckDB Result copy subset](../duckdb-identity-and-copy.md).
- **Databricks.** Preserve skipping an unavailable or ineligible source,
  including a newest source blocked by a pending write, and trying the next
  candidate. Keep exact current input bindings, positive epochs and complete
  producer evidence, checked source proof, and source revalidation at
  confirmation. The selected execution supplies the pending copy's source
  identity and input evidence. [Databricks submit-copy contract](../databricks-submit-copy.md),
  [public-path baseline #241](https://github.com/maxfirman/dbt-state-rs/issues/241).
- **Native Databricks limits.** Native A and B include explicit
  `file_format="delta"`, outside Rust's current automatic copy subset. The
  crafted Databricks fixtures omit that extra and replay the destination
  decisions exactly. Native B therefore gives hosted evidence for one
  confirmed-copy setup, while the Rust baseline retains its existing request
  gate and physical binding checks. [Databricks native report](databricks-upstream-origin-2026-11-21.md),
  [Databricks crafted replay](../../crates/harness/tests/conformance.rs).

The public replay baseline covers the complete response and selected source,
including the source epoch and runtime. Carry the winning evidence through
pending write, successful confirmation and unchanged resubmit. Retain the
existing confirmations that mark a failed copy as an ordinary fallback build,
and the copy permission and chain checks, seed and `RegisterClone` behavior.
The Snowflake `<FRESH>` Explain gaps were resolved in
[#252](https://github.com/maxfirman/dbt-state-rs/issues/252).
[Refactor acceptance criteria](https://github.com/maxfirman/dbt-state-rs/issues/243).
