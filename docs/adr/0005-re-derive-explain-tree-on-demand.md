# Re-derive the `dbt state explain` tree on demand

## Status

Decided (wayfinder ticket #21, map #19 — faithful warehouse-general
`dbt state explain` support). Amended by
[ADR 0006](0006-persist-the-decision-reason.md): the decision reason is also
persisted, and selects the tree shape. Amended by #73: the unmatched latest
build's id is also persisted ([below](#decision)). Amended by #83/#90: an
execution type change keeps the build it was judged against as
`matched_execution_id` ([below](#decision)). Amended by #117: a view decision
records whether its SQL reads a relation ([below](#decision)).

## Decision

The server does NOT store the structured `ExplainLine` / `ExplainBadge` /
`ExplainMarker` tree at decision time. `ExplainService.GetExplainMessages`
**re-derives** the tree on demand from already-persisted rows, keyed by the
`execution_decision_id`s the client sends. `decision.rs::decide` stays pure and
I/O-free.

The one piece of new persisted state this requires is a single nullable column
on `node_decisions`: `matched_execution_id BIGINT REFERENCES executions(id)`,
recording which confirmed `executions` row the SKIP/reuse decision matched (the
caller already holds this id). Everything else is re-derived.

> **Amended by #73** (as revised by the #40 code review): a second nullable
> id, `node_decisions.unmatched_execution_id BIGINT REFERENCES executions(id) ON DELETE SET NULL`
> (migration 0017), records, for a rebuild with no matched build, the latest
> build in scope, whose identity did not match. Hosted judges the SHAPE 7
> freshness listing against that build (`<FRESH>` / `<OUTDATED>`, the `«gap»`
> line and the summary; `explain_incremental_*.jsonl`,
> `explain_rebuild_reason_gaps.jsonl`), so Explain re-derives it from that row
> with the same shared helper. As with `matched_execution_id`, only the id is
> persisted, never a verdict. NULL for every other decision and for a rebuild
> recorded before the migration; Explain then omits the summary line, because
> unknown evidence never proves freshness.

> **Amended by #83/#90** (approved by the maintainer, 2026-11-11). Anchor
> selection gains two cases, both decided at submit time and persisted as
> `matched_execution_id`; Explain still loads only the row that id names
> (org-scoped) and never looks up a newer build:
>
> - the latest build in scope has the workload's identity but another
>   execution type, and no build of the workload's type stands in for it:
>   the rebuild (`no_reuse_candidate.execution_type_changed`, codes 2 / 2) is
>   anchored on that latest build, and Explain renders the node-query-unchanged
>   tree against it;
> - the latest build has the identity under another type and an older build
>   has the identity and the workload's type: the submit is judged against
>   that older build ([ADR 0007](0007-lag-tolerance-and-upstream-producers.md)),
>   and when its own target was modified or dropped since, the rebuild is
>   anchored on it, like any build rejected for its own target.
>
> Hosted explains both against those builds (VERIFIED LIVE 2026-11-10/11:
> `explain_snapshot_history.jsonl` entries 58–60,
> `explain_incremental_append_merge_history.jsonl` entries 74–79,
> `explain_execution_type_change.jsonl`). `unmatched_execution_id` stays NULL
> for both, since the anchored build has the workload's identity.

> **Amended by #93:** a client-requested copy (RegisterClone) keeps, on its
> own `executions` row, the names of the upstreams its source build recorded
> (`executions.clone_source_inputs JSONB`, migration 0018). In the SKIP tree
> over such a copy hosted gives a verdict only to those upstreams and lists
> any other with its `last updated` line only
> (`explain_post_copy_identity.jsonl`). The column is evidence written once,
> when the copy is registered; no verdict or tree is persisted, and the
> anchor selection is unchanged. NULL on every other row and on a copy
> registered before the migration, which renders as before.

> **Amended by #117:** a view decision also records whether the view's SQL
> reads a relation (`node_decisions.reads_relation BOOLEAN`, migration 0019):
> a table or view it names in a FROM or JOIN, other than a CTE in scope
> where it is named,
> judged once at submit time by parsing the request's SQL in the
> warehouse's dialect (`Warehouse::sql_reads_relation`). Client 2.0.11
> reports no upstreams for a view, and hosted words the view's tree from its
> SQL: `no upstream dependencies` for a view that reads no relation (no FROM,
> a CTE without FROM, VALUES, a table function), `upstream model queries have
> not changed` and `upstream data is up to date` for a view over a `ref`
> (`explain_view_reads_no_relation.jsonl`, `explain_reason_kind_cells.jsonl`
> `vc0`, `cv0`). Explain reads the flag with the decision, org-scoped, and
> never parses SQL or reads the warehouse itself; no verdict or tree is
> persisted, and decisions never read the flag. NULL for every other node
> kind, for a view whose SQL does not parse as one query, and for a view
> decided before the migration: such a view without reported upstreams
> renders nothing, because unknown evidence does not choose a wording.

## Why re-derive (not persist the tree)

### Explain visibility amendment (Databricks #132)

Submit still schedules capture without waiting for it and capture remains
best-effort. GetExplainMessages waits for a currently tracked capture of its
requested decision IDs, scoped to the requesting organization, before reading
the persisted rows. Unknown IDs and another organization's IDs have no capture
to await and retain their omission behavior. No polling or generated tree is
stored. Tracking is in-memory and removed when a task finishes or unwinds;
capture failures and decisions absent after restart retain the existing
best-effort persistence semantics.

This avoids returning an empty first Explain merely because its successful
Submit's capture is still running. A public regression delays capture writes
with a database lock; the original read returned zero messages, while six
fresh hosted submit→Explain probes each returned the requested message.
The read may now incur the remaining capture latency, while Submit latency
and the pure decision engine are unchanged.

Characterization capture (ticket #20,
`golden/fixtures/explain_snowflake_happy.jsonl`) plus a column-level inventory of
`node_decisions` / `executions` showed every explain line maps to either a
persisted field or a value re-computable at explain time:

| Explain line needs | Source |
|---|---|
| decision class, `is_stale`, `decision_description` | `node_decisions` (persisted) |
| `TARGET_TABLE_EXISTS`, `NODE_QUERY_CHANGED` / `NODE_QUERY_UNCHANGED` | hashes + decision class / persisted `decision_description` (NO live physical-existence probe — see "Own-target existence" below) |
| decision-time upstream set + epochs | `node_decisions.input_tables` (persisted) |
| matched prior build's upstream set + own-build epoch (freshness anchor) | `executions` row via the new `matched_execution_id` FK |
| per-upstream `FRESH` / `OUTDATED`, considered/drifted counts, name-set diff | re-derived from the two `input_tables` snapshots + anchor epoch |

`explain` is already an on-demand client flow (ticket #20: re-Submit every node,
then `GetExplainMessages`), so re-deriving at explain time matches how the hosted
service is exercised. Storing the tree would add a write path and schema for data
that is a pure function of rows we already keep — against the project's "minimal
persisted state, pure engine" preference.

## The one gap this closes with persistence, and why

`decision.rs::evaluate_freshness` computes the per-upstream drift verdict
(`current_epoch > own_build_epoch` per parent), the considered/drifted counts,
and the name-set diff, then **discards** them — only the aggregate `is_stale` and
the decision class survive into `node_decisions`. Those discarded per-upstream
signals are exactly what the `data freshness analysis` lines render.

They are re-derivable, but only against the **exact** `executions` row the
decision matched. `node_decisions` did not record that row's id, and
"latest confirmed for target" drifts as the append-only ledger grows, so
re-selecting the prior row at explain time could pick a different row than the
decision actually used — corrupting the freshness lines. Recording
`matched_execution_id` makes re-derivation exact and cheap.

### Why the column is nullable (NOT for backward compatibility)

It is nullable because the match is **intrinsically optional**, not to ease a
migration. dbt-state-rs is pre-production, so there is no historical data to
preserve and the migration is a plain `ALTER TABLE ... ADD COLUMN` with no
backfill and no dual-write transition. The column is `NULL` whenever the decision
matched no prior row:

- every **EXECUTE** (first build or fingerprint miss): `decide` ran with
  `confirmed = None` — there is no `executions.id` to point at;
- **cross-name CLONE** decision (`ready_to_clone`): its match is a
  `CloneEquivalent`, which carries no `executions.id`, so the column is NULL.
  This is the CLONE *decision* ONLY. Clone **adoption** reuse — a SKIP or a
  freshness-driven EXECUTE that reuses a clone-origin `executions` row through
  `decide` — is NOT NULL: the reuse candidate is `Some(that row)` and its id is
  recorded. The explain path therefore joins the clone-origin anchor and must apply
  the clone-adoption input normalization (`store::ExplainDecisionRow::anchor`),
  which shares one rule with the decision
  (`reuse_candidate::adopt_current_inputs`).
  > **Amended by [ADR 0007](0007-lag-tolerance-and-upstream-producers.md):** a
  > CLONE is not always NULL. A Result copy that stands in for a Target
  > relation's own build, when that build had to rebuild, keeps the build as
  > `matched_execution_id` (`DecisionRecord::result_copy_judged`). A rebuild of
  > a missing or modified Target relation whose latest build had the
  > workload's identity also keeps that build (`submit_decision::anchor_rejected`).
  > **Amended by #83/#90:** so does a rebuild whose latest build had the
  > identity under another execution type, anchored on that build or on the
  > older build of the workload's type it was judged against (see the
  > amendment under [Decision](#decision)).
- the `node_decisions` capture write is best-effort and separate from the
  `executions` write, so the id can legitimately be absent.

So `NULL` means "no matched prior build", a large fraction of rows, not "legacy
row". A future reader must not tighten it to `NOT NULL`. The re-derivation
branches on presence — exactly the `Some(prev)` vs `None` split `decide` already
has — rendering the EXECUTE tree from `node_decisions` alone when the FK is
absent, and joining `executions` for the freshness anchor when present.

## Scope / deferrals

- The per-upstream drift verdict, counts, and name-set diff are re-derived via a
  **shared pure helper** that both `decide` and the explain path call, so the two
  cannot drift.
- `WITHIN_TOLERANCE` is out of scope here: `freshness_tolerance_seconds` is
  deliberately never applied (the engine refuses to widen the skip window) and
  the happy-path capture never produced that badge. It is an unobserved-behavior
  question pending a within-freshness-tolerance capture (map "Not yet specified"),
  not a re-derivation gap.
  > **Amended by [ADR 0007](0007-lag-tolerance-and-upstream-producers.md):** Lag
  > tolerance is applied, and Explain renders the `WITHIN_TOLERANCE` line from
  > the tolerance and Build age persisted with the decision.
- `ExplainService` gains a DB handle by becoming `ExplainService(pub AppState)`,
  mirroring `SelectorServiceImpl`; the router constructor passes `state.clone()`.

## Consequences

### Own-target existence is inferred, not probed (known limitation)

> **Amended by [ADR 0006](0006-persist-the-decision-reason.md).** Explain no
> longer infers existence from the `decision_description` wording; it selects
> the shape from the persisted Decision reason. A dropped Target relation now
> persists `no_reuse_candidate.own_target_missing` and renders the first-build
> tree ([explain-catalog.md](../explain-catalog.md)). The text below records
> the original limitation.

The `table analysis` existence line (`TARGET_TABLE_EXISTS` for a table that
exists, "table does not exist" for a first build) is NOT re-derived from a live
physical-existence probe — the explain service performs ONE DB read
(`fetch_explain_decisions`) and the pure generator; it never queries the
warehouse catalog. Own-target existence is inferred from the persisted decision
class plus the `decision_description` wording (the first-build descriptions
contain "did not exist" / "no prior execution").

KNOWN CONSEQUENCE (PR #31 round-3 F6): a dropped-own-target rebuild with
UNCHANGED SQL persists the GENERIC rebuild description (not "did not exist"), so
the generator cannot tell it apart from a node-query-changed rebuild and renders
the SHAPE-7 "table exists already / query changed" tree — wrong on both counts
(the table is gone; the query did not change). This is accepted, not fixed: that
generic description is the SAME string the hosted-characterized SHAPE-7
node-query-changed EXECUTE carries, so any guard that suppressed the
dropped-target tree would also regress the captured SHAPE-7 fixture. The correct
hosted shape for a dropped-then-rebuilt target is UNOBSERVED. Closing it needs a
Snowflake capture plus a persisted own-target existence signal on
`ExplainDecisionRow` (follow-up).

- One additive, nullable column; no new table; no new write path in the decision
  engine; `decide` stays pure.
- Explain reads cost one lookup by `execution_decision_id` plus (for reuse
  decisions) one join to the matched `executions` row.
- The shared freshness helper becomes the single source of truth for per-upstream
  verdicts, used by both the decision and the explanation of it.

Evidence: ticket #20 capture (`golden/fixtures/explain_snowflake_happy.jsonl`,
since replaced by `explain_happy_history.jsonl`;
`experiments/explain_experiment.sh`); `crates/server/migrations/0001_init.sql`
(executions), `0003_ui_domain.sql` (node_decisions);
`crates/server/src/decision.rs::evaluate_freshness`;
`crates/server/src/services.rs::ExplainService`.
