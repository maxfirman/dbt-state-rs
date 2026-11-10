# Persist the decision reason alongside `matched_execution_id`

## Status

Accepted (#47, spec #42). Amends [ADR 0005](0005-re-derive-explain-tree-on-demand.md);
it does not reverse it.

## Decision

Each node decision persists its **decision reason** (see `GLOSSARY.md`) in a
second nullable `node_decisions` column, `decision_reason`, next to ADR 0005's
`matched_execution_id`. The value is the reason's stable lowercase string
(`decision::DecisionReason::as_str`, e.g. `first_build`,
`no_reuse_candidate.own_target_changed_or_missing`, `reused.tolerated_stale`).
Capture writes it from the decision record (migration `0014_decision_reason.sql`).

`dbt state explain` still re-derives the tree on demand (ADR 0005). It now
selects the tree shape from the persisted decision class, decision reason and
matched execution anchor (`explain.rs::shape`). It no longer reads
`decision_description` wording to choose a shape, and its freshness
re-derivation takes only persisted evidence (`decision::FreshnessInputs`: the
node kind, its own target and its decision-time inputs) instead of a decision
context with made-up policy and prior-build values.

## Why

ADR 0005 left two signals unrecoverable from persisted rows:

- **First build vs rebuild** was inferred by substring matching on the
  description ("did not exist" / "no prior execution"). Rewording a description
  could change a tree.
- **Dropped own target** (PR #31 round-3 F6) persists the same generic rebuild
  description as a node-query-changed rebuild, so ADR 0005 documented it as
  indistinguishable without a persisted signal.

The decision already knows its reason (`DecisionRecord.reason`); persisting it
costs one column and keeps the decision engine pure.

## Considered options

- **Keep the description heuristic.** Rejected: wording drives shape, and the
  dropped-own-target and no-build-anchor rebuilds stay indistinguishable.
- **Persist the rendered tree.** Rejected for the reasons in ADR 0005: a pure
  function of rows we already keep should not get its own write path.
- **Persist only an own-target existence flag.** Rejected: it fixes one case,
  while the reason distinguishes every rebuild cause with one value.

## Consequences

- Explain output is unchanged for every case covered today. Each reason maps to
  today's shape; `UpstreamOutdated` renders the SHAPE-9 OUTDATED tree only when
  the anchor re-derives an outdated upstream, as before.
- The column is nullable only for rows written before migration 0014. The
  project is pre-production, so there is no backfill. A NULL or unknown value
  renders an empty tree, the same as an uncharacterized node kind.
- Stored strings are persisted state: never reword an existing value. Adding a
  reason adds a new string.
- A client-requested copy (RegisterClone) records `result_copy`.
- The dropped-own-target (`no_reuse_candidate.own_target_changed_or_missing`)
  and no-build-anchor (`no_build_anchor`) reasons keep today's SHAPE-7
  rendering, flagged in `explain.rs::shape` and `docs/explain-catalog.md` as
  awaiting hosted capture (#40). Once captured, fixing them only changes that
  mapping.
  > **Amended by [ADR 0007](0007-lag-tolerance-and-upstream-producers.md):** the
  > shape fallbacks changed with its characterization. A dropped Target
  > relation now persists `no_reuse_candidate.own_target_missing` and renders
  > the first-build tree; `no_reuse_candidate.own_target_modified` has its own
  > shape; `own_target_changed_or_missing` now means ambiguous own-target
  > evidence and, with `no_build_anchor`, keeps SHAPE 7
  > ([explain-catalog.md](../explain-catalog.md#shape-selection)).
  >
  > **Resolved by #71 (spec #40):** crafted probes showed hosted never makes
  > either rebuild (#80, #82). Both reasons, and `own_target_modified` without
  > a matched build, now render nothing, as anticipated above: only their
  > mapping in `explain.rs::shape` changed
  > ([explain-catalog.md](../explain-catalog.md#ambiguous-rebuild-reasons)).
