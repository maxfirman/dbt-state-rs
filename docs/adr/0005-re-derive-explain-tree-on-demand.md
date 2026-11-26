# Re-derive the `dbt state explain` tree on demand

## Status

Decided (wayfinder ticket #21, map #19 — faithful warehouse-general
`dbt state explain` support). Amended by
[ADR 0006](0006-persist-the-decision-reason.md): the decision reason is also
persisted, and selects the tree shape. Amended by #73: the unmatched latest
build's id is also persisted ([below](#decision)). Amended by #83/#90: an
execution type change keeps the build it was judged against as
`matched_execution_id` ([below](#decision)). Amended by #117: a view decision
records whether its SQL reads a relation ([below](#decision)). Amended by #67
(Accepted): a decision records the values its Volatile evaluation rendered
and whether the submit requested evaluation ([below](#decision)). Amended by
#201 (maintainer decisions 1 and 3, 2026-11-19): a Snowflake reuse of
volatile SQL by its unrendered code, and the Explain anchor of a model's
rebuild refused at Reuse qualification ([below](#decision)). Amended by #200
(2026-11-21): a Snowflake data test's input read through a copy keeps the
copy's name in the decision-time inputs, and its reuse under
`compare_unrendered_code` is explained by the unrendered code
([below](#decision)).

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

> **Amended by #67** (maintainer decision 2026-11-17; Accepted in #197,
> after the #191 Snowflake captures and #192–#196). A decision that performs
> Volatile evaluation records the rendered values it returned in
> `transformed_nodes_by_query`, in `node_decisions.rendered_volatile_values`
> (JSONB, migration 0027): an array of `{query, function, value}` in the
> order the functions first appear, keyed exactly as returned. Hosted's
> Explain tree prints each value ("`RAND() -> rendered with '…'`",
> `databricks_submit_copy_controls.jsonl` entry 86; the Snowflake trees of
> `explain_volatile_evaluation.jsonl`). A rendered timestamp or random
> number cannot be re-derived later, so it must be persisted rather than
> recomputed. Snowflake also renders values with evaluation off, and hosted
> then prints "… but this is tolerated by config" instead of "… and this is
> not tolerated by config", so the values alone cannot choose the line: a
> second nullable column, `node_decisions.volatile_evaluation_requested`
> (BOOLEAN, migration 0028, #194), records whether the SQL submit requested
> evaluation. Explain reads both with the decision, org-scoped; decisions and
> Reuse qualification never read them, and neither is part of any identity
> or fingerprint. The captured Explain wording that reads like a query or
> Upstream producer change ("the model query has changed" for an unchanged
> volatile model, "an upstream model query has changed" over a volatile
> view) is derived from these two columns and the persisted decision
> reason; identity is unchanged
> (maintainer decision on the #191 check-in). Either column NULL, which
> includes every decision recorded before its migration: Explain then
> renders no volatile lines, because an unknown value is not invented. No
> backfill. As elsewhere in this ADR, only evidence is stored, never a
> verdict or tree.

> **Amended by #201** (maintainer decisions on the #202 check-in,
> 2026-11-19; implemented by #205 and #206).
>
> 1. *Volatile SQL reused by its unrendered code* (decision 1, #206). This
>    is a Reuse qualification rule; it is recorded here because it qualifies
>    the #67 amendment above, and [protocol.md](../protocol.md) rule 8 and
>    [architecture.md](../architecture.md) cite it. With Volatile evaluation
>    requested and `compare_unrendered_code: true`, a Snowflake model whose
>    SQL calls a Volatile function is not refused at Reuse qualification
>    when its template identity (`node_template_hash`) equals the build's.
>    The ordinary dual rendered/template identity rules then decide, and the
>    other qualification checks still apply: complete query dependencies,
>    non-empty SQL and a template body
>    (`reuse_candidate.rs::volatile_sql_compared_by_unrendered_code`).
>    Hosted SKIPs that rerun with its values in the map
>    (`explain_volatile_cells.jsonl` entry 140) and explains it by its
>    unrendered code, with no volatile lines (entry 198). For exactly that
>    SKIP the decision records the template comparison in the existing
>    `node_decisions.template_comparison` evidence
>    (`submit_decision::ExplainEvidence::of`,
>    `explain::TemplateComparison::unrendered`), so Explain prints hosted's
>    template lines; every other Snowflake decision records none. The rule
>    reads no rendered value, so rendered values still enter no identity,
>    fingerprint or Reuse qualification. A changed template,
>    `compare_unrendered_code` off, a data test (the capture is a model only)
>    and every other Warehouse keep the refusal. The older-match report on a
>    rebuild (`reuse_candidate::older_build_matches`) keeps the unrelaxed
>    check.
> 2. *Explain anchor of a rebuild refused at Reuse qualification*
>    (decision 3, #205, #206). When a model's latest build in scope has the
>    workload's identity and Reuse qualification refuses it
>    (`no_reuse_candidate.failed_reuse_qualification`), the rebuild records
>    that build as `matched_execution_id`, and the build's age at the submit
>    as `build_age_ms` (`reuse_candidate::Selection::unqualified`,
>    `submit_decision::anchor_rejected`). Hosted judges the rebuild's upstream
>    listing against that build: the `<FRESH>` verdicts and `last updated`
>    lines of the volatile model rerun over an upstream table
>    (`explain_volatile_cells.jsonl` entries 189, 190 and 195) and of the
>    volatile-dependency tree with an upstream table (entry 199). The anchor
>    is Explain-only: it is chosen after the decision, is never a Reuse
>    candidate, copy source or input to `decide`, and the wire response,
>    identity and Reuse qualification are unchanged. Explain still loads only
>    the row the id names, org-scoped. Data tests keep no anchor
>    (`reuse_candidate::for_data_test`): no captured data-test tree is judged
>    against a refused build. `matched_execution_id` is NULL for such a
>    rebuild recorded before #201. Explain then has no build to judge the
>    upstreams against, so the trees that need one (the volatile rerun or
>    volatile-dependency tree over an upstream table) render nothing,
>    because unknown evidence never proves freshness. A tree without
>    upstream tables does not read the anchor and renders as before. No
>    backfill.

> **Amended by #200** (maintainer decision option A, 2026-11-21; [ADR
> 0007](0007-lag-tolerance-and-upstream-producers.md#data-test-history-key)).
> A Snowflake data test's input read through a copy is persisted in the
> decision-time inputs (`node_decisions.input_tables`) under the relation it
> stands for, with the copy's name as reported (`InputTable::read_copy`, a
> JSON key absent on every other input). A reuse renders that input by the
> copy's name, dated at the build it stands for, without a verdict or
> anchor line (`data_test_history.jsonl` F36–F39, F57, F81, F83, F84); other
> decisions over a stand-in are uncaptured and list it by the copy's name
> in their ordinary shape. A Snowflake data test reused
> under `compare_unrendered_code` with the run's unrendered body records the
> template comparison (`submit_decision::unrendered_data_test_reuse`), so
> Explain prints hosted's unrendered-code lines also when the rendered SQL
> matches (F39, F67, F84, P59). Only evidence is stored; inputs recorded
> before #200 carry no copy name and render as before.

> **Amended by #252.** An unanchored Snowflake FULL copy into an absent
> Target relation records the id of the build the selected copy source holds
> (`node_decisions.copy_source_build_id`, migration 0036). Capture follows the
> execution ID in the copy plan, even if a newer execution at that relation
> confirms during source selection. Explain loads the held build in the
> decision's organization and compares its recorded upstreams against the
> decision-time inputs. Equal physical relations and known epochs
> produce the `<FRESH>` listing with the absent destination in the anchor
> line. Missing source evidence, including decisions recorded before the
> migration, yields the plain listing. The optional id is used by Explain
> only.

> **Amended by #211.** A Snowflake rerun refused at Reuse qualification for
> its volatile SQL records the captured form its decision established, if
> any (`node_decisions.volatile_rerun_form`, TEXT, migration 0037;
> `Warehouse::volatile_rerun_form`): `sole_source_merge` for a MERGE whose
> Target relation is physically as built over one source table (no
> confirmed State in scope, recorded with no producer at the refused build
> and now, its epoch known and unchanged or advanced), and
> `isolated_generator` for a FULL model without upstreams or query
> dependencies, its Target relation physically as built, whose SQL is one
> query calling only `uniform(1, 100, 1234)`. Both rest on decision-time
> facts the decision row does not keep: the submitted SQL, and whether an
> input that has no producer is a source or an External modification of a
> dbt-built relation (both record `Producer::NoBuild`). Explain therefore
> reads the recorded form instead of re-deriving it: the MERGE form, with
> zero Lag tolerance, prints the model rerun tree
> (`explain_volatile_generator_merge.jsonl` entries 37 and 45), and the
> generator form prints no tree, because hosted reuses that model (entries
> 28 and 35, pending the maintainer). The same form selects the MERGE map
> (`Outcome::RefusedVolatileSqlIn`). The form records evidence of the
> rerun's shape, never a tree; decisions and Reuse qualification never read
> it, and it is part of no identity. NULL, including every decision recorded
> before the migration, prints neither form.

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

The Databricks literal-view display also needs the auxiliary comparison for
its exact matched/unmatched execution. When that evidence is absent, Explain
waits for the same organization's capture keyed by that execution's request
UUID, then rereads the requested decisions. Request and decision IDs occupy
separate tracking domains. The reread is unconditional even if no pending
capture remains: completion between the first read and registry lookup must
not preserve a stale snapshot. This is one bounded dependency pass, without
polling, an organization-wide barrier or a search for older matching builds.
Genuinely absent, failed, malformed or pre-upgrade audit evidence still uses
the existing fallback. Submit and Confirm do not wait for audit persistence.

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
  > **Amended by #201:** so does a model's rebuild refused at Reuse
  > qualification, anchored on the refused build (Explain only; see the
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

## Databricks incremental transition display (#146, 2026-11-15)

The native dbt 2.0.11 APPEND/MERGE history
`databricks_append_transitions.jsonl` characterizes a narrow exception to the
NULL-unmatched rule above. An already forced BUILD keeps the rejected
submit-type build in `matched_execution_id` and its original decision-time
inputs. With zero Lag tolerance, ANY policy, known source-only evidence and an
unchanged current target, `unmatched_execution_id` separately identifies the
latest physical build used to display the transition's input freshness. Both
IDs are captured at submit time and fetched within the organization; Explain
does not look up a newer build. This display row never enters reuse selection,
Lag tolerance or producer evaluation. Other type changes, FULL refresh and
incomplete evidence keep their existing behavior; ADR 0007's selection rule
is unchanged. See [the acceptance evidence](../databricks-append.md).

## Databricks pre-copy refusal display (#152, 2026-11-15)

The native MERGE source refresh in `databricks_precopy.jsonl` reports an ordinary
BUILD and names a refused copy in its table-analysis section. The existing
`node_decisions.clone_source` presentation slot freezes that other physical name
at submit time. The diagnostic lookup remains organization/family/namespace
scoped, selects latest physical state before qualification, and returns a name
only. It requires the captured direct-input rendered identity, full UID and
complete current genuine input epoch/producer evidence. It cannot provide a
candidate or a foreign execution ID. The original own-build
`matched_execution_id` still anchors query/freshness analysis; the name never
enters Reuse qualification or automatic copy selection. SNAPSHOT's separate
1/3 refusal retains its ordinary tree. See [the evidence and limits](../databricks-precopy.md).
