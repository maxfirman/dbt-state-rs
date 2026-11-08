//! Re-derivation of the `dbt state explain` structured tree (ADR 0005).
//!
//! `ExplainService.GetExplainMessages` does NOT store the `ExplainLine` tree at
//! decision time. It re-derives it on demand from the persisted `node_decisions`
//! row keyed by `execution_decision_id`, plus (for the per-upstream freshness
//! subtree) the shared pure `decision::upstream_freshness` helper so the
//! decision and its explanation can never drift.
//!
//! This module is PURE: it takes an already-read `ExplainNode` snapshot and
//! returns the tree + the `decision_description`. All I/O (the store read) lives
//! in the service. The catalog of record is `docs/explain-catalog.md`; every
//! line/marker/badge here is pinned to a live hosted capture, never invented.
//!
//! The line generator is warehouse-agnostic: the only dialect-specific content
//! is the physical FQN, which is already quoted by the client and stored
//! verbatim in `node_decisions.target_table` / `input_tables`. Any future
//! dialect-specific WORDING must be confined to [`line_vocabulary`] (mirroring
//! the `warehouse.rs` "Snowflake characterized, others conservative" seam).

use crate::decision::{self, StaleUpstreamPolicy, SubmitContext};
use crate::query_cache as qc;
use crate::store::{ExecutionRow, InputTable};

// ── proto enum ints (explain_service.proto) ───────────────────────────────
// Markers.
const MARKER_SUCCESS: i32 = 1;
const MARKER_FAIL: i32 = 2;
// Badges (only the OBSERVED subset — see docs/explain-catalog.md).
const BADGE_FRESH: i32 = 1;
const BADGE_OUTDATED: i32 = 3;
const BADGE_NODE_QUERY_CHANGED: i32 = 4;
const BADGE_NODE_QUERY_UNCHANGED: i32 = 5;
// UPSTREAM_QUERY_CHANGED is emitted inside the OUTDATED-driven EXECUTE sub-shape
// (catalog SHAPE 9), which is anchor-dependent (ADR 0005 / ticket #30).
const BADGE_UPSTREAM_QUERY_CHANGED: i32 = 6;
const BADGE_TARGET_TABLE_EXISTS: i32 = 8;

// SubmitSQLResultType decision ints (shared.proto).
const DECISION_SKIP: i32 = 0;
const DECISION_EXECUTE: i32 = 1;
const DECISION_CLONE: i32 = 3;

// ModelExecutionType values that change wording/structure (shared.proto).
const ET_FULL: i32 = 1;
const ET_DBT_DATA_TEST: i32 = 8;
// VIEW is distinguished from FULL only in the zero-upstream SKIP freshness body
// (catalog SHAPE 0 vs 1); referenced by the generator's unit tests.
#[allow(dead_code)]
const ET_VIEW: i32 = 10;

/// The persisted decision class, re-read from `node_decisions.decision`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecisionClass {
    Skip,
    Build,
    Clone,
}

impl DecisionClass {
    pub fn from_decision_str(s: &str) -> Option<Self> {
        match s {
            "skip" => Some(Self::Skip),
            "build" => Some(Self::Build),
            "clone" => Some(Self::Clone),
            _ => None,
        }
    }

    fn decision_int(self) -> i32 {
        match self {
            Self::Skip => DECISION_SKIP,
            Self::Build => DECISION_EXECUTE,
            Self::Clone => DECISION_CLONE,
        }
    }
}

/// A snapshot of one persisted decision, everything the pure generator needs to
/// re-derive the explain tree. Read from a single `node_decisions` row (plus,
/// for #30, the matched `executions` anchor row). Keyed by `execution_decision_id`.
#[derive(Debug, Clone)]
pub struct ExplainNode {
    pub execution_decision_id: String,
    pub decision: DecisionClass,
    pub execution_type: i32,
    /// Persisted `decision_description` from the decision time. Used as the
    /// authoritative situation selector (the hosted service recorded exactly
    /// which coexisting SKIP reason applied); the tree is derived independently.
    pub decision_description: Option<String>,
    pub is_stale: bool,
    pub target_table: Option<String>,
    pub clone_source: Option<String>,
    pub input_tables: Vec<InputTable>,
    /// The matched prior `executions` row (freshness anchor). ADR 0005: present
    /// only when the SKIP/reuse decision matched a prior build; `None` for
    /// first-build EXECUTE / clone. The per-upstream freshness SUBTREE
    /// (FRESH/OUTDATED + «gap» + «rel_time») can only be re-derived when this is
    /// present — it lands with the `matched_execution_id` column (ticket #30).
    pub anchor: Option<ExecutionRow>,
}

impl ExplainNode {
    fn noun(&self) -> &'static str {
        if self.execution_type == ET_DBT_DATA_TEST {
            "test"
        } else {
            "model"
        }
    }

    fn is_data_test(&self) -> bool {
        self.execution_type == ET_DBT_DATA_TEST
    }

    /// Genuine upstream inputs: every input table except the node's own target.
    fn genuine_upstreams(&self) -> Vec<&InputTable> {
        let target = self.target_table.as_deref();
        self.input_tables
            .iter()
            .filter(|t| Some(t.name.as_str()) != target)
            .collect()
    }
}

// ── tree node constructors (ExplainLine) ───────────────────────────────────

fn line(text: impl Into<String>) -> qc::ExplainLine {
    qc::ExplainLine {
        text: text.into(),
        marker: None,
        badge: None,
        children: Vec::new(),
    }
}

fn leaf(text: impl Into<String>, marker: Option<i32>, badge: Option<i32>) -> qc::ExplainLine {
    qc::ExplainLine {
        text: text.into(),
        marker,
        badge,
        children: Vec::new(),
    }
}

fn section(text: impl Into<String>, children: Vec<qc::ExplainLine>) -> qc::ExplainLine {
    qc::ExplainLine {
        text: text.into(),
        marker: None,
        badge: None,
        children,
    }
}

// ── public entry point ─────────────────────────────────────────────────────

/// Re-derive the full `ExplainMessageEntry` (tree + description) for one node.
pub fn explain_entry(node: &ExplainNode) -> qc::ExplainMessageEntry {
    qc::ExplainMessageEntry {
        execution_decision_id: node.execution_decision_id.clone(),
        decision: node.decision.decision_int(),
        decision_description: decision_description(node),
        explain_lines: explain_lines(node),
    }
}

/// Select the `decision_description` by SITUATION (independent of the tree),
/// per docs/explain-catalog.md § decision_description. We trust the persisted
/// decision-time string when present (the hosted service recorded exactly which
/// of the coexisting SKIP reasons applied — e.g. plain vs lag-tolerance vs
/// multi-upstream — a distinction not recoverable from the tree alone), and
/// fall back to the engine's node-kind-specific pins otherwise.
pub fn decision_description(node: &ExplainNode) -> String {
    if let Some(d) = node
        .decision_description
        .as_deref()
        .filter(|s| !s.is_empty())
    {
        return d.to_string();
    }
    // Fallback: the engine's pinned wording (matches decision.rs). Used only
    // when the persisted description is absent.
    match node.decision {
        DecisionClass::Skip if node.is_data_test() => {
            "data test was a no-op because both its query and its upstream data are up to date"
                .to_string()
        }
        DecisionClass::Skip => decision::DESC_SKIP.to_string(),
        DecisionClass::Build => decision::DESC_EXECUTE.to_string(),
        DecisionClass::Clone => {
            "an equivalent model exists under another name so we cloned that one".to_string()
        }
    }
}

/// Whether (execution_type, decision) is a CHARACTERIZED combination that the
/// hosted service was observed to render and that we therefore reproduce. This
/// is a strict positive allowlist; everything else returns an empty tree.
///
/// Characterized matrix (verified against the explain fixtures — which carry
/// ONLY execution_type 1/10/8 — and docs/explain-catalog.md):
///   - model/table (ET_FULL=1) and view (ET_VIEW=10): Skip | Build | Clone.
///   - data test (ET_DBT_DATA_TEST=8): Skip only (Build/Clone unobserved — F4).
///   - everything else (seed ET_VALUES=9, snapshot ET_SNAPSHOT=7, incremental,
///     any other execution_type): NONE.
///
/// Anything outside this set is "Unobserved — do not implement until captured"
/// (docs/explain-catalog.md) and is explicitly Out-of-Scope for spec #27, whose
/// hard rule is "mark as unimplemented rather than invent ... never ship
/// invented output". Without this fence `explain_lines` dispatches on
/// `DecisionClass` alone and renders a model-shaped tree for seed/snapshot/
/// incremental decisions (e.g. a seed reload persists "seed was loaded because
/// its data changed" and would fall through to the SHAPE-7 "the model query has
/// changed" tree). PR #31 round-4 F7.
fn is_characterized(execution_type: i32, decision: DecisionClass) -> bool {
    match execution_type {
        ET_FULL | ET_VIEW => matches!(
            decision,
            DecisionClass::Skip | DecisionClass::Build | DecisionClass::Clone
        ),
        ET_DBT_DATA_TEST => matches!(decision, DecisionClass::Skip),
        _ => false,
    }
}

/// Build the structured tree. Dispatches on decision class + node kind.
pub fn explain_lines(node: &ExplainNode) -> Vec<qc::ExplainLine> {
    // F7 fence (PR #31 round 4): never render an invented tree for an
    // uncharacterized node kind. This subsumes the old data-test-EXECUTE guard
    // that lived inside `execute_tree`.
    if !is_characterized(node.execution_type, node.decision) {
        return Vec::new();
    }
    match node.decision {
        DecisionClass::Skip => skip_tree(node),
        DecisionClass::Build => execute_tree(node),
        DecisionClass::Clone => clone_tree(node),
    }
}

// ── SKIP ────────────────────────────────────────────────────────────────────

fn skip_tree(node: &ExplainNode) -> Vec<qc::ExplainLine> {
    let mut out = Vec::new();
    // Data tests produce no physical object → no `table analysis` section.
    if !node.is_data_test() {
        out.push(table_analysis_exists(node));
    }
    out.push(query_analysis_unchanged(node));
    out.push(data_freshness_analysis_fresh(node));
    out
}

/// `table analysis ("«FQN»")` → `the <noun> table exists already` [SUCCESS]
/// <TARGET_TABLE_EXISTS>. Rendered for every object-producing SKIP/EXECUTE.
fn table_analysis_exists(node: &ExplainNode) -> qc::ExplainLine {
    section(
        format!("table analysis ({})", fqn(node)),
        vec![leaf(
            format!("the {} table exists already", node.noun()),
            Some(MARKER_SUCCESS),
            Some(BADGE_TARGET_TABLE_EXISTS),
        )],
    )
}

/// `query analysis` with the node-query-UNCHANGED body. For a node with genuine
/// upstreams OR a VIEW/data-test, the `upstream model queries have not changed`
/// line is present; a FULL model with no genuine upstreams omits it (catalog
/// SHAPE 0 vs 1, pinned live).
fn query_analysis_unchanged(node: &ExplainNode) -> qc::ExplainLine {
    let mut children = vec![leaf(
        format!("the {} query has not changed", node.noun()),
        Some(MARKER_SUCCESS),
        Some(BADGE_NODE_QUERY_UNCHANGED),
    )];
    if has_upstream_query_line(node) {
        children.push(leaf(
            "upstream model queries have not changed",
            Some(MARKER_SUCCESS),
            None,
        ));
    }
    section("query analysis", children)
}

/// Whether the `upstream model queries have not changed [SUCCESS]` line is
/// present. VERIFIED (catalog): a FULL model with zero genuine upstreams omits
/// it (SHAPE 0); a VIEW, a data test, or any node with ≥1 genuine upstream
/// carries it (SHAPEs 1/2/4…).
fn has_upstream_query_line(node: &ExplainNode) -> bool {
    !node.genuine_upstreams().is_empty() || node.execution_type != ET_FULL
}

/// `data freshness analysis` body for a SKIP, by upstream situation.
fn data_freshness_analysis_fresh(node: &ExplainNode) -> qc::ExplainLine {
    let upstreams = node.genuine_upstreams();
    if upstreams.is_empty() {
        // No genuine upstreams. A FULL model renders `no upstream dependencies`;
        // a VIEW (which selects from an untracked source) renders `upstream data
        // is up to date` (catalog SHAPE 0 vs 1).
        let body = if node.execution_type == ET_FULL {
            leaf("no upstream dependencies", Some(MARKER_SUCCESS), None)
        } else {
            leaf("upstream data is up to date", Some(MARKER_SUCCESS), None)
        };
        return section("data freshness analysis", vec![body]);
    }
    // Per-upstream FRESH listing — the FK-anchored subtree (ticket #30). When the
    // anchor is absent we still render the structural `upstream dependencies`
    // parent with the per-upstream children so the tree SHAPE matches; the
    // FRESH/OUTDATED badge + «gap»/«rel_time» text are the anchor-dependent
    // slots that the conformance gate fences until #30.
    section(
        "data freshness analysis",
        vec![upstream_dependencies_freshness(node, &upstreams)],
    )
}

/// The per-upstream `upstream dependencies` listing with FRESH/OUTDATED badges,
/// the anchor-relative `«gap»` / `no updates since …` line and the humanized
/// `last updated: «rel_time»`. THIS is the subtree that requires the matched
/// `executions` anchor (ADR 0005 / ticket #30): without the anchor we cannot
/// know each upstream's FRESH/OUTDATED verdict or the gap. It is re-derived from
/// the shared pure `decision::upstream_freshness` helper so it can never drift
/// from the decision.
fn upstream_dependencies_freshness(
    node: &ExplainNode,
    upstreams: &[&InputTable],
) -> qc::ExplainLine {
    // Per-upstream verdicts via the shared helper when the anchor is present.
    let verdicts = anchored_verdicts(node);
    let anchor_name = node.target_table.clone().unwrap_or_default();
    let children = upstreams
        .iter()
        .map(|up| {
            let verdict = verdicts
                .as_ref()
                .and_then(|v| v.iter().find(|u| u.name == up.name));
            let drifted = verdict.map(|u| u.drifted);
            let badge = match drifted {
                Some(true) => Some(BADGE_OUTDATED),
                Some(false) => Some(BADGE_FRESH),
                // No anchor (first-build/clone cases ADR 0005 leaves NULL): emit
                // the FRESH badge as the pinned happy-path default for the
                // structural listing. An anchored decision always resolves to
                // Some above.
                None => Some(BADGE_FRESH),
            };
            // The first child line is anchor-relative: FRESH renders
            // "no updates since …", OUTDATED renders the "«gap»"
            // (`updated N seconds after … last executed`).
            let gap_or_fresh = match drifted {
                Some(true) => gap_line(node, &anchor_name, verdict),
                _ => fresh_anchor_line(node, &anchor_name),
            };
            qc::ExplainLine {
                text: quote(&up.name),
                marker: None,
                badge,
                children: vec![gap_or_fresh, line(rel_time_line(up))],
            }
        })
        .collect();
    section("upstream dependencies", children)
}

/// The FRESH first child: `no updates since «FQN» last executed` (model anchor)
/// or `no updates since test last executed` (data test).
fn fresh_anchor_line(node: &ExplainNode, anchor_name: &str) -> qc::ExplainLine {
    if node.is_data_test() {
        line("no updates since test last executed")
    } else {
        line(format!(
            "no updates since {} last executed",
            quote(anchor_name)
        ))
    }
}

/// The OUTDATED first child, the `«gap»`:
/// `updated N seconds after «FQN» last executed`, where N is the upstream's
/// epoch minus the freshness anchor epoch (both milliseconds → whole seconds).
/// The exact N is a runtime slot the gate masks to `«gap»`; we still render a
/// faithful value from the verdict's two epochs.
fn gap_line(
    node: &ExplainNode,
    anchor_name: &str,
    verdict: Option<&decision::UpstreamVerdict>,
) -> qc::ExplainLine {
    let seconds = verdict
        .and_then(|v| v.current_epoch.map(|c| (c - v.anchor_epoch).max(0) / 1000))
        .unwrap_or(0);
    let anchor_ref = if node.is_data_test() {
        "test".to_string()
    } else {
        quote(anchor_name)
    };
    line(format!(
        "updated {} seconds after {} last executed",
        seconds, anchor_ref
    ))
}

/// The humanized `last updated: …` line for an upstream. The absolute/relative
/// wall-clock text is a runtime slot the gate masks (`«rel_time»`). We render a
/// faithful `last updated: <epoch_ms> (UTC)` from the upstream's recorded epoch;
/// the exact humanized phrasing ("7 seconds ago, 2026-11-08 17:11:54 UTC") is a
/// client-side wall-clock-relative rendering out of this server's scope — the
/// gate masks it on both sides.
fn rel_time_line(up: &InputTable) -> String {
    match up.last_modified_epoch {
        Some(epoch) => format!("last updated: {epoch} (UTC)"),
        None => "last updated: unknown".to_string(),
    }
}

/// Re-derive the per-upstream FRESH/OUTDATED verdicts from the shared pure
/// helper, but ONLY when the matched `executions` anchor is present (ADR 0005).
/// Returns None when the anchor is absent (first-build/clone) — the caller then
/// renders the structural listing with the FRESH default badge.
fn anchored_verdicts(node: &ExplainNode) -> Option<Vec<decision::UpstreamVerdict>> {
    let prev = node.anchor.as_ref()?;
    let ctx = anchored_ctx(node);
    let f = decision::upstream_freshness(&ctx, prev);
    Some(f.upstreams)
}

/// Build the `SubmitContext` the freshness helper needs to re-derive verdicts
/// against the anchor. Mirrors the decision-time context for the fields the
/// helper reads (input set, target, execution type); policy is ANY (the helper
/// returns policy-neutral per-upstream verdicts regardless).
fn anchored_ctx(node: &ExplainNode) -> SubmitContext<'_> {
    SubmitContext {
        execution_type: node.execution_type,
        node_body_hash: None,
        input_tables: &node.input_tables,
        freshness_tolerance_seconds: 0,
        target_table: node.target_table.as_deref(),
        stale_upstream_policy: StaleUpstreamPolicy::Any,
        target_has_prior_build: true,
    }
}

/// Whether the anchored decision has ANY drifted (OUTDATED) upstream — the
/// signal that selects the SHAPE-9 OUTDATED EXECUTE tree over the SHAPE-7
/// node-query-changed tree.
fn has_outdated_upstream(node: &ExplainNode) -> bool {
    anchored_verdicts(node)
        .map(|v| v.iter().any(|u| u.drifted))
        .unwrap_or(false)
}

// ── EXECUTE ──────────────────────────────────────────────────────────────────

fn execute_tree(node: &ExplainNode) -> Vec<qc::ExplainLine> {
    // Data-test EXECUTE is UNOBSERVED / unimplemented (explain-catalog.md
    // "Data test EXECUTE (only SKIP captured)", ~line 222; spec #27 Out-of-Scope).
    // As of PR #31 round-4 F7 this is now fenced upstream by `is_characterized`
    // in `explain_lines` (ET_DBT_DATA_TEST allows only Skip), so this branch is
    // unreachable for a data test. Kept as commented defense-in-depth so the two
    // places cannot silently disagree; `is_first_build`'s `!is_data_test()`
    // hardening remains the belt-and-braces guard against the data-test execute
    // description ("no prior execution") tripping the first-build heuristic.
    debug_assert!(
        !node.is_data_test(),
        "data-test EXECUTE must be fenced by explain_lines::is_characterized before reaching execute_tree"
    );
    // First build (no matched anchor and no genuine upstreams recorded against a
    // prior build): table-analysis-only shape. We distinguish it from the
    // node-query-changed rebuild by the persisted description wording.
    if is_first_build(node) {
        return vec![first_build_table_analysis(node)];
    }
    // Upstream-drift EXECUTE (catalog SHAPE 9): the node query is UNCHANGED but
    // at least one upstream is OUTDATED against the matched build. This is
    // entirely anchor-driven (ADR 0005 / ticket #30) — re-derivable only when
    // the matched `executions` row is present and a per-upstream verdict drifted.
    if has_outdated_upstream(node) {
        return execute_outdated_tree(node);
    }
    // Node-query-changed rebuild (SHAPE 7): table exists, query CHANGED, upstream
    // queries unchanged, upstream data up to date.
    //
    // KNOWN LIMITATION (PR #31 round-3 F6 — DOCUMENTED, not guarded): a
    // dropped-own-target rebuild with UNCHANGED SQL also lands here and
    // mis-labels as "table exists already" + "the model query has changed".
    // Explain has no own-target existence signal and runs no physical probe, and
    // the dropped-target rebuild persists the SAME generic rebuild description as
    // a genuine node-query-change (see `decision.rs`
    // `decision_descriptions_match_hosted_wording`), so it is indistinguishable
    // from the hosted-characterized SHAPE-7 node-query-changed EXECUTE. Any guard
    // suppressing it would regress the captured SHAPE-7 fixture; the correct
    // hosted shape for a dropped-then-rebuilt target is unobserved. Closing it
    // needs a capture + a persisted own-target existence signal (follow-up). See
    // docs/explain-catalog.md "Known limitation — dropped-own-target rebuild"
    // and ADR 0005 "Own-target existence is inferred, not probed".
    let mut out = vec![table_analysis_exists(node)];
    out.push(query_analysis_changed(node));
    out.push(section(
        "data freshness analysis",
        vec![leaf(
            "upstream data is up to date",
            Some(MARKER_SUCCESS),
            None,
        )],
    ));
    out
}

/// The upstream-drift EXECUTE tree (catalog SHAPE 9). The node's own query is
/// unchanged, but ≥1 upstream is OUTDATED, so hosted renders:
///   query analysis → node-query-unchanged
///                  → `upstream dependencies` (OUTDATED parents, each with
///                    `query or upstream query changed` <UPSTREAM_QUERY_CHANGED>)
///                  → `an upstream model query has changed` [FAIL]
///   data freshness analysis → `upstream dependencies` (OUTDATED freshness
///                    subtree with «gap»/«rel_time») → `upstream data is
///                    outdated` [FAIL]
fn execute_outdated_tree(node: &ExplainNode) -> Vec<qc::ExplainLine> {
    let upstreams = node.genuine_upstreams();
    let verdicts = anchored_verdicts(node);
    // The OUTDATED upstreams, in the node's recorded input order.
    let outdated: Vec<&InputTable> = upstreams
        .iter()
        .copied()
        .filter(|up| {
            verdicts
                .as_ref()
                .and_then(|v| v.iter().find(|u| u.name == up.name))
                .map(|u| u.drifted)
                .unwrap_or(false)
        })
        .collect();

    let mut out = vec![table_analysis_exists(node)];

    // query analysis: node-query unchanged + the OUTDATED upstreams' logic-diff
    // listing + the FAIL summary.
    let mut query_children = vec![leaf(
        format!("the {} query has not changed", node.noun()),
        Some(MARKER_SUCCESS),
        Some(BADGE_NODE_QUERY_UNCHANGED),
    )];
    let query_listing: Vec<qc::ExplainLine> = outdated
        .iter()
        .map(|up| qc::ExplainLine {
            text: quote(&up.name),
            marker: None,
            badge: None,
            children: vec![leaf(
                "query or upstream query changed",
                None,
                Some(BADGE_UPSTREAM_QUERY_CHANGED),
            )],
        })
        .collect();
    query_children.push(section("upstream dependencies", query_listing));
    query_children.push(leaf(
        "an upstream model query has changed",
        Some(MARKER_FAIL),
        None,
    ));
    out.push(section("query analysis", query_children));

    // data freshness analysis: the per-upstream OUTDATED freshness subtree + the
    // FAIL summary.
    out.push(section(
        "data freshness analysis",
        vec![
            upstream_dependencies_freshness(node, &upstreams),
            leaf("upstream data is outdated", Some(MARKER_FAIL), None),
        ],
    ));
    out
}

/// A first build renders the table-analysis-only shape. Detected by the
/// persisted first-build description ("did not exist" / "no prior execution").
/// Hardened against the data-test execute wording: a data test has no physical
/// object, so a "first build table analysis" is meaningless for it, and its
/// pinned description ("...has no prior execution or its query changed") would
/// otherwise trip the substring heuristic. Guarding on `!is_data_test()` keeps
/// this robust even if the `execute_tree` data-test guard is later refactored.
fn is_first_build(node: &ExplainNode) -> bool {
    !node.is_data_test()
        && node
            .decision_description
            .as_deref()
            .map(|d| {
                d.contains("did not exist")
                    || d.contains("no prior execution")
                    || d.contains("has no prior")
            })
            .unwrap_or(false)
}

/// First build — `table analysis` only: the table does not exist and there is
/// no other table it could be cloned from (catalog "First build" shape).
fn first_build_table_analysis(node: &ExplainNode) -> qc::ExplainLine {
    section(
        format!("table analysis ({})", fqn(node)),
        vec![
            leaf(
                format!("the {} table does not exist", node.noun()),
                Some(MARKER_FAIL),
                None,
            ),
            leaf(
                format!(
                    "there is no other table that the {} could be cloned from",
                    node.noun()
                ),
                Some(MARKER_FAIL),
                None,
            ),
        ],
    )
}

/// `query analysis` with the node-query-CHANGED body (SHAPE 7).
fn query_analysis_changed(node: &ExplainNode) -> qc::ExplainLine {
    section(
        "query analysis",
        vec![
            leaf(
                format!("the {} query has changed", node.noun()),
                Some(MARKER_FAIL),
                Some(BADGE_NODE_QUERY_CHANGED),
            ),
            leaf(
                "upstream model queries have not changed",
                Some(MARKER_SUCCESS),
                None,
            ),
        ],
    )
}

// ── CLONE ────────────────────────────────────────────────────────────────────

fn clone_tree(node: &ExplainNode) -> Vec<qc::ExplainLine> {
    let mut out = Vec::new();
    // `table analysis`: the table does not exist but could be cloned from the
    // clone source (catalog CLONE shape).
    let clone_source = node.clone_source.clone().unwrap_or_default();
    out.push(section(
        format!("table analysis ({})", fqn(node)),
        vec![
            leaf(
                format!("the {} table does not exist", node.noun()),
                Some(MARKER_FAIL),
                None,
            ),
            leaf(
                format!(
                    "the {} table could be cloned from {}",
                    node.noun(),
                    quote(&clone_source)
                ),
                Some(MARKER_SUCCESS),
                None,
            ),
        ],
    ));
    // The query + freshness sections are suffixed `(vs clone)` — the comparison
    // is against the clone source, not a prior build of this node.
    let mut query_children = vec![leaf(
        format!("the {} query has not changed", node.noun()),
        Some(MARKER_SUCCESS),
        Some(BADGE_NODE_QUERY_UNCHANGED),
    )];
    query_children.push(leaf(
        "upstream model queries have not changed",
        Some(MARKER_SUCCESS),
        None,
    ));
    out.push(section("query analysis (vs clone)", query_children));

    let upstreams = node.genuine_upstreams();
    let mut freshness_children = Vec::new();
    if !upstreams.is_empty() {
        freshness_children.push(upstream_dependencies_freshness(node, &upstreams));
    }
    freshness_children.push(leaf(
        "upstream data is up to date",
        Some(MARKER_SUCCESS),
        None,
    ));
    out.push(section(
        "data freshness analysis (vs clone)",
        freshness_children,
    ));
    out
}

// ── FQN / quoting helpers (the single dialect-specific seam) ────────────────

/// The node's own fully-qualified target, as stored (already quoted by client).
fn fqn(node: &ExplainNode) -> String {
    quote(node.target_table.as_deref().unwrap_or(""))
}

/// Physical names are stored already quoted by the client (`"DB"."S"."T"`).
/// Preserve them verbatim — this is the only dialect-specific content, and it
/// never needs re-quoting. Placed here so any future dialect-specific WORDING
/// has one home (the warehouse.rs seam mirror).
fn quote(name: &str) -> String {
    name.to_string()
}

#[allow(dead_code)]
fn line_vocabulary() {} // seam marker: dialect-specific wording lives in this module

#[cfg(test)]
mod tests {
    use super::*;

    fn tbl(name: &str) -> InputTable {
        InputTable {
            name: name.into(),
            last_modified_epoch: Some(100),
        }
    }

    fn node(decision: DecisionClass, et: i32, desc: &str) -> ExplainNode {
        ExplainNode {
            execution_decision_id: "edid".into(),
            decision,
            execution_type: et,
            decision_description: Some(desc.into()),
            is_stale: false,
            target_table: Some("\"DB\".\"S\".\"T\"".into()),
            clone_source: None,
            input_tables: vec![tbl("\"DB\".\"S\".\"T\"")],
            anchor: None,
        }
    }

    /// Flatten the tree into (depth, text, marker, badge), masking the runtime
    /// slots the catalog marks as `«FQN»`/`«rel_time»`/`«gap»` so we assert the
    /// STRUCTURE + fixed text against the catalog without the anchor.
    fn flat(
        lines: &[qc::ExplainLine],
        depth: usize,
        out: &mut Vec<(usize, String, Option<i32>, Option<i32>)>,
    ) {
        for l in lines {
            out.push((depth, mask(&l.text), l.marker, l.badge));
            flat(&l.children, depth + 1, out);
        }
    }

    fn mask(t: &str) -> String {
        // Mask a quoted three-part FQN and the humanized time slots.
        let mut s = t.to_string();
        if s.contains("\".\"") {
            // crude FQN mask: any "...."...."...." becomes «FQN»
            if let (Some(a), Some(_b)) = (s.find('"'), s.rfind('"')) {
                let _ = a;
            }
        }
        // table analysis ("FQN") → mask the parenthetical
        if s.starts_with("table analysis (") {
            s = "table analysis («FQN»)".to_string();
        } else if s.starts_with("last updated:") {
            s = "last updated: «rel_time»".to_string();
        } else if s.starts_with("no updates since") {
            s = "no updates since «FQN» last executed".to_string();
        } else if s.contains("table could be cloned from \"") {
            s = "the model table could be cloned from «FQN»".to_string();
        } else if s.starts_with('"') && s.ends_with('"') {
            s = "«FQN»".to_string();
        }
        s
    }

    fn render(node: &ExplainNode) -> Vec<(usize, String, Option<i32>, Option<i32>)> {
        let mut out = Vec::new();
        flat(&explain_lines(node), 0, &mut out);
        out
    }

    #[test]
    fn model_skip_full_no_upstreams_is_shape0() {
        // et=1 FULL, zero genuine upstreams → `no upstream dependencies`.
        let mut n = node(
            DecisionClass::Skip,
            ET_FULL,
            "model was a no-op because both its query and its upstream data are up to date",
        );
        n.input_tables = vec![tbl("\"DB\".\"S\".\"T\"")]; // only the target
        let got = render(&n);
        let expected = vec![
            (0, "table analysis («FQN»)".to_string(), None, None),
            (
                1,
                "the model table exists already".to_string(),
                Some(1),
                Some(8),
            ),
            (0, "query analysis".to_string(), None, None),
            (
                1,
                "the model query has not changed".to_string(),
                Some(1),
                Some(5),
            ),
            (0, "data freshness analysis".to_string(), None, None),
            (1, "no upstream dependencies".to_string(), Some(1), None),
        ];
        assert_eq!(got, expected);
    }

    #[test]
    fn model_skip_view_no_upstreams_is_shape1() {
        // et=10 VIEW, zero genuine upstreams → upstream-query line + up-to-date.
        let mut n = node(
            DecisionClass::Skip,
            ET_VIEW,
            "model was a no-op because both its query and its upstream data are up to date",
        );
        n.input_tables = vec![tbl("\"DB\".\"S\".\"T\"")];
        let got = render(&n);
        let expected = vec![
            (0, "table analysis («FQN»)".to_string(), None, None),
            (
                1,
                "the model table exists already".to_string(),
                Some(1),
                Some(8),
            ),
            (0, "query analysis".to_string(), None, None),
            (
                1,
                "the model query has not changed".to_string(),
                Some(1),
                Some(5),
            ),
            (
                1,
                "upstream model queries have not changed".to_string(),
                Some(1),
                None,
            ),
            (0, "data freshness analysis".to_string(), None, None),
            (1, "upstream data is up to date".to_string(), Some(1), None),
        ];
        assert_eq!(got, expected);
    }

    #[test]
    fn model_execute_node_query_changed_is_shape7() {
        let n = node(
            DecisionClass::Build,
            ET_VIEW,
            "model was executed because either its query didn't match or its upstream data is out of date",
        );
        let got = render(&n);
        let expected = vec![
            (0, "table analysis («FQN»)".to_string(), None, None),
            (
                1,
                "the model table exists already".to_string(),
                Some(1),
                Some(8),
            ),
            (0, "query analysis".to_string(), None, None),
            (
                1,
                "the model query has changed".to_string(),
                Some(2),
                Some(4),
            ),
            (
                1,
                "upstream model queries have not changed".to_string(),
                Some(1),
                None,
            ),
            (0, "data freshness analysis".to_string(), None, None),
            (1, "upstream data is up to date".to_string(), Some(1), None),
        ];
        assert_eq!(got, expected);
    }

    #[test]
    fn model_first_build_is_table_analysis_only() {
        let n = node(
            DecisionClass::Build,
            ET_FULL,
            "model was executed because its table did not exist",
        );
        let got = render(&n);
        let expected = vec![
            (0, "table analysis («FQN»)".to_string(), None, None),
            (
                1,
                "the model table does not exist".to_string(),
                Some(2),
                None,
            ),
            (
                1,
                "there is no other table that the model could be cloned from".to_string(),
                Some(2),
                None,
            ),
        ];
        assert_eq!(got, expected);
    }

    #[test]
    fn data_test_execute_emits_nothing() {
        // F4 regression (PR #31): a data-test EXECUTE must NOT render a
        // model-shaped first-build `table analysis` tree. The pinned data-test
        // execute description contains "no prior execution", which used to trip
        // the `is_first_build` substring heuristic. Data-test EXECUTE is
        // UNOBSERVED/unimplemented (explain-catalog.md ~line 222, spec #27
        // Out-of-Scope "only SKIP captured"), so we emit nothing rather than
        // invent a structurally-impossible tree.
        let mut n = node(
            DecisionClass::Build,
            ET_DBT_DATA_TEST,
            "data test was executed because it has no prior execution or its query changed",
        );
        n.target_table = None; // tests produce no physical object
        assert!(
            explain_lines(&n).is_empty(),
            "data-test EXECUTE is unobserved → must emit no invented lines, got {:?}",
            render(&n)
        );
    }

    #[test]
    fn data_test_skip_has_no_table_analysis() {
        // et=8 data test SKIP: no `table analysis`; query + per-upstream freshness.
        let mut n = node(
            DecisionClass::Skip,
            ET_DBT_DATA_TEST,
            "data test was a no-op because both its query and its upstream data are up to date",
        );
        n.target_table = None; // tests produce no object
        n.input_tables = vec![tbl("\"DB\".\"S\".\"UP\"")];
        let got = render(&n);
        // Structure: query analysis (test query unchanged + upstream q unchanged),
        // then data freshness analysis → upstream dependencies → the upstream.
        assert_eq!(got[0], (0, "query analysis".to_string(), None, None));
        assert_eq!(
            got[1],
            (
                1,
                "the test query has not changed".to_string(),
                Some(1),
                Some(5)
            )
        );
        assert_eq!(
            got[2],
            (
                1,
                "upstream model queries have not changed".to_string(),
                Some(1),
                None
            )
        );
        assert_eq!(
            got[3],
            (0, "data freshness analysis".to_string(), None, None)
        );
        assert_eq!(got[4], (1, "upstream dependencies".to_string(), None, None));
        assert_eq!(got[5], (2, "«FQN»".to_string(), None, Some(1)));
    }

    #[test]
    fn clone_tree_has_vs_clone_sections() {
        let mut n = node(
            DecisionClass::Clone,
            ET_FULL,
            "an equivalent model exists under another name so we cloned that one",
        );
        n.clone_source = Some("\"DB\".\"S\".\"SRC\"".into());
        n.input_tables = vec![tbl("\"DB\".\"S\".\"UP\"")];
        let lines = explain_lines(&n);
        assert_eq!(lines[0].text, "table analysis (\"DB\".\"S\".\"T\")");
        assert_eq!(lines[0].children[0].text, "the model table does not exist");
        assert_eq!(lines[0].children[0].marker, Some(MARKER_FAIL));
        assert_eq!(
            lines[0].children[1].text,
            "the model table could be cloned from \"DB\".\"S\".\"SRC\""
        );
        assert_eq!(lines[1].text, "query analysis (vs clone)");
        assert_eq!(lines[2].text, "data freshness analysis (vs clone)");
    }

    // ── F7 (PR #31 round 4): fence uncharacterized node kinds ──────────────
    // explain_lines dispatches on DecisionClass alone; without an
    // execution-type fence, seed (ET_VALUES=9), snapshot (ET_SNAPSHOT=7) and
    // incremental decisions render model-shaped trees that the hosted service
    // was never observed to produce (docs/explain-catalog.md "Unobserved — do
    // not implement until captured"; spec #27 Out-of-Scope). They must emit
    // nothing.
    const ET_SNAPSHOT: i32 = 7;
    const ET_VALUES: i32 = 9;

    #[test]
    fn seed_skip_emits_nothing() {
        // ET_VALUES=9 seed SKIP: unobserved → no invented model-SKIP tree.
        let mut n = node(
            DecisionClass::Skip,
            ET_VALUES,
            "seed was a no-op because its data has not changed",
        );
        n.input_tables = vec![tbl("\"DB\".\"S\".\"T\"")];
        assert!(
            explain_lines(&n).is_empty(),
            "seed SKIP is unobserved → must emit no invented lines, got {:?}",
            render(&n)
        );
    }

    #[test]
    fn seed_execute_emits_nothing() {
        // The finding's concrete reachable case: submit_values captures seed
        // decision ids; a seed reload persists "seed was loaded because its
        // data changed", which would otherwise fall through to SHAPE-7
        // "the model query has changed".
        let n = node(
            DecisionClass::Build,
            ET_VALUES,
            "seed was loaded because its data changed",
        );
        assert!(
            explain_lines(&n).is_empty(),
            "seed EXECUTE is unobserved → must emit no invented lines, got {:?}",
            render(&n)
        );
    }

    #[test]
    fn snapshot_execute_emits_nothing() {
        // ET_SNAPSHOT=7 snapshot: unobserved → no invented tree.
        let n = node(DecisionClass::Build, ET_SNAPSHOT, "snapshot was executed");
        assert!(
            explain_lines(&n).is_empty(),
            "snapshot is unobserved → must emit no invented lines, got {:?}",
            render(&n)
        );
    }

    #[test]
    fn decision_description_prefers_persisted_situation_string() {
        let n = node(
            DecisionClass::Skip,
            ET_FULL,
            "model was a no-op because its query is up to date and its data is within lag tolerance",
        );
        assert_eq!(
            decision_description(&n),
            "model was a no-op because its query is up to date and its data is within lag tolerance"
        );
    }
}
