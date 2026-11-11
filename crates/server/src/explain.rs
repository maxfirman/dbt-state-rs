//! Re-derivation of the `dbt state explain` structured tree (ADR 0005).
//!
//! `ExplainService.GetExplainMessages` does NOT store the `ExplainLine` tree at
//! decision time. It re-derives it on demand from the persisted `node_decisions`
//! row keyed by `execution_decision_id`, plus (for the per-upstream freshness
//! subtree) the shared pure `decision::upstream_freshness` helper so the
//! decision and its explanation can never drift. The tree SHAPE is selected
//! from the persisted decision class, decision reason and anchor (ADR 0006,
//! [`shape`]); description wording never selects a shape.
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

use crate::decision::{self, DecisionKind, DecisionReason, NoCandidate, Reuse};
use crate::query_cache as qc;
use crate::query_cache::ModelExecutionType;
use crate::store::{ExecutionRow, InputTable};

// ── proto enum ints (explain_service.proto) ───────────────────────────────
// Markers.
const MARKER_SUCCESS: i32 = 1;
const MARKER_FAIL: i32 = 2;
// Badges (only the OBSERVED subset — see docs/explain-catalog.md).
const BADGE_FRESH: i32 = 1;
const BADGE_WITHIN_TOLERANCE: i32 = 2;
const BADGE_OUTDATED: i32 = 3;
const BADGE_NODE_QUERY_CHANGED: i32 = 4;
const BADGE_NODE_QUERY_UNCHANGED: i32 = 5;
// UPSTREAM_QUERY_CHANGED is emitted inside the OUTDATED-driven EXECUTE sub-shape
// (catalog SHAPE 9), which is anchor-dependent (ADR 0005 / ticket #30).
const BADGE_UPSTREAM_QUERY_CHANGED: i32 = 6;
const BADGE_UPSTREAM_QUERY_UNCHANGED: i32 = 7;
const BADGE_TARGET_TABLE_EXISTS: i32 = 8;

/// A snapshot of one persisted decision, everything the pure generator needs to
/// re-derive the explain tree. Read from a single `node_decisions` row (plus,
/// for #30, the matched `executions` anchor row). Keyed by `execution_decision_id`.
#[derive(Debug, Clone)]
pub struct ExplainNode {
    pub execution_decision_id: String,
    pub decision: DecisionKind,
    pub execution_type: i32,
    /// Persisted `decision_description` from the decision time. Used as the
    /// authoritative situation selector (the hosted service recorded exactly
    /// which coexisting SKIP reason applied); the tree is derived independently.
    pub decision_description: Option<String>,
    /// Persisted decision reason (ADR 0006): what the decision knew about why
    /// it reused, rebuilt or copied. Selects the tree shape together with the
    /// class and the anchor. `None` for a row written before the reason was
    /// persisted (or an unknown stored value): such a row renders no tree.
    pub reason: Option<DecisionReason>,
    pub is_stale: bool,
    pub target_table: Option<String>,
    pub clone_source: Option<String>,
    pub input_tables: Vec<InputTable>,
    /// The node's Lag tolerance (0: none) and the reuse candidate's build age
    /// at decision time (ADR 0007).
    pub tolerance_ms: i64,
    pub build_age_ms: Option<i64>,
    /// The decision's wire skip rejection code, when persisted.
    pub skip_rejection_reason: Option<i32>,
    /// A rebuild without a matched build: the latest build in scope, which
    /// did not have the workload's identity (migration 0017). The SHAPE 7
    /// freshness listing is judged against it. `None` otherwise, and for a
    /// rebuild recorded before the migration.
    pub unmatched: Option<ExecutionRow>,
    /// The matched prior `executions` row (freshness anchor). ADR 0005: present
    /// only when the SKIP/reuse decision matched a prior build; `None` for
    /// first-build EXECUTE / clone. The per-upstream freshness SUBTREE
    /// (FRESH/OUTDATED + «gap» + «rel_time») can only be re-derived when this is
    /// present — it lands with the `matched_execution_id` column (ticket #30).
    pub anchor: Option<ExecutionRow>,
}

impl ExplainNode {
    fn noun(&self) -> &'static str {
        match ModelExecutionType::try_from(self.execution_type) {
            Ok(ModelExecutionType::DbtDataTest) => "test",
            Ok(ModelExecutionType::Values) => "seed",
            // VERIFIED LIVE 2026-11-10 (explain_snapshot_history.jsonl).
            Ok(ModelExecutionType::Snapshot) => "snapshot",
            _ => "model",
        }
    }

    fn is_seed(&self) -> bool {
        self.execution_type == ModelExecutionType::Values as i32
    }

    /// A model whose decision-time inputs do not report its own target: the
    /// table does not exist.
    fn own_target_absent(&self) -> bool {
        match self.target_table.as_deref() {
            Some(target) if !target.is_empty() && !self.is_seed() && !self.is_data_test() => {
                !self.input_tables.iter().any(|t| t.name == target)
            }
            _ => false,
        }
    }

    fn is_data_test(&self) -> bool {
        self.execution_type == ModelExecutionType::DbtDataTest as i32
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
        decision: node.decision.wire_decision(),
        decision_description: decision_description(node),
        explain_lines: explain_lines(node),
    }
}

/// Select the `decision_description` by SITUATION (independent of the tree;
/// the strings are listed in docs/protocol.md). We trust the persisted
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
        DecisionKind::Skip if node.is_data_test() => {
            "data test was a no-op because both its query and its upstream data are up to date"
                .to_string()
        }
        DecisionKind::Skip => decision::DESC_SKIP.to_string(),
        DecisionKind::Build => decision::DESC_EXECUTE.to_string(),
        DecisionKind::Clone => {
            "an equivalent model exists under another name so we cloned that one".to_string()
        }
    }
}

/// Whether (execution_type, decision) is a CHARACTERIZED combination that the
/// hosted service was observed to render and that we therefore reproduce. This
/// is a strict positive allowlist; everything else returns an empty tree.
///
/// Characterized matrix (docs/explain-catalog.md "Execution type (node kind) coverage"):
///   - model/table (FULL=1): Skip | Build | Clone.
///   - view (VIEW=10): Skip | Build (Clone unreachable on 2.0.11, #76).
///   - data test (DBT_DATA_TEST=8): Skip | Build (Clone out of scope → #68).
///   - seed (VALUES=9): Skip | Build (Clone fenced until #91).
///   - snapshot (SNAPSHOT=7): Skip | Build (Clone unreachable on 2.0.11, #76).
///   - custom node (DBT_CUSTOM=11): Skip | Build (Clone unreachable on
///     2.0.11: hosted never copies one on submit, nor does this server, #86).
///   - incrementals (APPEND=2, MERGE=3, INSERT_OVERWRITE=4, DELETE_INSERT=5,
///     MICROBATCH=6): Skip | Build (Clone unreachable on 2.0.11: hosted copies
///     one only through RegisterClone, which has no Explain message, and this
///     server copies only FULL on submit, #101; DuckDB's checked delete+insert
///     copy is local-only).
///   - everything else (any other
///     execution_type): NONE.
///
/// Anything outside this set renders nothing (docs/explain-catalog.md "Not
/// rendered" gives each cell its status) and is explicitly Out-of-Scope for spec #27, whose
/// hard rule is "mark as unimplemented rather than invent ... never ship
/// invented output". Without this fence `explain_lines` dispatches on
/// `DecisionKind` alone and renders a model-shaped tree for seed/snapshot/
/// incremental decisions (e.g. a seed reload persists "seed was loaded because
/// its data changed" and would fall through to the SHAPE-7 "the model query has
/// changed" tree). PR #31 round-4 F7.
fn is_characterized(execution_type: i32, decision: DecisionKind) -> bool {
    match ModelExecutionType::try_from(execution_type) {
        Ok(ModelExecutionType::Full) => matches!(
            decision,
            DecisionKind::Skip | DecisionKind::Build | DecisionKind::Clone
        ),
        // VIEW CLONE is unreachable on 2.0.11 (#76, explain_copy_probes.jsonl
        // entry 13): hosted refuses a non-FULL copy on submit (1/3,
        // EXECUTION_TYPE_NOT_FULL) and the client sends RegisterClone only for
        // incrementals and snapshots.
        Ok(ModelExecutionType::View) => {
            matches!(decision, DecisionKind::Skip | DecisionKind::Build)
        }
        Ok(ModelExecutionType::DbtDataTest) => {
            // EXECUTE: VERIFIED LIVE 2026-11-10 (explain_data_test_history.jsonl,
            // client 2.0.11). Data-test CLONE is out of scope (→ #68).
            matches!(decision, DecisionKind::Skip | DecisionKind::Build)
        }
        // VERIFIED LIVE 2026-11-10 (explain_snapshot_history.jsonl, client
        // 2.0.11). Snapshot CLONE is unreachable on 2.0.11 (#76): hosted
        // refuses a submit copy as for views, and a RegisterClone copy mints
        // no execution_decision_id, so it has no Explain message.
        Ok(ModelExecutionType::Snapshot) => {
            matches!(decision, DecisionKind::Skip | DecisionKind::Build)
        }
        // VERIFIED LIVE 2026-11-10 (lag_seed_upstream.jsonl). Hosted copies
        // seeds on submit; VALUES CLONE stays fenced until #91.
        Ok(ModelExecutionType::Values) => {
            matches!(decision, DecisionKind::Skip | DecisionKind::Build)
        }
        // VERIFIED LIVE 2026-11-10 (explain_custom_history.jsonl, client
        // 2.0.11). CLONE is unreachable on 2.0.11: hosted never copies a
        // custom node (explain_custom_copy.jsonl, #86), and this server copies
        // only FULL on submit.
        Ok(ModelExecutionType::DbtCustom) => {
            matches!(decision, DecisionKind::Skip | DecisionKind::Build)
        }
        // VERIFIED LIVE 2026-11-10, client 2.0.11: every incremental strategy
        // renders the MODEL trees (explain_incremental_append_merge_history.jsonl,
        // explain_incremental_insert_overwrite.jsonl,
        // explain_incremental_delete_insert.jsonl,
        // explain_incremental_microbatch.jsonl). CLONE is unreachable on
        // 2.0.11: hosted rebuilds an incremental on submit, into an absent dev
        // relation (explain_incremental_submit_copy.jsonl) and over an
        // existing one (explain_incremental_copy_over.jsonl, #101), and its
        // RegisterClone copy mints no execution_decision_id. This server
        // copies only FULL on submit; DuckDB's checked delete+insert copy is
        // local-only and stays fenced.
        Ok(
            ModelExecutionType::Append
            | ModelExecutionType::Merge
            | ModelExecutionType::InsertOverwrite
            | ModelExecutionType::DeleteInsert
            | ModelExecutionType::Microbatch,
        ) => matches!(decision, DecisionKind::Skip | DecisionKind::Build),
        _ => false,
    }
}

/// The catalogued tree shapes (docs/explain-catalog.md) Explain can render.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shape {
    /// SKIP (catalog SHAPEs 0/1/2/4, by node kind and upstreams).
    Skip,
    /// First build: `table analysis` only.
    FirstBuild,
    /// Node-query-changed rebuild (catalog SHAPE 7).
    QueryChanged,
    /// SHAPE 7 over a RegisterClone copy the submit could not adopt
    /// ([`copy_logic_changed_tree`]).
    CopyLogicChanged,
    /// Upstream-drift rebuild (catalog SHAPE 9), rendered against the anchor.
    UpstreamOutdated,
    /// The SKIP-shaped rebuild against the anchor: own target modified after
    /// the build (VERIFIED LIVE 2026-11-10, lag_tolerance_matrix `lfs_u0`,
    /// lag_copy_candidates `ome`), or an execution type change (#83).
    OwnTargetModified,
    /// A seed whose table exists: its data changed (`true`) or not.
    Seed { changed: bool },
    /// A copy of an equivalent over a target whose own build had to rebuild
    /// ("but it's old, so we will clone from …"), rendered against the
    /// target's own build (VERIFIED LIVE 2026-11-10, lag_copy_candidates).
    CopyOver,
    /// CLONE (`vs clone` sections).
    Clone,
    /// A build whose copy of an equivalent the submit disallowed
    /// (`allow_clones: false`): `table analysis` names the equivalent
    /// (VERIFIED LIVE 2026-11-11, explain_copy_logic_mismatch.jsonl `cf4`).
    CopyDisabled,
    /// A data test that executes: its tested upstreams, without verdicts
    /// (VERIFIED LIVE 2026-11-10, explain_data_test_history.jsonl).
    DataTestExecute,
}

/// Select the tree shape from the persisted decision reason, the class and
/// the anchor (ADR 0006). No description wording is consulted. `None` when
/// there is nothing to select from: a NULL reason (a row written before the
/// reason was persisted) or a reason that contradicts the class (no write
/// produces one).
fn shape(node: &ExplainNode) -> Option<Shape> {
    use DecisionReason as R;
    let reason = node.reason?;
    if node.is_seed() {
        // VERIFIED LIVE (lag_seed_upstream.jsonl): a seed's tree is its table
        // and its data. A rebuild whose older build still matched reports
        // LATEST_QUERY_HASH_NOT_MATCH and "the seed data has not changed".
        return Some(match (node.decision, reason) {
            (DecisionKind::Skip, R::Reused(Reuse::Fresh)) => Shape::Seed { changed: false },
            (
                DecisionKind::Build,
                R::FirstBuild | R::NoReuseCandidate(NoCandidate::OwnTargetMissing),
            ) => Shape::FirstBuild,
            (DecisionKind::Build, R::NoReuseCandidate(NoCandidate::IdentityMismatch)) => {
                Shape::Seed {
                    changed: node.skip_rejection_reason
                        != Some(decision::REJECTION_LATEST_QUERY_HASH_NOT_MATCH),
                }
            }
            _ => return None,
        });
    }
    if node.is_data_test() {
        // VERIFIED LIVE 2026-11-10 and 2026-11-11 (explain_data_test_history.jsonl,
        // explain_rebuild_reason_gaps.jsonl, client 2.0.11): one EXECUTE tree
        // for a first execution, a logic change, upstream data drift and an
        // upstream whose logic changed, whether the test passed or failed; a
        // test without upstreams says `no upstream dependencies` instead of
        // listing them. Hosted SKIPs a test whose cached results are
        // incomplete (#87), so our rebuild for that reason renders nothing,
        // as does every other data-test rebuild reason, including a
        // dependency-set change, which no capture shows (a store_failures
        // test's own audit relation is not an input, #88, unless the request
        // does not identify it, #106;
        // docs/explain-catalog.md "Decision reason coverage"). A data test's
        // Lag tolerance is 0 and it rebuilds on any drift, so its only reuse
        // is a fresh one.
        return Some(match (node.decision, reason) {
            (DecisionKind::Skip, R::Reused(Reuse::Fresh)) => Shape::Skip,
            (
                DecisionKind::Build,
                R::FirstBuild | R::NoReuseCandidate(NoCandidate::IdentityMismatch),
            ) => Shape::DataTestExecute,
            (DecisionKind::Build, R::UpstreamOutdated | R::UpstreamQueryChanged)
                if !node.genuine_upstreams().is_empty() =>
            {
                Shape::DataTestExecute
            }
            _ => return None,
        });
    }
    // Every arm below renders only for the node kinds (and upstream
    // situations) a tracked hosted capture backs; any other Decision reason ×
    // node kind renders nothing (docs/explain-catalog.md "Decision reason
    // coverage", which gives each such cell its final status).
    let kind = ModelExecutionType::try_from(node.execution_type).ok();
    let full = kind == Some(ModelExecutionType::Full);
    let upstreams = !node.genuine_upstreams().is_empty();
    let listing = freshness_listing_captured(kind, upstreams);
    let incremental = matches!(
        kind,
        Some(
            ModelExecutionType::Append
                | ModelExecutionType::Merge
                | ModelExecutionType::InsertOverwrite
                | ModelExecutionType::DeleteInsert
                | ModelExecutionType::Microbatch
        )
    );
    Some(match (node.decision, reason) {
        (DecisionKind::Skip, R::Reused(Reuse::Fresh | Reuse::WithinLagTolerance)) if listing => {
            Shape::Skip
        }
        // An ALL-policy reuse with tolerated drift: captured for FULL
        // (lag_tolerance_matrix.jsonl, crafted) and, with an upstream, for
        // the five incrementals and DBT_CUSTOM (explain_freshness_cells.jsonl
        // `fa_*`). A snapshot cannot be sent the ALL policy on 2.0.11 (the
        // client reads `state` from models only), so it has no capture.
        (DecisionKind::Skip, R::Reused(Reuse::ToleratedStale))
            if listing && kind != Some(ModelExecutionType::Snapshot) =>
        {
            Shape::Skip
        }
        (
            DecisionKind::Build,
            R::FirstBuild | R::NoReuseCandidate(NoCandidate::OwnTargetMissing),
        ) => Shape::FirstBuild,
        // SHAPE 9 is anchor-relative: it renders only when the matched build
        // re-derives at least one upstream that provably drifted (data-only:
        // FULL, the five incrementals, SNAPSHOT, DBT_CUSTOM;
        // explain_freshness_cells.jsonl `fz_*`, `fd_sn`) or an Upstream
        // producer change (FULL, SNAPSHOT, DBT_CUSTOM, and the five
        // incremental strategies: explain_reason_kind_cells.jsonl `*_q`). A
        // rebuild for an unknown upstream epoch is local-only: hosted reuses
        // it (explain_unknown_epoch.jsonl, crafted).
        (DecisionKind::Build, R::UpstreamOutdated)
            if has_provably_outdated_upstream(node)
                // The equivalent's age words the Lag tolerance line; an
                // unjudged equivalent (another environment) has none.
                && !(reports_equivalent(node) && node.build_age_ms.is_none())
                && matches!(
                    kind,
                    Some(
                        ModelExecutionType::Full
                            | ModelExecutionType::Append
                            | ModelExecutionType::Merge
                            | ModelExecutionType::InsertOverwrite
                            | ModelExecutionType::DeleteInsert
                            | ModelExecutionType::Microbatch
                            | ModelExecutionType::Snapshot
                            | ModelExecutionType::DbtCustom
                    )
                ) =>
        {
            Shape::UpstreamOutdated
        }
        (DecisionKind::Build, R::UpstreamQueryChanged)
            if node.anchor.is_some()
                && (incremental
                    || matches!(
                        kind,
                        Some(
                            ModelExecutionType::Full
                                | ModelExecutionType::Snapshot
                                | ModelExecutionType::DbtCustom
                        )
                    )) =>
        {
            Shape::UpstreamOutdated
        }
        // Captured for FULL (lag_copy_candidates `ome`, lag_tolerance_matrix
        // `lfs_u0`), for the five incremental strategies with and without an
        // upstream after an External modification or a later FULL full
        // refresh (explain_reason_kind_cells.jsonl `*_e`, `*_ez`, `*_c`,
        // `*_cz`; explain_incremental_append_merge_history.jsonl entries
        // 78-79). Hosted never makes it for a SNAPSHOT or a DBT_CUSTOM node:
        // it reuses them over a modified Target relation (#115), so ours
        // renders nothing. A VIEW's (`vc0`) needs to know whether the view
        // reads a relation (#117).
        (DecisionKind::Build, R::NoReuseCandidate(NoCandidate::OwnTargetModified))
            if (full && (node.anchor.is_some() || !upstreams))
                || (incremental && node.anchor.is_some()) =>
        {
            Shape::OwnTargetModified
        }
        // An execution type change, explained against the latest row: the
        // same SKIP-shaped tree (explain_snapshot_history.jsonl entry 60,
        // explain_execution_type_change.jsonl `tv`, explain_reason_kind_cells.jsonl
        // `*_e`, `*_ez`, `sn_c`, `sn_cz`, `vc0`): FULL, the incrementals and
        // SNAPSHOT with and without an upstream, DBT_CUSTOM without one. Only
        // over a latest row with the node's genuine upstream names: hosted
        // treats a VIEW <-> DBT_CUSTOM switch over an upstream (the view
        // reports none) as another identity (#116). A VIEW's (`cv0`) needs
        // to know whether the view reads a relation (#117).
        (DecisionKind::Build, R::NoReuseCandidate(NoCandidate::ExecutionTypeChanged))
            if node
                .anchor
                .as_ref()
                .is_some_and(|a| same_upstream_names(node, a))
                && (full
                    || incremental
                    || kind == Some(ModelExecutionType::Snapshot)
                    || (kind == Some(ModelExecutionType::DbtCustom) && !upstreams)) =>
        {
            Shape::OwnTargetModified
        }
        // VERIFIED LIVE (2026-11-10, lag_copy_candidates `dt_m`; 2026-11-11,
        // explain_reason_kind_cells.jsonl `*_d`, every model kind): a model
        // whose logic changed while its Target relation was dropped reads
        // like a first build.
        (DecisionKind::Build, R::NoReuseCandidate(NoCandidate::IdentityMismatch))
            if node.own_target_absent() =>
        {
            Shape::FirstBuild
        }
        // SHAPE 7, captured for every model kind (explain_snapshot_history,
        // explain_custom_history, explain_incremental_*, explain_rebuild_reason_gaps),
        // also with the Lag tolerance line of a stale older match under drift
        // (explain_freshness_cells.jsonl `fr_*`, every kind but VIEW, whose
        // tolerance is 0). Without the older build's recorded age that line
        // cannot be worded, so the tree renders nothing.
        (DecisionKind::Build, R::NoReuseCandidate(NoCandidate::IdentityMismatch))
            if listing && !query_changed_lacks_build_age(node) =>
        {
            Shape::QueryChanged
        }
        // Over a copy whose Target relation is gone, a first build, as for an
        // identity mismatch: captured for FULL and APPEND
        // (explain_copy_logic_crafted.jsonl `x11`, `x12`).
        (DecisionKind::Build, R::NoReuseCandidate(NoCandidate::CopyLogicMismatch))
            if node.own_target_absent() =>
        {
            if !matches!(
                kind,
                Some(ModelExecutionType::Full | ModelExecutionType::Append)
            ) {
                return None;
            }
            Shape::FirstBuild
        }
        // SHAPE 7 over a RegisterClone copy whose logic differs from its
        // source build's, even read in the copy source's schema, judged
        // against the copy: captured for every model kind that can follow a
        // copy, with and without upstreams, except a view with one, which
        // client 2.0.11 never reports (explain_copy_logic_mismatch.jsonl,
        // explain_copy_logic_crafted.jsonl). A stale one whose summary would
        // carry a Lag tolerance line is not captured (unlike an identity
        // mismatch, `fr_*`), so it renders nothing, even with a recorded age.
        (DecisionKind::Build, R::NoReuseCandidate(NoCandidate::CopyLogicMismatch))
            if copy_logic_mismatch_captured(kind, !node.genuine_upstreams().is_empty())
                && !node.own_target_absent()
                && !query_changed_needs_tolerance_line(node) =>
        {
            Shape::CopyLogicChanged
        }
        (DecisionKind::Clone, R::ResultCopy)
            if node.anchor.is_some() && !node.own_target_absent() =>
        {
            Shape::CopyOver
        }
        (DecisionKind::Clone, R::ResultCopy) => Shape::Clone,
        // Captured for FULL only, the one kind a submit copies (#101).
        (DecisionKind::Build, R::CopyDisabled)
            if full && node.own_target_absent() && node.clone_source.is_some() =>
        {
            Shape::CopyDisabled
        }
        // Everything else renders nothing:
        // - LOCAL-ONLY (crafted probes d1–d4, explain_ambiguous_rebuilds.jsonl):
        //   `own_target_changed_or_missing` (#80), `no_build_anchor` (#82),
        //   and `own_target_modified` with upstreams but no matched build (the
        //   anchor's execution row disappeared), rebuilds hosted never makes;
        //   likewise `upstream_outdated` / `upstream_query_changed` without
        //   an anchor;
        // - and every other reason × kind, which no tracked capture backs.
        _ => return None,
    })
}

/// Whether a tracked capture backs the freshness listing of `kind` with
/// (`upstreams`) or without genuine upstreams. A FULL model, a custom node,
/// the incrementals and snapshots were captured both ways (without upstreams:
/// explain_reason_kind_cells.jsonl `*_z`, `*_cz`, `*_ez`); a view only
/// without (client 2.0.11 reports none for a view,
/// explain_snapshot_history.jsonl `s72vl`).
fn freshness_listing_captured(kind: Option<ModelExecutionType>, upstreams: bool) -> bool {
    use ModelExecutionType as K;
    match kind {
        Some(
            K::Full
            | K::DbtCustom
            | K::Append
            | K::Merge
            | K::InsertOverwrite
            | K::DeleteInsert
            | K::Microbatch
            | K::Snapshot,
        ) => true,
        Some(K::View) => !upstreams,
        _ => false,
    }
}

/// Whether `row` recorded the node's genuine upstream names (its own target
/// excluded).
fn same_upstream_names(node: &ExplainNode, row: &ExecutionRow) -> bool {
    let target = node.target_table.as_deref();
    let names = |tables: &[InputTable]| -> std::collections::BTreeSet<String> {
        tables
            .iter()
            .map(|t| t.name.clone())
            .filter(|n| Some(n.as_str()) != target)
            .collect()
    };
    names(&node.input_tables) == names(&row.input_tables)
}

/// Build the structured tree: fenced by node kind, then shaped by the
/// persisted decision reason.
pub fn explain_lines(node: &ExplainNode) -> Vec<qc::ExplainLine> {
    // F7 fence (PR #31 round 4): never render an invented tree for an
    // uncharacterized node kind (this also fences data-test EXECUTE/CLONE).
    if !is_characterized(node.execution_type, node.decision) {
        return Vec::new();
    }
    match shape(node) {
        None => Vec::new(),
        Some(Shape::Skip) => skip_tree(node),
        Some(Shape::FirstBuild) => vec![first_build_table_analysis(node)],
        Some(Shape::QueryChanged) => query_changed_tree(node),
        Some(Shape::CopyLogicChanged) => copy_logic_changed_tree(node),
        Some(Shape::UpstreamOutdated) => execute_outdated_tree(node),
        Some(Shape::OwnTargetModified) => skip_tree(node),
        Some(Shape::Seed { changed }) => seed_tree(node, changed),
        Some(Shape::CopyOver) => copy_over_tree(node),
        Some(Shape::Clone) => clone_tree(node),
        Some(Shape::CopyDisabled) => vec![copy_disabled_table_analysis(node)],
        Some(Shape::DataTestExecute) => vec![data_test_execute_freshness(node)],
    }
}

/// A data test that executes: its tested upstreams from the decision-time
/// inputs, without verdicts, or `no upstream dependencies` for a test that
/// reads none (VERIFIED LIVE 2026-11-11, explain_rebuild_reason_gaps.jsonl).
fn data_test_execute_freshness(node: &ExplainNode) -> qc::ExplainLine {
    let upstreams = node.genuine_upstreams();
    let body = if upstreams.is_empty() {
        leaf("no upstream dependencies", Some(MARKER_SUCCESS), None)
    } else {
        clone_upstream_dependencies(&upstreams)
    };
    section("data freshness analysis", vec![body])
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
/// present. VERIFIED (catalog): a node with zero genuine upstreams omits it
/// (SHAPE 0) unless it is a VIEW; a VIEW or any node with ≥1 genuine upstream
/// carries it (SHAPEs 1/2/4…).
fn has_upstream_query_line(node: &ExplainNode) -> bool {
    !node.genuine_upstreams().is_empty() || !says_no_upstream_dependencies(node)
}

/// The node kinds whose tree says `no upstream dependencies` (SHAPE 0) when
/// they have no genuine upstreams: a FULL model, a custom node (VERIFIED LIVE
/// 2026-11-10, explain_custom_history.jsonl `c74c0`), a data test (VERIFIED
/// LIVE 2026-11-11, explain_rebuild_reason_gaps.jsonl `r40t0`), the five
/// incremental strategies and a snapshot (VERIFIED LIVE 2026-11-11,
/// explain_reason_kind_cells.jsonl `*_z`, `*_cz`, `*_ez`). A view over a
/// `ref` (which client 2.0.11 does not report) says `upstream data is up to
/// date` instead (explain_snapshot_history.jsonl `s72vl`); a view that reads
/// no relation says `no upstream dependencies` (`vc0`, `cv0`), which this
/// server cannot tell apart, so those VIEW cells render nothing (#117).
fn says_no_upstream_dependencies(node: &ExplainNode) -> bool {
    use ModelExecutionType as K;
    matches!(
        K::try_from(node.execution_type),
        Ok(K::Full
            | K::DbtCustom
            | K::DbtDataTest
            | K::Append
            | K::Merge
            | K::InsertOverwrite
            | K::DeleteInsert
            | K::Microbatch
            | K::Snapshot)
    )
}

/// `data freshness analysis` body for a SKIP, by upstream situation.
fn data_freshness_analysis_fresh(node: &ExplainNode) -> qc::ExplainLine {
    let upstreams = node.genuine_upstreams();
    if upstreams.is_empty() {
        // No genuine upstreams. A FULL model, custom node or data test renders
        // `no upstream dependencies`; a VIEW (which selects from an untracked
        // source) renders `upstream data is up to date` (catalog SHAPE 0 vs 1).
        let body = if says_no_upstream_dependencies(node) {
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
    let mut children = vec![upstream_dependencies_freshness(node, &upstreams)];
    children.extend(freshness_summary(node));
    section("data freshness analysis", children)
}

/// Whether the anchor is a client-requested copy (RegisterClone) whose source
/// build recorded at least one input (`clone_source_inputs`, migration 0018).
fn copy_source_read_upstreams(node: &ExplainNode) -> bool {
    node.anchor.as_ref().is_some_and(|a| {
        a.clone_source_table.is_some()
            && a.node_sql_hash.is_none()
            && a.clone_source_inputs
                .as_ref()
                .is_some_and(|i| !i.is_empty())
    })
}

/// Every genuine upstream's current epoch equals the epoch recorded for it at
/// the anchored build. Without an anchor nothing proves it.
fn upstreams_unchanged(node: &ExplainNode) -> bool {
    node.anchor
        .as_ref()
        .is_some_and(|prev| decision::upstream_freshness(freshness_inputs(node), prev).unchanged())
}

// ── SEED ────────────────────────────────────────────────────────────────────

/// A seed whose table exists: `the seed file exists already`, then whether
/// its data changed (VERIFIED LIVE 2026-11-10, lag_seed_upstream.jsonl). No
/// freshness section: a seed has no upstreams.
fn seed_tree(node: &ExplainNode, changed: bool) -> Vec<qc::ExplainLine> {
    let data = if changed {
        leaf("the seed data has changed", Some(MARKER_FAIL), None)
    } else {
        leaf(
            "the seed data has not changed",
            Some(MARKER_SUCCESS),
            Some(BADGE_NODE_QUERY_UNCHANGED),
        )
    };
    vec![
        section(
            format!("table analysis ({})", fqn(node)),
            vec![leaf(
                "the seed file exists already",
                Some(MARKER_SUCCESS),
                Some(BADGE_TARGET_TABLE_EXISTS),
            )],
        ),
        section("query analysis", vec![data]),
    ]
}

// ── COPY OVER AN EXISTING TARGET ────────────────────────────────────────────

/// The target's own analysis against its own build (query listing and
/// per-upstream freshness), with `(vs clone)` sections, the "but it's old"
/// table line, and a summary saying how fresh the copied equivalent is: up to
/// date, or outdated with its build age against the tolerance (VERIFIED LIVE
/// 2026-11-10, lag_copy_candidates `fe`, `st`, `wba`).
fn copy_over_tree(node: &ExplainNode) -> Vec<qc::ExplainLine> {
    let source = node.clone_source.clone().unwrap_or_default();
    let mut out = vec![section(
        format!("table analysis ({})", fqn(node)),
        vec![
            leaf(
                format!("the {} table exists already", node.noun()),
                Some(MARKER_SUCCESS),
                Some(BADGE_TARGET_TABLE_EXISTS),
            ),
            leaf(
                format!(
                    "but it's old, so we will clone from {} to recreate it",
                    quote(&source)
                ),
                Some(MARKER_FAIL),
                None,
            ),
        ],
    )];
    let upstreams = node.genuine_upstreams();
    let mut query = query_analysis_vs_build(node, &upstreams);
    query.text = "query analysis (vs clone)".into();
    out.push(query);
    let mut freshness = Vec::new();
    if !upstreams.is_empty() {
        freshness.push(upstream_dependencies_freshness(node, &upstreams));
    }
    freshness.extend(copied_freshness_summary(node));
    out.push(section("data freshness analysis (vs clone)", freshness));
    out
}

/// `the <noun> was last built «age» ago, within|exceeds lag tolerance of «T»`
/// (badge WITHIN_TOLERANCE + SUCCESS, or OUTDATED + FAIL), from the build age
/// and tolerance persisted with the decision. Durations are worded like the
/// hosted service's (`humanize_duration`).
fn lag_tolerance_line(node: &ExplainNode) -> qc::ExplainLine {
    let age = node.build_age_ms.unwrap_or(i64::MAX);
    let within = age < node.tolerance_ms;
    let text = format!(
        "the {} was last built {} ago, {} lag tolerance of {}",
        node.noun(),
        humanize_duration(age),
        if within { "within" } else { "exceeds" },
        humanize_duration(node.tolerance_ms)
    );
    if within {
        leaf(text, Some(MARKER_SUCCESS), Some(BADGE_WITHIN_TOLERANCE))
    } else {
        leaf(text, Some(MARKER_FAIL), Some(BADGE_OUTDATED))
    }
}

/// A duration worded like the hosted service's Explain ("a moment", "a
/// second", "7 seconds", "a minute", "10 minutes", "an hour", "3 hours", "a
/// day", "10 days", "a month", "2 years"; whole units, rounded down; negative
/// reads as zero). Observed live: "7 seconds", "25 seconds", "a minute",
/// "10 minutes", "20 seconds", "a moment".
pub(crate) fn humanize_duration(ms: i64) -> String {
    let seconds = ms.max(0) / 1000;
    let days = seconds / 86_400;
    let plural = |n: i64, one: &str, unit: &str| {
        if n == 1 {
            one.to_string()
        } else {
            format!("{n} {unit}")
        }
    };
    match (days, seconds) {
        (0, 0) => "a moment".to_string(),
        (0, s) if s < 60 => plural(s, "a second", "seconds"),
        (0, s) if s < 3_600 => plural(s / 60, "a minute", "minutes"),
        (0, s) => plural(s / 3_600, "an hour", "hours"),
        (d, _) if d < 31 => plural(d, "a day", "days"),
        (d, _) if d < 365 => plural((d as f64 / 30.5).round() as i64, "a month", "months"),
        (d, _) => plural(d / 365, "a year", "years"),
    }
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
            if verdict_unrecorded_by_copy_source(node, up) {
                return clone_upstream_line(up);
            }
            let verdict = verdicts
                .as_ref()
                .and_then(|v| v.iter().find(|u| u.name == up.name));
            // VERIFIED LIVE 2026-11-11 (explain_execution_type_change.jsonl
            // `tv`): an upstream the judged build did not record (a view
            // reports none) has no verdict: no badge, no anchor line.
            if verdict.is_none() && !anchor_records(node, &up.name) {
                return qc::ExplainLine {
                    text: quote(&up.name),
                    marker: None,
                    badge: None,
                    children: vec![line(rel_time_line(up))],
                };
            }
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

/// A SKIP that reused a client-requested copy (RegisterClone) has no verdict
/// for an upstream the copy's source build did not record: hosted lists it
/// with its `last updated` line only, while an upstream the source recorded
/// is FRESH (VERIFIED LIVE 2026-11-11, explain_post_copy_identity.jsonl `c93st`
/// vs `c93sa`/`c93sv`, #93). Unknown for a copy registered before its
/// source's inputs were kept, which renders as before.
fn verdict_unrecorded_by_copy_source(node: &ExplainNode, up: &InputTable) -> bool {
    let Some(anchor) = node.anchor.as_ref() else {
        return false;
    };
    let Some(recorded) = anchor.clone_source_inputs.as_ref() else {
        return false;
    };
    node.decision == DecisionKind::Skip
        && anchor.clone_source_table.is_some()
        && anchor.node_sql_hash.is_none()
        && !recorded.iter().any(|name| {
            decision::physical_relation_key(name) == decision::physical_relation_key(&up.name)
        })
}

/// Whether the node's freshness anchor is absent or recorded `name` as an
/// input. Without an anchor the listing keeps its structural default.
fn anchor_records(node: &ExplainNode, name: &str) -> bool {
    node.anchor
        .as_ref()
        .is_none_or(|prev| prev.input_tables.iter().any(|t| t.name == name))
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
/// renders the structural listing with the FRESH default badge. Uses only
/// persisted evidence: the node decision's kind, target and decision-time
/// inputs, against the anchor.
fn anchored_verdicts(node: &ExplainNode) -> Option<Vec<decision::UpstreamVerdict>> {
    let prev = node.anchor.as_ref()?;
    Some(decision::upstream_freshness(freshness_inputs(node), prev).upstreams)
}

/// The persisted decision-time evidence per-upstream freshness is judged on.
fn freshness_inputs(node: &ExplainNode) -> decision::FreshnessInputs<'_> {
    decision::FreshnessInputs {
        execution_type: node.execution_type,
        input_tables: &node.input_tables,
        target_table: node.target_table.as_deref(),
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

/// The node-query-changed rebuild tree (catalog SHAPE 7): table exists, query
/// CHANGED, upstream queries unchanged, then the SKIP tree's freshness body
/// judged against the latest build in scope, whose identity did not match
/// (VERIFIED LIVE: a view without reported upstreams says `upstream data is up
/// to date`, explain_snapshot_history.jsonl; an upstream is FRESH or OUTDATED
/// against that build, with the matching summary, explain_incremental_*.jsonl
/// and explain_rebuild_reason_gaps.jsonl `r40q`). Rendered only for
/// `no_reuse_candidate.identity_mismatch` (see [`shape`]).
fn query_changed_tree(node: &ExplainNode) -> Vec<qc::ExplainLine> {
    let judged = judged_against_unmatched(node);
    vec![
        table_analysis_exists(node),
        query_analysis_changed(node),
        data_freshness_analysis_fresh(&judged),
    ]
}

/// Whether a capture backs the SHAPE 7 tree over a RegisterClone copy for
/// `kind` with (`upstreams`) or without genuine upstreams.
fn copy_logic_mismatch_captured(kind: Option<ModelExecutionType>, upstreams: bool) -> bool {
    use ModelExecutionType as K;
    match kind {
        Some(K::View) => !upstreams,
        Some(
            K::Full
            | K::Append
            | K::Merge
            | K::InsertOverwrite
            | K::DeleteInsert
            | K::Microbatch
            | K::Snapshot
            | K::DbtCustom,
        ) => true,
        _ => false,
    }
}

/// SHAPE 7 judged against a RegisterClone copy (VERIFIED LIVE 2026-11-11,
/// explain_copy_logic_mismatch.jsonl, explain_copy_logic_crafted.jsonl). With
/// genuine upstreams it is the ordinary SHAPE 7 listing. Without any, the
/// upstreams the copy's source build recorded decide, whatever the node kind:
/// with none, `no upstream dependencies` and no `upstream model queries`
/// line (crafted `x2`–`x10`); with some (a view over an incremental's copy,
/// `kv4`), the `upstream model queries have not changed` line and an empty
/// `data freshness analysis` section.
fn copy_logic_changed_tree(node: &ExplainNode) -> Vec<qc::ExplainLine> {
    if !node.genuine_upstreams().is_empty() {
        return query_changed_tree(node);
    }
    let source_read = copy_source_read_upstreams(&judged_against_unmatched(node));
    let mut query = vec![leaf(
        format!("the {} query has changed", node.noun()),
        Some(MARKER_FAIL),
        Some(BADGE_NODE_QUERY_CHANGED),
    )];
    let mut freshness = Vec::new();
    if source_read {
        query.push(leaf(
            "upstream model queries have not changed",
            Some(MARKER_SUCCESS),
            None,
        ));
    } else {
        freshness.push(leaf("no upstream dependencies", Some(MARKER_SUCCESS), None));
    }
    vec![
        table_analysis_exists(node),
        section("query analysis", query),
        section("data freshness analysis", freshness),
    ]
}

/// The node with its freshness anchor replaced by the unmatched latest build,
/// which SHAPE 7 judges its listing against.
fn judged_against_unmatched(node: &ExplainNode) -> ExplainNode {
    ExplainNode {
        anchor: node.unmatched.clone(),
        ..node.clone()
    }
}

/// Whether SHAPE 7's summary would carry a Lag tolerance line: the rebuild
/// is stale (an older matching build would have been reused stale,
/// `submit_decision::older_match`), the tolerance is non-zero and an upstream
/// drifted against the unmatched build. Captured for an identity mismatch
/// (explain_freshness_cells.jsonl `fr_*`), not over a RegisterClone copy.
fn query_changed_needs_tolerance_line(node: &ExplainNode) -> bool {
    node.is_stale && node.tolerance_ms > 0 && has_outdated_upstream(&judged_against_unmatched(node))
}

/// Whether SHAPE 7's summary needs a Lag tolerance line it cannot word: it
/// needs one ([`query_changed_needs_tolerance_line`]), but the decision
/// recorded no build age for that older build (a row written before #104).
fn query_changed_lacks_build_age(node: &ExplainNode) -> bool {
    query_changed_needs_tolerance_line(node) && node.build_age_ms.is_none()
}

/// Whether the anchored decision has an upstream whose drift is proven: its
/// current and recorded epochs are both known and the current one is later.
/// An unknown epoch is OUTDATED for the decision (it never proves freshness)
/// but proves no drift either.
fn has_provably_outdated_upstream(node: &ExplainNode) -> bool {
    anchored_verdicts(node)
        .map(|v| v.iter().any(|u| u.drifted && !u.unprovable))
        .unwrap_or(false)
}

/// A data-only rebuild that reports an equivalent under another name it
/// did not copy (TARGET_TABLE_MISMATCH, `with_refused_copy`): its SHAPE 9
/// summary carries the Lag tolerance line of the equivalent's build age
/// (VERIFIED LIVE 2026-11-11, explain_freshness_cells.jsonl `fd_sn`, "exceeds";
/// crafted, explain_rebuild_tolerance_line.jsonl `s2`, `i2`, "within").
/// Without an equivalent there is no line (`s1`, `i1`, `f0`).
fn reports_equivalent(node: &ExplainNode) -> bool {
    node.decision == DecisionKind::Build
        && node.reason == Some(DecisionReason::UpstreamOutdated)
        && node.skip_rejection_reason == Some(decision::REJECTION_TARGET_TABLE_MISMATCH)
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
    let mut out = vec![table_analysis_exists(node)];
    out.push(query_analysis_vs_build(node, &upstreams));
    let mut freshness = vec![upstream_dependencies_freshness(node, &upstreams)];
    freshness.extend(freshness_summary(node));
    out.push(section("data freshness analysis", freshness));
    out
}

/// The summary lines under a node's per-upstream freshness listing, judged
/// against its own build (VERIFIED LIVE 2026-11-10): outdated when an
/// upstream drifted, with the tolerance line when the decision knowingly
/// serves (or, for a rebuild, would have served) stale data; up to date when
/// no upstream epoch moved; nothing when one only moved back.
fn freshness_summary(node: &ExplainNode) -> Vec<qc::ExplainLine> {
    if has_outdated_upstream(node) {
        let mut lines = vec![leaf("upstream data is outdated", Some(MARKER_FAIL), None)];
        let reused = node.decision == DecisionKind::Skip;
        if node.tolerance_ms > 0 && (reused || node.is_stale || reports_equivalent(node)) {
            lines.push(lag_tolerance_line(node));
        }
        lines
    } else if upstreams_unchanged(node) {
        vec![leaf(
            "upstream data is up to date",
            Some(MARKER_SUCCESS),
            None,
        )]
    } else {
        Vec::new()
    }
}

/// `query analysis` for a node judged against its own build: the node query
/// unchanged, then either every upstream's query verdict and the FAIL
/// summary (an upstream's producing build changed, ADR 0007), or the SUCCESS
/// summary alone (VERIFIED LIVE 2026-11-10: a data-only rebuild lists no
/// upstream queries).
fn query_analysis_vs_build(node: &ExplainNode, upstreams: &[&InputTable]) -> qc::ExplainLine {
    let verdicts = anchored_verdicts(node);
    let changed = |up: &InputTable| {
        verdicts
            .as_ref()
            .and_then(|v| v.iter().find(|u| u.name == up.name))
            .is_some_and(|u| u.producer_changed)
    };
    // query analysis: node-query unchanged, then either every upstream's
    // query verdict and the FAIL summary (an upstream's producing build
    // changed, ADR 0007), or the SUCCESS summary alone (VERIFIED LIVE
    // 2026-11-10: a data-only rebuild lists no upstream queries).
    let mut query_children = vec![leaf(
        format!("the {} query has not changed", node.noun()),
        Some(MARKER_SUCCESS),
        Some(BADGE_NODE_QUERY_UNCHANGED),
    )];
    if upstreams.iter().any(|up| changed(up)) {
        let query_listing: Vec<qc::ExplainLine> = upstreams
            .iter()
            .map(|up| {
                let (text, badge) = if changed(up) {
                    (
                        "query or upstream query changed",
                        BADGE_UPSTREAM_QUERY_CHANGED,
                    )
                } else {
                    (
                        "query or upstream query unchanged",
                        BADGE_UPSTREAM_QUERY_UNCHANGED,
                    )
                };
                qc::ExplainLine {
                    text: quote(&up.name),
                    marker: None,
                    badge: None,
                    children: vec![leaf(text, None, Some(badge))],
                }
            })
            .collect();
        query_children.push(section("upstream dependencies", query_listing));
        query_children.push(leaf(
            "an upstream model query has changed",
            Some(MARKER_FAIL),
            None,
        ));
    } else {
        query_children.push(leaf(
            "upstream model queries have not changed",
            Some(MARKER_SUCCESS),
            None,
        ));
    }
    section("query analysis", query_children)
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

/// `table analysis` for a build whose copy the submit disallowed: the table
/// does not exist, and the equivalent it could have been copied from
/// (VERIFIED LIVE 2026-11-11, explain_copy_logic_mismatch.jsonl `cf4`).
fn copy_disabled_table_analysis(node: &ExplainNode) -> qc::ExplainLine {
    let source = node.clone_source.clone().unwrap_or_default();
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
                    "the {} table could have been cloned from {}, but cloning is disabled",
                    node.noun(),
                    quote(&source)
                ),
                Some(MARKER_FAIL),
                None,
            ),
        ],
    )
}

/// `query analysis` with the node-query-CHANGED body (SHAPE 7).
fn query_analysis_changed(node: &ExplainNode) -> qc::ExplainLine {
    let mut lines = vec![leaf(
        format!("the {} query has changed", node.noun()),
        Some(MARKER_FAIL),
        Some(BADGE_NODE_QUERY_CHANGED),
    )];
    // A node without upstreams lists no upstream query verdict (VERIFIED LIVE
    // 2026-11-11, explain_reason_kind_cells.jsonl `*_z`), as for a SKIP.
    if has_upstream_query_line(node) {
        lines.push(leaf(
            "upstream model queries have not changed",
            Some(MARKER_SUCCESS),
            None,
        ));
    }
    section("query analysis", lines)
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
        // Against the target's own build when the decision kept one as its
        // anchor (VERIFIED LIVE 2026-11-10, lag_copy_candidates `de`).
        freshness_children.push(if node.anchor.is_some() {
            upstream_dependencies_freshness(node, &upstreams)
        } else {
            clone_upstream_dependencies(&upstreams)
        });
    }
    freshness_children.extend(copied_freshness_summary(node));
    out.push(section(
        "data freshness analysis (vs clone)",
        freshness_children,
    ));
    out
}

/// How fresh the copied equivalent is: up to date, or outdated with its build
/// age against the tolerance (VERIFIED LIVE 2026-11-10, lag_copy_candidates
/// `st`, `wba`; lag_dropped_targets `dset600`).
fn copied_freshness_summary(node: &ExplainNode) -> Vec<qc::ExplainLine> {
    if !node.is_stale {
        return vec![leaf(
            "upstream data is up to date",
            Some(MARKER_SUCCESS),
            None,
        )];
    }
    let mut lines = vec![leaf("upstream data is outdated", Some(MARKER_FAIL), None)];
    if node.tolerance_ms > 0 {
        lines.push(lag_tolerance_line(node));
    }
    lines
}

/// The CLONE `upstream dependencies` listing. Unlike the SKIP/anchored listing
/// ([`upstream_dependencies_freshness`]), a clone has NO own-build anchor (ADR
/// 0005: a CLONE records no `matched_execution_id`), so the hosted service does
/// NOT emit a FRESH/OUTDATED badge or the anchor-relative `no updates since …`
/// line — it lists each upstream with ONLY the humanized `last updated: …`
/// child. VERIFIED LIVE (golden/fixtures/explain_clone_history.jsonl, the
/// history-bearing CLONE capture from #34). The earlier generator reused the
/// anchored SKIP listing here and wrongly emitted a `<FRESH>` badge plus a
/// `no updates since … last executed` line that hosted never renders for a
/// clone. A data-test EXECUTE lists its decision-time upstreams the same way,
/// even when it has a matched build (VERIFIED LIVE 2026-11-10,
/// explain_data_test_history.jsonl).
fn clone_upstream_dependencies(upstreams: &[&InputTable]) -> qc::ExplainLine {
    let children = upstreams.iter().map(|up| clone_upstream_line(up)).collect();
    section("upstream dependencies", children)
}

/// One upstream without a verdict: its name and `last updated` line only.
fn clone_upstream_line(up: &InputTable) -> qc::ExplainLine {
    qc::ExplainLine {
        text: quote(&up.name),
        marker: None,
        badge: None,
        children: vec![line(rel_time_line(up))],
    }
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
    use crate::store::Producer;

    fn tbl(name: &str) -> InputTable {
        InputTable {
            name: name.into(),
            last_modified_epoch: Some(100),
            ..Default::default()
        }
    }

    fn node(decision: DecisionKind, et: i32, desc: &str) -> ExplainNode {
        ExplainNode {
            execution_decision_id: "edid".into(),
            decision,
            execution_type: et,
            decision_description: Some(desc.into()),
            reason: Some(match decision {
                DecisionKind::Skip => DecisionReason::Reused(Reuse::Fresh),
                // A rebuild whose identity no longer matches (SHAPE 7).
                DecisionKind::Build => {
                    DecisionReason::NoReuseCandidate(NoCandidate::IdentityMismatch)
                }
                DecisionKind::Clone => DecisionReason::ResultCopy,
            }),
            is_stale: false,
            target_table: Some("\"DB\".\"S\".\"T\"".into()),
            clone_source: None,
            input_tables: vec![tbl("\"DB\".\"S\".\"T\"")],
            tolerance_ms: 0,
            build_age_ms: None,
            skip_rejection_reason: None,
            unmatched: None,
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
            DecisionKind::Skip,
            ModelExecutionType::Full as i32,
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
            DecisionKind::Skip,
            ModelExecutionType::View as i32,
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
            DecisionKind::Build,
            ModelExecutionType::View as i32,
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
        let mut n = node(
            DecisionKind::Build,
            ModelExecutionType::Full as i32,
            "model was executed because its table did not exist",
        );
        n.reason = Some(DecisionReason::FirstBuild);
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
    fn data_test_execute_lists_each_upstream_without_a_verdict() {
        // VERIFIED LIVE 2026-11-10 (explain_data_test_history.jsonl, client
        // 2.0.11): a data-test EXECUTE has no table or query analysis; it lists
        // each tested upstream with no badge and only `last updated`. The same
        // tree for a first execution, a logic change and upstream data drift,
        // and for a passing and a failing test; also after an upstream's logic
        // changed (2026-11-11, explain_rebuild_reason_gaps.jsonl `r40tm`). Not
        // the model-shaped first build `table analysis` (F4 regression, PR #31).
        let expected = vec![
            (0, "data freshness analysis".to_string(), None, None),
            (1, "upstream dependencies".to_string(), None, None),
            (2, "«FQN»".to_string(), None, None),
            (3, "last updated: «rel_time»".to_string(), None, None),
        ];
        for reason in [
            DecisionReason::FirstBuild,
            DecisionReason::NoReuseCandidate(NoCandidate::IdentityMismatch),
            DecisionReason::UpstreamOutdated,
            DecisionReason::UpstreamQueryChanged,
        ] {
            let mut n = node(
                DecisionKind::Build,
                ModelExecutionType::DbtDataTest as i32,
                "data test was executed because it has no prior execution or its query changed",
            );
            n.target_table = None; // tests produce no physical object
            n.input_tables = vec![tbl("\"DB\".\"S\".\"UP\"")];
            n.reason = Some(reason);
            assert_eq!(render(&n), expected, "{reason:?}");
        }
    }

    #[test]
    fn data_test_execute_renders_nothing_for_uncaptured_or_divergent_reasons() {
        // Hosted SKIPs a data test whose cached results are incomplete (#75
        // captures, divergence #87), so our rebuild for that reason has no
        // hosted tree. No capture shows a data test whose dependency set
        // changed (a store_failures test's own audit relation is not an
        // input, #88; one the request does not identify is, #106) or that
        // failed reuse qualification.
        for reason in [
            DecisionReason::NoReuseCandidate(NoCandidate::IncompleteCachedTestResults),
            DecisionReason::DependencySetChanged,
            DecisionReason::NoReuseCandidate(NoCandidate::FailedReuseQualification),
        ] {
            let mut n = node(
                DecisionKind::Build,
                ModelExecutionType::DbtDataTest as i32,
                "data test was executed because it has no prior execution or its query changed",
            );
            n.target_table = None;
            n.input_tables = vec![tbl("\"DB\".\"S\".\"UP\"")];
            n.reason = Some(reason);
            assert!(explain_lines(&n).is_empty(), "{reason:?}: {:?}", render(&n));
        }
    }

    #[test]
    fn data_test_skip_has_no_table_analysis() {
        // et=8 data test SKIP: no `table analysis`; query + per-upstream freshness.
        let mut n = node(
            DecisionKind::Skip,
            ModelExecutionType::DbtDataTest as i32,
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
            DecisionKind::Clone,
            ModelExecutionType::Full as i32,
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
        // CLONE upstream listing (#34): a clone has no own-build anchor, so the
        // upstream is listed with NO badge and ONLY a `last updated: …` child —
        // NOT the SKIP-style `<FRESH>` badge + `no updates since …` anchor line.
        // Pinned to golden/fixtures/explain_clone_history.jsonl.
        let deps = &lines[2].children[0];
        assert_eq!(deps.text, "upstream dependencies");
        let up = &deps.children[0];
        assert_eq!(up.text, "\"DB\".\"S\".\"UP\"");
        assert_eq!(
            up.badge, None,
            "clone upstream carries no FRESH/OUTDATED badge"
        );
        assert_eq!(up.marker, None);
        assert_eq!(up.children.len(), 1, "only the `last updated` child");
        assert!(
            up.children[0].text.starts_with("last updated:"),
            "clone upstream's sole child is the humanized last-updated line, got {:?}",
            up.children[0].text
        );
        assert_eq!(
            lines[2].children[1].text, "upstream data is up to date",
            "the clone freshness section still ends with the up-to-date line"
        );
    }

    // ── F7 (PR #31 round 4): fence uncharacterized node kinds ──────────────
    // explain_lines dispatches on DecisionKind alone; without an
    // execution-type fence, incremental decisions (and a snapshot or custom
    // CLONE) render model-shaped trees that the hosted service was never
    // observed to produce (docs/explain-catalog.md "Not rendered"; spec #27
    // Out-of-Scope). They must emit
    // nothing. Seeds, snapshot and custom SKIP/EXECUTE, and the append, merge,
    // insert_overwrite, delete+insert and microbatch SKIP/EXECUTE were
    // characterized on 2026-11-10.

    // ── Seeds (VERIFIED LIVE 2026-11-10, lag_seed_upstream.jsonl) ───────────
    fn seed(decision: DecisionKind, reason: DecisionReason) -> ExplainNode {
        let mut n = node(decision, ModelExecutionType::Values as i32, "");
        n.reason = Some(reason);
        n
    }

    fn seed_tree_of(changed: Option<bool>) -> Vec<(usize, String, Option<i32>, Option<i32>)> {
        let data = match changed {
            Some(true) => (
                1,
                "the seed data has changed".into(),
                Some(MARKER_FAIL),
                None,
            ),
            _ => (
                1,
                "the seed data has not changed".into(),
                Some(MARKER_SUCCESS),
                Some(BADGE_NODE_QUERY_UNCHANGED),
            ),
        };
        vec![
            (0, "table analysis («FQN»)".into(), None, None),
            (
                1,
                "the seed file exists already".into(),
                Some(MARKER_SUCCESS),
                Some(BADGE_TARGET_TABLE_EXISTS),
            ),
            (0, "query analysis".into(), None, None),
            data,
        ]
    }

    #[test]
    fn seed_skip_says_its_data_has_not_changed() {
        let n = seed(DecisionKind::Skip, DecisionReason::Reused(Reuse::Fresh));
        assert_eq!(render(&n), seed_tree_of(Some(false)));
    }

    #[test]
    fn seed_reload_says_whether_its_data_changed() {
        let mut n = seed(
            DecisionKind::Build,
            DecisionReason::NoReuseCandidate(NoCandidate::IdentityMismatch),
        );
        assert_eq!(render(&n), seed_tree_of(Some(true)));
        // An older load of the same values still matched (arm H): the data
        // has not changed, though the latest load differs.
        n.skip_rejection_reason = Some(decision::REJECTION_LATEST_QUERY_HASH_NOT_MATCH);
        assert_eq!(render(&n), seed_tree_of(Some(false)));
    }

    #[test]
    fn seed_load_into_a_missing_table_is_a_first_build() {
        for reason in [
            DecisionReason::FirstBuild,
            DecisionReason::NoReuseCandidate(NoCandidate::OwnTargetMissing),
        ] {
            let n = seed(DecisionKind::Build, reason);
            assert_eq!(
                render(&n),
                [
                    (0, "table analysis («FQN»)".into(), None, None),
                    (
                        1,
                        "the seed table does not exist".into(),
                        Some(MARKER_FAIL),
                        None
                    ),
                    (
                        1,
                        "there is no other table that the seed could be cloned from".into(),
                        Some(MARKER_FAIL),
                        None
                    ),
                ],
                "{reason:?}"
            );
        }
    }

    /// The safety-floor reload of an externally modified seed has no hosted
    /// counterpart (hosted skips it), so no tree is invented for it.
    #[test]
    fn seed_reload_over_a_modified_table_emits_nothing() {
        let n = seed(
            DecisionKind::Build,
            DecisionReason::NoReuseCandidate(NoCandidate::OwnTargetChangedOrMissing),
        );
        assert!(explain_lines(&n).is_empty(), "{:?}", render(&n));
    }

    #[test]
    fn custom_clone_emits_nothing() {
        // DBT_CUSTOM=11 CLONE is unreachable on 2.0.11: hosted rebuilds
        // (explain_custom_copy.jsonl entry 9, #86) → fenced, no tree.
        let mut n = node(
            DecisionKind::Clone,
            ModelExecutionType::DbtCustom as i32,
            "an equivalent model exists under another name so we cloned that one",
        );
        n.clone_source = Some("\"DB\".\"S\".\"SRC\"".into());
        assert!(
            explain_lines(&n).is_empty(),
            "a custom CLONE is unreachable on 2.0.11 → must emit no invented lines, got {:?}",
            render(&n)
        );
    }

    #[test]
    fn incremental_clone_emits_nothing() {
        // Every incremental strategy's CLONE is unreachable on 2.0.11:
        // hosted rebuilds on submit (explain_incremental_copy_over.jsonl
        // entries 37–41, #101); only DuckDB's checked delete+insert copy
        // reaches here, local-only → no tree, over an existing Target relation
        // or an absent one.
        for et in [
            ModelExecutionType::Append,
            ModelExecutionType::Merge,
            ModelExecutionType::InsertOverwrite,
            ModelExecutionType::DeleteInsert,
            ModelExecutionType::Microbatch,
        ] {
            for target_listed in [false, true] {
                let mut n = node(
                    DecisionKind::Clone,
                    et as i32,
                    "an equivalent model exists under another name so we cloned that one",
                );
                n.clone_source = Some("\"DB\".\"S\".\"SRC\"".into());
                if target_listed {
                    let target = n.target_table.clone().unwrap();
                    n.input_tables.push(tbl(&target));
                }
                assert!(
                    explain_lines(&n).is_empty(),
                    "an incremental CLONE ({et:?}) is local-only → must emit no invented \
                     lines, got {:?}",
                    render(&n)
                );
            }
        }
    }

    #[test]
    fn snapshot_clone_emits_nothing() {
        // SNAPSHOT=7 CLONE: unreachable on 2.0.11 (#76) → no invented tree.
        let mut n = node(
            DecisionKind::Clone,
            ModelExecutionType::Snapshot as i32,
            "an equivalent model exists under another name so we cloned that one",
        );
        n.clone_source = Some("\"DB\".\"S\".\"SRC\"".into());
        assert!(
            explain_lines(&n).is_empty(),
            "snapshot CLONE is uncaptured → must emit no invented lines, got {:?}",
            render(&n)
        );
    }

    /// EXHAUSTIVE FENCE MATRIX. Sweep every `ModelExecutionType` value
    /// (shared.proto 0..=11, plus a couple of out-of-range sentinels) crossed
    /// with every `DecisionKind`, and assert the fence contract exactly:
    /// a CHARACTERIZED (execution_type, decision) pair renders a non-empty
    /// cataloged tree; every other pair renders NOTHING.
    ///
    /// This is the generative guard the point tests above only sample. F4 and
    /// F7 were the SAME omission (data-test EXECUTE, then seed/snapshot/
    /// incremental) surfacing one kind at a time because no test swept the whole
    /// matrix; keep this exhaustive so a newly rendered kind fails HERE, up
    /// front, rather than in a review round. When a kind graduates from
    /// unobserved to characterized, add it to BOTH `is_characterized` and the
    /// expected set below in one edit (the two cannot silently drift: a kind
    /// that renders but is not expected characterized, or vice versa, fails).
    #[test]
    fn fence_matrix_is_exhaustive_and_matches_is_characterized() {
        // The ONLY pairs the fence may render, verified against the explain
        // fixtures (execution_type 1/10/8) + docs/explain-catalog.md.
        let characterized = |et: i32, d: DecisionKind| -> bool {
            matches!(
                (et, d),
                (1, DecisionKind::Skip)        // model/table SKIP
                    | (1, DecisionKind::Build) // model/table EXECUTE
                    | (1, DecisionKind::Clone) // model/table CLONE
                    | (10, DecisionKind::Skip) // view SKIP
                    | (10, DecisionKind::Build) // view EXECUTE (CLONE unreachable on 2.0.11, #76)
                    | (8, DecisionKind::Skip) // data test SKIP
                    | (8, DecisionKind::Build) // data test EXECUTE (explain_data_test_history; CLONE → #68)
                    | (9, DecisionKind::Skip) // seed SKIP (lag_seed_upstream)
                    | (9, DecisionKind::Build) // seed reload (CLONE fenced until #91)
                    | (7, DecisionKind::Skip) // snapshot SKIP (explain_snapshot_history)
                    | (7, DecisionKind::Build) // snapshot EXECUTE (CLONE unreachable on 2.0.11, #76)
                    | (11, DecisionKind::Skip) // custom SKIP (explain_custom_history)
                    | (11, DecisionKind::Build) // custom EXECUTE (CLONE unreachable on 2.0.11, #86)
                    | (2, DecisionKind::Skip) // append SKIP (explain_incremental_append_merge_history)
                    | (2, DecisionKind::Build) // append EXECUTE (CLONE unreachable on 2.0.11, #101)
                    | (3, DecisionKind::Skip) // merge SKIP
                    | (3, DecisionKind::Build) // merge EXECUTE (CLONE unreachable on 2.0.11, #101)
                    | (4, DecisionKind::Skip) // insert_overwrite SKIP (explain_incremental_insert_overwrite)
                    | (4, DecisionKind::Build) // insert_overwrite EXECUTE (CLONE unreachable on 2.0.11, #101)
                    | (5, DecisionKind::Skip) // delete+insert SKIP (explain_incremental_delete_insert)
                    | (5, DecisionKind::Build) // delete+insert EXECUTE (CLONE unreachable on 2.0.11, #101)
                    | (6, DecisionKind::Skip) // microbatch SKIP (explain_incremental_microbatch)
                    | (6, DecisionKind::Build) // microbatch EXECUTE (CLONE unreachable on 2.0.11, #101)
            )
        };

        // A persisted description that DOES let a Build render SHAPE-7 for a
        // characterized kind (node-query-changed), so a rendered-vs-empty
        // mismatch is attributable to the fence, not a missing description.
        let desc = |d: DecisionKind| {
            match d {
            DecisionKind::Skip => {
                "model was a no-op because both its query and its upstream data are up to date"
            }
            DecisionKind::Build => {
                "model was executed because either its query didn't match or its upstream data is out of date"
            }
            DecisionKind::Clone => {
                "an equivalent model exists under another name so we cloned that one"
            }
        }
        };

        // 0..=11 covers the whole shared.proto ModelExecutionType enum; the
        // sentinels guard the `_ => false` arm against an unknown/garbage value.
        let execution_types = (0i32..=11).chain([99, -1]);
        let decisions = [DecisionKind::Skip, DecisionKind::Build, DecisionKind::Clone];
        for et in execution_types {
            for &d in &decisions {
                // is_characterized is the fence predicate; the local table is an
                // INDEPENDENT restatement of the catalog so the two cross-check.
                assert_eq!(
                    is_characterized(et, d),
                    characterized(et, d),
                    "is_characterized disagrees with the catalog matrix for \
                     (execution_type={et}, {d:?})"
                );

                let mut n = node(d, et, desc(d));
                // CLONE needs a source to render its table-analysis line; give
                // one so a characterized CLONE is genuinely non-empty.
                if d == DecisionKind::Clone {
                    n.clone_source = Some("\"DB\".\"S\".\"SRC\"".into());
                }
                // A data test has no target (hosted and the client send none),
                // so its one input is a tested upstream. Other kinds read one
                // upstream, except a view (client 2.0.11 reports none for it).
                if et == ModelExecutionType::DbtDataTest as i32 {
                    n.target_table = None;
                } else if et != ModelExecutionType::View as i32 {
                    n.input_tables.push(tbl("\"DB\".\"S\".\"UP\""));
                }
                let empty = explain_lines(&n).is_empty();
                assert_eq!(
                    !empty,
                    characterized(et, d),
                    "fence mismatch for (execution_type={et}, {d:?}): \
                     characterized={} but rendered_non_empty={}; tree={:?}",
                    characterized(et, d),
                    !empty,
                    render(&n)
                );
            }
        }
    }

    /// The first-build shape is selected by the persisted reason alone: the
    /// description wording does not choose a tree (rewording one cannot change
    /// Explain output).
    #[test]
    fn first_build_shape_is_selected_by_reason_not_description() {
        let first_build_tree = {
            let mut n = node(
                DecisionKind::Build,
                ModelExecutionType::Full as i32,
                "model was executed because its table did not exist",
            );
            n.reason = Some(DecisionReason::FirstBuild);
            render(&n)
        };
        let shape7_tree = render(&node(
            DecisionKind::Build,
            ModelExecutionType::Full as i32,
            decision::DESC_EXECUTE,
        ));
        assert_ne!(first_build_tree, shape7_tree);

        // A first build with any wording renders the first-build tree.
        let mut reworded = node(
            DecisionKind::Build,
            ModelExecutionType::Full as i32,
            decision::DESC_EXECUTE,
        );
        reworded.reason = Some(DecisionReason::FirstBuild);
        assert_eq!(render(&reworded), first_build_tree);

        // A rebuild whose description happens to say "did not exist" or "no
        // prior execution" is still a rebuild.
        for desc in [
            "model was executed because its table did not exist",
            "model has no prior execution",
        ] {
            let n = node(DecisionKind::Build, ModelExecutionType::Full as i32, desc);
            assert_eq!(render(&n), shape7_tree, "{desc}");
        }
    }

    /// Every reason a characterized rebuild can carry selects its tree. Only
    /// an identity mismatch renders SHAPE 7; no other rebuild reason falls
    /// back to it (#40). The rebuilds hosted never makes render nothing
    /// (crafted probes d1–d4, explain_ambiguous_rebuilds.jsonl; #80, #82),
    /// and so does every rebuild reason no tracked capture backs. A reason
    /// that contradicts the class renders nothing.
    #[test]
    fn each_decision_reason_selects_its_shape() {
        let shape7 = render(&node(
            DecisionKind::Build,
            ModelExecutionType::Full as i32,
            decision::DESC_EXECUTE,
        ));
        assert_eq!(shape7[2].1, "query analysis");
        assert_eq!(shape7[3].1, "the model query has changed");
        for reason in [
            DecisionReason::NoReuseCandidate(NoCandidate::FailedReuseQualification),
            DecisionReason::NoReuseCandidate(NoCandidate::IncompleteCachedTestResults),
            DecisionReason::DependencySetChanged,
            DecisionReason::CopyChainLimit,
            // Without the equivalent it could have been copied from.
            DecisionReason::CopyDisabled,
            // Without an anchor neither SHAPE 9 tree can render.
            DecisionReason::UpstreamOutdated,
            DecisionReason::UpstreamQueryChanged,
        ] {
            let mut n = node(
                DecisionKind::Build,
                ModelExecutionType::Full as i32,
                decision::DESC_EXECUTE,
            );
            n.reason = Some(reason);
            assert!(explain_lines(&n).is_empty(), "{reason:?}: {:?}", render(&n));
        }

        // Local-only rebuilds: with or without genuine upstreams, and whether
        // or not the decision-time inputs report the Target relation.
        for reason in [
            DecisionReason::NoReuseCandidate(NoCandidate::OwnTargetChangedOrMissing),
            DecisionReason::NoBuildAnchor,
        ] {
            for (upstream, own_target) in [(false, true), (true, true), (true, false)] {
                let mut n = node(
                    DecisionKind::Build,
                    ModelExecutionType::Full as i32,
                    decision::DESC_EXECUTE,
                );
                n.reason = Some(reason);
                if upstream {
                    n.input_tables
                        .push(InputTable::at("\"DB\".\"S\".\"UP\"", Some(5_000)));
                }
                if !own_target {
                    n.input_tables.retain(|t| t.name != "\"DB\".\"S\".\"T\"");
                }
                assert!(
                    explain_lines(&n).is_empty(),
                    "{reason:?} upstream={upstream} own_target={own_target}"
                );
            }
        }
        // Own target modified with genuine upstreams but no matched build:
        // only when the anchor's execution row disappeared.
        let mut n = node(
            DecisionKind::Build,
            ModelExecutionType::Full as i32,
            decision::DESC_EXECUTE,
        );
        n.reason = Some(DecisionReason::NoReuseCandidate(
            NoCandidate::OwnTargetModified,
        ));
        n.input_tables
            .push(InputTable::at("\"DB\".\"S\".\"UP\"", Some(5_000)));
        assert!(explain_lines(&n).is_empty());

        for (d, reason) in [
            (DecisionKind::Skip, DecisionReason::FirstBuild),
            (DecisionKind::Skip, DecisionReason::ResultCopy),
            (DecisionKind::Build, DecisionReason::Reused(Reuse::Fresh)),
            (DecisionKind::Build, DecisionReason::ResultCopy),
            (DecisionKind::Clone, DecisionReason::Reused(Reuse::Fresh)),
            (DecisionKind::Clone, DecisionReason::CopyChainLimit),
            (DecisionKind::Clone, DecisionReason::CopyDisabled),
        ] {
            let mut n = node(d, ModelExecutionType::Full as i32, decision::DESC_EXECUTE);
            n.clone_source = Some("\"DB\".\"S\".\"SRC\"".into());
            n.reason = Some(reason);
            assert!(explain_lines(&n).is_empty(), "{d:?} with {reason:?}");
        }

        // A tolerated-stale reuse renders the same SKIP tree as a fresh one.
        let skip = |reuse| {
            let mut n = node(
                DecisionKind::Skip,
                ModelExecutionType::Full as i32,
                decision::DESC_SKIP,
            );
            n.reason = Some(DecisionReason::Reused(reuse));
            render(&n)
        };
        assert_eq!(skip(Reuse::ToleratedStale), skip(Reuse::Fresh));
    }

    /// The incremental strategies, by name for loops.
    const INCREMENTALS: [ModelExecutionType; 5] = [
        ModelExecutionType::Append,
        ModelExecutionType::Merge,
        ModelExecutionType::InsertOverwrite,
        ModelExecutionType::DeleteInsert,
        ModelExecutionType::Microbatch,
    ];

    /// `n` without genuine upstreams: its own target only, and an anchor
    /// that recorded none.
    fn without_upstreams(mut n: ExplainNode) -> ExplainNode {
        n.input_tables.truncate(1);
        if let Some(a) = n.anchor.as_mut() {
            a.input_tables.clear();
        }
        n
    }

    /// A rebuild because the latest build had the identity under another
    /// node kind renders the SKIP-shaped tree against that build (VERIFIED
    /// LIVE: a FULL submit over SNAPSHOT history, explain_snapshot_history.jsonl
    /// entry 60 and explain_reason_kind_cells.jsonl `sn_cz`; an APPEND submit
    /// over FULL history, explain_execution_type_change.jsonl `tv`; every
    /// incremental strategy and a snapshot over a FULL full refresh, with and
    /// without an upstream, explain_reason_kind_cells.jsonl `*_e`, `*_ez`; a
    /// custom node without one over a view build, `vc0`). Nothing for a VIEW
    /// (#117), a custom node with an upstream (unreachable), without the
    /// build, or over a build with other upstream names (#116).
    #[test]
    fn execution_type_change_renders_the_unchanged_tree_against_the_latest_build() {
        let changed = DecisionReason::NoReuseCandidate(NoCandidate::ExecutionTypeChanged);
        let mut skip = anchored(
            DecisionKind::Skip,
            DecisionReason::Reused(Reuse::Fresh),
            900,
            900,
        );
        let mut n = anchored(DecisionKind::Build, changed, 900, 900);
        let rendered = [ModelExecutionType::Full, ModelExecutionType::Snapshot];
        for et in rendered.into_iter().chain(INCREMENTALS) {
            n.execution_type = et as i32;
            skip.execution_type = et as i32;
            assert_eq!(render(&n), render(&skip), "{et:?}");
            assert_eq!(n.anchor.as_ref().map(|a| a.id), Some(7));
            let (bare, bare_skip) = (
                without_upstreams(n.clone()),
                without_upstreams(skip.clone()),
            );
            assert_eq!(
                render(&bare),
                render(&bare_skip),
                "{et:?} without upstreams"
            );
        }
        n.execution_type = ModelExecutionType::DbtCustom as i32;
        skip.execution_type = ModelExecutionType::DbtCustom as i32;
        assert!(explain_lines(&n).is_empty(), "custom with an upstream");
        assert_eq!(
            render(&without_upstreams(n.clone())),
            render(&without_upstreams(skip.clone()))
        );
        n.execution_type = ModelExecutionType::View as i32;
        assert!(
            explain_lines(&without_upstreams(n.clone())).is_empty(),
            "view"
        );
        n.execution_type = ModelExecutionType::Full as i32;
        let mut unanchored = n.clone();
        unanchored.anchor = None;
        assert!(explain_lines(&unanchored).is_empty());
        let mut other_upstreams = n.clone();
        other_upstreams
            .anchor
            .as_mut()
            .unwrap()
            .input_tables
            .clear();
        assert!(explain_lines(&other_upstreams).is_empty());
    }

    /// Own target modified: the SKIP-shaped tree against the matched build
    /// for every incremental strategy, with and without an upstream
    /// (VERIFIED LIVE 2026-11-10/11, explain_incremental_append_merge_history.jsonl
    /// entries 78-79, explain_execution_type_change.jsonl `mi`,
    /// explain_reason_kind_cells.jsonl `*_c`, `*_cz`, `*_e`, `*_ez`), with
    /// the build. Nothing for a snapshot or a custom node, which hosted
    /// reuses (#115), nor for a VIEW (#117).
    #[test]
    fn own_target_modified_renders_the_unchanged_tree_for_the_incrementals() {
        let modified = DecisionReason::NoReuseCandidate(NoCandidate::OwnTargetModified);
        let mut skip = anchored(
            DecisionKind::Skip,
            DecisionReason::Reused(Reuse::Fresh),
            900,
            900,
        );
        let mut n = anchored(DecisionKind::Build, modified, 900, 900);
        for et in INCREMENTALS {
            n.execution_type = et as i32;
            skip.execution_type = et as i32;
            assert_eq!(render(&n), render(&skip), "{et:?}");
            assert_eq!(
                render(&without_upstreams(n.clone())),
                render(&without_upstreams(skip.clone())),
                "{et:?} without upstreams"
            );
            let mut unanchored = n.clone();
            unanchored.anchor = None;
            assert!(explain_lines(&unanchored).is_empty(), "{et:?}");
        }
        for et in [
            ModelExecutionType::Snapshot,
            ModelExecutionType::DbtCustom,
            ModelExecutionType::View,
        ] {
            n.execution_type = et as i32;
            assert!(explain_lines(&n).is_empty(), "{et:?}");
            assert!(
                explain_lines(&without_upstreams(n.clone())).is_empty(),
                "{et:?}"
            );
        }
    }

    /// An upstream the judged build did not record has no verdict: no badge
    /// and no anchor line (VERIFIED LIVE 2026-11-11,
    /// explain_execution_type_change.jsonl `tv`, SHAPE 7 judged against a
    /// view build, which reports no upstreams). A recorded upstream keeps
    /// its verdict.
    #[test]
    fn an_upstream_the_judged_build_did_not_record_has_no_verdict() {
        let identity = DecisionReason::NoReuseCandidate(NoCandidate::IdentityMismatch);
        let mut n = anchored(DecisionKind::Build, identity, 900, 900);
        let mut unmatched = n.anchor.take().expect("anchored");
        unmatched.input_tables.clear();
        n.unmatched = Some(unmatched);
        let lines = explain_lines(&n);
        let up = &lines[2].children[0].children[0];
        assert_eq!(up.badge, None);
        assert_eq!(up.children.len(), 1, "{:?}", render(&n));
        assert!(up.children[0].text.starts_with("last updated"));
        assert_eq!(freshness_texts(&lines), ["upstream dependencies"]);

        let mut recorded = anchored(DecisionKind::Build, identity, 900, 900);
        recorded.unmatched = recorded.anchor.take();
        let lines = explain_lines(&recorded);
        assert_eq!(lines[2].children[0].children[0].badge, Some(BADGE_FRESH));
    }

    /// A rebuild over a RegisterClone copy the submit could not adopt, even
    /// read in the copy source's schema (#93), renders SHAPE 7 judged against
    /// the copy (VERIFIED LIVE 2026-11-11): with an upstream, the ordinary
    /// SHAPE 7 listing (explain_copy_logic_mismatch.jsonl, the five
    /// incrementals, a snapshot and a custom node; crafted `x1`, a table).
    /// Without one, what the copy's source build recorded decides: `no
    /// upstream dependencies` when it recorded none (crafted `x2`–`x10`), an
    /// empty freshness section after `upstream model queries have not
    /// changed` when it recorded some (`kv4`, a view). A view with an upstream
    /// (never reported by client 2.0.11) and an absent Target relation render
    /// nothing.
    #[test]
    fn copy_logic_mismatch_renders_shape7_against_the_copy() {
        use ModelExecutionType as K;
        const UP: &str = "\"DB\".\"RAW\".\"UP\"";
        let rebuild = |et: K, upstream: bool, source_read: bool| {
            let mut n = node(DecisionKind::Build, et as i32, decision::DESC_EXECUTE);
            n.reason = Some(DecisionReason::NoReuseCandidate(
                NoCandidate::CopyLogicMismatch,
            ));
            if upstream {
                n.input_tables.push(tbl(UP));
            }
            n.unmatched = Some(ExecutionRow {
                target_table: "\"DB\".\"S\".\"T\"".into(),
                execution_type: K::Append as i32,
                last_modified_epoch: Some(1_000),
                clone_source_table: Some("\"DB\".\"PROD\".\"T\"".into()),
                input_tables: if upstream { vec![tbl(UP)] } else { Vec::new() },
                clone_source_inputs: Some(if source_read {
                    vec![UP.into()]
                } else {
                    Vec::new()
                }),
                ..ExecutionRow::default()
            });
            n
        };
        let kinds = [
            K::Full,
            K::Append,
            K::Merge,
            K::InsertOverwrite,
            K::DeleteInsert,
            K::Microbatch,
            K::Snapshot,
            K::DbtCustom,
        ];
        for et in kinds {
            let n = rebuild(et, true, true);
            let mut shape7 = n.clone();
            shape7.reason = Some(DecisionReason::NoReuseCandidate(
                NoCandidate::IdentityMismatch,
            ));
            assert!(!explain_lines(&n).is_empty(), "{et:?}");
            assert_eq!(render(&n), render(&shape7), "{et:?}");
        }
        let noun = |et| {
            if et == K::Snapshot {
                "snapshot"
            } else {
                "model"
            }
        };
        for et in kinds.into_iter().chain([K::View]) {
            assert_eq!(
                render(&rebuild(et, false, false)),
                vec![
                    (0, "table analysis («FQN»)".to_string(), None, None),
                    (
                        1,
                        format!("the {} table exists already", noun(et)),
                        Some(MARKER_SUCCESS),
                        Some(BADGE_TARGET_TABLE_EXISTS)
                    ),
                    (0, "query analysis".to_string(), None, None),
                    (
                        1,
                        format!("the {} query has changed", noun(et)),
                        Some(MARKER_FAIL),
                        Some(BADGE_NODE_QUERY_CHANGED)
                    ),
                    (0, "data freshness analysis".to_string(), None, None),
                    (
                        1,
                        "no upstream dependencies".to_string(),
                        Some(MARKER_SUCCESS),
                        None
                    ),
                ],
                "{et:?}"
            );
        }
        assert_eq!(
            render(&rebuild(K::View, false, true)),
            vec![
                (0, "table analysis («FQN»)".to_string(), None, None),
                (
                    1,
                    "the model table exists already".to_string(),
                    Some(MARKER_SUCCESS),
                    Some(BADGE_TARGET_TABLE_EXISTS)
                ),
                (0, "query analysis".to_string(), None, None),
                (
                    1,
                    "the model query has changed".to_string(),
                    Some(MARKER_FAIL),
                    Some(BADGE_NODE_QUERY_CHANGED)
                ),
                (
                    1,
                    "upstream model queries have not changed".to_string(),
                    Some(MARKER_SUCCESS),
                    None
                ),
                (0, "data freshness analysis".to_string(), None, None),
            ]
        );
        assert!(explain_lines(&rebuild(K::View, true, true)).is_empty());
        // Over a dropped copy: the first-build tree for FULL and APPEND
        // (crafted `x11`, `x12`), nothing for the other kinds.
        for et in kinds {
            let mut absent = rebuild(et, true, true);
            absent.input_tables.retain(|t| t.name == UP);
            let lines = render(&absent);
            if matches!(et, K::Full | K::Append) {
                assert_eq!(lines[1].1, "the model table does not exist", "{et:?}");
                assert_eq!(lines.len(), 3, "{et:?}");
            } else {
                assert!(lines.is_empty(), "{et:?}: {lines:?}");
            }
        }
    }

    /// A FULL build whose copy of an equivalent the submit disallowed
    /// (VERIFIED LIVE 2026-11-11, explain_copy_logic_mismatch.jsonl `cf4`)
    /// names the equivalent; the chain-limit build, which no 2.0.11 capture
    /// shows, renders nothing, as does a disallowed copy without its source.
    #[test]
    fn copy_disabled_names_the_equivalent() {
        let mut n = node(
            DecisionKind::Build,
            ModelExecutionType::Full as i32,
            "model was executed because cloning is disabled",
        );
        n.reason = Some(DecisionReason::CopyDisabled);
        n.input_tables = vec![tbl("\"DB\".\"RAW\".\"UP\"")];
        n.clone_source = Some("\"DB\".\"PROD\".\"T\"".into());
        let lines = explain_lines(&n);
        assert_eq!(lines.len(), 1);
        let texts: Vec<_> = lines[0]
            .children
            .iter()
            .map(|l| (l.text.as_str(), l.marker, l.badge))
            .collect();
        assert_eq!(
            texts,
            vec![
                ("the model table does not exist", Some(MARKER_FAIL), None),
                (
                    "the model table could have been cloned from \"DB\".\"PROD\".\"T\", but cloning is disabled",
                    Some(MARKER_FAIL),
                    None
                ),
            ]
        );

        let mut chain = n.clone();
        chain.reason = Some(DecisionReason::CopyChainLimit);
        assert!(explain_lines(&chain).is_empty());
        let mut unnamed = n.clone();
        unnamed.clone_source = None;
        assert!(explain_lines(&unnamed).is_empty());
        let mut snapshot = n;
        snapshot.execution_type = ModelExecutionType::Snapshot as i32;
        assert!(explain_lines(&snapshot).is_empty());
    }

    /// A SKIP that reused a RegisterClone copy (VERIFIED LIVE 2026-11-11,
    /// explain_post_copy_identity.jsonl, #93): an upstream the copy's source
    /// build recorded (a fixed prod source, `c93sa`) is FRESH with the `no
    /// updates since` line; one resolved per target under another name (the
    /// dev relation of a ref'd model, `c93st`) is listed with its `last
    /// updated` line only. Both are up to date.
    #[test]
    fn skip_over_a_copy_verdicts_only_upstreams_its_source_recorded() {
        const TARGET: &str = "\"DB\".\"DEV\".\"SNAP\"";
        const FIXED: &str = "\"DB\".\"PROD\".\"SRC\"";
        const PER_TARGET: &str = "\"DB\".\"DEV\".\"UP\"";
        let mut n = node(
            DecisionKind::Skip,
            ModelExecutionType::Snapshot as i32,
            "snapshot was a no-op because both its query and its upstream data are up to date",
        );
        n.target_table = Some(TARGET.into());
        n.input_tables = vec![tbl(TARGET), tbl(FIXED), tbl(PER_TARGET)];
        n.anchor = Some(ExecutionRow {
            target_table: TARGET.into(),
            execution_type: ModelExecutionType::Snapshot as i32,
            last_modified_epoch: Some(1_000),
            clone_source_table: Some("\"DB\".\"PROD\".\"SNAP\"".into()),
            clone_source_inputs: Some(vec![FIXED.into(), "\"DB\".\"PROD\".\"UP\"".into()]),
            // Adopted inputs (`reuse_candidate::adopt_current_inputs`).
            input_tables: vec![tbl(FIXED), tbl(PER_TARGET)],
            ..ExecutionRow::default()
        });
        let freshness: Vec<_> = render(&n)
            .into_iter()
            .skip_while(|(_, t, _, _)| t != "upstream dependencies")
            .collect();
        assert_eq!(
            freshness,
            vec![
                (1, "upstream dependencies".to_string(), None, None),
                (2, "«FQN»".to_string(), None, Some(BADGE_FRESH)),
                (
                    3,
                    "no updates since «FQN» last executed".to_string(),
                    None,
                    None
                ),
                (3, "last updated: «rel_time»".to_string(), None, None),
                (2, "«FQN»".to_string(), None, None),
                (3, "last updated: «rel_time»".to_string(), None, None),
                (1, "upstream data is up to date".to_string(), Some(1), None),
            ]
        );

        // A copy registered before its source's inputs were kept: as before.
        n.anchor.as_mut().unwrap().clone_source_inputs = None;
        let badges: Vec<_> = render(&n)
            .into_iter()
            .filter(|(d, t, _, _)| *d == 2 && t == "«FQN»")
            .map(|(_, _, _, b)| b)
            .collect();
        assert_eq!(badges, vec![Some(BADGE_FRESH), Some(BADGE_FRESH)]);
    }

    /// An upstream-outdated rebuild with its anchor renders SHAPE 9: the
    /// OUTDATED upstream is re-derived from the persisted inputs against the
    /// matched build. A data-only rebuild lists no upstream queries; a changed
    /// producer lists each upstream's query verdict (VERIFIED LIVE 2026-11-10).
    #[test]
    fn upstream_outdated_with_anchor_is_shape9() {
        let shape9 = |reason, producers: (Producer, Producer)| {
            let mut n = node(
                DecisionKind::Build,
                ModelExecutionType::Full as i32,
                decision::DESC_EXECUTE,
            );
            n.reason = Some(reason);
            n.input_tables = vec![InputTable {
                name: "\"DB\".\"S\".\"UP\"".into(),
                last_modified_epoch: Some(5_000),
                producer: producers.1,
            }];
            let mut recorded = tbl("\"DB\".\"S\".\"UP\"");
            recorded.producer = producers.0;
            n.anchor = Some(ExecutionRow {
                id: 7,
                target_table: "\"DB\".\"S\".\"T\"".into(),
                execution_type: ModelExecutionType::Full as i32,
                last_modified_epoch: Some(1_000),
                input_tables: vec![recorded],
                status: "confirmed".into(),
                ..ExecutionRow::default()
            });
            explain_lines(&n)
        };
        let texts = |lines: &[qc::ExplainLine]| -> Vec<String> {
            lines[1].children.iter().map(|l| l.text.clone()).collect()
        };
        let data_only = shape9(
            DecisionReason::UpstreamOutdated,
            (Producer::Build("a".into()), Producer::Build("a".into())),
        );
        assert_eq!(
            texts(&data_only),
            [
                "the model query has not changed",
                "upstream model queries have not changed"
            ]
        );
        let changed = shape9(
            DecisionReason::UpstreamQueryChanged,
            (Producer::Build("a".into()), Producer::NoBuild),
        );
        assert_eq!(
            texts(&changed),
            [
                "the model query has not changed",
                "upstream dependencies",
                "an upstream model query has changed"
            ]
        );
        assert_eq!(
            changed[1].children[1].children[0].children[0].badge,
            Some(BADGE_UPSTREAM_QUERY_CHANGED)
        );
        let lines = data_only;
        let up = &lines[2].children[0].children[0];
        assert_eq!(up.badge, Some(BADGE_OUTDATED));
        assert_eq!(
            up.children[0].text,
            "updated 4 seconds after \"DB\".\"S\".\"T\" last executed"
        );
        assert_eq!(lines[2].children[1].text, "upstream data is outdated");
    }

    /// VERIFIED LIVE 2026-11-11 (explain_freshness_cells.jsonl `fd_sn`) and
    /// crafted (explain_rebuild_tolerance_line.jsonl `s2`, `i2` vs `s1`,
    /// `i1`, `f0`): a data-only rebuild past a non-zero Lag tolerance ends
    /// with the Lag tolerance line only when it reports an equivalent under
    /// another name (TARGET_TABLE_MISMATCH), worded with the equivalent's
    /// build age. Without that age it renders nothing.
    #[test]
    fn shape9_reporting_an_equivalent_has_its_tolerance_line() {
        let rebuild = |srr, build_age_ms| {
            let mut n = anchored(
                DecisionKind::Build,
                DecisionReason::UpstreamOutdated,
                5_000,
                900,
            );
            n.execution_type = ModelExecutionType::Snapshot as i32;
            n.tolerance_ms = 20_000;
            n.skip_rejection_reason = Some(srr);
            n.build_age_ms = build_age_ms;
            explain_lines(&n)
        };
        let with_equivalent = rebuild(decision::REJECTION_TARGET_TABLE_MISMATCH, Some(1_500));
        assert_eq!(
            freshness_texts(&with_equivalent),
            [
                "upstream dependencies",
                "upstream data is outdated",
                "the snapshot was last built a second ago, within lag tolerance of 20 seconds"
            ]
        );
        let exceeds = rebuild(decision::REJECTION_TARGET_TABLE_MISMATCH, Some(60_000));
        let line = exceeds[2].children.last().unwrap();
        assert_eq!(
            line.text,
            "the snapshot was last built a minute ago, exceeds lag tolerance of 20 seconds"
        );
        assert_eq!(
            (line.marker, line.badge),
            (Some(MARKER_FAIL), Some(BADGE_OUTDATED))
        );
        assert_eq!(
            freshness_texts(&rebuild(6, Some(26_000))),
            ["upstream dependencies", "upstream data is outdated"]
        );
        assert!(rebuild(decision::REJECTION_TARGET_TABLE_MISMATCH, None).is_empty());
    }

    /// Hosted reuses a build whose upstream epoch is unknown (crafted,
    /// explain_unknown_epoch.jsonl, accepted divergence #113); this server
    /// rebuilds it, and SHAPE 9 needs a proven drift, so that rebuild renders
    /// nothing. A proven drift beside it still renders SHAPE 9.
    #[test]
    fn upstream_outdated_over_an_unknown_epoch_renders_nothing() {
        for (current, recorded) in [(None, Some(900)), (Some(5_000), None), (None, None)] {
            let mut n = anchored(
                DecisionKind::Build,
                DecisionReason::UpstreamOutdated,
                5_000,
                900,
            );
            n.input_tables[1].last_modified_epoch = current;
            n.anchor.as_mut().unwrap().input_tables[0].last_modified_epoch = recorded;
            assert!(explain_lines(&n).is_empty(), "{current:?} / {recorded:?}");

            let other = |epoch| InputTable {
                name: "\"DB\".\"S\".\"UP2\"".into(),
                last_modified_epoch: Some(epoch),
                producer: Producer::Build("b".into()),
            };
            n.input_tables.push(other(5_000));
            n.anchor.as_mut().unwrap().input_tables.push(other(900));
            assert!(!explain_lines(&n).is_empty(), "{current:?} / {recorded:?}");
        }
    }

    /// A node of `decision`/`reason` that read UP at `current`, anchored on a
    /// build (epoch 1_000) that recorded UP at `recorded`, same producer.
    fn anchored(
        decision: DecisionKind,
        reason: DecisionReason,
        current: i64,
        recorded: i64,
    ) -> ExplainNode {
        let mut n = node(decision, ModelExecutionType::Full as i32, "");
        n.reason = Some(reason);
        let up = |epoch| InputTable {
            name: "\"DB\".\"S\".\"UP\"".into(),
            last_modified_epoch: Some(epoch),
            producer: Producer::Build("a".into()),
        };
        n.input_tables = vec![tbl("\"DB\".\"S\".\"T\""), up(current)];
        n.anchor = Some(ExecutionRow {
            id: 7,
            target_table: "\"DB\".\"S\".\"T\"".into(),
            execution_type: ModelExecutionType::Full as i32,
            last_modified_epoch: Some(1_000),
            input_tables: vec![up(recorded)],
            status: "confirmed".into(),
            ..ExecutionRow::default()
        });
        n
    }

    /// SHAPE 7 judges its listing against the unmatched latest build
    /// (VERIFIED LIVE 2026-11-11, explain_rebuild_reason_gaps.jsonl `r40q`:
    /// drift is OUTDATED with `upstream data is outdated`; the moved-back and
    /// unchanged cases are pinned by the replay). Without that build, a
    /// rebuild recorded before migration 0017, nothing proves freshness, so
    /// the summary line is omitted.
    #[test]
    fn shape7_listing_is_judged_against_the_unmatched_build() {
        let identity = DecisionReason::NoReuseCandidate(NoCandidate::IdentityMismatch);
        let shape7 = |current: i64, recorded: Option<i64>| {
            let mut n = anchored(
                DecisionKind::Build,
                identity,
                current,
                recorded.unwrap_or(0),
            );
            n.unmatched = n.anchor.take().filter(|_| recorded.is_some());
            explain_lines(&n)
        };
        let drifted = shape7(5_000, Some(900));
        assert_eq!(drifted[1].children[0].text, "the model query has changed");
        assert_eq!(
            drifted[2].children[0].children[0].badge,
            Some(BADGE_OUTDATED)
        );
        assert_eq!(
            freshness_texts(&drifted),
            ["upstream dependencies", "upstream data is outdated"]
        );
        assert_eq!(
            freshness_texts(&shape7(900, Some(900))),
            ["upstream dependencies", "upstream data is up to date"]
        );
        assert_eq!(
            freshness_texts(&shape7(800, Some(900))),
            ["upstream dependencies"]
        );
        let unrecorded = shape7(5_000, None);
        assert_eq!(freshness_texts(&unrecorded), ["upstream dependencies"]);
    }

    /// The same stale rebuild over a RegisterClone copy it could not adopt
    /// (`copy_logic_mismatch`) is not captured with a Lag tolerance line, so
    /// it renders nothing even with the older build's recorded age (#104).
    #[test]
    fn stale_copy_logic_mismatch_under_drift_renders_nothing() {
        let mut n = anchored(
            DecisionKind::Build,
            DecisionReason::NoReuseCandidate(NoCandidate::CopyLogicMismatch),
            5_000,
            900,
        );
        n.unmatched = n.anchor.take().map(|mut copy| {
            copy.clone_source_table = Some("\"DB\".\"PROD\".\"T\"".into());
            copy.clone_source_inputs =
                Some(copy.input_tables.iter().map(|t| t.name.clone()).collect());
            copy
        });
        n.tolerance_ms = 2_700_000;
        assert!(!explain_lines(&n).is_empty(), "fresh copy logic mismatch");
        n.is_stale = true;
        n.build_age_ms = Some(32_000);
        assert!(explain_lines(&n).is_empty());
    }

    /// VERIFIED LIVE (2026-11-11, explain_freshness_cells.jsonl `fr_*`): an
    /// identity-mismatch rebuild whose older matching build would have been
    /// reused stale ends SHAPE 7 with the Lag tolerance line of that older
    /// build's age. A row that recorded no build age for it (written before
    /// #104) renders nothing instead of an unknown age.
    #[test]
    fn shape7_with_a_stale_older_match_under_drift_has_the_tolerance_line() {
        let mut n = anchored(
            DecisionKind::Build,
            DecisionReason::NoReuseCandidate(NoCandidate::IdentityMismatch),
            5_000,
            900,
        );
        n.unmatched = n.anchor.take();
        n.tolerance_ms = 2_700_000;
        n.is_stale = true;
        n.build_age_ms = Some(32_000);
        let lines = explain_lines(&n);
        assert_eq!(
            freshness_texts(&lines),
            [
                "upstream dependencies",
                "upstream data is outdated",
                "the model was last built 32 seconds ago, within lag tolerance of 45 minutes"
            ]
        );
        let tolerance = lines[2].children.last().unwrap();
        assert_eq!(tolerance.marker, Some(MARKER_SUCCESS));
        assert_eq!(tolerance.badge, Some(BADGE_WITHIN_TOLERANCE));

        n.build_age_ms = None;
        assert!(explain_lines(&n).is_empty());
        n.is_stale = false;
        assert_eq!(
            freshness_texts(&explain_lines(&n)),
            ["upstream dependencies", "upstream data is outdated"]
        );
    }

    fn freshness_texts(lines: &[qc::ExplainLine]) -> Vec<String> {
        let section = lines
            .iter()
            .find(|l| l.text.starts_with("data freshness analysis"))
            .expect("freshness section");
        section.children.iter().map(|l| l.text.clone()).collect()
    }

    /// VERIFIED LIVE (2026-11-10, lag_seed_upstream 32, explain_tolerance 1,
    /// lag_tolerance_matrix `dsdec_young`): an anchored SKIP ends its freshness
    /// section with `upstream data is up to date` only when every upstream
    /// epoch equals its recorded epoch. An older-than-recorded upstream is
    /// fresh, with no summary line.
    #[test]
    fn fresh_skip_summary_needs_every_upstream_epoch_unchanged() {
        let skip = |current, reuse| {
            freshness_texts(&explain_lines(&anchored(
                DecisionKind::Skip,
                DecisionReason::Reused(reuse),
                current,
                900,
            )))
        };
        assert_eq!(
            skip(900, Reuse::Fresh),
            ["upstream dependencies", "upstream data is up to date"]
        );
        assert_eq!(
            skip(700, Reuse::WithinLagTolerance),
            ["upstream dependencies"]
        );
    }

    /// VERIFIED LIVE (2026-11-10, lag_copy_candidates `dt`, `dt_m`; 2026-11-11,
    /// explain_reason_kind_cells.jsonl `*_d`): a rebuild of an absent own
    /// target reads like a first build, for a dropped Target relation and for
    /// one dropped while the logic changed, for every model kind (the noun
    /// follows the kind). No other reason was captured over an absent target.
    #[test]
    fn rebuild_of_an_absent_target_is_a_first_build() {
        for reason in [
            DecisionReason::NoReuseCandidate(NoCandidate::OwnTargetMissing),
            DecisionReason::NoReuseCandidate(NoCandidate::IdentityMismatch),
        ] {
            let kinds = [
                ModelExecutionType::Full,
                ModelExecutionType::Snapshot,
                ModelExecutionType::View,
                ModelExecutionType::DbtCustom,
            ];
            for et in kinds.into_iter().chain(INCREMENTALS) {
                let mut n = anchored(DecisionKind::Build, reason, 900, 900);
                n.execution_type = et as i32;
                n.input_tables.retain(|t| t.name != "\"DB\".\"S\".\"T\"");
                let lines = explain_lines(&n);
                assert_eq!(lines.len(), 1, "{reason:?} {et:?}: {:?}", render(&n));
                assert_eq!(
                    lines[0].children[0].text,
                    format!("the {} table does not exist", n.noun())
                );
            }
        }
        let mut n = anchored(
            DecisionKind::Build,
            DecisionReason::DependencySetChanged,
            900,
            900,
        );
        n.input_tables.retain(|t| t.name != "\"DB\".\"S\".\"T\"");
        assert!(explain_lines(&n).is_empty(), "{:?}", render(&n));
    }

    /// The incremental strategies and a snapshot without genuine upstreams
    /// render SHAPE 0 like a FULL model: `no upstream dependencies`, and no
    /// upstream query line, for a reuse and for a logic change (VERIFIED LIVE
    /// 2026-11-11, explain_reason_kind_cells.jsonl `*_z`).
    #[test]
    fn incrementals_and_snapshots_without_upstreams_say_no_upstream_dependencies() {
        let kinds = [ModelExecutionType::Snapshot];
        for et in kinds.into_iter().chain(INCREMENTALS) {
            for reason in [
                DecisionReason::Reused(Reuse::Fresh),
                DecisionReason::NoReuseCandidate(NoCandidate::IdentityMismatch),
            ] {
                let class = if reason == DecisionReason::Reused(Reuse::Fresh) {
                    DecisionKind::Skip
                } else {
                    DecisionKind::Build
                };
                let mut n = without_upstreams(anchored(class, reason, 900, 900));
                if class == DecisionKind::Build {
                    n.unmatched = n.anchor.take();
                }
                n.execution_type = et as i32;
                let lines = explain_lines(&n);
                assert_eq!(lines.len(), 3, "{et:?} {reason:?}: {:?}", render(&n));
                assert_eq!(
                    lines[1].children.len(),
                    1,
                    "{et:?} {reason:?}: {:?}",
                    render(&n)
                );
                assert_eq!(
                    freshness_texts(&lines),
                    ["no upstream dependencies"],
                    "{et:?} {reason:?}"
                );
            }
        }
    }

    /// An Upstream producer change renders SHAPE 9 with the per-upstream query
    /// listing for every incremental strategy, as for FULL (VERIFIED LIVE
    /// 2026-11-11, explain_reason_kind_cells.jsonl `*_q`), with the build.
    #[test]
    fn incremental_upstream_query_change_is_shape9() {
        let changed = DecisionReason::UpstreamQueryChanged;
        let mut full = anchored(DecisionKind::Build, changed, 5_000, 900);
        full.input_tables[1].producer = Producer::Build("b".into());
        let expected = render(&full);
        assert!(!expected.is_empty());
        for et in INCREMENTALS {
            let mut n = full.clone();
            n.execution_type = et as i32;
            assert_eq!(render(&n), expected, "{et:?}");
            n.anchor = None;
            assert!(explain_lines(&n).is_empty(), "{et:?}");
        }
    }

    /// VERIFIED LIVE (2026-11-10, lag_copy_candidates `fe`, `st`, `wba`): a
    /// copy over an existing target whose own build had to rebuild says the
    /// table is old, analyses the target against its own build, and
    /// summarises how fresh the copied equivalent is.
    #[test]
    fn copy_over_an_existing_target_is_old_then_judged() {
        let copy = |is_stale: bool, tolerance_ms, build_age_ms| {
            let mut n = anchored(DecisionKind::Clone, DecisionReason::ResultCopy, 5_000, 900);
            n.clone_source = Some("\"DB\".\"S\".\"SRC\"".into());
            n.is_stale = is_stale;
            n.tolerance_ms = tolerance_ms;
            n.build_age_ms = build_age_ms;
            explain_lines(&n)
        };
        let fresh = copy(false, 0, None);
        let table: Vec<_> = fresh[0].children.iter().map(|l| l.text.clone()).collect();
        assert_eq!(
            table,
            [
                "the model table exists already",
                "but it's old, so we will clone from \"DB\".\"S\".\"SRC\" to recreate it"
            ]
        );
        assert_eq!(fresh[1].text, "query analysis (vs clone)");
        let freshness = &fresh[2];
        assert_eq!(freshness.text, "data freshness analysis (vs clone)");
        assert_eq!(
            freshness.children[0].children[0].badge,
            Some(BADGE_OUTDATED),
            "the target's upstream is judged against the target's own build"
        );
        assert_eq!(
            freshness_texts(&fresh),
            ["upstream dependencies", "upstream data is up to date"]
        );

        let stale = copy(true, 600_000, Some(3_000));
        assert_eq!(
            freshness_texts(&stale),
            [
                "upstream dependencies",
                "upstream data is outdated",
                "the model was last built 3 seconds ago, within lag tolerance of 10 minutes"
            ]
        );

        // Without the target's own build there is nothing to call old: the
        // ordinary copy tree.
        let mut plain = anchored(DecisionKind::Clone, DecisionReason::ResultCopy, 5_000, 900);
        plain.clone_source = Some("\"DB\".\"S\".\"SRC\"".into());
        plain.anchor = None;
        assert_eq!(
            explain_lines(&plain)[0].children[0].text,
            "the model table does not exist"
        );

        // VERIFIED LIVE (2026-11-10, lag_dropped_targets `dset600`): a stale
        // copy into a dropped target is the ordinary copy tree, anchored on
        // the dropped build, with the stale summary.
        let mut dropped = anchored(DecisionKind::Clone, DecisionReason::ResultCopy, 5_000, 900);
        dropped
            .input_tables
            .retain(|t| t.name != "\"DB\".\"S\".\"T\"");
        dropped.clone_source = Some("\"DB\".\"S\".\"SRC\"".into());
        dropped.is_stale = true;
        dropped.tolerance_ms = 600_000;
        dropped.build_age_ms = Some(4_000);
        let lines = explain_lines(&dropped);
        assert_eq!(lines[0].children[0].text, "the model table does not exist");
        assert_eq!(lines[2].children[0].children[0].badge, Some(BADGE_OUTDATED));
        assert_eq!(
            freshness_texts(&lines),
            [
                "upstream dependencies",
                "upstream data is outdated",
                "the model was last built 4 seconds ago, within lag tolerance of 10 minutes"
            ]
        );
    }

    /// A row written before the decision reason was persisted (NULL reason)
    /// renders an empty tree, the same as an uncharacterized node kind: Explain
    /// has nothing to select a shape from.
    #[test]
    fn null_decision_reason_renders_an_empty_tree() {
        for (d, desc) in [
            (DecisionKind::Skip, decision::DESC_SKIP),
            (
                DecisionKind::Build,
                "model was executed because its table did not exist",
            ),
            (DecisionKind::Build, decision::DESC_EXECUTE),
            (
                DecisionKind::Clone,
                "an equivalent model exists under another name so we cloned that one",
            ),
        ] {
            let mut n = node(d, ModelExecutionType::Full as i32, desc);
            n.clone_source = Some("\"DB\".\"S\".\"SRC\"".into());
            n.reason = None;
            assert!(
                explain_lines(&n).is_empty(),
                "a NULL reason must render nothing for {d:?} ({desc}), got {:?}",
                render(&n)
            );
            assert_eq!(
                explain_entry(&n).decision_description,
                desc,
                "the description is still served"
            );
        }
    }

    #[test]
    fn decision_description_prefers_persisted_situation_string() {
        let n = node(
            DecisionKind::Skip,
            ModelExecutionType::Full as i32,
            "model was a no-op because its query is up to date and its data is within lag tolerance",
        );
        assert_eq!(
            decision_description(&n),
            "model was a no-op because its query is up to date and its data is within lag tolerance"
        );
    }
}
