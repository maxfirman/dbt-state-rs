//! Pure freshness policy for a prevalidated confirmed candidate.
//! Store/services validate fingerprints, target existence and cached outcomes.
//! Exact physical upstream identities and known, unambiguous metadata are
//! required. Freshness compares each upstream epoch against the node's own
//! last-build epoch (ANY/ALL policy); this anchor and the no-tolerance-
//! absorption behavior were verified live — see correctness-handoff.md.

use crate::query_cache as qc;
use crate::store::{ExecutionRow, InputTable};

/// stale_upstream_policy values (shared.proto StaleUpstreamPolicy).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StaleUpstreamPolicy {
    /// ANY upstream drift → stale (default).
    Any,
    /// Only stale when ALL upstreams drift.
    All,
}

impl StaleUpstreamPolicy {
    pub fn from_i32(v: i32) -> Self {
        match v {
            1 => Self::All,
            _ => Self::Any,
        }
    }
}

/// Decision verdict with the fields needed to build a SubmitSQLResponse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Skip {
        description: String,
        /// Hosted `is_stale`: true when upstream drift IS present but the node is
        /// still reused (e.g. ALL policy and not every direct parent drifted).
        /// VERIFIED LIVE: a skip serving slightly-stale data reports is_stale=true;
        /// a no-drift skip and every execute report false. See lag_freshness.rs.
        is_stale: bool,
    },
    Execute {
        description: String,
        skip_rejection_reason: i32,
        clone_rejection_reason: i32,
        /// Whether the execute was driven by upstream staleness (vs a hash miss).
        is_stale: bool,
    },
}

pub const DESC_SKIP: &str =
    "model was a no-op because both its query and its upstream data are up to date";
pub const DESC_EXECUTE: &str =
    "model was executed because either its query didn't match or its upstream data is out of date";

// RejectionReason enum values (from shared.proto).
pub const REJECTION_NO_SUITABLE_MATCH_FOUND: i32 = 6;

// ModelExecutionType values relevant to node-kind-specific wording.
const ET_SNAPSHOT: i32 = 7;
const ET_DBT_DATA_TEST: i32 = 8;
const ET_VALUES: i32 = 9;

/// A human noun for the node kind, matching the hosted service's wording.
fn node_noun(execution_type: i32) -> &'static str {
    match execution_type {
        ET_SNAPSHOT => "snapshot",
        ET_DBT_DATA_TEST => "data test",
        ET_VALUES => "seed",
        _ => "model",
    }
}

/// SKIP description, node-kind specific (verified live against api.state.dbt.com).
fn skip_description(execution_type: i32) -> String {
    match execution_type {
        ET_VALUES => "seed was a no-op because its data has not changed".to_string(),
        ET_DBT_DATA_TEST => {
            "data test was a no-op because both its query and its upstream data are up to date"
                .to_string()
        }
        ET_SNAPSHOT => {
            "snapshot was a no-op because both its query and its upstream data are up to date"
                .to_string()
        }
        _ => "model was a no-op because both its query and its upstream data are up to date"
            .to_string(),
    }
}

/// EXECUTE description, node-kind + reason specific. `had_prior` distinguishes a
/// first build ("did not exist" / "no prior execution") from a rebuild of an
/// existing node ("query didn't match or upstream out of date").
fn execute_description(execution_type: i32, had_prior: bool) -> String {
    match (execution_type, had_prior) {
        (ET_VALUES, false) => "seed was loaded because it did not exist".to_string(),
        (ET_VALUES, true) => "seed was loaded because its data changed".to_string(),
        (ET_DBT_DATA_TEST, _) => {
            "data test was executed because it has no prior execution or its query changed"
                .to_string()
        }
        (et, false) => format!("{} was executed because its table did not exist", node_noun(et)),
        (et, true) => format!(
            "{} was executed because either its query didn't match or its upstream data is out of date",
            node_noun(et)
        ),
    }
}

/// Inputs distilled from a SubmitEnrichedSQLRequest for the decision.
///
/// `freshness_tolerance_seconds` is deliberately absent: a non-zero tolerance
/// does not absorb upstream drift (verified live; a 300s tolerance did not
/// absorb a 120s upstream-newer-than-build gap). It is preserved on the wire
/// only, so the engine never sees it.
pub struct SubmitContext<'a> {
    pub execution_type: i32,
    pub input_tables: &'a [InputTable],
    /// The node's own target table (fully-qualified). Its presence in
    /// `input_tables` is for own-existence/freshness tracking and must NOT be
    /// treated as upstream drift.
    pub target_table: Option<&'a str>,
    pub stale_upstream_policy: StaleUpstreamPolicy,
    /// Whether the server has ANY confirmed prior build of this physical target
    /// (regardless of fingerprint/provenance). Distinguishes a genuine FIRST
    /// build ("did not exist") from a REBUILD whose logic fingerprint no longer
    /// matches ("query didn't match or upstream out of date"): hosted reports
    /// the rebuild rationale when the target existed, even if no fingerprint
    /// matched. Review #2 5th follow-up P1 #4.
    pub target_has_prior_build: bool,
}

/// Decide SKIP vs EXECUTE given the reuse candidate (if any; see
/// `reuse_candidate`).
pub fn decide(ctx: &SubmitContext, candidate: Option<&ExecutionRow>) -> Verdict {
    match candidate {
        // No reuse candidate. The target either
        // never existed (first build → "did not exist") or existed under a
        // different fingerprint (rebuild → "query didn't match / out of date").
        // `target_has_prior_build` carries the physical-existence signal.
        None => execute(ctx.execution_type, false, ctx.target_has_prior_build),
        Some(prev) => {
            let f = evaluate_freshness(ctx, prev);
            if f.should_rebuild {
                execute(ctx.execution_type, true, true)
            } else {
                // Reuse. The hosted service still reports is_stale=true when
                // upstream drift is present but tolerated (e.g. ALL policy with
                // only some parents drifted) — the reused result is knowingly a
                // little stale. A no-drift reuse reports is_stale=false.
                Verdict::Skip {
                    description: skip_description(ctx.execution_type),
                    is_stale: f.drift_present,
                }
            }
        }
    }
}

/// Result of the freshness evaluation: whether to rebuild (policy-applied) and
/// whether any upstream drift is present at all (for hosted is_stale reporting).
struct Freshness {
    should_rebuild: bool,
    drift_present: bool,
}

fn execute(execution_type: i32, _stale_driven: bool, had_prior: bool) -> Verdict {
    Verdict::Execute {
        description: execute_description(execution_type, had_prior),
        skip_rejection_reason: REJECTION_NO_SUITABLE_MATCH_FOUND,
        clone_rejection_reason: REJECTION_NO_SUITABLE_MATCH_FOUND,
        // VERIFIED LIVE: the hosted service reports is_stale=FALSE on every
        // EXECUTE, including an upstream-drift-driven rebuild (lag fixture entry
        // 5, and the multi-upstream ANY captures). is_stale is only ever true on
        // a tolerated-stale SKIP. The rebuild reason is carried by srr/crr, not
        // is_stale.
        is_stale: false,
    }
}

/// Metadata is already warehouse-qualified by clients. Preserve its physical
/// identity exactly: schemas and quoted case distinguish genuine dependencies.
/// Cross-environment mapping needs explicit provenance, not schema stripping.
pub(crate) fn physical_relation_key(name: &str) -> &str {
    name
}

/// The FRESH/OUTDATED verdict for a single genuine upstream parent, together
/// with the two epochs the verdict was derived from. `drifted == true` means
/// OUTDATED (the explain `OUTDATED` badge); `false` means FRESH.
///
/// `anchor_epoch` is the epoch this upstream was compared against with strict
/// `>`: the node's OWN last-build epoch for object-producing nodes, or this
/// upstream's RECORDED baseline epoch for a data test (which has no own build).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UpstreamVerdict {
    pub name: String,
    pub current_epoch: Option<i64>,
    pub anchor_epoch: i64,
    pub drifted: bool,
    /// True when this upstream's freshness could not be PROVEN (unknown current
    /// epoch, or a data-test upstream with no recorded baseline epoch). The
    /// pre-extraction engine short-circuited to a rebuild on the first such
    /// upstream regardless of policy, so the fold must force EXECUTE when any
    /// upstream is unprovable (an unprovable upstream is always also `drifted`).
    pub unprovable: bool,
}

/// Why the per-upstream freshness evidence resolved the way it did. These are
/// the mutually-exclusive outcomes `decide` folds into SKIP/EXECUTE and the
/// explain path renders as the "data freshness analysis" branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FreshnessEvidence {
    /// The genuine upstream name set changed (added/removed/ambiguous). The
    /// recorded dependency evidence no longer applies → safety rebuild.
    DependencySetChanged,
    /// No genuine upstreams to compare. A hash/identity match alone is safe to
    /// skip; there is no freshness axis to violate.
    NoUpstreams,
    /// An object-producing node has upstreams but no recorded own-build epoch,
    /// so freshness cannot be proven → safety rebuild.
    NoBuildAnchor,
    /// The upstreams were evaluated against an anchor: per-upstream verdicts and
    /// the considered/drifted counts are populated.
    Evaluated,
}

/// Shared, pure per-upstream freshness reasoning: the single source of truth for
/// the per-upstream FRESH/OUTDATED verdict and the considered/drifted counts.
/// It also detects a changed genuine upstream name set (added/removed/ambiguous)
/// and short-circuits to `DependencySetChanged`; the hosted service surfaces no
/// per-name diff for that case (#32), so the changed names are not retained.
/// Both `decide` (via `evaluate_freshness`) and the explain generator (ADR 0005)
/// call THIS helper, so the decision and its explanation can never drift. Pure:
/// no I/O, no DB, no async.
///
/// This returns the raw per-upstream signals WITHOUT applying the ANY/ALL
/// `stale_upstream_policy`; policy folding into a rebuild/skip is the caller's
/// job (`evaluate_freshness`), so the explain path sees the same per-parent
/// verdicts the policy was applied to.
///
/// The per-`UpstreamVerdict` `name`/`current_epoch` and this struct's
/// `upstreams`/`anchor_epoch`/`considered`/`drifted` are now consumed by the
/// explain generator (ADR 0005, ticket #29/#30 anchor render path) and the
/// decision fold, so the former struct-level dead-code allow is removed.
#[derive(Debug, Clone)]
pub(crate) struct UpstreamFreshness {
    pub evidence: FreshnessEvidence,
    /// Per-genuine-upstream verdicts; populated only when `Evaluated`.
    pub upstreams: Vec<UpstreamVerdict>,
    /// The anchor epoch for object-producing nodes (own last-build epoch); None
    /// for data tests (which anchor per upstream on the recorded baseline).
    pub anchor_epoch: Option<i64>,
    /// Number of genuine upstreams considered (len of `upstreams` when Evaluated).
    pub considered: usize,
    /// Number of considered upstreams that drifted (OUTDATED).
    pub drifted: usize,
}

pub(crate) fn upstream_freshness(ctx: &SubmitContext, prev: &ExecutionRow) -> UpstreamFreshness {
    // No tolerance applies here; see `SubmitContext`.
    let genuine = |name: &str| ctx.target_table != Some(name);

    // Name-set diff over GENUINE upstreams (the node's own target excluded).
    let current_names: std::collections::BTreeSet<&str> = ctx
        .input_tables
        .iter()
        .filter(|t| genuine(&t.name))
        .map(|t| t.name.as_str())
        .collect();
    let recorded_names: std::collections::BTreeSet<&str> = prev
        .input_tables
        .iter()
        .filter(|t| genuine(&t.name))
        .map(|t| t.name.as_str())
        .collect();
    let genuine_count = ctx.input_tables.iter().filter(|t| genuine(&t.name)).count();
    // Missing, additional or ambiguous dependency evidence: the recorded run no
    // longer describes this submit's dependency set → safety rebuild. We only
    // need to KNOW the set changed, not WHICH names changed: #32 captured that
    // the hosted service renders no dependency-set-diff subtree (it surfaces a
    // node/upstream-query change by physical FQN and never lists the
    // added/removed/ambiguous names), so the diff itself has no faithful reader.
    // See docs/explain-catalog.md § "Unobserved" (Dependency-set change).
    let added = !current_names.is_subset(&recorded_names);
    let removed = !recorded_names.is_subset(&current_names);
    // Fewer distinct current names than genuine inputs ⇒ duplicate/ambiguous.
    let ambiguous = current_names.len() != genuine_count;
    if added || removed || ambiguous {
        return UpstreamFreshness {
            evidence: FreshnessEvidence::DependencySetChanged,
            upstreams: Vec::new(),
            anchor_epoch: None,
            considered: 0,
            drifted: 0,
        };
    }

    // The genuine upstreams to evaluate (own target excluded). A node with none
    // (a source-less model, or a data test with no input freshness) is never
    // stale on this axis — a hash/identity match alone is safe to skip.
    let genuine_upstreams: Vec<&InputTable> = ctx
        .input_tables
        .iter()
        .filter(|t| genuine(&t.name))
        .collect();
    if genuine_upstreams.is_empty() {
        return UpstreamFreshness {
            evidence: FreshnessEvidence::NoUpstreams,
            upstreams: Vec::new(),
            anchor_epoch: None,
            considered: 0,
            drifted: 0,
        };
    }

    // Freshness anchor. For object-producing nodes it is the node's OWN
    // last-build epoch (strict `>`), verified live against api.state.dbt.com.
    // A missing own-build epoch means either a data test (which has no physical
    // object and anchors per-upstream on the RECORDED baseline — issue #2
    // finding 1) or an object-producing node we cannot prove fresh.
    let data_test = ctx.execution_type == ET_DBT_DATA_TEST;
    let own_build_epoch = match prev.last_modified_epoch {
        Some(epoch) => Some(epoch),
        None if data_test => None, // per-upstream recorded-baseline anchor below
        None => {
            // Object-producing node with no build epoch → cannot prove freshness.
            return UpstreamFreshness {
                evidence: FreshnessEvidence::NoBuildAnchor,
                upstreams: Vec::new(),
                anchor_epoch: None,
                considered: 0,
                drifted: 0,
            };
        }
    };

    // Per-upstream FRESH/OUTDATED verdicts. Any upstream whose evidence cannot
    // establish freshness (unknown current epoch, missing/ambiguous recorded
    // baseline, or a data test with no recorded baseline epoch) is OUTDATED
    // against a `0` anchor — identical to the old safety-rebuild short-circuits,
    // because `evaluate_freshness` turns any such drift into a rebuild and the
    // ANY/ALL fold treats "all drifted" as rebuild. We never skip on unprovable
    // freshness.
    let mut upstreams = Vec::with_capacity(genuine_upstreams.len());
    let mut drifted = 0usize;
    for current in &genuine_upstreams {
        // Locate the recorded counterpart(s). The name set was validated above
        // as an exact, non-ambiguous match on the CURRENT side, but a confirmed
        // row may still record the SAME physical relation MORE THAN ONCE (the
        // recorded set collapses duplicates when compared as a BTreeSet). `main`
        // treated such a duplicate recorded baseline as ambiguous and forced a
        // rebuild in BOTH `evaluate_freshness` and `data_test_upstream_freshness`
        // (`matches.next().is_some()` after the first → rebuild): we cannot prove
        // which epoch to anchor against, so we must not skip. Count the matches
        // and mark an ambiguous (>1) baseline unprovable (F1 regression).
        let mut recorded = prev
            .input_tables
            .iter()
            .filter(|t| physical_relation_key(&t.name) == physical_relation_key(&current.name));
        let first_recorded = recorded.next();
        let ambiguous_recorded = recorded.next().is_some();
        let recorded_epoch = first_recorded.and_then(|t| t.last_modified_epoch);
        // The anchor for THIS upstream: own-build epoch for object nodes, the
        // upstream's own recorded baseline for a data test.
        let anchor = match own_build_epoch {
            Some(e) => e,
            None => recorded_epoch.unwrap_or(0),
        };
        // Freshness is UNPROVABLE when the current epoch is unknown, when the
        // recorded baseline is ambiguous (duplicate recorded counterparts), or
        // (data test) the upstream has no recorded baseline epoch to anchor
        // against.
        let unprovable = current.last_modified_epoch.is_none()
            || ambiguous_recorded
            || (own_build_epoch.is_none() && recorded_epoch.is_none());
        let is_drift = match current.last_modified_epoch {
            // Unknown current epoch → unprovable → OUTDATED.
            None => true,
            // Ambiguous recorded baseline → unprovable → OUTDATED (force rebuild).
            Some(_) if ambiguous_recorded => true,
            // Data test with no recorded baseline epoch → unprovable → OUTDATED.
            Some(_) if own_build_epoch.is_none() && recorded_epoch.is_none() => true,
            Some(current_epoch) => current_epoch > anchor,
        };
        if is_drift {
            drifted += 1;
        }
        upstreams.push(UpstreamVerdict {
            name: current.name.clone(),
            current_epoch: current.last_modified_epoch,
            anchor_epoch: anchor,
            drifted: is_drift,
            unprovable,
        });
    }

    let considered = upstreams.len();
    UpstreamFreshness {
        evidence: FreshnessEvidence::Evaluated,
        upstreams,
        anchor_epoch: own_build_epoch,
        considered,
        drifted,
    }
}

/// Fold the shared per-upstream freshness signals into a rebuild/skip decision,
/// applying the ANY/ALL `stale_upstream_policy`. This is the ONLY place the
/// policy is applied; `upstream_freshness` stays policy-neutral so the explain
/// path can render the same per-parent verdicts the policy acted on.
fn evaluate_freshness(ctx: &SubmitContext, prev: &ExecutionRow) -> Freshness {
    let f = upstream_freshness(ctx, prev);
    match f.evidence {
        // Missing/ambiguous evidence or an unprovable anchor always rebuilds;
        // its is_stale is irrelevant (executes report false).
        FreshnessEvidence::DependencySetChanged | FreshnessEvidence::NoBuildAnchor => Freshness {
            should_rebuild: true,
            drift_present: true,
        },
        // Nothing to compare → a hash match alone is safe to skip.
        FreshnessEvidence::NoUpstreams => Freshness {
            should_rebuild: false,
            drift_present: false,
        },
        FreshnessEvidence::Evaluated => {
            // Any upstream whose freshness could not be proven forces a rebuild
            // regardless of policy — the pre-extraction engine short-circuited
            // to a rebuild on the first such upstream before reaching the
            // ANY/ALL fold.
            let any_unprovable = f.upstreams.iter().any(|u| u.unprovable);
            // A data test (no own-build epoch) gates ANY-style on its tested
            // data regardless of the configured policy; object-producing nodes
            // honor the ANY/ALL policy.
            let data_test = f.anchor_epoch.is_none();
            let drift_present = f.drifted > 0;
            let should_rebuild = if any_unprovable {
                true
            } else if data_test {
                drift_present
            } else {
                match ctx.stale_upstream_policy {
                    StaleUpstreamPolicy::Any => drift_present,
                    StaleUpstreamPolicy::All => f.drifted == f.considered,
                }
            };
            Freshness {
                should_rebuild,
                drift_present,
            }
        }
    }
}

/// Map a SubmitEnrichedSQLRequest into the fields we persist / match on.
pub fn node_body_hash_of(req: &qc::SubmitEnrichedSqlRequest) -> Option<String> {
    req.dbt_node_state
        .as_ref()
        .and_then(|s| s.node_body_hash.clone())
}

pub fn input_tables_of(req: &qc::SubmitEnrichedSqlRequest) -> Vec<InputTable> {
    req.tables
        .iter()
        .map(|t| InputTable {
            name: t.name.clone(),
            last_modified_epoch: t.last_modified_epoch,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx<'a>(tables: &'a [InputTable]) -> SubmitContext<'a> {
        SubmitContext {
            execution_type: 10,
            input_tables: tables,
            target_table: None,
            stale_upstream_policy: StaleUpstreamPolicy::Any,
            target_has_prior_build: false,
        }
    }

    fn confirmed(hash: &str, tables: Vec<InputTable>, built: Option<i64>) -> ExecutionRow {
        ExecutionRow {
            id: 1,
            org_id: "o".into(),
            target_table: "t".into(),
            execution_type: 10,
            node_body_hash: Some(hash.into()),
            node_sql_hash: None,
            node_namespace_logic_hash: None,
            table_namespace: Some("ns".into()),
            node_unique_id: None,
            last_modified_epoch: built,
            execution_runtime_ms: None,
            execution_results: None,
            input_tables: tables,
            status: "confirmed".into(),
            request_id: "r".into(),
            clone_source_table: None,
        }
    }

    fn tbl(name: &str, epoch: i64) -> InputTable {
        InputTable {
            name: name.into(),
            last_modified_epoch: Some(epoch),
        }
    }

    fn desc_of(v: &Verdict) -> String {
        match v {
            Verdict::Skip { description, .. } => description.clone(),
            Verdict::Execute { description, .. } => description.clone(),
        }
    }

    /// Node-type/reason-specific decision_description strings, pinned to the
    /// exact wording captured live from api.state.dbt.com.
    /// A data test has no physical target, so the server does not look up
    /// target existence for it (`reuse_candidate::for_data_test` reports
    /// `false`). Pin that its EXECUTE response would be the same either way.
    #[test]
    fn data_test_execute_ignores_target_existence() {
        let up = vec![tbl("a", 100)];
        let with = |prior: bool| {
            let mut c = data_test_ctx(&up);
            c.target_has_prior_build = prior;
            decide(&c, None)
        };
        assert_eq!(with(false), with(true));
    }

    #[test]
    fn decision_descriptions_match_hosted_wording() {
        let up = vec![tbl("a", 100)];
        // First build (no prior) → "did not exist" per node kind.
        let first = |et: i32| {
            let mut c = ctx(&up);
            c.execution_type = et;
            desc_of(&decide(&c, None))
        };
        assert_eq!(
            first(1),
            "model was executed because its table did not exist"
        );
        assert_eq!(
            first(10),
            "model was executed because its table did not exist"
        );
        assert_eq!(
            first(7),
            "snapshot was executed because its table did not exist"
        );
        assert_eq!(
            first(8),
            "data test was executed because it has no prior execution or its query changed"
        );
        assert_eq!(first(9), "seed was loaded because it did not exist");

        // Rebuild of an existing node (stale upstream) → "query didn't match …".
        let prev = confirmed("h", vec![tbl("a", 100)], Some(100));
        let drift = vec![tbl("a", 9_000_000)];
        let stale = |et: i32| {
            let mut c = ctx(&drift);
            c.execution_type = et;
            desc_of(&decide(&c, Some(&prev)))
        };
        assert_eq!(
            stale(1),
            "model was executed because either its query didn't match or its upstream data is out of date"
        );
        assert_eq!(
            stale(7),
            "snapshot was executed because either its query didn't match or its upstream data is out of date"
        );
        assert_eq!(stale(9), "seed was loaded because its data changed");

        // Skip (confirmed + fresh) → node-kind no-op wording.
        let fresh = vec![tbl("a", 100)];
        let skip = |et: i32| {
            let mut c = ctx(&fresh);
            c.execution_type = et;
            desc_of(&decide(&c, Some(&prev)))
        };
        assert_eq!(
            skip(1),
            "model was a no-op because both its query and its upstream data are up to date"
        );
        assert_eq!(
            skip(8),
            "data test was a no-op because both its query and its upstream data are up to date"
        );
        assert_eq!(
            skip(7),
            "snapshot was a no-op because both its query and its upstream data are up to date"
        );
        assert_eq!(skip(9), "seed was a no-op because its data has not changed");
    }

    #[test]
    fn physical_relation_key_preserves_schema() {
        assert_ne!(
            physical_relation_key("\"DB\".\"PROD_SCHEMA\".\"T\""),
            physical_relation_key("\"DB\".\"DEV_SCHEMA\".\"T\"")
        );
        assert_eq!(
            physical_relation_key("\"DB\".\"S\".\"T\""),
            "\"DB\".\"S\".\"T\""
        );
        assert_eq!(physical_relation_key("schema.table"), "schema.table");
    }

    /// Different physical upstreams require explicit cross-environment provenance.
    #[test]
    fn cross_environment_upstream_requires_explicit_mapping() {
        let recorded = vec![tbl("\"DB\".\"PROD\".\"CUSTOMERS\"", 1_000_000)];
        let current = vec![tbl("\"DB\".\"DEV\".\"CUSTOMERS\"", 500_000)]; // dev, older
        let prev = confirmed("h", recorded, Some(1_000_000));
        let v = decide(&ctx(&current), Some(&prev));
        assert!(
            matches!(v, Verdict::Execute { .. }),
            "different physical inputs must not share evidence implicitly"
        );
    }

    #[test]
    fn no_history_executes_not_stale() {
        let v = decide(&ctx(&[]), None);
        match v {
            Verdict::Execute { is_stale, .. } => assert!(!is_stale),
            _ => panic!("expected execute"),
        }
    }

    #[test]
    fn match_with_fresh_inputs_skips() {
        let tables = vec![tbl("a", 100)];
        let prev = confirmed("h", tables.clone(), Some(100));
        let v = decide(&ctx(&tables), Some(&prev));
        assert!(matches!(v, Verdict::Skip { .. }));
    }

    #[test]
    fn match_with_newer_upstream_executes_stale() {
        let recorded = vec![tbl("a", 100)];
        let current = vec![tbl("a", 500)];
        let prev = confirmed("h", recorded, Some(100));
        let v = decide(&ctx(&current), Some(&prev));
        match v {
            // Upstream drift under ANY rebuilds; the hosted service reports
            // is_stale=FALSE on the execute (verified live, lag fixture entry 5).
            Verdict::Execute { is_stale, .. } => {
                assert!(!is_stale, "an execute always reports is_stale=false")
            }
            _ => panic!("expected execute"),
        }
    }

    #[test]
    fn self_table_freshness_change_still_skips() {
        let target = "\"DB\".\"S\".\"T\"";
        let recorded = vec![tbl(target, 100)];
        let current = vec![tbl(target, 999)];
        let mut prev = confirmed("h", recorded, Some(100));
        prev.target_table = target.to_string();
        let mut c = ctx(&current);
        c.target_table = Some(target);
        let v = decide(&c, Some(&prev));
        assert!(
            matches!(v, Verdict::Skip { .. }),
            "self-table change must skip"
        );
    }

    /// VERIFIED LIVE (api.state.dbt.com, Fusion client, Snowflake): a non-zero
    /// `freshness_tolerance_seconds` does NOT absorb upstream drift. The service
    /// rebuilds as soon as an upstream is newer than the own-build epoch, even
    /// with a large tolerance (a 300s tolerance did not absorb a 120s gap).
    /// The engine never receives the tolerance (see `SubmitContext`); the wire
    /// behavior is pinned with a non-zero tolerance by `lag_policy.rs`.
    #[test]
    fn freshness_tolerance_does_not_absorb_drift() {
        // own build epoch = 1_000_000; upstream only 1000ms newer.
        let recorded = vec![tbl("a", 1_000_000)];
        let current = vec![tbl("a", 1_001_000)];
        let prev = confirmed("h", recorded, Some(1_000_000));
        let c = ctx(&current);
        assert!(
            matches!(
                decide(&c, Some(&prev)),
                Verdict::Execute {
                    is_stale: false,
                    ..
                }
            ),
            "upstream newer than own build must execute even within tolerance"
        );
    }

    #[test]
    fn policy_any_executes_if_one_upstream_drifts() {
        let recorded = vec![tbl("a", 100), tbl("b", 100)];
        let current = vec![tbl("a", 100), tbl("b", 999)]; // b drifted
        let prev = confirmed("h", recorded, Some(100));
        let mut c = ctx(&current);
        c.stale_upstream_policy = StaleUpstreamPolicy::Any;
        assert!(matches!(decide(&c, Some(&prev)), Verdict::Execute { .. }));
    }

    #[test]
    fn policy_all_skips_if_one_upstream_fresh() {
        let recorded = vec![tbl("a", 100), tbl("b", 100)];
        let current = vec![tbl("a", 100), tbl("b", 999)]; // only b drifted
        let prev = confirmed("h", recorded, Some(100));
        let mut c = ctx(&current);
        c.stale_upstream_policy = StaleUpstreamPolicy::All;
        // ALL policy: stale only if every upstream drifted; a is fresh → skip
        assert!(matches!(decide(&c, Some(&prev)), Verdict::Skip { .. }));
    }

    #[test]
    fn policy_all_executes_if_all_upstreams_drift() {
        let recorded = vec![tbl("a", 100), tbl("b", 100)];
        let current = vec![tbl("a", 900), tbl("b", 999)]; // both drifted
        let prev = confirmed("h", recorded, Some(100));
        let mut c = ctx(&current);
        c.stale_upstream_policy = StaleUpstreamPolicy::All;
        assert!(matches!(decide(&c, Some(&prev)), Verdict::Execute { .. }));
    }

    // ---- is_stale reporting (VERIFIED LIVE, multi-upstream ALL policy) -----
    //
    // Live truth table (api.state.dbt.com, Fusion client, Snowflake, 2 upstreams
    // lp_up_a + lp_up_b, lag_experiment.sh):
    //   ANY, no drift          -> skip,    is_stale=false
    //   ANY, upstream drift     -> execute, is_stale=false
    //   ALL, one-of-two drift   -> skip,    is_stale=TRUE   (tolerated staleness)
    //   ALL, all drift          -> execute, is_stale=false
    // i.e. is_stale is true ONLY on a reuse that knowingly serves slightly stale
    // data (drift present but tolerated by the ALL policy). It is NEVER set on an
    // execute. lag_tolerance does NOT gate: ALL/one-drift skips even at lag=0, and
    // ANY/drift executes even within tolerance (own-build anchor). See
    // docs/correctness-handoff.md §C and crates/harness/tests/lag_freshness.rs.

    /// ALL policy, only one of two parents drifted → SKIP with is_stale=TRUE.
    #[test]
    fn all_policy_partial_drift_skips_with_is_stale_true() {
        let recorded = vec![tbl("a", 100), tbl("b", 100)];
        let current = vec![tbl("a", 100), tbl("b", 999)]; // only b drifted
        let prev = confirmed("h", recorded, Some(100));
        let mut c = ctx(&current);
        c.stale_upstream_policy = StaleUpstreamPolicy::All;
        match decide(&c, Some(&prev)) {
            Verdict::Skip { is_stale, .. } => {
                assert!(
                    is_stale,
                    "ALL-policy tolerated-staleness skip reports is_stale=true"
                )
            }
            _ => panic!("expected skip"),
        }
    }

    /// A no-drift skip reports is_stale=FALSE (both ANY and ALL).
    #[test]
    fn no_drift_skip_reports_is_stale_false() {
        let tables = vec![tbl("a", 100), tbl("b", 100)];
        let prev = confirmed("h", tables.clone(), Some(100));
        for policy in [StaleUpstreamPolicy::Any, StaleUpstreamPolicy::All] {
            let mut c = ctx(&tables);
            c.stale_upstream_policy = policy;
            match decide(&c, Some(&prev)) {
                Verdict::Skip { is_stale, .. } => {
                    assert!(!is_stale, "a fresh (no-drift) skip is not stale")
                }
                _ => panic!("expected skip"),
            }
        }
    }

    /// Under ANY policy, drift drives an EXECUTE and is_stale is FALSE (never set
    /// on an execute, even though drift is present).
    #[test]
    fn any_policy_drift_executes_with_is_stale_false() {
        let recorded = vec![tbl("a", 100), tbl("b", 100)];
        let current = vec![tbl("a", 100), tbl("b", 999)];
        let prev = confirmed("h", recorded, Some(100));
        let mut c = ctx(&current);
        c.stale_upstream_policy = StaleUpstreamPolicy::Any;
        assert!(matches!(
            decide(&c, Some(&prev)),
            Verdict::Execute {
                is_stale: false,
                ..
            }
        ));
    }

    // ---- Non-happy-path scenarios (hardening) ----------------------------

    /// MISSING TARGET / NO HISTORY: a confirmed row with `last_modified_epoch =
    /// None` (never recorded a build time) is not a safe baseline. A current
    /// upstream the recorded run never saw has no baseline at all → treat as
    /// drift → EXECUTE. The warehouse object may not exist, so we must not skip.
    #[test]
    fn unseen_upstream_without_baseline_executes() {
        let recorded: Vec<InputTable> = vec![]; // recorded run saw no inputs
        let current = vec![tbl("brand_new", 500)];
        let prev = confirmed("h", recorded, None); // no build epoch baseline
        let v = decide(&ctx(&current), Some(&prev));
        assert!(
            matches!(v, Verdict::Execute { .. }),
            "an upstream with no recorded baseline must execute, not skip"
        );
    }

    /// FRESHNESS BOUNDARY (verified): drift is a strict `>` comparison against
    /// the OWN BUILD epoch, not the recorded-upstream epoch, and tolerance does
    /// not shift it. Upstream == own-build is fresh (skip); own-build + 1ms is
    /// stale (execute). A large tolerance does not move this boundary.
    #[test]
    fn freshness_boundary_is_own_build_strict_greater_than() {
        let prev = confirmed("h", vec![tbl("a", 1_000_000)], Some(1_000_000));

        // Upstream exactly equal to own build: NOT drift → skip (even with tol).
        let at = vec![tbl("a", 1_000_000)];
        let c = ctx(&at);
        assert!(
            matches!(decide(&c, Some(&prev)), Verdict::Skip { .. }),
            "upstream equal to own build must skip"
        );

        // One ms newer than own build: drift → execute, tolerance notwithstanding.
        let beyond = vec![tbl("a", 1_000_001)];
        let c2 = ctx(&beyond);
        assert!(
            matches!(
                decide(&c2, Some(&prev)),
                Verdict::Execute {
                    is_stale: false,
                    ..
                }
            ),
            "upstream one ms newer than own build must execute even with 300s tolerance"
        );
    }

    /// Different schema input cannot match recorded physical evidence.
    #[test]
    fn upstream_schema_change_requires_execution() {
        let recorded = vec![tbl("\"DB\".\"PROD\".\"CUSTOMERS\"", 1_000_000)];
        let current = vec![tbl("\"DB\".\"DEV\".\"CUSTOMERS\"", 5_000_000)]; // newer, diff schema
        let prev = confirmed("h", recorded, Some(1_000_000));
        let v = decide(&ctx(&current), Some(&prev));
        assert!(
            matches!(
                v,
                Verdict::Execute {
                    is_stale: false,
                    ..
                }
            ),
            "a different physical input requires execution"
        );
    }

    /// Missing previously recorded input metadata cannot establish freshness.
    #[test]
    fn dropped_all_upstreams_requires_execution() {
        let recorded = vec![tbl("a", 100), tbl("b", 100)];
        let current: Vec<InputTable> = vec![]; // all upstreams removed this run
        let prev = confirmed("h", recorded, Some(100));
        assert!(
            matches!(decide(&ctx(&current), Some(&prev)), Verdict::Execute { .. }),
            "missing dependency evidence requires execution"
        );
    }

    /// ADDED UPSTREAM: a brand-new upstream not in the recorded run, newer than
    /// the recorded build, falls back to the recorded build epoch and is drift
    /// → EXECUTE under ANY. The new dependency's data was never incorporated.
    #[test]
    fn added_newer_upstream_executes_under_any() {
        let recorded = vec![tbl("a", 100)];
        let current = vec![tbl("a", 100), tbl("new_dep", 10_000_000)];
        let prev = confirmed("h", recorded, Some(100));
        let mut c = ctx(&current);
        c.stale_upstream_policy = StaleUpstreamPolicy::Any;
        assert!(
            matches!(
                decide(&c, Some(&prev)),
                Verdict::Execute {
                    is_stale: false,
                    ..
                }
            ),
            "a new, newer, untracked upstream must execute"
        );
    }

    /// CHANGED CONTRACT / CHANGED SQL: a different body hash means the store
    /// never returns a matching confirmed row for this fingerprint; the engine
    /// sees `confirmed = None` and must EXECUTE as a hash miss (is_stale=false,
    /// rejection NO_SUITABLE_MATCH_FOUND).
    #[test]
    fn changed_contract_is_a_hash_miss_execute() {
        // A changed contract changes the identity, so no reuse candidate exists.
        let v = decide(&ctx(&[tbl("a", 100)]), None);
        match v {
            Verdict::Execute {
                is_stale,
                skip_rejection_reason,
                clone_rejection_reason,
                ..
            } => {
                assert!(!is_stale, "a hash miss is not staleness-driven");
                assert_eq!(skip_rejection_reason, REJECTION_NO_SUITABLE_MATCH_FOUND);
                assert_eq!(clone_rejection_reason, REJECTION_NO_SUITABLE_MATCH_FOUND);
            }
            _ => panic!("expected execute"),
        }
    }

    /// Zero tolerance with any positive drift must execute (the common
    /// "freshness must be exact" configuration).
    #[test]
    fn zero_tolerance_any_drift_executes() {
        let prev = confirmed("h", vec![tbl("a", 100)], Some(100));
        let current = vec![tbl("a", 101)]; // 1ms newer
        let c = ctx(&current);
        assert!(matches!(
            decide(&c, Some(&prev)),
            Verdict::Execute {
                is_stale: false,
                ..
            }
        ));
    }

    /// OLDER upstream than our own build (clock skew / restore) must NOT be
    /// treated as drift — only an upstream strictly newer than the own-build
    /// epoch forces a rebuild.
    #[test]
    fn older_upstream_than_recorded_skips() {
        let prev = confirmed("h", vec![tbl("a", 1_000_000)], Some(1_000_000));
        let current = vec![tbl("a", 10)]; // older than recorded
        assert!(matches!(
            decide(&ctx(&current), Some(&prev)),
            Verdict::Skip { .. }
        ));
    }

    // ---- DATA-TEST upstream freshness (VERIFIED LIVE, issue #2 finding 1) ----
    //
    // Live truth (api.state.dbt.com, Fusion client, Snowflake;
    // experiments/data_test_freshness{,_control}.sh): a data test
    // (execution_type = 8) is confirmed with NO own object epoch
    // (ConfirmExecution.last_modified_epoch = None, only test results). A later
    // submit whose upstream input epoch is NEWER than the epoch recorded for
    // that upstream at the test's last confirmation EXECUTES ("data test was
    // executed because it has no prior execution or its query changed"); an
    // identical re-submit with the SAME upstream epoch SKIPS ("…a no-op because
    // both its query and its upstream data are up to date"). So a data test's
    // freshness anchor is the RECORDED UPSTREAM epoch (it has no own-build
    // epoch), compared per input with strict `>`.
    // Golden fixture: golden/fixtures/data_test_upstream_freshness.jsonl.

    fn data_test_ctx<'a>(tables: &'a [InputTable]) -> SubmitContext<'a> {
        SubmitContext {
            execution_type: ET_DBT_DATA_TEST,
            input_tables: tables,
            target_table: None,
            stale_upstream_policy: StaleUpstreamPolicy::Any,
            target_has_prior_build: false,
        }
    }

    /// A data test confirmed with no own epoch must EXECUTE when its upstream is
    /// newer than the epoch recorded at its last confirmation (VERIFIED LIVE).
    #[test]
    fn data_test_executes_when_upstream_newer_than_recorded() {
        let recorded = vec![tbl("DB.S.INPUT", 100)];
        let mut prev = confirmed("h", recorded, None); // tests have no build epoch
        prev.execution_type = ET_DBT_DATA_TEST;
        let current = vec![tbl("DB.S.INPUT", 200)]; // upstream advanced
        let v = decide(&data_test_ctx(&current), Some(&prev));
        assert!(
            matches!(v, Verdict::Execute { .. }),
            "a data test must re-run when its upstream data changed, even with no own epoch"
        );
    }

    /// Boundary: upstream exactly equal to the recorded epoch is fresh → SKIP
    /// (VERIFIED LIVE control arm).
    #[test]
    fn data_test_skips_when_upstream_equal_to_recorded() {
        let recorded = vec![tbl("DB.S.INPUT", 200)];
        let mut prev = confirmed("h", recorded, None);
        prev.execution_type = ET_DBT_DATA_TEST;
        let current = vec![tbl("DB.S.INPUT", 200)]; // unchanged
        let v = decide(&data_test_ctx(&current), Some(&prev));
        assert!(
            matches!(v, Verdict::Skip { .. }),
            "an unchanged data test (same upstream epoch) must skip"
        );
    }

    /// An older-or-equal upstream is still fresh (clock skew / restore): only a
    /// strictly newer upstream re-runs the test.
    #[test]
    fn data_test_skips_when_upstream_older_than_recorded() {
        let recorded = vec![tbl("DB.S.INPUT", 200)];
        let mut prev = confirmed("h", recorded, None);
        prev.execution_type = ET_DBT_DATA_TEST;
        let current = vec![tbl("DB.S.INPUT", 50)];
        assert!(matches!(
            decide(&data_test_ctx(&current), Some(&prev)),
            Verdict::Skip { .. }
        ));
    }

    /// A data test whose upstream carries an UNKNOWN (None) epoch cannot be
    /// proven fresh → EXECUTE (we never skip on unprovable freshness).
    #[test]
    fn data_test_executes_when_upstream_epoch_unknown() {
        let recorded = vec![tbl("DB.S.INPUT", 200)];
        let mut prev = confirmed("h", recorded, None);
        prev.execution_type = ET_DBT_DATA_TEST;
        let current = vec![InputTable {
            name: "DB.S.INPUT".into(),
            last_modified_epoch: None,
        }];
        assert!(matches!(
            decide(&data_test_ctx(&current), Some(&prev)),
            Verdict::Execute { .. }
        ));
    }

    /// A data test with NO genuine upstream inputs (e.g. a test over a seed or a
    /// source-less assertion) still skips on a pure identity match — there is no
    /// upstream freshness axis to violate.
    #[test]
    fn data_test_without_upstreams_still_skips_on_match() {
        let mut prev = confirmed("h", vec![], None);
        prev.execution_type = ET_DBT_DATA_TEST;
        let current: Vec<InputTable> = vec![];
        assert!(matches!(
            decide(&data_test_ctx(&current), Some(&prev)),
            Verdict::Skip { .. }
        ));
    }

    /// A data test over MULTIPLE upstreams re-runs if ANY one drifted past its
    /// recorded epoch (tests gate on their tested data, ANY-style).
    #[test]
    fn data_test_executes_if_any_upstream_drifts() {
        let recorded = vec![tbl("DB.S.A", 100), tbl("DB.S.B", 100)];
        let mut prev = confirmed("h", recorded, None);
        prev.execution_type = ET_DBT_DATA_TEST;
        let current = vec![tbl("DB.S.A", 100), tbl("DB.S.B", 999)]; // B drifted
        assert!(matches!(
            decide(&data_test_ctx(&current), Some(&prev)),
            Verdict::Execute { .. }
        ));
    }

    /// EPOCH MAGNITUDE INVARIANT: every `last_modified_epoch` (upstream tables,
    /// recorded build time, ConfirmExecution) is in MILLISECONDS — as sent by
    /// the real client and seen in the golden fixtures (13-digit values
    /// ~1.79e12). Freshness compares the upstream epoch directly against the
    /// own-build epoch in those native millisecond units; a 1ms difference is
    /// decisive. This pins that the comparison is not accidentally scaled or
    /// unit-mixed. `freshness_tolerance_seconds` is deliberately irrelevant here
    /// (verified live: it does not absorb upstream drift).
    #[test]
    fn freshness_compares_millisecond_epochs_against_own_build() {
        // Realistic millisecond epoch (matches golden fixture magnitudes).
        let base = 1_791_320_539_084i64;
        let prev = confirmed("h", vec![tbl("a", base)], Some(base));

        // Upstream equal to own build (to the ms) → fresh, even with tolerance.
        let at = vec![tbl("a", base)];
        let c = ctx(&at);
        assert!(
            matches!(decide(&c, Some(&prev)), Verdict::Skip { .. }),
            "upstream equal to own build (ms) is fresh"
        );

        // One millisecond newer than own build → stale, tolerance notwithstanding.
        let beyond = vec![tbl("a", base + 1)];
        let c2 = ctx(&beyond);
        assert!(
            matches!(
                decide(&c2, Some(&prev)),
                Verdict::Execute {
                    is_stale: false,
                    ..
                }
            ),
            "upstream 1ms newer than own build is stale regardless of tolerance"
        );
    }

    // ---- Shared pure helper: `upstream_freshness` (#28) --------------------
    //
    // These characterize the STRUCTURE the explain generator renders (per-ADR
    // 0005): the per-upstream FRESH/OUTDATED verdict, the considered/drifted
    // counts, and the dependency-set-change short-circuit. `decide` and the
    // explain path both call this ONE helper, so its returned struct is the
    // single source of truth for the "data freshness analysis" lines. The
    // proptests in decision_proptest.rs remain the regression oracle for
    // `decide` itself.

    /// Clean per-upstream evaluation: one fresh, one outdated, against the
    /// node's OWN build epoch (strict `>`). Counts and per-upstream verdicts are
    /// exposed; the clean (unchanged) name set reaches `Evaluated`.
    #[test]
    fn upstream_freshness_reports_per_upstream_verdicts_and_counts() {
        let recorded = vec![tbl("a", 100), tbl("b", 100)];
        let current = vec![tbl("a", 100), tbl("b", 999)]; // b drifted
        let prev = confirmed("h", recorded, Some(100));
        let f = upstream_freshness(&ctx(&current), &prev);

        // A clean, unchanged name set does not short-circuit to
        // DependencySetChanged; it reaches the per-upstream evaluation.
        assert!(matches!(f.evidence, FreshnessEvidence::Evaluated));
        assert_eq!(f.anchor_epoch, Some(100), "anchor is the own-build epoch");
        assert_eq!(f.considered, 2);
        assert_eq!(f.drifted, 1);

        // Per-upstream FRESH/OUTDATED verdicts are returned, not discarded.
        let a = f.upstreams.iter().find(|u| u.name == "a").unwrap();
        let b = f.upstreams.iter().find(|u| u.name == "b").unwrap();
        assert!(!a.drifted, "a (epoch 100 == own build) is FRESH");
        assert!(b.drifted, "b (epoch 999 > own build 100) is OUTDATED");
        assert_eq!(a.current_epoch, Some(100));
        assert_eq!(b.current_epoch, Some(999));
    }

    /// A changed genuine upstream name set short-circuits to
    /// `DependencySetChanged` regardless of HOW it changed (added, removed, or
    /// duplicate/ambiguous). #32 captured that the hosted service surfaces no
    /// per-name diff for this case, so the helper no longer retains WHICH names
    /// changed — only that the set is dirty drives the rebuild.
    #[test]
    fn upstream_freshness_reports_name_set_change() {
        let is_set_change = |rec: Vec<InputTable>, cur: &[InputTable]| {
            let prev = confirmed("h", rec, Some(100));
            let f = upstream_freshness(&ctx(cur), &prev);
            matches!(f.evidence, FreshnessEvidence::DependencySetChanged)
        };

        // ADDED: a current upstream the recorded run never saw.
        assert!(
            is_set_change(vec![tbl("a", 100)], &[tbl("a", 100), tbl("new_dep", 100)]),
            "an added genuine upstream is a dependency-set change"
        );
        // REMOVED: a recorded upstream absent from the current submit.
        assert!(
            is_set_change(vec![tbl("a", 100), tbl("gone", 100)], &[tbl("a", 100)]),
            "a removed genuine upstream is a dependency-set change"
        );
        // AMBIGUOUS: the same current name appears twice (distinct count < inputs).
        assert!(
            is_set_change(vec![tbl("a", 100)], &[tbl("a", 100), tbl("a", 200)]),
            "a duplicate/ambiguous current upstream is a dependency-set change"
        );
    }

    /// No genuine upstreams → the helper reports `NoUpstreams`: a hash match
    /// alone is safe to skip, nothing to render on the freshness axis.
    #[test]
    fn upstream_freshness_reports_no_upstreams() {
        let prev = confirmed("h", vec![], Some(100));
        let f = upstream_freshness(&ctx(&[]), &prev);
        assert!(matches!(f.evidence, FreshnessEvidence::NoUpstreams));
        assert_eq!(f.considered, 0);
        assert_eq!(f.drifted, 0);
    }

    /// An object-producing node with upstreams but no recorded own-build epoch
    /// cannot prove freshness → `NoBuildAnchor`.
    #[test]
    fn upstream_freshness_reports_missing_build_anchor() {
        let recorded = vec![tbl("a", 100)];
        let current = vec![tbl("a", 100)];
        let prev = confirmed("h", recorded, None); // no own-build epoch
        let f = upstream_freshness(&ctx(&current), &prev);
        assert!(matches!(f.evidence, FreshnessEvidence::NoBuildAnchor));
    }

    /// A data test (no own-build epoch) anchors on the RECORDED UPSTREAM epoch
    /// per input; a drifted upstream is OUTDATED against its own recorded
    /// baseline, not an own-build epoch.
    #[test]
    fn upstream_freshness_data_test_anchors_on_recorded_upstream() {
        let recorded = vec![tbl("DB.S.INPUT", 100)];
        let mut prev = confirmed("h", recorded, None);
        prev.execution_type = ET_DBT_DATA_TEST;
        let current = vec![tbl("DB.S.INPUT", 200)]; // newer than recorded 100
        let mut c = ctx(&current);
        c.execution_type = ET_DBT_DATA_TEST;
        let f = upstream_freshness(&c, &prev);

        assert!(matches!(f.evidence, FreshnessEvidence::Evaluated));
        assert_eq!(f.considered, 1);
        assert_eq!(f.drifted, 1);
        let u = &f.upstreams[0];
        assert_eq!(
            u.anchor_epoch, 100,
            "data test anchors on recorded upstream"
        );
        assert!(u.drifted, "200 > recorded 100 is OUTDATED");
    }

    /// REGRESSION (behavior parity): under the ALL policy, an upstream whose
    /// CURRENT epoch is unknown cannot be proven fresh. The pre-extraction
    /// engine short-circuited to a rebuild on the first such upstream REGARDLESS
    /// of policy (it never reached the ANY/ALL fold). The extracted helper must
    /// preserve that: an unprovable upstream forces EXECUTE even when a sibling
    /// is fresh and the policy is ALL (which would otherwise require every
    /// parent to drift). This is the exact edge the `tables_strategy` proptests
    /// never generate (they only emit `Some` epochs), so it is pinned here.
    #[test]
    fn unprovable_current_epoch_executes_under_all_policy() {
        let recorded = vec![tbl("a", 100), tbl("b", 100)];
        let current = vec![
            tbl("a", 100), // fresh
            InputTable {
                name: "b".into(),
                last_modified_epoch: None, // unknown → unprovable
            },
        ];
        let prev = confirmed("h", recorded, Some(100));
        let mut c = ctx(&current);
        c.stale_upstream_policy = StaleUpstreamPolicy::All;
        assert!(
            matches!(decide(&c, Some(&prev)), Verdict::Execute { .. }),
            "an unprovable upstream epoch must execute regardless of ALL policy"
        );
    }

    // ---- DUPLICATE RECORDED UPSTREAM (ambiguous baseline) — F1 regression ----
    //
    // The pre-extraction engine (`main`) rejected a confirmed baseline that
    // recorded MORE THAN ONE counterpart for a single current upstream (same
    // `physical_relation_key`) in BOTH `evaluate_freshness` (object nodes) and
    // `data_test_upstream_freshness`: `matches.next().is_some()` after the first
    // match forced a safety rebuild — the recorded baseline is ambiguous, so we
    // cannot prove which epoch to anchor against and must not skip. The #28
    // refactor replaced that per-upstream uniqueness check with a single
    // `.find()` (takes the FIRST recorded row), silently anchoring on whichever
    // duplicate comes first — a skip where `main` rebuilt (unsafe reuse). The
    // name-set diff does NOT catch this: `recorded_names` is a BTreeSet so the
    // duplicate collapses to one entry and `ambiguous` only measures the CURRENT
    // side. These pin the restored behavior for an object node AND a data test.

    /// OBJECT NODE: a confirmed row recording two physical counterparts for the
    /// same upstream (`a@200`, `a@50`) is an ambiguous baseline. A clean current
    /// submit (`a@100`, older than both) must REBUILD, not anchor on the first
    /// recorded row and SKIP. (`main` parity: `evaluate_freshness` uniqueness.)
    #[test]
    fn duplicate_recorded_upstream_executes_object_node() {
        let recorded = vec![tbl("a", 200), tbl("a", 50)]; // same name twice
        let current = vec![tbl("a", 100)]; // clean single current upstream
        let prev = confirmed("h", recorded, Some(100));
        let v = decide(&ctx(&current), Some(&prev));
        assert!(
            matches!(v, Verdict::Execute { .. }),
            "a duplicate recorded upstream is an ambiguous baseline → rebuild"
        );
    }

    /// DATA TEST: the same ambiguity on a data test (no own-build epoch, which
    /// anchors per-upstream on the RECORDED baseline). Two recorded `a` rows
    /// mean no single provable baseline → EXECUTE. (`main` parity:
    /// `data_test_upstream_freshness` uniqueness.)
    #[test]
    fn duplicate_recorded_upstream_executes_data_test() {
        let recorded = vec![tbl("DB.S.A", 200), tbl("DB.S.A", 50)]; // same name twice
        let mut prev = confirmed("h", recorded, None); // data tests have no build epoch
        prev.execution_type = ET_DBT_DATA_TEST;
        let current = vec![tbl("DB.S.A", 100)];
        let v = decide(&data_test_ctx(&current), Some(&prev));
        assert!(
            matches!(v, Verdict::Execute { .. }),
            "a data test with a duplicate recorded upstream has no provable baseline → rebuild"
        );
    }

    /// The ambiguity is surfaced on the shared helper itself: the matched
    /// upstream is `unprovable` and `drifted`, so the fold forces EXECUTE. This
    /// pins the mechanism (not just the folded verdict) the explain path reads.
    #[test]
    fn upstream_freshness_marks_duplicate_recorded_unprovable() {
        let recorded = vec![tbl("a", 200), tbl("a", 50)];
        let current = vec![tbl("a", 100)];
        let prev = confirmed("h", recorded, Some(100));
        let f = upstream_freshness(&ctx(&current), &prev);

        assert!(matches!(f.evidence, FreshnessEvidence::Evaluated));
        assert_eq!(f.considered, 1);
        let u = &f.upstreams[0];
        assert!(
            u.unprovable,
            "a duplicate recorded counterpart makes the upstream unprovable"
        );
        assert!(u.drifted, "an unprovable upstream is OUTDATED");
    }
}

#[cfg(test)]
#[path = "decision_proptest.rs"]
mod decision_proptest;
