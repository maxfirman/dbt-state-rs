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
pub struct SubmitContext<'a> {
    pub execution_type: i32,
    pub node_body_hash: Option<&'a str>,
    pub input_tables: &'a [InputTable],
    pub freshness_tolerance_seconds: i64,
    /// The node's own target table (fully-qualified). Its presence in
    /// `input_tables` is for own-existence/freshness tracking and must NOT be
    /// treated as upstream drift.
    pub target_table: Option<&'a str>,
    pub stale_upstream_policy: StaleUpstreamPolicy,
}

/// Decide SKIP vs EXECUTE given the latest matching confirmed record (if any).
pub fn decide(ctx: &SubmitContext, confirmed: Option<&ExecutionRow>) -> Verdict {
    match confirmed {
        // No prior execution with a matching fingerprint → the node did not
        // exist / had no prior execution → EXECUTE (not stale; a hash miss).
        None => execute(ctx.execution_type, false, false),
        Some(prev) => {
            if is_stale(ctx, prev) {
                execute(ctx.execution_type, true, true)
            } else {
                Verdict::Skip {
                    description: skip_description(ctx.execution_type),
                }
            }
        }
    }
}

fn execute(execution_type: i32, is_stale: bool, had_prior: bool) -> Verdict {
    Verdict::Execute {
        description: execute_description(execution_type, had_prior),
        skip_rejection_reason: REJECTION_NO_SUITABLE_MATCH_FOUND,
        clone_rejection_reason: REJECTION_NO_SUITABLE_MATCH_FOUND,
        is_stale,
    }
}

/// Metadata is already warehouse-qualified by clients. Preserve its physical
/// identity exactly: schemas and quoted case distinguish genuine dependencies.
/// Cross-environment mapping needs explicit provenance, not schema stripping.
pub(crate) fn physical_relation_key(name: &str) -> &str {
    name
}

fn is_stale(ctx: &SubmitContext, prev: &ExecutionRow) -> bool {
    let tolerance_ms = ctx.freshness_tolerance_seconds.saturating_mul(1000);

    let genuine = |name: &str| ctx.target_table != Some(name);
    let current_names: std::collections::BTreeSet<_> = ctx
        .input_tables
        .iter()
        .filter(|t| genuine(&t.name))
        .map(|t| t.name.as_str())
        .collect();
    let recorded_names: std::collections::BTreeSet<_> = prev
        .input_tables
        .iter()
        .filter(|t| genuine(&t.name))
        .map(|t| t.name.as_str())
        .collect();
    if current_names != recorded_names
        || current_names.len() != ctx.input_tables.iter().filter(|t| genuine(&t.name)).count()
    {
        return true; // Missing, additional or ambiguous dependency evidence.
    }

    // Freshness anchor: the node's OWN last build epoch. A node is stale when an
    // upstream input is newer than the object we last built. This was verified
    // live against api.state.dbt.com (Fusion client, Snowflake) — see the lag
    // characterization in docs/correctness-handoff.md and the golden fixture:
    // the service rebuilds iff `max_upstream_epoch > own_last_build_epoch`,
    // comparing against the own-build epoch (NOT the upstream epoch recorded at
    // that build) and WITHOUT `freshness_tolerance_seconds` absorbing the gap
    // (a 300s tolerance did not absorb a 120s upstream-newer-than-build gap).
    //
    // `freshness_tolerance_seconds` is intentionally NOT applied here. Applying
    // it (widening the skip window) would reintroduce an unsafe skip that serves
    // data staler than the hosted service would: the only safe direction when a
    // policy is uncertain is to execute more, never skip more. See the F-series
    // tests below for the pinned boundary.
    let _ = tolerance_ms; // field preserved on the wire; see note above.

    // Evaluate each genuine upstream input (excluding the node's own table).
    // The own-build epoch only matters when there is an upstream to compare it
    // against: a node with no genuine upstreams (e.g. a source-less model or a
    // data test with no input freshness) is never stale on this axis, even if
    // it never recorded a build epoch.
    let genuine_upstreams: Vec<&InputTable> = ctx
        .input_tables
        .iter()
        .filter(|t| ctx.target_table != Some(&t.name))
        .collect();
    if genuine_upstreams.is_empty() {
        // No upstream inputs to compare → a hash match alone is safe to skip.
        return false;
    }

    let Some(own_build_epoch) = prev.last_modified_epoch else {
        // No own-build epoch recorded. Data tests (execution_type = 8) never
        // produce a physical object and so have no build epoch nor an own-build
        // freshness axis — they are governed by identity + logic match, and the
        // hosted service skips them on a match (verified in the test-node golden
        // fixture, where ConfirmExecution carries last_modified_epoch = None yet
        // the re-submit skips). For object-producing node types a missing build
        // epoch means we cannot prove freshness → execute conservatively rather
        // than risk serving stale data.
        return ctx.execution_type != ET_DBT_DATA_TEST;
    };

    let mut considered = 0usize;
    let mut drifted = 0usize;

    // Compare genuine inputs against recorded physical identities.
    for current in genuine_upstreams {
        considered += 1;

        let Some(current_epoch) = current.last_modified_epoch else {
            return true;
        };
        // The recorded dependency set is still validated for identity/ambiguity
        // (missing, extra or duplicate names already forced stale above and
        // here), but the drift decision anchors on the own-build epoch.
        let mut matches = prev
            .input_tables
            .iter()
            .filter(|t| physical_relation_key(&t.name) == physical_relation_key(&current.name));
        if matches.next().is_none() {
            return true;
        }
        if matches.next().is_some() {
            return true;
        }
        let is_drift = current_epoch > own_build_epoch;
        if is_drift {
            drifted += 1;
        }
    }

    if considered == 0 {
        // Defensive: unreachable given the genuine_upstreams emptiness check.
        return false;
    }

    match ctx.stale_upstream_policy {
        // ANY drift makes it stale.
        StaleUpstreamPolicy::Any => drifted > 0,
        // Stale only when every upstream drifted.
        StaleUpstreamPolicy::All => drifted == considered,
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

    fn ctx<'a>(hash: &'a str, tables: &'a [InputTable]) -> SubmitContext<'a> {
        SubmitContext {
            execution_type: 10,
            node_body_hash: Some(hash),
            input_tables: tables,
            freshness_tolerance_seconds: 0,
            target_table: None,
            stale_upstream_policy: StaleUpstreamPolicy::Any,
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
            table_namespace: Some("ns".into()),
            node_unique_id: None,
            last_modified_epoch: built,
            execution_runtime_ms: None,
            execution_results: None,
            input_tables: tables,
            status: "confirmed".into(),
            request_id: "r".into(),
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
            Verdict::Skip { description } => description.clone(),
            Verdict::Execute { description, .. } => description.clone(),
        }
    }

    /// Node-type/reason-specific decision_description strings, pinned to the
    /// exact wording captured live from api.state.dbt.com.
    #[test]
    fn decision_descriptions_match_hosted_wording() {
        let up = vec![tbl("a", 100)];
        // First build (no prior) → "did not exist" per node kind.
        let first = |et: i32| {
            let mut c = ctx("h", &up);
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
            let mut c = ctx("h", &drift);
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
            let mut c = ctx("h", &fresh);
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
        let v = decide(&ctx("h", &current), Some(&prev));
        assert!(
            matches!(v, Verdict::Execute { .. }),
            "different physical inputs must not share evidence implicitly"
        );
    }

    #[test]
    fn no_history_executes_not_stale() {
        let v = decide(&ctx("h", &[]), None);
        match v {
            Verdict::Execute { is_stale, .. } => assert!(!is_stale),
            _ => panic!("expected execute"),
        }
    }

    #[test]
    fn match_with_fresh_inputs_skips() {
        let tables = vec![tbl("a", 100)];
        let prev = confirmed("h", tables.clone(), Some(100));
        let v = decide(&ctx("h", &tables), Some(&prev));
        assert!(matches!(v, Verdict::Skip { .. }));
    }

    #[test]
    fn match_with_newer_upstream_executes_stale() {
        let recorded = vec![tbl("a", 100)];
        let current = vec![tbl("a", 500)];
        let prev = confirmed("h", recorded, Some(100));
        let v = decide(&ctx("h", &current), Some(&prev));
        match v {
            Verdict::Execute { is_stale, .. } => assert!(is_stale, "upstream drift is stale"),
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
        let mut c = ctx("h", &current);
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
    /// So any upstream newer than our build is stale regardless of tolerance.
    #[test]
    fn freshness_tolerance_does_not_absorb_drift() {
        // own build epoch = 1_000_000; upstream only 1000ms newer; tolerance 2s.
        let recorded = vec![tbl("a", 1_000_000)];
        let current = vec![tbl("a", 1_001_000)];
        let prev = confirmed("h", recorded, Some(1_000_000));
        let mut c = ctx("h", &current);
        c.freshness_tolerance_seconds = 2;
        assert!(
            matches!(
                decide(&c, Some(&prev)),
                Verdict::Execute { is_stale: true, .. }
            ),
            "upstream newer than own build must execute even within tolerance"
        );
    }

    #[test]
    fn policy_any_executes_if_one_upstream_drifts() {
        let recorded = vec![tbl("a", 100), tbl("b", 100)];
        let current = vec![tbl("a", 100), tbl("b", 999)]; // b drifted
        let prev = confirmed("h", recorded, Some(100));
        let mut c = ctx("h", &current);
        c.stale_upstream_policy = StaleUpstreamPolicy::Any;
        assert!(matches!(decide(&c, Some(&prev)), Verdict::Execute { .. }));
    }

    #[test]
    fn policy_all_skips_if_one_upstream_fresh() {
        let recorded = vec![tbl("a", 100), tbl("b", 100)];
        let current = vec![tbl("a", 100), tbl("b", 999)]; // only b drifted
        let prev = confirmed("h", recorded, Some(100));
        let mut c = ctx("h", &current);
        c.stale_upstream_policy = StaleUpstreamPolicy::All;
        // ALL policy: stale only if every upstream drifted; a is fresh → skip
        assert!(matches!(decide(&c, Some(&prev)), Verdict::Skip { .. }));
    }

    #[test]
    fn policy_all_executes_if_all_upstreams_drift() {
        let recorded = vec![tbl("a", 100), tbl("b", 100)];
        let current = vec![tbl("a", 900), tbl("b", 999)]; // both drifted
        let prev = confirmed("h", recorded, Some(100));
        let mut c = ctx("h", &current);
        c.stale_upstream_policy = StaleUpstreamPolicy::All;
        assert!(matches!(decide(&c, Some(&prev)), Verdict::Execute { .. }));
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
        let v = decide(&ctx("h", &current), Some(&prev));
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
        let mut c = ctx("h", &at);
        c.freshness_tolerance_seconds = 300;
        assert!(
            matches!(decide(&c, Some(&prev)), Verdict::Skip { .. }),
            "upstream equal to own build must skip"
        );

        // One ms newer than own build: drift → execute, tolerance notwithstanding.
        let beyond = vec![tbl("a", 1_000_001)];
        let mut c2 = ctx("h", &beyond);
        c2.freshness_tolerance_seconds = 300;
        assert!(
            matches!(
                decide(&c2, Some(&prev)),
                Verdict::Execute { is_stale: true, .. }
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
        let v = decide(&ctx("h", &current), Some(&prev));
        assert!(
            matches!(v, Verdict::Execute { is_stale: true, .. }),
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
            matches!(
                decide(&ctx("h", &current), Some(&prev)),
                Verdict::Execute { .. }
            ),
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
        let mut c = ctx("h", &current);
        c.stale_upstream_policy = StaleUpstreamPolicy::Any;
        assert!(
            matches!(
                decide(&c, Some(&prev)),
                Verdict::Execute { is_stale: true, .. }
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
        let v = decide(
            &ctx("new-hash-after-contract-change", &[tbl("a", 100)]),
            None,
        );
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
        let mut c = ctx("h", &current);
        c.freshness_tolerance_seconds = 0;
        assert!(matches!(
            decide(&c, Some(&prev)),
            Verdict::Execute { is_stale: true, .. }
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
            decide(&ctx("h", &current), Some(&prev)),
            Verdict::Skip { .. }
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
        let mut c = ctx("h", &at);
        c.freshness_tolerance_seconds = 300;
        assert!(
            matches!(decide(&c, Some(&prev)), Verdict::Skip { .. }),
            "upstream equal to own build (ms) is fresh"
        );

        // One millisecond newer than own build → stale, tolerance notwithstanding.
        let beyond = vec![tbl("a", base + 1)];
        let mut c2 = ctx("h", &beyond);
        c2.freshness_tolerance_seconds = 300;
        assert!(
            matches!(
                decide(&c2, Some(&prev)),
                Verdict::Execute { is_stale: true, .. }
            ),
            "upstream 1ms newer than own build is stale regardless of tolerance"
        );
    }
}

#[cfg(test)]
#[path = "decision_proptest.rs"]
mod decision_proptest;
