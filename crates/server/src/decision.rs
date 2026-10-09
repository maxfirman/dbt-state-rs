//! The dbt State decision engine: given a submitted node and the recorded
//! execution history, decide SKIP / EXECUTE / CLONE.
//!
//! This mirrors the observed behavior of the hosted service. The client sends
//! precomputed semantic hashes in `dbt_node_state` (so the server does not need
//! a SQL engine for the core decision) plus per-input freshness in `tables`.
//!
//! Logic (refined via TDD against the golden corpus):
//!   - EXECUTE when there is no confirmed prior execution with a matching
//!     `node_body_hash` for this (org, target_table, execution_type).
//!   - EXECUTE when a matching record exists but upstream data is "stale"
//!     relative to the recorded run (per the stale_upstream_policy).
//!   - SKIP otherwise.
//!
//! stale_upstream_policy (from sql_service.proto):
//!   - ANY (0, default): every upstream must be within tolerance to skip; if
//!     ANY upstream drifted beyond tolerance the node is stale → EXECUTE.
//!     (matches dbt `updates_on=any`.)
//!   - ALL (1): at least one upstream must be within tolerance to skip; the node
//!     is stale only when ALL upstreams drifted. (matches
//!     `freshness.build_after.updates_on=all`.)

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
        // No prior execution with a matching fingerprint → the query didn't
        // match any suitable candidate → EXECUTE (not stale; a hash miss).
        None => execute(false),
        Some(prev) => {
            if is_stale(ctx, prev) {
                execute(true)
            } else {
                Verdict::Skip {
                    description: DESC_SKIP.to_string(),
                }
            }
        }
    }
}

fn execute(is_stale: bool) -> Verdict {
    Verdict::Execute {
        description: DESC_EXECUTE.to_string(),
        skip_rejection_reason: REJECTION_NO_SUITABLE_MATCH_FOUND,
        clone_rejection_reason: REJECTION_NO_SUITABLE_MATCH_FOUND,
        is_stale,
    }
}

/// Determine whether the node is stale relative to the recorded run, honoring
/// the stale_upstream_policy. Returns true → EXECUTE, false → candidate to SKIP.
/// Normalize a fully-qualified relation name to a logical identity that is
/// stable across environments: strip the middle (schema) component so that
/// `"DB"."PROD_SCHEMA"."T"` and `"DB"."DEV_SCHEMA"."T"` compare equal. The
/// hosted service reuses state across environments by logical identity, so
/// upstream freshness must be matched the same way. Names with other shapes are
/// returned lowercased/unquoted unchanged.
///
/// Splitting is quote-aware: a `.` inside a double-quoted identifier (e.g.
/// `"DB"."PROD"."my.table"`) is part of the identifier, NOT a component
/// separator. A naive `split('.')` would mis-count the parts and fail to strip
/// the schema, causing the SAME logical table in two environments to compare
/// UNEQUAL — a cross-environment false "execute" (stale SKIP avoided, but a
/// faithful SKIP lost). We therefore split on unquoted dots only.
pub(crate) fn logical_relation_key(name: &str) -> String {
    let parts = split_relation_parts(name);
    let joined = match parts.len() {
        // catalog.schema.table -> catalog..table (drop schema)
        3 => format!("{}..{}", parts[0], parts[2]),
        // schema.table -> ..table (drop schema)
        2 => format!("..{}", parts[1]),
        _ => parts.join("."),
    };
    joined.to_ascii_lowercase()
}

/// Split a dotted relation name into components, treating a `.` inside a
/// double-quoted segment as a literal (not a separator), and trimming the
/// surrounding quotes from each component. Mirrors how warehouses quote
/// identifiers that contain dots or reserved characters.
fn split_relation_parts(name: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    for ch in name.chars() {
        match ch {
            '"' => in_quotes = !in_quotes,
            '.' if !in_quotes => {
                parts.push(std::mem::take(&mut cur));
            }
            other => cur.push(other),
        }
    }
    parts.push(cur);
    parts
}

fn is_stale(ctx: &SubmitContext, prev: &ExecutionRow) -> bool {
    let tolerance_ms = ctx.freshness_tolerance_seconds.saturating_mul(1000);

    // Evaluate each genuine upstream input (excluding the node's own table).
    let mut considered = 0usize;
    let mut drifted = 0usize;

    // Index the recorded inputs by logical identity for cross-environment match.
    for current in ctx.input_tables {
        if let Some(target) = ctx.target_table {
            if current.name == target {
                continue; // own table modification is not upstream drift
            }
        }
        considered += 1;

        let cur_key = logical_relation_key(&current.name);
        let recorded = prev
            .input_tables
            .iter()
            .find(|t| logical_relation_key(&t.name) == cur_key)
            .map(|t| t.last_modified_epoch)
            // Fall back to the recorded node build time for inputs we never saw.
            .or(prev.last_modified_epoch);

        let is_drift = match recorded {
            Some(recorded_epoch) => {
                current.last_modified_epoch > recorded_epoch.saturating_add(tolerance_ms)
            }
            // No baseline at all → treat as drift (conservative execute).
            None => true,
        };
        if is_drift {
            drifted += 1;
        }
    }

    if considered == 0 {
        // No upstream inputs to compare (e.g. a source-less model); a hash match
        // alone means not stale.
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
            last_modified_epoch: t.last_modified_epoch.unwrap_or(0),
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
            table_namespace: Some("ns".into()),
            last_modified_epoch: built,
            execution_runtime_ms: None,
            input_tables: tables,
            status: "confirmed".into(),
            request_id: "r".into(),
        }
    }

    fn tbl(name: &str, epoch: i64) -> InputTable {
        InputTable {
            name: name.into(),
            last_modified_epoch: epoch,
        }
    }

    #[test]
    fn logical_relation_key_strips_environment_schema() {
        assert_eq!(
            logical_relation_key("\"DB\".\"PROD_SCHEMA\".\"T\""),
            logical_relation_key("\"DB\".\"DEV_SCHEMA\".\"T\"")
        );
        assert_eq!(logical_relation_key("\"DB\".\"S\".\"T\""), "db..t");
        assert_eq!(logical_relation_key("schema.table"), "..table");
    }

    /// Cross-environment reuse: a confirmed run under one schema makes the same
    /// logical node (same body hash, upstream reachable by logical identity)
    /// skip under a different schema even with an older epoch.
    #[test]
    fn cross_environment_upstream_matches_by_logical_identity() {
        let recorded = vec![tbl("\"DB\".\"PROD\".\"CUSTOMERS\"", 1_000_000)];
        let current = vec![tbl("\"DB\".\"DEV\".\"CUSTOMERS\"", 500_000)]; // dev, older
        let prev = confirmed("h", recorded, Some(1_000_000));
        let v = decide(&ctx("h", &current), Some(&prev));
        assert!(
            matches!(v, Verdict::Skip { .. }),
            "cross-env logical match must skip"
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

    #[test]
    fn freshness_tolerance_absorbs_small_drift() {
        let recorded = vec![tbl("a", 1_000_000)];
        // drift of 1000ms, tolerance 2s = 2000ms → within tolerance → skip
        let current = vec![tbl("a", 1_001_000)];
        let prev = confirmed("h", recorded, Some(1_000_000));
        let mut c = ctx("h", &current);
        c.freshness_tolerance_seconds = 2;
        assert!(matches!(decide(&c, Some(&prev)), Verdict::Skip { .. }));
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

    /// MODIFIED UPSTREAM exactly at the tolerance boundary. tolerance is a
    /// strict `>` comparison: drift == tolerance is still fresh (skip); drift ==
    /// tolerance+1ms is stale (execute). Pin both sides of the boundary.
    #[test]
    fn tolerance_boundary_is_strict_greater_than() {
        let prev = confirmed("h", vec![tbl("a", 1_000_000)], Some(1_000_000));

        // Exactly at tolerance (2000ms): NOT drift → skip.
        let at = vec![tbl("a", 1_002_000)];
        let mut c = ctx("h", &at);
        c.freshness_tolerance_seconds = 2;
        assert!(
            matches!(decide(&c, Some(&prev)), Verdict::Skip { .. }),
            "drift exactly at tolerance must skip"
        );

        // One ms beyond tolerance: drift → execute.
        let beyond = vec![tbl("a", 1_002_001)];
        let mut c2 = ctx("h", &beyond);
        c2.freshness_tolerance_seconds = 2;
        assert!(
            matches!(
                decide(&c2, Some(&prev)),
                Verdict::Execute { is_stale: true, .. }
            ),
            "drift one ms beyond tolerance must execute"
        );
    }

    /// SCHEMA CHANGE on an upstream (different environment schema, same logical
    /// table) must still match by logical identity. A dev-schema upstream newer
    /// than the recorded prod-schema upstream is genuine drift and must EXECUTE.
    #[test]
    fn upstream_schema_change_still_compared_by_logical_identity() {
        let recorded = vec![tbl("\"DB\".\"PROD\".\"CUSTOMERS\"", 1_000_000)];
        let current = vec![tbl("\"DB\".\"DEV\".\"CUSTOMERS\"", 5_000_000)]; // newer, diff schema
        let prev = confirmed("h", recorded, Some(1_000_000));
        let v = decide(&ctx("h", &current), Some(&prev));
        assert!(
            matches!(v, Verdict::Execute { is_stale: true, .. }),
            "a newer upstream (matched cross-schema) is drift and must execute"
        );
    }

    /// DELETED UPSTREAM: the current run no longer references an upstream the
    /// recorded run had. With no current inputs to compare, a hash match alone
    /// skips (there is nothing stale to force a rebuild). Pins the documented
    /// `considered == 0 => not stale` behavior.
    #[test]
    fn dropped_all_upstreams_skips_on_hash_match() {
        let recorded = vec![tbl("a", 100), tbl("b", 100)];
        let current: Vec<InputTable> = vec![]; // all upstreams removed this run
        let prev = confirmed("h", recorded, Some(100));
        assert!(
            matches!(
                decide(&ctx("h", &current), Some(&prev)),
                Verdict::Skip { .. }
            ),
            "no current upstreams to compare → hash match alone skips"
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

    /// OLDER upstream than recorded (clock skew / restore) must NOT be treated
    /// as drift — only strictly-newer data forces a rebuild.
    #[test]
    fn older_upstream_than_recorded_skips() {
        let prev = confirmed("h", vec![tbl("a", 1_000_000)], Some(1_000_000));
        let current = vec![tbl("a", 10)]; // older than recorded
        assert!(matches!(
            decide(&ctx("h", &current), Some(&prev)),
            Verdict::Skip { .. }
        ));
    }

    /// EPOCH-UNIT INVARIANT (F4): `freshness_tolerance_seconds` is in SECONDS,
    /// but every `last_modified_epoch` (upstream tables, recorded build time,
    /// ConfirmExecution) is in MILLISECONDS — as sent by the real client and
    /// seen in the golden fixtures (13-digit values ~1.79e12). The engine
    /// bridges the units with a single `* 1000`. This test pins that contract:
    /// a tolerance of T seconds absorbs exactly T*1000 ms of drift and no more.
    /// If a future change ever mixed the units (e.g. treated epochs as seconds),
    /// every freshness comparison would silently invert; this test fails loudly.
    #[test]
    fn tolerance_is_seconds_epochs_are_milliseconds() {
        // Realistic millisecond epoch (matches golden fixture magnitudes).
        let base = 1_791_320_539_084i64;
        let prev = confirmed("h", vec![tbl("a", base)], Some(base));

        // 5 seconds of tolerance = 5000 ms. Drift of exactly 5000 ms is fresh.
        let at = vec![tbl("a", base + 5_000)];
        let mut c = ctx("h", &at);
        c.freshness_tolerance_seconds = 5;
        assert!(
            matches!(decide(&c, Some(&prev)), Verdict::Skip { .. }),
            "5s tolerance must absorb exactly 5000ms of drift"
        );

        // 5001 ms of drift is beyond 5 s → stale.
        let beyond = vec![tbl("a", base + 5_001)];
        let mut c2 = ctx("h", &beyond);
        c2.freshness_tolerance_seconds = 5;
        assert!(
            matches!(
                decide(&c2, Some(&prev)),
                Verdict::Execute { is_stale: true, .. }
            ),
            "5001ms of drift exceeds a 5s (5000ms) tolerance → execute"
        );

        // Sanity: if epochs were mistakenly treated as seconds, a 5000ms drift
        // (which is only 5 "units") would be absorbed by ANY tolerance >= 5 and
        // the boundary at 5001 would NOT flip — the assertion above would fail.
    }
}

#[cfg(test)]
#[path = "decision_proptest.rs"]
mod decision_proptest;
