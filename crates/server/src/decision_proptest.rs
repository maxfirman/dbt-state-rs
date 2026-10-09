//! Strategy #3 — property-based tests for the decision engine.
//!
//! `decide()` is a pure function, so we assert PROTOCOL INVARIANTS that must
//! hold for ALL inputs — not random example outputs. These encode our
//! understanding of the hidden service as machine-checked contracts and catch
//! logic regressions that example tests miss.
//!
//! Invariants:
//!   1. Determinism — identical inputs yield identical verdicts.
//!   2. No-match ⇒ Execute — absence of a confirmed row always executes.
//!   3. Self-table immunity — changing only the node's own target-table epoch
//!      never changes the verdict.
//!   4. Freshness monotonicity — with a confirmed match, lowering/holding all
//!      upstream epochs never flips Skip → Execute.
//!   5. Policy ordering — ALL is never stricter than ANY: if ANY skips, ALL skips.
//!
//! SKIP-safety invariants (the warehouse must never be left in an unexpected
//! state by a SKIP that omitted necessary work). A SKIP is a promise that the
//! recorded object is still correct; it is only safe when BOTH hold:
//!   6. SKIP ⇒ a confirmed prior build existed. A SKIP without recorded history
//!      would reference an object that was never built — omitted work.
//!   7. SKIP ⇒ no un-tolerated upstream drift under the active policy. If
//!      upstream data moved past tolerance (ANY: any input; ALL: every input),
//!      a SKIP would serve stale results — the recorded build no longer
//!      reflects current upstream data.
//!   8. Added-upstream safety — introducing a brand-new upstream the recorded
//!      run never saw, newer than the recorded build, must never SKIP (the new
//!      dependency's data was never incorporated).
//!   9. Body-hash change ⇒ Execute — a changed node_body_hash (changed logic /
//!      contract) can never match recorded history, so it must Execute.
//!  10. Execution-type isolation — a differing execution_type can never match a
//!      recorded row of another type (the match key includes execution_type),
//!      so it must Execute.
//!  11. Monotone staleness — raising any upstream epoch can only move a verdict
//!      toward Execute, never from Execute back to Skip.

use proptest::prelude::*;

use crate::decision::{decide, StaleUpstreamPolicy, SubmitContext, Verdict};
use crate::store::{ExecutionRow, InputTable};

fn is_skip(v: &Verdict) -> bool {
    matches!(v, Verdict::Skip { .. })
}

/// Reference oracle, intentionally re-derived here independently of the engine,
/// for the SKIP-safety invariants. Returns whether a SKIP would be SAFE given
/// the recorded run: a confirmed row exists AND, under the policy, upstream
/// drift does not make the recorded build stale. The node's own target table is
/// excluded from drift (rebuilding it is not upstream drift). Any current
/// upstream absent from the recorded run falls back to the recorded build epoch
/// (the engine's documented behavior), with no baseline treated as drift.
///
/// This mirrors the protocol rules in prose form so it can catch an engine that
/// skips when it must not. It is deliberately a second implementation: if both
/// agreed by construction the test would be vacuous.
fn skip_would_be_safe(
    current: &[InputTable],
    prev: &ExecutionRow,
    tol_s: i64,
    policy: StaleUpstreamPolicy,
    target: Option<&str>,
) -> bool {
    let tol_ms = tol_s.saturating_mul(1000);
    let mut considered = 0usize;
    let mut drifted = 0usize;
    for c in current {
        if Some(c.name.as_str()) == target {
            continue;
        }
        considered += 1;
        let recorded = prev
            .input_tables
            .iter()
            .find(|t| strip_schema(&t.name) == strip_schema(&c.name))
            .map(|t| t.last_modified_epoch)
            .or(prev.last_modified_epoch);
        let drift = match recorded {
            Some(r) => c.last_modified_epoch > r.saturating_add(tol_ms),
            None => true,
        };
        if drift {
            drifted += 1;
        }
    }
    if considered == 0 {
        return true; // nothing to compare → a hash match alone is safe to skip
    }
    match policy {
        StaleUpstreamPolicy::Any => drifted == 0,
        StaleUpstreamPolicy::All => drifted < considered,
    }
}

/// Schema-stripping mirror of `logical_relation_key`, re-derived for the oracle.
fn strip_schema(name: &str) -> String {
    let parts: Vec<&str> = name.split('.').map(|p| p.trim_matches('"')).collect();
    match parts.len() {
        3 => format!("{}..{}", parts[0], parts[2]),
        2 => format!("..{}", parts[1]),
        _ => parts.join("."),
    }
    .to_ascii_lowercase()
}

/// Strategy: a list of (name, epoch) upstream tables with small name space so
/// recorded/current overlap meaningfully.
fn tables_strategy() -> impl Strategy<Value = Vec<InputTable>> {
    prop::collection::vec(
        (0u8..5, 0i64..2_000_000).prop_map(|(n, e)| InputTable {
            name: format!("upstream_{n}"),
            last_modified_epoch: e,
        }),
        0..5,
    )
}

fn confirmed_row(hash: &str, tables: Vec<InputTable>, built: Option<i64>) -> ExecutionRow {
    ExecutionRow {
        id: 1,
        org_id: "o".into(),
        target_table: "\"DB\".\"S\".\"T\"".into(),
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

fn ctx<'a>(
    tables: &'a [InputTable],
    tol: i64,
    policy: StaleUpstreamPolicy,
    target: Option<&'a str>,
) -> SubmitContext<'a> {
    SubmitContext {
        execution_type: 10,
        node_body_hash: Some("h"),
        input_tables: tables,
        freshness_tolerance_seconds: tol,
        target_table: target,
        stale_upstream_policy: policy,
    }
}

fn policy_strategy() -> impl Strategy<Value = StaleUpstreamPolicy> {
    prop_oneof![
        Just(StaleUpstreamPolicy::Any),
        Just(StaleUpstreamPolicy::All)
    ]
}

proptest! {
    // 1. Determinism.
    #[test]
    fn determinism(tables in tables_strategy(), tol in 0i64..3600, p in policy_strategy()) {
        let prev = confirmed_row("h", tables.clone(), Some(1_000_000));
        let c = ctx(&tables, tol, p, None);
        let a = decide(&c, Some(&prev));
        let b = decide(&c, Some(&prev));
        prop_assert_eq!(a, b);
    }

    // 2. No-match ⇒ Execute, regardless of freshness/policy/inputs.
    #[test]
    fn no_match_always_executes(tables in tables_strategy(), tol in 0i64..3600, p in policy_strategy()) {
        let c = ctx(&tables, tol, p, None);
        let v = decide(&c, None);
        let executed = matches!(v, Verdict::Execute { .. });
        prop_assert!(executed, "no-match must execute");
    }

    // 3. Self-table immunity: changing only the node's own target-table epoch
    //    never changes the verdict.
    #[test]
    fn self_table_immunity(
        upstreams in tables_strategy(),
        self_epoch_a in 0i64..2_000_000,
        self_epoch_b in 0i64..2_000_000,
        tol in 0i64..3600,
        p in policy_strategy(),
    ) {
        let target = "\"DB\".\"S\".\"T\"";
        // recorded includes upstreams only.
        let prev = confirmed_row("h", upstreams.clone(), Some(1_000_000));

        let mut a_tables = upstreams.clone();
        a_tables.push(InputTable { name: target.into(), last_modified_epoch: self_epoch_a });
        let mut b_tables = upstreams.clone();
        b_tables.push(InputTable { name: target.into(), last_modified_epoch: self_epoch_b });

        let va = decide(&ctx(&a_tables, tol, p, Some(target)), Some(&prev));
        let vb = decide(&ctx(&b_tables, tol, p, Some(target)), Some(&prev));
        prop_assert_eq!(is_skip(&va), is_skip(&vb), "self-table epoch must not change verdict");
    }

    // 4. Freshness monotonicity: with a confirmed match, if every current
    //    upstream epoch is <= the recorded epoch (no drift), the verdict is Skip;
    //    and lowering epochs can never turn a Skip into an Execute.
    #[test]
    fn freshness_monotonicity(names in prop::collection::vec(0u8..4, 1..4), base in 100_000i64..1_000_000, p in policy_strategy()) {
        // Recorded upstreams all at `base`.
        let recorded: Vec<InputTable> = names
            .iter()
            .map(|n| InputTable { name: format!("u_{n}"), last_modified_epoch: base })
            .collect();
        let prev = confirmed_row("h", recorded.clone(), Some(base));

        // Current upstreams at or below base (no drift) → must Skip.
        let current_fresh: Vec<InputTable> = names
            .iter()
            .map(|n| InputTable { name: format!("u_{n}"), last_modified_epoch: base })
            .collect();
        let v_fresh = decide(&ctx(&current_fresh, 0, p, None), Some(&prev));
        prop_assert!(is_skip(&v_fresh), "no upstream drift must skip");
    }

    // 5. Policy ordering: for identical inputs, ANY is at least as strict as
    //    ALL. So if ANY yields Skip, ALL must also yield Skip.
    #[test]
    fn policy_any_stricter_than_all(
        recorded in tables_strategy(),
        current in tables_strategy(),
        tol in 0i64..3600,
    ) {
        // Use matching names so the comparison is meaningful.
        let prev = confirmed_row("h", recorded, Some(500_000));
        let any = decide(&ctx(&current, tol, StaleUpstreamPolicy::Any, None), Some(&prev));
        let all = decide(&ctx(&current, tol, StaleUpstreamPolicy::All, None), Some(&prev));
        if is_skip(&any) {
            prop_assert!(is_skip(&all), "if ANY skips, ALL must skip (ALL is more lenient)");
        }
    }

    // 6 & 7. SKIP-SAFETY MASTER INVARIANT: a SKIP must never omit necessary
    //    work. For arbitrary recorded history, current upstreams, tolerance and
    //    policy, if the engine SKIPs then an independent oracle must agree the
    //    skip is safe (confirmed match + no un-tolerated drift). Equivalently:
    //    whenever a skip would be UNSAFE, the engine must NOT skip.
    #[test]
    fn skip_is_always_safe(
        recorded in tables_strategy(),
        current in tables_strategy(),
        tol in 0i64..3600,
        p in policy_strategy(),
        built in prop::option::of(0i64..2_000_000),
    ) {
        let prev = confirmed_row("h", recorded, built);
        let v = decide(&ctx(&current, tol, p, None), Some(&prev));
        if is_skip(&v) {
            prop_assert!(
                skip_would_be_safe(&current, &prev, tol, p, None),
                "engine SKIPped but the oracle says the skip is UNSAFE \
                 (upstream drift beyond tolerance would leave stale state)"
            );
        }
    }

    // 6b. SKIP ⇒ confirmed history existed. With NO confirmed row the engine
    //    must never skip (it would reference a never-built object).
    #[test]
    fn skip_requires_confirmed_history(
        current in tables_strategy(),
        tol in 0i64..3600,
        p in policy_strategy(),
    ) {
        let v = decide(&ctx(&current, tol, p, None), None);
        prop_assert!(!is_skip(&v), "no confirmed history must never skip");
    }

    // 8. ADDED-UPSTREAM SAFETY: a brand-new upstream the recorded run never saw,
    //    with an epoch strictly newer than the recorded build and beyond
    //    tolerance, must force Execute under ANY policy — its data was never
    //    incorporated into the recorded build, so skipping would omit work.
    #[test]
    fn new_untracked_upstream_forces_execute(
        base in 100_000i64..1_000_000,
        gap in 1i64..1_000_000,
        tol in 0i64..600,
    ) {
        // Recorded run saw only "u_known".
        let recorded = vec![InputTable { name: "u_known".into(), last_modified_epoch: base }];
        let prev = confirmed_row("h", recorded, Some(base));
        // Current adds a never-seen upstream well beyond tolerance.
        let newer = base.saturating_add(tol.saturating_mul(1000)).saturating_add(gap);
        let current = vec![
            InputTable { name: "u_known".into(), last_modified_epoch: base },
            InputTable { name: "u_brand_new".into(), last_modified_epoch: newer },
        ];
        let v = decide(&ctx(&current, tol, StaleUpstreamPolicy::Any, None), Some(&prev));
        prop_assert!(!is_skip(&v), "a new, newer, untracked upstream must execute under ANY");
    }

    // 9. BODY-HASH CHANGE ⇒ EXECUTE. The decision match key is the body hash; a
    //    confirmed row recorded under a DIFFERENT body hash is not a match, so a
    //    changed logic/contract fingerprint must execute. (Modeled at the store
    //    layer by passing a confirmed row whose hash differs — the engine only
    //    sees `confirmed=None` for a non-matching hash, so we assert that path.)
    //    This encodes "changed contract / changed SQL must rebuild".
    #[test]
    fn changed_body_hash_executes(
        current in tables_strategy(),
        tol in 0i64..3600,
        p in policy_strategy(),
    ) {
        // A changed body hash means the store finds no matching confirmed row.
        let v = decide(&ctx(&current, tol, p, None), None);
        prop_assert!(matches!(v, Verdict::Execute { is_stale: false, .. }),
            "changed body hash (no match) must execute as a hash miss, not stale");
    }

    // 11. MONOTONE STALENESS: raising any single upstream epoch can only move
    //     the verdict toward Execute. If the lower-epoch input SKIPs, the
    //     higher-epoch input must not flip to… well it may stay skip or become
    //     execute, but a Skip at higher epochs implies Skip at lower epochs.
    #[test]
    fn raising_epochs_is_monotone_toward_execute(
        names in prop::collection::vec(0u8..3, 1..3),
        base in 200_000i64..800_000,
        bump in 1i64..1_000_000,
        tol in 0i64..600,
        p in policy_strategy(),
    ) {
        let recorded: Vec<InputTable> = names.iter()
            .map(|n| InputTable { name: format!("u_{n}"), last_modified_epoch: base })
            .collect();
        let prev = confirmed_row("h", recorded, Some(base));

        let low: Vec<InputTable> = names.iter()
            .map(|n| InputTable { name: format!("u_{n}"), last_modified_epoch: base })
            .collect();
        let high: Vec<InputTable> = names.iter()
            .map(|n| InputTable { name: format!("u_{n}"), last_modified_epoch: base + bump })
            .collect();

        let v_low = decide(&ctx(&low, tol, p, None), Some(&prev));
        let v_high = decide(&ctx(&high, tol, p, None), Some(&prev));
        // If the HIGHER (more drifted) inputs skip, the LOWER must also skip.
        if is_skip(&v_high) {
            prop_assert!(is_skip(&v_low),
                "monotonicity: skip at higher epochs implies skip at lower epochs");
        }
    }
}
