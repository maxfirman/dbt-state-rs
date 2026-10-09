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

use proptest::prelude::*;

use crate::decision::{decide, StaleUpstreamPolicy, SubmitContext, Verdict};
use crate::store::{ExecutionRow, InputTable};

fn is_skip(v: &Verdict) -> bool {
    matches!(v, Verdict::Skip { .. })
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
}
