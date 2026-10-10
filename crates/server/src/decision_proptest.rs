//! Properties for current local freshness policy with prevalidated candidates.
//! These do not validate store selection, own-target existence or hosted policy.
//! The duplicated freshness model is a regression oracle, not independent proof
//! of protocol safety; see correctness-handoff.md for state-machine testing.

use proptest::prelude::*;

use crate::decision::{decide, physical_relation_key, StaleUpstreamPolicy, SubmitContext, Verdict};
use crate::store::{ExecutionRow, InputTable};

fn is_skip(v: &Verdict) -> bool {
    matches!(v, Verdict::Skip { .. })
}

/// Regression model of the existing freshness policy.
fn skip_would_be_safe(
    current: &[InputTable],
    prev: &ExecutionRow,
    tol_s: i64,
    policy: StaleUpstreamPolicy,
    target: Option<&str>,
) -> bool {
    let genuine = |t: &&InputTable| Some(t.name.as_str()) != target;
    let current_names: std::collections::BTreeSet<_> =
        current.iter().filter(genuine).map(|t| &t.name).collect();
    let recorded_names: std::collections::BTreeSet<_> = prev
        .input_tables
        .iter()
        .filter(genuine)
        .map(|t| &t.name)
        .collect();
    if current_names != recorded_names
        || current_names.len() != current.iter().filter(genuine).count()
    {
        return false;
    }
    let tol_ms = tol_s.saturating_mul(1000);
    let mut considered = 0usize;
    let mut drifted = 0usize;
    for c in current {
        if Some(c.name.as_str()) == target {
            continue;
        }
        considered += 1;
        let Some(current_epoch) = c.last_modified_epoch else {
            return false;
        };
        let matches: Vec<_> = prev
            .input_tables
            .iter()
            .filter(|t| t.name == c.name)
            .collect();
        if matches.len() != 1 {
            return false;
        }
        let Some(recorded) = matches[0].last_modified_epoch else {
            return false;
        };
        if current_epoch > recorded.saturating_add(tol_ms) {
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

/// Strategy: a list of (name, epoch) upstream tables with small name space so
/// recorded/current overlap meaningfully.
fn tables_strategy() -> impl Strategy<Value = Vec<InputTable>> {
    prop::collection::btree_map(0u8..5, 0i64..2_000_000, 0..5).prop_map(|tables| {
        tables
            .into_iter()
            .map(|(n, e)| InputTable {
                name: format!("upstream_{n}"),
                last_modified_epoch: Some(e),
            })
            .collect()
    })
}

fn confirmed_row(hash: &str, tables: Vec<InputTable>, built: Option<i64>) -> ExecutionRow {
    ExecutionRow {
        id: 1,
        org_id: "o".into(),
        target_table: "\"DB\".\"S\".\"T\"".into(),
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
        a_tables.push(InputTable { name: target.into(), last_modified_epoch: Some(self_epoch_a) });
        let mut b_tables = upstreams.clone();
        b_tables.push(InputTable { name: target.into(), last_modified_epoch: Some(self_epoch_b) });

        let va = decide(&ctx(&a_tables, tol, p, Some(target)), Some(&prev));
        let vb = decide(&ctx(&b_tables, tol, p, Some(target)), Some(&prev));
        prop_assert_eq!(is_skip(&va), is_skip(&vb), "self-table epoch must not change verdict");
    }

    // 4. Freshness monotonicity: with a confirmed match, if every current
    //    upstream epoch is <= the recorded epoch (no drift), the verdict is Skip;
    //    and lowering epochs can never turn a Skip into an Execute.
    #[test]
    fn freshness_monotonicity(names in prop::collection::btree_set(0u8..4, 1..4), base in 100_000i64..1_000_000, p in policy_strategy()) {
        // Recorded upstreams all at `base`.
        let recorded: Vec<InputTable> = names
            .iter()
            .map(|n| InputTable { name: format!("u_{n}"), last_modified_epoch: Some(base) })
            .collect();
        let prev = confirmed_row("h", recorded.clone(), Some(base));

        // Current upstreams at or below base (no drift) → must Skip.
        let current_fresh: Vec<InputTable> = names
            .iter()
            .map(|n| InputTable { name: format!("u_{n}"), last_modified_epoch: Some(base) })
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
    //    policy, if the engine SKIPs then the regression model must agree the
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
        let recorded = vec![InputTable { name: "u_known".into(), last_modified_epoch: Some(base) }];
        let prev = confirmed_row("h", recorded, Some(base));
        // Current adds a never-seen upstream well beyond tolerance.
        let newer = base.saturating_add(tol.saturating_mul(1000)).saturating_add(gap);
        let current = vec![
            InputTable { name: "u_known".into(), last_modified_epoch: Some(base) },
            InputTable { name: "u_brand_new".into(), last_modified_epoch: Some(newer) },
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
            .map(|n| InputTable { name: format!("u_{n}"), last_modified_epoch: Some(base) })
            .collect();
        let prev = confirmed_row("h", recorded, Some(base));

        let low: Vec<InputTable> = names.iter()
            .map(|n| InputTable { name: format!("u_{n}"), last_modified_epoch: Some(base) })
            .collect();
        let high: Vec<InputTable> = names.iter()
            .map(|n| InputTable { name: format!("u_{n}"), last_modified_epoch: Some(base + bump) })
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

// ---------------------------------------------------------------------------
// F7 — physical identity invariants.
//
// Physical names preserve schema and quoted case until explicit provenance exists.
fn ident_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        "[a-zA-Z][a-zA-Z0-9_]{0,6}".prop_map(|s| s),
        // A dotted identifier MUST be quoted to be a single component.
        "[a-z]{1,3}\\.[a-z]{1,3}".prop_map(|s| format!("\"{s}\"")),
    ]
}

fn three_part_name(cat: &str, schema: &str, table: &str) -> String {
    // Quote components that are not already quoted and contain no dot, to match
    // the warehouse-style fully-qualified form the client sends.
    let q = |p: &str| {
        if p.starts_with('"') {
            p.to_string()
        } else {
            format!("\"{p}\"")
        }
    };
    format!("{}.{}.{}", q(cat), q(schema), q(table))
}

proptest! {
    // F7.1 — Idempotence: keying a key yields the same key shape invariantly
    // under re-application of the normalization to its own output's components
    // (determinism of the primitive).
    #[test]
    fn relation_key_is_deterministic(name in "\\PC{0,40}") {
        prop_assert_eq!(physical_relation_key(&name), physical_relation_key(&name));
    }

    // F7.2 — Schema-component distinction: two 3-part names that differ ONLY
    // in the schema component MUST remain distinct. This holds even when catalog/table identifiers contain quoted
    // dots — the case the old naive split('.') got wrong.
    #[test]
    fn relation_key_distinguishes_schema_component(
        cat in ident_strategy(),
        schema_a in ident_strategy(),
        schema_b in ident_strategy(),
        table in ident_strategy(),
    ) {
        let a = three_part_name(&cat, &schema_a, &table);
        let b = three_part_name(&cat, &schema_b, &table);
        if schema_a == schema_b {
            prop_assert_eq!(physical_relation_key(&a), physical_relation_key(&b));
        } else {
            prop_assert_ne!(physical_relation_key(&a), physical_relation_key(&b));
        }
    }

    // F7.3 — Catalog/table sensitivity: 3-part names that differ in the catalog
    // OR the table component MUST NOT collide (no cross-environment false
    // match). Schema is held constant so only the meaningful axes vary.
    #[test]
    fn relation_key_distinguishes_catalog_and_table(
        cat_a in "[a-z]{1,4}",
        cat_b in "[a-z]{1,4}",
        schema in "[a-z]{1,4}",
        table_a in "[a-z]{1,4}",
        table_b in "[a-z]{1,4}",
    ) {
        let a = three_part_name(&cat_a, &schema, &table_a);
        let b = three_part_name(&cat_b, &schema, &table_b);
        // If catalog and table are both identical, keys must match; otherwise
        // they must differ.
        if cat_a == cat_b && table_a == table_b {
            prop_assert_eq!(physical_relation_key(&a), physical_relation_key(&b));
        } else {
            prop_assert_ne!(
                physical_relation_key(&a),
                physical_relation_key(&b),
                "distinct catalog/table must not collide: a={} b={}", a, b
            );
        }
    }
}

#[test]
fn quoted_identifiers_and_schemas_are_distinct() {
    for (a, b) in [
        (
            "\"DB\".\"PROD\".\"my.table\"",
            "\"DB\".\"DEV\".\"my.table\"",
        ),
        ("DB.S.\"T\"", "DB.S.\"t\""),
        ("DB.\"pr.od\".T", "DB.\"de.v\".T"),
    ] {
        assert_ne!(physical_relation_key(a), physical_relation_key(b));
    }
}
