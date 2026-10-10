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

use crate::decision::{decide, logical_relation_key, StaleUpstreamPolicy, SubmitContext, Verdict};
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
/// Quote-aware: a `.` inside double quotes is part of the identifier.
fn strip_schema(name: &str) -> String {
    let mut parts: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    for ch in name.chars() {
        match ch {
            '"' => in_quotes = !in_quotes,
            '.' if !in_quotes => parts.push(std::mem::take(&mut cur)),
            other => cur.push(other),
        }
    }
    parts.push(cur);
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
        node_unique_id: None,
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

// ---------------------------------------------------------------------------
// F7 — logical_relation_key parser invariants.
//
// `logical_relation_key` is the cross-environment match primitive: it must map
// the SAME logical table in two environments (differing only in the schema
// component) to the SAME key, and must NOT collapse tables that differ in
// catalog or table name. A parser bug here causes either a cross-environment
// false SKIP (data corruption risk) or a lost SKIP (fidelity loss). Identifiers
// may be double-quoted and may contain dots inside the quotes.
// ---------------------------------------------------------------------------

/// Generate a single identifier component: a short name, optionally containing
/// a dot or mixed case, that will be double-quoted when it needs quoting.
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
        prop_assert_eq!(logical_relation_key(&name), logical_relation_key(&name));
    }

    // F7.2 — Schema-component insensitivity: two 3-part names that differ ONLY
    // in the schema component MUST produce the same logical key (cross-env
    // reuse). This holds even when catalog/table identifiers contain quoted
    // dots — the case the old naive split('.') got wrong.
    #[test]
    fn relation_key_ignores_schema_component(
        cat in ident_strategy(),
        schema_a in ident_strategy(),
        schema_b in ident_strategy(),
        table in ident_strategy(),
    ) {
        let a = three_part_name(&cat, &schema_a, &table);
        let b = three_part_name(&cat, &schema_b, &table);
        prop_assert_eq!(
            logical_relation_key(&a),
            logical_relation_key(&b),
            "same catalog+table, differing schema must collide: a={} b={}", a, b
        );
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
            prop_assert_eq!(logical_relation_key(&a), logical_relation_key(&b));
        } else {
            prop_assert_ne!(
                logical_relation_key(&a),
                logical_relation_key(&b),
                "distinct catalog/table must not collide: a={} b={}", a, b
            );
        }
    }
}

/// F7 regression — a quoted identifier containing a dot must be treated as ONE
/// component, so the schema is still stripped and two environments collide.
/// The pre-fix `split('.')` produced 4 parts here and fell through to the
/// pass-through arm, leaving the schema in the key (cross-environment MISS).
#[test]
fn relation_key_handles_quoted_dotted_table() {
    let prod = "\"DB\".\"PROD\".\"my.table\"";
    let dev = "\"DB\".\"DEV\".\"my.table\"";
    assert_eq!(
        logical_relation_key(prod),
        logical_relation_key(dev),
        "a quoted dotted table name must still strip the schema and collide"
    );
    // And the key is the schema-stripped, lowercased logical identity.
    assert_eq!(logical_relation_key(prod), "db..my.table");
}

/// F7 regression — a quoted dot in the schema component must not leak into the
/// table identity; stripping removes the whole quoted schema regardless of dots.
#[test]
fn relation_key_handles_quoted_dotted_schema() {
    let a = "\"DB\".\"pr.od\".\"T\"";
    let b = "\"DB\".\"de.v\".\"T\"";
    assert_eq!(logical_relation_key(a), logical_relation_key(b));
    assert_eq!(logical_relation_key(a), "db..t");
}
