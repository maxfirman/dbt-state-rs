//! SQL lexical-normalization conformance (live-verified boundary).
//!
//! Disproves the "server compares logical plans" hypothesis and pins the
//! verified mechanism: the hosted service lexes + normalizes the token stream
//! (strip comments, collapse inter-token whitespace, case-fold keywords/unquoted
//! identifiers, preserve string literals & quoted identifiers) but does NOT
//! canonicalize semantics. Each (baseline, variant, expected) row below was
//! VERIFIED live against api.state.dbt.com (see experiments/SQL_NORMALIZATION.md).
//! This test replays each pair against OUR server (execute+confirm the baseline,
//! then submit the variant) and asserts we reproduce the hosted skip/execute.

#[path = "support.rs"]
mod support;

use support::scenario::{model, Scenario};

/// Replay (baseline execute+confirm, then variant submit) and return our verdict.
/// Both share one model `target` with its own target present and one upstream.
async fn verdict_for(baseline_sql: &str, variant_sql: &str, target: &str) -> &'static str {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let node = model(target)
        .namespace("ns")
        .upstream("\"DB\".\"S\".\"UP\"", 100)
        .body("h");

    sc.build(node.clone().sql(baseline_sql), 100).await;
    sc.verdict(node.sql(variant_sql)).await
}

#[tokio::test]
async fn reproduces_live_sql_normalization_boundary() {
    let base = "select customer_id as id, count(*) as n from up where customer_id is not null group by customer_id";

    // (label, variant_sql, expected) — expected VERIFIED live.
    // SKIP cases: comment, case-fold.
    let skips = [
        ("line_comment", format!("-- c\n{base}")),
        ("block_comment", "select customer_id as id, /* b */ count(*) as n from up where customer_id is not null group by customer_id".to_string()),
        ("uppercase", "SELECT customer_id AS id, COUNT(*) AS n FROM up WHERE customer_id IS NOT NULL GROUP BY customer_id".to_string()),
        ("ident_case", "select CUSTOMER_ID as id, count(*) as n from up where CUSTOMER_ID is not null group by CUSTOMER_ID".to_string()),
        ("whitespace", "select   customer_id as id,count(*) as n from up where customer_id is not null group by customer_id".to_string()),
        // Lexically-canonicalized (implemented): trailing semicolon, hint.
        ("trailing_semicolon", format!("{base};")),
        ("optimizer_hint", base.replace("select customer_id", "select /*+ no_merge */ customer_id")),
    ];
    // EXECUTE cases: semantics-preserving-but-token-different (disproves plan
    // comparison), genuine changes, AND the documented parser-gap synonyms
    // (cast shorthand / type / function synonyms) where we over-execute.
    let execs = [
        ("group_by_ordinal", base.replace("group by customer_id", "group by 1")),
        ("redundant_parens", base.replace("where customer_id is not null", "where (customer_id is not null)")),
        ("pred_rewrite", base.replace("where customer_id is not null", "where not (customer_id is null)")),
        ("add_column", base.replace("count(*) as n", "count(*) as n, 1 as extra")),
        ("string_ws", "select customer_id as id, 'a  b' as g from up".to_string()),
        ("string_case", "select customer_id as id, 'ABC' as g from up where customer_id is not null and 'ABC'='abc'".to_string()),
    ];

    let mut tn = 0;
    for (label, variant) in &skips {
        tn += 1;
        let v = verdict_for(base, variant, &format!("\"DB\".\"S\".\"N{tn}\"")).await;
        assert_eq!(
            v, "skip_execution",
            "{label}: expected skip (lexically equivalent)"
        );
    }
    for (label, variant) in &execs {
        tn += 1;
        let v = verdict_for(base, variant, &format!("\"DB\".\"S\".\"N{tn}\"")).await;
        assert_eq!(
            v, "ready_to_execute",
            "{label}: expected execute (token stream differs — NOT a logical-plan match)"
        );
    }

    // AST-level synonym canonicalization (parser-backed): these pairs differ
    // only by a synonym the hosted service unifies, so the VARIANT must SKIP
    // against its own baseline. Verified live.
    let synonym_pairs: [(&str, &str, &str); 3] = [
        (
            "cast_shorthand",
            "select cast(customer_id as varchar) as c from up",
            "select customer_id::varchar as c from up",
        ),
        (
            "type_synonym",
            "select cast(customer_id as varchar) as c from up",
            "select cast(customer_id as text) as c from up",
        ),
        (
            "function_synonym",
            "select coalesce(customer_id, 0) as c from up",
            "select nvl(customer_id, 0) as c from up",
        ),
    ];
    for (label, base_sql, variant_sql) in synonym_pairs {
        tn += 1;
        let v = verdict_for(base_sql, variant_sql, &format!("\"DB\".\"S\".\"N{tn}\"")).await;
        assert_eq!(
            v, "skip_execution",
            "{label}: parser must canonicalize the synonym → skip (matches hosted)"
        );
    }
}
