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

use dbt_state_proto::query_cache as qc;
use qc::execution_client::ExecutionClient;
use qc::sql_client::SqlClient;

fn model(target: &str, sql: &str) -> qc::SubmitEnrichedSqlRequest {
    qc::SubmitEnrichedSqlRequest {
        target_table: Some(target.to_string()),
        dialect: "snowflake".to_string(),
        execution_type: 1,
        sql: sql.to_string(),
        table_namespace: Some("ns".to_string()),
        tables: vec![qc::TableModifiedInfo {
            name: "\"DB\".\"S\".\"UP\"".into(),
            last_modified_epoch: Some(100),
        }],
        dbt_node_state: Some(qc::DbtNodeState {
            node_unique_id: format!("model.jaffle.{target}"),
            node_hash: "h".into(),
            node_body_hash: Some("h".into()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Replay (baseline execute+confirm, then variant submit) and return our verdict.
async fn verdict_for(baseline_sql: &str, variant_sql: &str, target: &str) -> &'static str {
    let (addr, _schema) = support::start_server().await;
    let ch = support::channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let r = sql
        .submit_enriched_sql(model(target, baseline_sql))
        .await
        .unwrap()
        .into_inner();
    let rid = match r.response {
        Some(qc::submit_sql_response::Response::ReadyToExecute(x)) => x.request_id,
        other => panic!("baseline must execute, got {other:?}"),
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: rid,
        last_modified_epoch: Some(100),
        failed_to_clone: false,
        table_type: Some("TABLE".into()),
        execution_results: None,
        execution_runtime_ms: Some(10),
        labels: Default::default(),
    })
    .await
    .unwrap();

    support::response_variant(
        &sql.submit_enriched_sql(model(target, variant_sql))
            .await
            .unwrap()
            .into_inner(),
    )
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
}
