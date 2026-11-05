//! DuckDB identity through the production router, including Record/Submit parity.
mod support;
use dbt_state_proto::query_cache as qc;
use qc::{execution_client::ExecutionClient, sql_client::SqlClient};
use support::{channel, response_variant, start_server};

fn request(sql: &str) -> qc::SubmitEnrichedSqlRequest {
    qc::SubmitEnrichedSqlRequest {
        target_table: Some("db.dev.target".into()),
        dialect: "duckdb".into(),
        table_namespace: Some("one-file-generation".into()),
        default_catalog: "db".into(),
        default_schema: Some("dev".into()),
        execution_type: 1,
        tolerate_nondeterminism: true,
        sql: sql.into(),
        tables: vec![qc::TableModifiedInfo {
            name: "db.dev.target".into(),
            last_modified_epoch: Some(100),
        }],
        ..Default::default()
    }
}
async fn build(
    sql: &mut SqlClient<tonic::transport::Channel>,
    exec: &mut ExecutionClient<tonic::transport::Channel>,
    req: qc::SubmitEnrichedSqlRequest,
) {
    let response = sql.submit_enriched_sql(req).await.unwrap().into_inner();
    let Some(qc::submit_sql_response::Response::ReadyToExecute(ready)) = response.response else {
        panic!("expected BUILD")
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: ready.request_id,
        last_modified_epoch: Some(100),
        table_type: Some("TABLE".into()),
        ..Default::default()
    })
    .await
    .unwrap();
}
async fn assert_variant(
    sql: &mut SqlClient<tonic::transport::Channel>,
    req: qc::SubmitEnrichedSqlRequest,
    expected: &str,
) {
    let response = sql.submit_enriched_sql(req).await.unwrap().into_inner();
    assert_eq!(response_variant(&response), expected);
}

#[tokio::test]
async fn duckdb_formatting_preserves_identifiers_context_configuration_and_bindings() {
    let (addr, _) = start_server().await;
    let channel = channel(addr).await;
    let mut sql = SqlClient::new(channel.clone());
    let mut exec = ExecutionClient::new(channel);
    let mut base = request("select id, amount + 1 as \"Output.Name\" from db.dev.input");
    base.query_dependencies = vec![
        qc::QueryDependency {
            name: "db.dev.\"a.b\"".into(),
            query: "select 1 as id".into(),
            default_catalog: "db".into(),
            default_schema: "dev".into(),
        },
        qc::QueryDependency {
            name: "db.\"dev.a\".b".into(),
            query: "select 2 as id".into(),
            default_catalog: "db".into(),
            default_schema: "dev.a".into(),
        },
    ];
    build(&mut sql, &mut exec, base.clone()).await;
    let mut formatted = base.clone();
    formatted.sql = "SELECT /* safe */ id,\namount + 1 AS \"Output.Name\" FROM db.dev.input".into();
    formatted.query_dependencies[0].query = "SELECT /* dep */ 1 AS id".into();
    assert_variant(&mut sql, formatted, "skip_execution").await;
    let mut changed = vec![];
    let mut r = base.clone();
    r.sql = r.sql.replace("+ 1", "+ 2");
    changed.push(r);
    let mut r = base.clone();
    r.sql = r.sql.replace("Output.Name", "output.name");
    changed.push(r);
    let mut r = base.clone();
    r.default_schema = Some("prod".into());
    changed.push(r);
    let mut r = base.clone();
    r.default_catalog = "another".into();
    changed.push(r);
    let mut r = base.clone();
    r.semantic_extras
        .insert("contract".into(), "changed".into());
    changed.push(r);
    let mut r = base.clone();
    r.query_dependencies[0].default_schema = "other".into();
    changed.push(r);
    let mut r = base.clone();
    let q = r.query_dependencies[0].query.clone();
    r.query_dependencies[0].query = r.query_dependencies[1].query.clone();
    r.query_dependencies[1].query = q;
    changed.push(r);
    let mut r = base.clone();
    r.query_dependencies[0].name = "db.dev.a.b".into();
    changed.push(r);
    let mut r = base.clone();
    r.target_table = Some("db.prod.target".into());
    r.tables[0].name = "db.prod.target".into();
    changed.push(r);
    for r in changed {
        assert_variant(&mut sql, r, "ready_to_execute").await;
    }
}

#[tokio::test]
async fn duckdb_dotted_reference_bindings_do_not_collapse_or_swap() {
    let (addr, _) = start_server().await;
    let channel = channel(addr).await;
    let mut sql = SqlClient::new(channel.clone());
    let mut exec = ExecutionClient::new(channel);
    let mut base = request("select a.id as id from db.dev.\"a.b\" a cross join db.\"dev.a\".b b");
    base.query_dependencies = vec![
        qc::QueryDependency {
            name: "db.dev.\"a.b\"".into(),
            query: "select 1 as id".into(),
            default_catalog: "db".into(),
            default_schema: "dev".into(),
        },
        qc::QueryDependency {
            name: "db.\"dev.a\".b".into(),
            query: "select 2 as id".into(),
            default_catalog: "db".into(),
            default_schema: "dev.a".into(),
        },
    ];
    build(&mut sql, &mut exec, base.clone()).await;
    assert_variant(&mut sql, base.clone(), "skip_execution").await;
    let mut swapped = base.clone();
    swapped.query_dependencies[0].query = "select 2 as id".into();
    swapped.query_dependencies[1].query = "select 1 as id".into();
    assert_variant(&mut sql, swapped, "ready_to_execute").await;
    let mut split = base;
    split.sql = split.sql.replace("db.dev.\"a.b\"", "db.\"dev.a\".b");
    split.query_dependencies.remove(0);
    assert_variant(&mut sql, split, "ready_to_execute").await;
}

#[tokio::test]
async fn unsupported_duckdb_syntax_and_implicit_column_names_keep_exact_identity() {
    let (addr, _) = start_server().await;
    let channel = channel(addr).await;
    let mut sql = SqlClient::new(channel.clone());
    let mut exec = ExecutionClient::new(channel);
    for text in [
        "select 1 + 2",
        "select sum(amount) as total from input",
        "from input select id",
        "select * exclude (id) from input",
        "select [1, 2] as array",
        "select 1 as id; select 2 as id",
    ] {
        let base = request(text);
        build(&mut sql, &mut exec, base.clone()).await;
        assert_variant(&mut sql, base.clone(), "skip_execution").await;
        let mut formatted = base;
        formatted.sql.push_str(" /* unknown semantics */");
        assert_variant(&mut sql, formatted, "ready_to_execute").await;
    }
}

#[tokio::test]
async fn recorded_duckdb_formatting_uses_the_same_identity_as_submission() {
    let (addr, _) = start_server().await;
    let channel = channel(addr).await;
    let mut sql = SqlClient::new(channel.clone());
    let mut exec = ExecutionClient::new(channel);
    let base = request("select id, amount + 1 as amount from db.dev.input");
    exec.record_executions(qc::RecordExecutionsRequest {
        records: vec![qc::ExecutionRecord {
            input: Some(qc::execution_record::Input::EnrichedSql(qc::SqlExecution {
                target_table: base.target_table.clone(),
                dialect: base.dialect.clone(),
                table_namespace: base.table_namespace.clone(),
                default_catalog: base.default_catalog.clone(),
                default_schema: base.default_schema.clone(),
                execution_type: base.execution_type,
                sql: base.sql.clone(),
                ..Default::default()
            })),
            outcome: Some(qc::ExecutionOutcome {
                last_modified_epoch: Some(100),
                table_type: Some("TABLE".into()),
                ..Default::default()
            }),
        }],
    })
    .await
    .unwrap();
    let mut formatted = base;
    formatted.sql = "SELECT id, /* trivia */ amount + 1 AS amount FROM db.dev.input".into();
    assert_variant(&mut sql, formatted, "skip_execution").await;
}

#[tokio::test]
async fn old_exact_duckdb_history_cold_misses_after_identity_upgrade() {
    let (addr, _, state) = support::start_server_with_state().await;
    let channel = channel(addr).await;
    let mut sql = SqlClient::new(channel.clone());
    let mut exec = ExecutionClient::new(channel);
    // Already-canonical SQL isolates version invalidation from formatting.
    let base = request("SELECT id, amount + 1 AS amount FROM db.dev.input");
    build(&mut sql, &mut exec, base.clone()).await;
    assert_variant(&mut sql, base.clone(), "skip_execution").await;
    // Frozen b51cca4 exact-byte digest: length-framed v4, duckdb/db/dev,
    // rendered, the SQL above, empty extras and dependencies. This literal
    // does not call today's hashing code with a pretend historical version.
    sqlx::query("UPDATE executions SET node_sql_hash = $1 WHERE status = 'confirmed'")
        .bind("5c1424fbf75b4ea08a9b41339bc91c32d063026bdc7919ae85a7470df86b7b4a")
        .execute(state.store.pool())
        .await
        .unwrap();
    assert_variant(&mut sql, base, "ready_to_execute").await;
}
