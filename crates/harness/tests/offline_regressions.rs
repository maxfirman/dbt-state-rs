//! Correctness contracts established without warehouse or hosted-service access.
//! These test the public gRPC boundary and durable Postgres state.
use dbt_state_proto::query_cache as qc;
use qc::{execution_client::ExecutionClient, sql_client::SqlClient};
use tonic::transport::Channel;

#[path = "support.rs"]
mod support;
use support::{channel, response_variant, start_server};

fn model(target: &str, sql: &str) -> qc::SubmitEnrichedSqlRequest {
    qc::SubmitEnrichedSqlRequest {
        target_table: Some(target.into()),
        dialect: "snowflake".into(),
        default_catalog: "DB".into(),
        default_schema: Some("S".into()),
        execution_type: 1,
        sql: sql.into(),
        tolerate_nondeterminism: true,
        tables: vec![table(target, Some(100))],
        dbt_node_state: Some(qc::DbtNodeState {
            node_unique_id: format!("model.p.{target}"),
            node_body_hash: Some("body".into()),
            project_id: Some("p".into()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn table(name: &str, epoch: Option<i64>) -> qc::TableModifiedInfo {
    qc::TableModifiedInfo {
        name: name.into(),
        last_modified_epoch: epoch,
    }
}

fn results(failures: i64) -> qc::Struct {
    serde_json::from_value(serde_json::json!({"fields": {
        "failures": {"kind": {"int_value": failures}},
        "should_error": {"kind": {"bool_value": failures > 0}},
        "should_warn": {"kind": {"bool_value": false}},
        "nested": {"kind": {"list_value": {"values": [{"kind": {"string_value": "é"}}]}}}
    }}))
    .unwrap()
}

async fn clients() -> (SqlClient<Channel>, ExecutionClient<Channel>) {
    let (addr, _) = start_server().await;
    let ch = channel(addr).await;
    (SqlClient::new(ch.clone()), ExecutionClient::new(ch))
}

async fn confirm(
    exec: &mut ExecutionClient<Channel>,
    response: qc::SubmitSqlResponse,
    epoch: Option<i64>,
    execution_results: Option<qc::Struct>,
) {
    let Some(qc::submit_sql_response::Response::ReadyToExecute(r)) = response.response else {
        panic!("expected build before confirmation");
    };
    assert!(
        exec.confirm_execution(qc::ConfirmExecutionRequest {
            request_id: r.request_id,
            last_modified_epoch: epoch,
            execution_results,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .success
    );
}

#[tokio::test]
async fn overwritten_history_and_execution_types_cannot_be_resurrected() {
    let (mut sql, mut exec) = clients().await;
    let a = model("DB.S.T", "select 1");
    confirm(
        &mut exec,
        sql.submit_enriched_sql(a.clone())
            .await
            .unwrap()
            .into_inner(),
        Some(100),
        None,
    )
    .await;
    let b = model("DB.S.T", "select 2");
    confirm(
        &mut exec,
        sql.submit_enriched_sql(b).await.unwrap().into_inner(),
        Some(200),
        None,
    )
    .await;
    let mut reverted = a.clone();
    reverted.tables = vec![table("DB.S.T", Some(200))];
    assert_eq!(
        response_variant(
            &sql.submit_enriched_sql(reverted)
                .await
                .unwrap()
                .into_inner()
        ),
        "ready_to_execute"
    );
    let mut view = a.clone();
    view.execution_type = 10;
    confirm(
        &mut exec,
        sql.submit_enriched_sql(view).await.unwrap().into_inner(),
        Some(300),
        None,
    )
    .await;
    assert_eq!(
        response_variant(&sql.submit_enriched_sql(a).await.unwrap().into_inner()),
        "ready_to_execute"
    );
}

#[tokio::test]
async fn missing_changed_and_cross_target_objects_do_not_skip() {
    let (mut sql, mut exec) = clients().await;
    let mut req = model("DB.S.T", "select 1");
    req.table_namespace = Some("ns".into());
    confirm(
        &mut exec,
        sql.submit_enriched_sql(req.clone())
            .await
            .unwrap()
            .into_inner(),
        Some(100),
        None,
    )
    .await;
    assert_eq!(
        response_variant(
            &sql.submit_enriched_sql(req.clone())
                .await
                .unwrap()
                .into_inner()
        ),
        "skip_execution"
    );
    for tables in [
        vec![],
        vec![table("DB.S.T", None)],
        vec![table("DB.S.T", Some(200))],
    ] {
        let mut changed = req.clone();
        changed.tables = tables;
        assert_eq!(
            response_variant(&sql.submit_enriched_sql(changed).await.unwrap().into_inner()),
            "ready_to_execute"
        );
    }
    let mut ignored = req.clone();
    ignored.tables[0].last_modified_epoch = Some(200);
    ignored.ignore_external_modifications = true;
    assert_eq!(
        response_variant(&sql.submit_enriched_sql(ignored).await.unwrap().into_inner()),
        "skip_execution"
    );
    let mut other = req.clone();
    other.target_table = Some("DB.DEV.T".into());
    other.tables = vec![];
    other.allow_clones = Some(false);
    assert_eq!(
        response_variant(&sql.submit_enriched_sql(other).await.unwrap().into_inner()),
        "ready_to_execute"
    );
    req.table_namespace = Some("different-ns".into());
    assert_eq!(
        response_variant(&sql.submit_enriched_sql(req).await.unwrap().into_inner()),
        "ready_to_execute"
    );
}

#[tokio::test]
async fn data_test_identity_logic_and_results_round_trip() {
    let (mut sql, mut exec) = clients().await;
    let mut req = model("test-id", "select 7 as failures");
    req.target_table = None;
    req.tables.clear();
    req.execution_type = 8;
    let cached = results(7);
    let response = sql
        .submit_enriched_sql(req.clone())
        .await
        .unwrap()
        .into_inner();
    confirm(&mut exec, response, None, Some(cached.clone())).await;
    let response = sql
        .submit_enriched_sql(req.clone())
        .await
        .unwrap()
        .into_inner();
    let Some(qc::submit_sql_response::Response::SkipExecution(r)) = response.response else {
        panic!("expected cached test")
    };
    assert_eq!(r.execution_results, Some(cached));
    let mut changed = req.clone();
    changed.sql = "select 0 as failures".into();
    assert_eq!(
        response_variant(&sql.submit_enriched_sql(changed).await.unwrap().into_inner()),
        "ready_to_execute"
    );
    let mut changed = req.clone();
    changed
        .semantic_extras
        .insert("severity".into(), "\"warn\"".into());
    assert_eq!(
        response_variant(&sql.submit_enriched_sql(changed).await.unwrap().into_inner()),
        "ready_to_execute"
    );
    let mut other_project = req.clone();
    other_project.dbt_node_state.as_mut().unwrap().project_id = Some("other".into());
    assert_eq!(
        response_variant(
            &sql.submit_enriched_sql(other_project)
                .await
                .unwrap()
                .into_inner()
        ),
        "ready_to_execute"
    );
    req.dbt_node_state = None;
    req.labels
        .insert("dbt_node_unique_id".into(), "test.python.first".into());
    confirm(
        &mut exec,
        sql.submit_enriched_sql(req.clone())
            .await
            .unwrap()
            .into_inner(),
        None,
        Some(results(0)),
    )
    .await;
    assert_eq!(
        response_variant(
            &sql.submit_enriched_sql(req.clone())
                .await
                .unwrap()
                .into_inner()
        ),
        "skip_execution"
    );
    req.labels
        .insert("dbt_node_unique_id".into(), "test.python.other".into());
    assert_eq!(
        response_variant(
            &sql.submit_enriched_sql(req.clone())
                .await
                .unwrap()
                .into_inner()
        ),
        "ready_to_execute"
    );
    req.labels.clear();
    assert_eq!(
        response_variant(&sql.submit_enriched_sql(req).await.unwrap().into_inner()),
        "ready_to_execute"
    );
}

#[tokio::test]
async fn unknown_freshness_and_distinct_schema_inputs_are_not_hidden() {
    let (mut sql, mut exec) = clients().await;
    let mut req = model("DB.S.T", "select * from DB.A.U join DB.B.U using(id)");
    req.tables
        .extend([table("DB.A.U", Some(1000)), table("DB.B.U", Some(100))]);
    confirm(
        &mut exec,
        sql.submit_enriched_sql(req.clone())
            .await
            .unwrap()
            .into_inner(),
        Some(100),
        None,
    )
    .await;
    req.tables[2].last_modified_epoch = Some(500);
    assert_eq!(
        response_variant(
            &sql.submit_enriched_sql(req.clone())
                .await
                .unwrap()
                .into_inner()
        ),
        "ready_to_execute"
    );
    req.tables[2].last_modified_epoch = None;
    req.stale_upstream_policy = 1;
    assert_eq!(
        response_variant(&sql.submit_enriched_sql(req).await.unwrap().into_inner()),
        "ready_to_execute"
    );
}

#[tokio::test]
async fn dependency_context_and_volatile_policy_prevent_unsupported_reuse() {
    let (mut sql, mut exec) = clients().await;
    let mut req = model("DB.S.T", "select * from V");
    req.query_dependencies = vec![qc::QueryDependency {
        name: "DB.S.V".into(),
        query: "select 1".into(),
        ..Default::default()
    }];
    confirm(
        &mut exec,
        sql.submit_enriched_sql(req.clone())
            .await
            .unwrap()
            .into_inner(),
        Some(100),
        None,
    )
    .await;
    assert_eq!(
        response_variant(
            &sql.submit_enriched_sql(req.clone())
                .await
                .unwrap()
                .into_inner()
        ),
        "skip_execution"
    );
    let mut changed = req.clone();
    changed.query_dependencies[0].query = "select 2".into();
    assert_eq!(
        response_variant(&sql.submit_enriched_sql(changed).await.unwrap().into_inner()),
        "ready_to_execute"
    );
    let mut changed = req.clone();
    changed.default_schema = Some("OTHER".into());
    assert_eq!(
        response_variant(&sql.submit_enriched_sql(changed).await.unwrap().into_inner()),
        "ready_to_execute"
    );
    req.tolerate_nondeterminism = false;
    assert_eq!(
        response_variant(&sql.submit_enriched_sql(req).await.unwrap().into_inner()),
        "ready_to_execute"
    );
}

#[tokio::test]
async fn seeds_match_configuration_current_object_and_latest_build() {
    let (mut sql, mut exec) = clients().await;
    let mut req = qc::SubmitValuesRequest {
        target_table: "DB.S.SEED".into(),
        dialect: "snowflake".into(),
        values_hash: "bytes-A".into(),
        last_modified_epoch: Some(100),
        ..Default::default()
    };
    confirm(
        &mut exec,
        sql.submit_values(req.clone()).await.unwrap().into_inner(),
        Some(100),
        None,
    )
    .await;
    assert_eq!(
        response_variant(&sql.submit_values(req.clone()).await.unwrap().into_inner()),
        "skip_execution"
    );
    let mut changed = req.clone();
    changed
        .semantic_extras
        .insert("column_types".into(), "{\"id\":\"varchar\"}".into());
    assert_eq!(
        response_variant(&sql.submit_values(changed).await.unwrap().into_inner()),
        "ready_to_execute"
    );
    let mut missing = req.clone();
    missing.last_modified_epoch = None;
    assert_eq!(
        response_variant(&sql.submit_values(missing).await.unwrap().into_inner()),
        "ready_to_execute"
    );
    req.values_hash = "bytes-B".into();
    confirm(
        &mut exec,
        sql.submit_values(req.clone()).await.unwrap().into_inner(),
        Some(200),
        None,
    )
    .await;
    req.values_hash = "bytes-A".into();
    req.last_modified_epoch = Some(200);
    assert_eq!(
        response_variant(&sql.submit_values(req).await.unwrap().into_inner()),
        "ready_to_execute"
    );
}

#[tokio::test]
async fn unsupported_history_queries_are_errors_not_empty_successes() {
    let (addr, _) = start_server().await;
    let ch = channel(addr).await;
    let result = qc::selector_service_client::SelectorServiceClient::new(ch.clone())
        .get_state_selection(qc::SelectorRequest::default())
        .await;
    assert_eq!(result.unwrap_err().code(), tonic::Code::Unimplemented);
    let result = ExecutionClient::new(ch)
        .resolve_deferred_relations(qc::ResolveDeferredRelationsRequest::default())
        .await;
    assert_eq!(result.unwrap_err().code(), tonic::Code::Unimplemented);
}

#[tokio::test]
async fn batch_recording_preserves_results_and_shares_submit_fingerprints() {
    let (mut sql, mut exec) = clients().await;
    let mut req = model("test-id", "select 7 as failures");
    req.execution_type = 8;
    req.target_table = None;
    req.tables.clear();
    let outcome = results(i64::MAX);
    let record = qc::ExecutionRecord {
        input: Some(qc::execution_record::Input::EnrichedSql(qc::SqlExecution {
            dialect: req.dialect.clone(),
            default_catalog: req.default_catalog.clone(),
            default_schema: req.default_schema.clone(),
            sql: req.sql.clone(),
            execution_type: req.execution_type,
            dbt_node_state: req.dbt_node_state.clone(),
            ..Default::default()
        })),
        outcome: Some(qc::ExecutionOutcome {
            execution_results: Some(outcome.clone()),
            ..Default::default()
        }),
    };
    assert_eq!(
        exec.record_executions(qc::RecordExecutionsRequest {
            records: vec![record]
        })
        .await
        .unwrap()
        .into_inner()
        .records_stored,
        1
    );
    let response = sql.submit_enriched_sql(req).await.unwrap().into_inner();
    let Some(qc::submit_sql_response::Response::SkipExecution(r)) = response.response else {
        panic!("recorded test must reuse")
    };
    assert_eq!(r.execution_results, Some(outcome));

    let mut seed = qc::SubmitValuesRequest {
        target_table: "DB.S.SEED".into(),
        dialect: "snowflake".into(),
        values_hash: "data".into(),
        last_modified_epoch: Some(100),
        ..Default::default()
    };
    seed.semantic_extras
        .insert("column_types".into(), "{\"id\":\"integer\"}".into());
    exec.record_executions(qc::RecordExecutionsRequest {
        records: vec![qc::ExecutionRecord {
            input: Some(qc::execution_record::Input::Values(qc::ValuesExecution {
                target_table: seed.target_table.clone(),
                dialect: seed.dialect.clone(),
                values_hash: seed.values_hash.clone(),
                semantic_extras: seed.semantic_extras.clone(),
                ..Default::default()
            })),
            outcome: Some(qc::ExecutionOutcome {
                last_modified_epoch: Some(100),
                ..Default::default()
            }),
        }],
    })
    .await
    .unwrap();
    assert_eq!(
        response_variant(&sql.submit_values(seed).await.unwrap().into_inner()),
        "skip_execution"
    );
}

#[tokio::test]
async fn non_finite_protobuf_results_and_duplicate_confirmation_are_lossless() {
    let (mut sql, mut exec) = clients().await;
    let mut req = model("test-id", "select 0 as failures");
    req.execution_type = 8;
    req.target_table = None;
    req.tables.clear();
    let response = sql
        .submit_enriched_sql(req.clone())
        .await
        .unwrap()
        .into_inner();
    let Some(qc::submit_sql_response::Response::ReadyToExecute(r)) = response.response else {
        panic!("build test")
    };
    let mut cached = results(0);
    cached.fields.insert(
        "non_finite".into(),
        qc::Value {
            kind: Some(qc::value::Kind::DoubleValue(f64::INFINITY)),
        },
    );
    for value in [cached.clone(), results(9)] {
        assert!(
            exec.confirm_execution(qc::ConfirmExecutionRequest {
                request_id: r.request_id.clone(),
                execution_results: Some(value),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner()
            .success
        );
    }
    let response = sql.submit_enriched_sql(req).await.unwrap().into_inner();
    let Some(qc::submit_sql_response::Response::SkipExecution(r)) = response.response else {
        panic!("reuse test")
    };
    assert_eq!(r.execution_results, Some(cached));
}

#[tokio::test]
async fn missing_or_incomplete_test_results_require_execution() {
    let (mut sql, mut exec) = clients().await;
    let mut req = model("test", "select 0 as failures");
    req.execution_type = 8;
    req.target_table = None;
    req.tables.clear();
    for payload in [None, Some(qc::Struct::default())] {
        confirm(
            &mut exec,
            sql.submit_enriched_sql(req.clone())
                .await
                .unwrap()
                .into_inner(),
            None,
            payload,
        )
        .await;
        assert_eq!(
            response_variant(
                &sql.submit_enriched_sql(req.clone())
                    .await
                    .unwrap()
                    .into_inner()
            ),
            "ready_to_execute"
        );
    }
    let mut warning = results(3);
    warning.fields.insert(
        "should_error".into(),
        qc::Value {
            kind: Some(qc::value::Kind::BoolValue(false)),
        },
    );
    warning.fields.insert(
        "should_warn".into(),
        qc::Value {
            kind: Some(qc::value::Kind::BoolValue(true)),
        },
    );
    confirm(
        &mut exec,
        sql.submit_enriched_sql(req.clone())
            .await
            .unwrap()
            .into_inner(),
        None,
        Some(warning.clone()),
    )
    .await;
    let response = sql.submit_enriched_sql(req).await.unwrap().into_inner();
    let Some(qc::submit_sql_response::Response::SkipExecution(r)) = response.response else {
        panic!("cached warning")
    };
    assert_eq!(r.execution_results, Some(warning));
}

#[tokio::test]
async fn incomplete_or_ambiguous_dependency_evidence_requires_execution() {
    let (mut sql, mut exec) = clients().await;
    let mut req = model("DB.S.T", "select * from U");
    req.tables.push(table("DB.S.U", Some(100)));
    confirm(
        &mut exec,
        sql.submit_enriched_sql(req.clone())
            .await
            .unwrap()
            .into_inner(),
        Some(100),
        None,
    )
    .await;
    let mut missing = req.clone();
    missing.tables.pop();
    missing.stale_upstream_policy = 1;
    assert_eq!(
        response_variant(&sql.submit_enriched_sql(missing).await.unwrap().into_inner()),
        "ready_to_execute"
    );
    req.tables.push(table("DB.S.U", Some(100)));
    assert_eq!(
        response_variant(&sql.submit_enriched_sql(req).await.unwrap().into_inner()),
        "ready_to_execute"
    );
}

#[tokio::test]
async fn corrupted_history_errors_and_old_fingerprints_cold_miss() {
    let (addr, schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);
    let req = model("DB.S.T", "select 1");
    confirm(
        &mut exec,
        sql.submit_enriched_sql(req.clone())
            .await
            .unwrap()
            .into_inner(),
        Some(100),
        None,
    )
    .await;
    let pool = support::schema_pool(&schema).await;
    sqlx::query("UPDATE executions SET input_tables = '{}'::jsonb")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        sql.submit_enriched_sql(req.clone())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Internal
    );
    sqlx::query(
        "UPDATE executions SET input_tables = '[]'::jsonb, execution_results = decode('ff','hex')",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        sql.submit_enriched_sql(req.clone())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Internal
    );
    sqlx::query("UPDATE executions SET node_sql_hash = 'legacy-hash'")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        response_variant(&sql.submit_enriched_sql(req).await.unwrap().into_inner()),
        "ready_to_execute"
    );
}

#[tokio::test]
async fn clone_errors_do_not_leave_pending_executions() {
    let (addr, schema) = start_server().await;
    let mut clone = qc::clone_client::CloneClient::new(channel(addr).await);
    for (dialect, kind) in [("snowflake", "VIEW"), ("unknown", "TABLE")] {
        let err = clone
            .register_clone(qc::CloneRequest {
                dialect: dialect.into(),
                clone_source_table: "DB.S.SOURCE".into(),
                target_table: "DB.S.T".into(),
                clone_source_table_type: Some(kind.into()),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
    }
    let pool = support::schema_pool(&schema).await;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM executions")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn ordering_is_irrelevant_but_incomplete_view_definitions_cannot_reuse() {
    let (mut sql, mut exec) = clients().await;
    let mut req = model("DB.S.T", "select * from V union all select * from W");
    req.query_dependencies = vec![
        qc::QueryDependency {
            name: "DB.S.V".into(),
            query: "select 1".into(),
            ..Default::default()
        },
        qc::QueryDependency {
            name: "DB.S.W".into(),
            query: "select 2".into(),
            ..Default::default()
        },
    ];
    req.tables
        .extend([table("DB.S.A", Some(100)), table("DB.S.B", Some(100))]);
    confirm(
        &mut exec,
        sql.submit_enriched_sql(req.clone())
            .await
            .unwrap()
            .into_inner(),
        Some(100),
        None,
    )
    .await;
    req.query_dependencies.reverse();
    req.tables.reverse();
    assert_eq!(
        response_variant(
            &sql.submit_enriched_sql(req.clone())
                .await
                .unwrap()
                .into_inner()
        ),
        "skip_execution"
    );
    req.query_dependencies[0].query.clear();
    confirm(
        &mut exec,
        sql.submit_enriched_sql(req.clone())
            .await
            .unwrap()
            .into_inner(),
        Some(100),
        None,
    )
    .await;
    assert_eq!(
        response_variant(&sql.submit_enriched_sql(req).await.unwrap().into_inner()),
        "ready_to_execute"
    );
}
