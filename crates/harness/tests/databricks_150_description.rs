//! Diagnostic-only minimized replay of retained #150 hosted capture entries 3,5,17,21.
//! This is outside the accepted golden corpus; the original entire capture remains the gate.
#[path = "support.rs"]
mod support;
use dbt_state_harness::diff;
use dbt_state_proto::query_cache as qc;
use serde_json::Value;

fn normalize(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                if matches!(key.as_str(), "request_id" | "execution_decision_id") {
                    if !value.is_null() {
                        *value = "<transport-id>".into();
                    }
                } else {
                    normalize(value);
                }
            }
        }
        Value::Array(values) => values.iter_mut().for_each(normalize),
        _ => (),
    }
}

#[tokio::test]
async fn microbatch_source_and_target_drift_hosted_description() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/diagnostic/databricks_150_description.jsonl"
    );
    let entries = diff::load_golden(path).unwrap();
    let server = support::start_server().await;
    let channel = support::channel(server.addr).await;
    let mut sql = qc::sql_client::SqlClient::new(channel.clone());
    let mut execution = qc::execution_client::ExecutionClient::new(channel.clone());
    let mut explain = qc::explain_client::ExplainClient::new(channel);
    let mut local_request = String::new();
    let mut local_decision = String::new();
    let mut differences = Vec::new();
    for (index, entry) in entries.iter().enumerate() {
        let actual = match entry.method.as_str() {
            "SubmitEnrichedSQL" => {
                let body: qc::SubmitEnrichedSqlRequest =
                    serde_json::from_value(entry.request.clone()).unwrap();
                let mut request = support::captured_request(body, entry);
                let hosted = &entry.response["response"]
                    .as_object()
                    .unwrap()
                    .values()
                    .next()
                    .unwrap()["execution_decision_id"];
                let id = uuid::Uuid::parse_str(hosted.as_str().unwrap()).unwrap();
                request.metadata_mut().insert(
                    dbt_state_server::REPLAY_NOW_HEADER,
                    ((id.as_u128() >> 80) as i64).to_string().parse().unwrap(),
                );
                let response = sql.submit_enriched_sql(request).await.unwrap().into_inner();
                let value = serde_json::to_value(response).unwrap();
                let inner = value["response"]
                    .as_object()
                    .unwrap()
                    .values()
                    .next()
                    .unwrap();
                if let Some(request_id) = inner["request_id"].as_str() {
                    local_request = request_id.into();
                }
                local_decision = inner["execution_decision_id"].as_str().unwrap().into();
                value
            }
            "ConfirmExecution" => {
                let mut body: qc::ConfirmExecutionRequest =
                    serde_json::from_value(entry.request.clone()).unwrap();
                body.request_id = local_request.clone();
                serde_json::to_value(
                    execution
                        .confirm_execution(support::captured_request(body, entry))
                        .await
                        .unwrap()
                        .into_inner(),
                )
                .unwrap()
            }
            "GetExplainMessages" => {
                let mut body: qc::GetExplainMessagesRequest =
                    serde_json::from_value(entry.request.clone()).unwrap();
                body.execution_decision_ids = vec![local_decision.clone()];
                let mut response = None;
                for _ in 0..100 {
                    let got = explain
                        .get_explain_messages(support::captured_request(body.clone(), entry))
                        .await
                        .unwrap()
                        .into_inner();
                    if !got.messages.is_empty() {
                        response = Some(got);
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                serde_json::to_value(response.expect("decision audit becomes visible")).unwrap()
            }
            method => panic!("unexpected {method}"),
        };
        let mut actual = actual;
        let mut expected = entry.response.clone();
        normalize(&mut actual);
        normalize(&mut expected);
        if actual != expected {
            differences.push(format!(
                "entry {index} {}\nactual: {actual}\nexpected: {expected}",
                entry.method
            ));
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
}

/// Adjacent cells are conservative descriptor guards, not new hosted policy claims.
#[tokio::test]
async fn adjacent_evidence_keeps_existing_descriptions() {
    let entries = diff::load_golden(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/diagnostic/databricks_150_description.jsonl"
    ))
    .unwrap();
    let server = support::start_server().await;
    let channel = support::channel(server.addr).await;
    let mut sql = qc::sql_client::SqlClient::new(channel.clone());
    let mut execution = qc::execution_client::ExecutionClient::new(channel);
    let original: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(entries[0].request.clone()).unwrap();
    let update: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(entries[2].request.clone()).unwrap();
    let confirmed: qc::ConfirmExecutionRequest =
        serde_json::from_value(entries[1].request.clone()).unwrap();
    let modified = "model was executed because the table was modified after the cached execution";
    let generic = "model was executed because either its query didn't match or its upstream data is out of date";
    for case in [
        "target_only",
        "unknown_source",
        "older_source",
        "zero_source",
        "recorded_unknown_source",
        "recorded_zero_source",
        "recorded_zero_target",
        "mixed_zero_source",
        "mixed_zero_current_source",
        "all_policy",
        "unknown_target",
        "older_target",
        "zero_target",
        "snowflake",
        "append",
        "lag",
        "sql_changed",
        "config_changed",
    ] {
        let mut first = original.clone();
        let mut next = update.clone();
        // Each case owns its protocol scope while retaining the actual captured shape.
        first.table_namespace = Some(case.into());
        next.table_namespace = first.table_namespace.clone();
        let target = next.target_table.clone().unwrap();
        let source_epoch = first.tables[0].last_modified_epoch.unwrap();
        let mut expected = modified;
        let mut build_epoch = confirmed.last_modified_epoch;
        match case {
            "target_only" => next
                .tables
                .iter_mut()
                .filter(|t| t.name != target)
                .for_each(|t| t.last_modified_epoch = Some(source_epoch)),
            "unknown_source" => next
                .tables
                .iter_mut()
                .filter(|t| t.name != target)
                .for_each(|t| t.last_modified_epoch = None),
            "older_source" => next
                .tables
                .iter_mut()
                .filter(|t| t.name != target)
                .for_each(|t| t.last_modified_epoch = Some(source_epoch - 1)),
            "zero_source" => next
                .tables
                .iter_mut()
                .filter(|t| t.name != target)
                .for_each(|t| t.last_modified_epoch = Some(0)),
            "recorded_unknown_source" => first.tables[0].last_modified_epoch = None,
            "recorded_zero_source" => first.tables[0].last_modified_epoch = Some(0),
            "recorded_zero_target" => build_epoch = Some(0),
            "mixed_zero_source" | "mixed_zero_current_source" => {
                let name = format!("{}_extra", first.tables[0].name);
                first.tables.push(qc::TableModifiedInfo {
                    name: name.clone(),
                    last_modified_epoch: Some(if case == "mixed_zero_source" {
                        0
                    } else {
                        source_epoch
                    }),
                });
                next.tables.push(qc::TableModifiedInfo {
                    name,
                    last_modified_epoch: Some(0),
                });
            }
            "all_policy" => {
                first.stale_upstream_policy = 1;
                next.stale_upstream_policy = 1;
            }
            "unknown_target" => {
                next.tables
                    .iter_mut()
                    .filter(|t| t.name == target)
                    .for_each(|t| t.last_modified_epoch = None);
                expected = generic;
            }
            "older_target" | "zero_target" => {
                next.tables
                    .iter_mut()
                    .filter(|t| t.name == target)
                    .for_each(|t| {
                        t.last_modified_epoch = Some(if case == "zero_target" {
                            0
                        } else {
                            confirmed.last_modified_epoch.unwrap() - 1
                        })
                    });
                expected = generic;
            }
            "snowflake" => {
                first.dialect = "snowflake".into();
                next.dialect = first.dialect.clone();
            }
            "append" => {
                first.execution_type = 2;
                next.execution_type = 2;
            }
            "lag" => {
                first.freshness_tolerance_seconds = 1000;
                next.freshness_tolerance_seconds = 1000;
            }
            "sql_changed" => {
                next.sql = next.sql.replace("select *", "select *, 1 as new_column");
                expected = generic;
            }
            "config_changed" => {
                next.semantic_extras.insert("lookback".into(), "2".into());
                expected = generic;
            }
            _ => unreachable!(),
        }
        let built = sql
            .submit_enriched_sql(support::captured_request(first, &entries[0]))
            .await
            .unwrap()
            .into_inner();
        let Some(qc::submit_sql_response::Response::ReadyToExecute(built)) = built.response else {
            panic!("{case}: first build");
        };
        let mut confirmation = confirmed.clone();
        confirmation.request_id = built.request_id;
        confirmation.last_modified_epoch = build_epoch;
        assert!(
            execution
                .confirm_execution(support::captured_request(confirmation, &entries[1]))
                .await
                .unwrap()
                .into_inner()
                .success
        );
        let response = sql
            .submit_enriched_sql(support::captured_request(next, &entries[2]))
            .await
            .unwrap()
            .into_inner();
        let Some(qc::submit_sql_response::Response::ReadyToExecute(response)) = response.response
        else {
            panic!("{case}: must build");
        };
        let decision = response.explained_decision.unwrap();
        assert_eq!(decision.decision_description, expected, "{case}");
        assert_eq!(decision.decision, 1, "{case}");
        assert!(!decision.is_stale, "{case}");
        assert_eq!(decision.skip_rejection_reason, Some(6), "{case}");
        assert_eq!(decision.clone_rejection_reason, Some(6), "{case}");
    }
}
#[tokio::test]
async fn unconfirmed_microbatch_target_display_requires_current_positive_scoped_evidence() {
    let rows: Vec<serde_json::Value> =
        include_str!("../../../golden/fixtures/databricks_moving_windows.jsonl")
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
    let original: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(rows[10]["request"].clone()).unwrap();
    let server = support::start_server().await;
    let channel = support::channel(server.addr).await;
    let mut sql = qc::sql_client::SqlClient::new(channel.clone());
    let mut explain = qc::explain_client::ExplainClient::new(channel);
    for case in [
        "positive",
        "absent",
        "unknown",
        "zero",
        "negative",
        "unknown_source",
        "zero_source",
        "snowflake",
        "duckdb",
        "merge",
        "lag",
        "all",
    ] {
        let mut request = original.clone();
        request.table_namespace = Some(format!("unconfirmed-{case}"));
        let target = request.target_table.clone().unwrap();
        match case {
            "absent" => request.tables.retain(|t| t.name != target),
            "unknown" | "zero" | "negative" => request
                .tables
                .iter_mut()
                .filter(|t| t.name == target)
                .for_each(|t| {
                    t.last_modified_epoch = match case {
                        "unknown" => None,
                        "zero" => Some(0),
                        _ => Some(-1),
                    }
                }),
            "unknown_source" | "zero_source" => request
                .tables
                .iter_mut()
                .filter(|t| t.name != target)
                .for_each(|t| {
                    t.last_modified_epoch = if case == "unknown_source" {
                        None
                    } else {
                        Some(0)
                    }
                }),
            "snowflake" | "duckdb" => request.dialect = case.into(),
            "merge" => request.execution_type = 3,
            "lag" => request.freshness_tolerance_seconds = 1,
            "all" => request.stale_upstream_policy = 1,
            _ => {}
        }
        let response = sql.submit_enriched_sql(request).await.unwrap().into_inner();
        let qc::submit_sql_response::Response::ReadyToExecute(build) = response.response.unwrap()
        else {
            panic!("unconfirmed physical result must BUILD: {case}")
        };
        let decision = build.explained_decision.unwrap();
        assert_eq!(
            (
                decision.decision,
                decision.skip_rejection_reason,
                decision.clone_rejection_reason,
                decision.is_stale
            ),
            (1, Some(6), Some(6), false),
            "{case}"
        );
        assert_eq!(
            decision.decision_description,
            if case == "positive" {
                "model was executed because either its query didn't match or its upstream data is out of date"
            } else {
                "model was executed because its table did not exist"
            },
            "{case}"
        );
        let messages = explain
            .get_explain_messages(qc::GetExplainMessagesRequest {
                execution_decision_ids: vec![build.execution_decision_id.unwrap()],
            })
            .await
            .unwrap()
            .into_inner();
        let text = serde_json::to_string(&messages).unwrap();
        if case == "positive" {
            assert!(text.contains("the model table exists already"));
            assert!(!text.contains("upstream data is up to date"));
            assert!(!text.contains("no updates since"));
        } else {
            assert!(text.contains("the model table does not exist"), "{case}");
        }
    }
}
