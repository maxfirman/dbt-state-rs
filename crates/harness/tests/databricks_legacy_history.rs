//! Public upgrade behavior from complete historical local-server row exports.
//! These are old self-hosted observations, not hosted persisted State exports.
mod support;

use dbt_state_proto::query_cache as qc;
use serde_json::Value;
use support::scenario::{built_at, into_execute, into_skip, Scenario};

async fn explain(scenario: &Scenario, id: &str, decision: i32) {
    let response = qc::explain_client::ExplainClient::new(scenario.channel())
        .get_explain_messages(qc::GetExplainMessagesRequest {
            execution_decision_ids: vec![id.into()],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.messages.len(), 1);
    let message = &response.messages[0];
    assert_eq!(message.execution_decision_id, id);
    assert_eq!(message.decision, decision);
    assert!(!message.explain_lines.is_empty());
}

#[tokio::test]
async fn complete_v3_sql_history_rebuilds_then_reuses_its_new_confirmed_result() {
    for bytes in [
        include_str!("../../../golden/upgrade/databricks_identity_v3_rendered.json"),
        include_str!("../../../golden/upgrade/databricks_identity_v3_template.json"),
        include_str!("../../../golden/upgrade/databricks_identity_v3_record.json"),
    ] {
        let fixture: Value = serde_json::from_str(bytes).unwrap();
        for pending in [false, true] {
            if pending && fixture.get("pending_rows").is_none() {
                continue; // RecordExecutions publishes only confirmed history.
            }
            let server = support::start_server().await;
            let scenario = Scenario::connect(&server).await;
            let row = &fixture[if pending { "pending_rows" } else { "rows" }][0];
            support::legacy::import_complete_execution(&server, row).await;
            if pending {
                let outcome = serde_json::from_value(fixture["outcome"].clone()).unwrap();
                assert!(scenario.try_confirm(outcome).await.unwrap().success);
            }
            let repeat: qc::SubmitEnrichedSqlRequest =
                serde_json::from_value(fixture["unchanged_request"].clone()).unwrap();
            let ready = scenario.expect_execute(repeat.clone()).await;
            explain(
                &scenario,
                ready.execution_decision_id.as_deref().unwrap(),
                1,
            )
            .await;
            let epoch = fixture["rows"][0]["last_modified_epoch"].as_i64().unwrap() + 1000;
            assert!(
                scenario
                    .confirm(&ready.request_id, built_at(epoch).runtime(16001))
                    .await
            );
            let mut current = repeat;
            for table in &mut current.tables {
                if Some(&table.name) == current.target_table.as_ref() {
                    table.last_modified_epoch = Some(epoch);
                }
            }
            let reused = scenario.expect_skip(current).await;
            assert_eq!(reused.execution_runtime_ms, Some(16001));
            explain(
                &scenario,
                reused.execution_decision_id.as_deref().unwrap(),
                0,
            )
            .await;
        }
    }
}

#[tokio::test]
async fn complete_old_seed_rows_keep_compatible_reuse_and_reject_changed_values_or_config() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../golden/upgrade/databricks_seed_v2_3021560.json"
    ))
    .unwrap();
    let episodes = fixture["episodes"].as_array().unwrap();
    for (index, episode) in episodes.iter().enumerate() {
        for pending in [false, true] {
            let server = support::start_server().await;
            let scenario = Scenario::connect(&server).await;
            support::legacy::import_complete_execution(
                &server,
                &episode[if pending { "pending_row" } else { "row" }],
            )
            .await;
            if pending {
                assert!(
                    scenario
                        .try_confirm(serde_json::from_value(episode["outcome"].clone()).unwrap())
                        .await
                        .unwrap()
                        .success
                );
            }
            let unchanged: qc::SubmitValuesRequest =
                serde_json::from_value(episode["unchanged_request"].clone()).unwrap();
            let reused = into_skip(scenario.submit_values(unchanged.clone()).await);
            assert_eq!(
                reused.execution_runtime_ms,
                episode["row"]["execution_runtime_ms"].as_i64()
            );
            explain(
                &scenario,
                reused.execution_decision_id.as_deref().unwrap(),
                0,
            )
            .await;
            // Independent positive history must not leak across organization,
            // warehouse family or absent/empty/different connection scopes.
            assert_eq!(
                scenario
                    .org("another-org")
                    .values_verdict(unchanged.clone())
                    .await,
                "ready_to_execute"
            );
            for namespace in [None, Some(""), Some("different-connection")] {
                let mut other_scope = unchanged.clone();
                other_scope.table_namespace = namespace.map(str::to_owned);
                assert_eq!(
                    scenario.values_verdict(other_scope).await,
                    "ready_to_execute"
                );
            }
            let mut other_warehouse = unchanged.clone();
            other_warehouse.dialect = "snowflake".into();
            assert_eq!(
                scenario.values_verdict(other_warehouse).await,
                "ready_to_execute"
            );
            // Actual next captured edit: values, column type, delimiter, or
            // quote_columns. No old identity is recomputed by this test.
            if let Some(next) = episodes.get(index + 1) {
                let changed: qc::SubmitValuesRequest =
                    serde_json::from_value(next["request"].clone()).unwrap();
                let ready = into_execute(scenario.submit_values(changed.clone()).await);
                explain(
                    &scenario,
                    ready.execution_decision_id.as_deref().unwrap(),
                    1,
                )
                .await;
                let epoch = next["row"]["last_modified_epoch"].as_i64().unwrap();
                assert!(
                    scenario
                        .confirm(&ready.request_id, built_at(epoch).runtime(16002))
                        .await
                );
                let mut current = changed;
                current.last_modified_epoch = Some(epoch);
                let reused = into_skip(scenario.submit_values(current).await);
                assert_eq!(reused.execution_runtime_ms, Some(16002));
                explain(
                    &scenario,
                    reused.execution_decision_id.as_deref().unwrap(),
                    0,
                )
                .await;
            }
        }
    }
}
