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

fn copy_fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../golden/upgrade/databricks_copy_v1_45e94c3.json"
    ))
    .unwrap()
}

fn native_copy_requests() -> Vec<dbt_state_harness::diff::GoldenEntry> {
    include_str!("fixtures/databricks_precopy_native.jsonl")
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[tokio::test]
async fn historical_copy_source_and_pending_or_confirmed_copy_survive_upgrade_with_grants_off() {
    let native = native_copy_requests();
    for episode in copy_fixture()["episodes"].as_array().unwrap() {
        let offset = episode["native_offset"].as_u64().unwrap() as usize;
        let rows = &native[offset..offset + 8];
        for pending in [false, true] {
            let server = support::start_server().await; // New copy grants default OFF.
            let sc = Scenario::connect(&server).await;
            // Allocate a real current row first: restoration must remap source
            // IDs, rather than accidentally working because old IDs start at 1.
            sc.expect_execute(support::scenario::model("unrelated").sql("select 160 as unrelated"))
                .await;
            support::legacy::import_complete_history(
                &server,
                &[
                    episode["source_row"].clone(),
                    episode[if pending {
                        "pending_copy"
                    } else {
                        "confirmed_copy"
                    }]
                    .clone(),
                ],
            )
            .await;
            let mut source: qc::SubmitEnrichedSqlRequest =
                serde_json::from_value(rows[0].request.clone()).unwrap();
            source.tables.push(qc::TableModifiedInfo {
                name: source.target_table.clone().unwrap(),
                last_modified_epoch: episode["source_row"]["last_modified_epoch"].as_i64(),
            });
            sc.expect_skip(source).await;
            let copy: qc::CloneRequest = serde_json::from_value(rows[2].request.clone()).unwrap();
            assert!(sc
                .register_clone(copy)
                .await
                .unwrap()
                .ready_to_clone
                .is_none());
            let adopter: qc::SubmitEnrichedSqlRequest =
                serde_json::from_value(rows[4].request.clone()).unwrap();
            if pending {
                sc.expect_execute(adopter).await;
                assert!(
                    sc.try_confirm(
                        serde_json::from_value(episode["copy_outcome"].clone()).unwrap()
                    )
                    .await
                    .unwrap()
                    .success
                );
            }
            for row in &rows[4..7] {
                let request: qc::SubmitEnrichedSqlRequest =
                    serde_json::from_value(row.request.clone()).unwrap();
                if row.response["response"].get("skip_execution").is_some() {
                    let skipped = sc.expect_skip(request).await;
                    assert_eq!(
                        skipped.execution_runtime_ms,
                        episode["source_row"]["execution_runtime_ms"].as_i64()
                    );
                    explain(&sc, skipped.execution_decision_id.as_deref().unwrap(), 0).await;
                } else {
                    let ready = sc.expect_execute(request.clone()).await;
                    explain(&sc, ready.execution_decision_id.as_deref().unwrap(), 1).await;
                    let mut outcome: qc::ConfirmExecutionRequest =
                        serde_json::from_value(rows[7].request.clone()).unwrap();
                    outcome.request_id = ready.request_id;
                    let epoch = outcome.last_modified_epoch;
                    assert!(sc.try_confirm(outcome).await.unwrap().success);
                    let mut repeat = request;
                    for table in &mut repeat.tables {
                        if Some(&table.name) == repeat.target_table.as_ref() {
                            table.last_modified_epoch = epoch;
                        }
                    }
                    sc.expect_skip(repeat).await;
                }
            }
        }
    }
}

#[tokio::test]
async fn historical_copy_is_not_adopted_across_scope_identity_or_input_changes() {
    let native = native_copy_requests();
    for episode in copy_fixture()["episodes"].as_array().unwrap() {
        let offset = episode["native_offset"].as_u64().unwrap() as usize;
        let server = support::start_server().await;
        let sc = Scenario::connect(&server).await;
        support::legacy::import_complete_history(
            &server,
            &[
                episode["source_row"].clone(),
                episode["confirmed_copy"].clone(),
            ],
        )
        .await;
        let adopter: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(native[offset + 4].request.clone()).unwrap();
        sc.expect_skip(adopter.clone()).await;
        sc.org("another-org").expect_execute(adopter.clone()).await;
        for case in [
            "missing_namespace",
            "empty_namespace",
            "other_namespace",
            "warehouse_family",
            "full_uid",
            "sql",
            "config",
            "input_before_copy",
            "unknown_input",
            "template_without_signatures",
        ] {
            let mut changed = adopter.clone();
            match case {
                "missing_namespace" => changed.table_namespace = None,
                "empty_namespace" => changed.table_namespace = Some(String::new()),
                "other_namespace" => changed.table_namespace = Some("other-connection".into()),
                "warehouse_family" => changed.dialect = "snowflake".into(),
                // Keep the short node name: the complete package-qualified UID matters.
                "full_uid" => {
                    let uid = &mut changed.dbt_node_state.as_mut().unwrap().node_unique_id;
                    *uid = uid.replacen("survey_20261112e134ef", "another_project", 1);
                }
                "sql" => changed.sql.push_str(" where id > 0"),
                "config" => {
                    changed
                        .semantic_extras
                        .insert("unique_key".into(), "\"another\"".into());
                }
                "input_before_copy" | "unknown_input" => {
                    let input = changed
                        .tables
                        .iter_mut()
                        .find(|t| Some(&t.name) != changed.target_table.as_ref())
                        .unwrap();
                    input.last_modified_epoch = if case == "unknown_input" {
                        None
                    } else {
                        Some(
                            episode["confirmed_copy"]["last_modified_epoch"]
                                .as_i64()
                                .unwrap()
                                - 1000,
                        )
                    };
                }
                "template_without_signatures" => {
                    changed.compare_unrendered_code = true;
                    changed.dbt_node_state.as_mut().unwrap().node_body_hash = None;
                    changed.dbt_node_state.as_mut().unwrap().node_configs_hash = None;
                }
                _ => unreachable!(),
            }
            assert_eq!(sc.verdict(changed).await, "ready_to_execute", "{case}");
        }
        // Negative submissions never replace the confirmed copy's identity.
        sc.expect_skip(adopter).await;
    }
}

#[tokio::test]
async fn damaged_latest_copy_identity_cold_misses_without_resurrecting_valid_older_history() {
    let native = native_copy_requests();
    for episode in copy_fixture()["episodes"].as_array().unwrap() {
        let offset = episode["native_offset"].as_u64().unwrap() as usize;
        for case in [
            "legacy_null",
            "unknown_version",
            "malformed",
            "unknown_identity",
            "missing_source",
            "missing_uid",
            "unknown_producer",
        ] {
            let server = support::start_server().await;
            let sc = Scenario::connect(&server).await;
            // Explicit storage faults, NOT historical server output. Preserve
            // a valid older copy at the exact same epoch so a history search
            // for a matching identity would incorrectly resurrect its SKIP.
            let mut damaged = episode["confirmed_copy"].clone();
            damaged["id"] = serde_json::json!(damaged["id"].as_i64().unwrap() + 1000);
            damaged["request_id"] = serde_json::json!(uuid::Uuid::new_v4().to_string());
            match case {
                "legacy_null" => damaged["direct_copy_provenance"] = Value::Null,
                "unknown_version" => {
                    damaged["direct_copy_provenance"]["version"] = serde_json::json!(255)
                }
                "malformed" => {
                    damaged["direct_copy_provenance"] = serde_json::json!({"version": "broken"})
                }
                "unknown_identity" => {
                    damaged["direct_copy_provenance"]["identity"] =
                        serde_json::json!("unknown-old-identity")
                }
                "missing_source" => {
                    damaged["direct_copy_provenance"]["source_execution_id"] = Value::Null
                }
                "missing_uid" => damaged["node_unique_id"] = Value::Null,
                "unknown_producer" => {
                    damaged["input_tables"][0]
                        .as_object_mut()
                        .unwrap()
                        .remove("producer");
                }
                _ => unreachable!(),
            }
            support::legacy::import_complete_history(
                &server,
                &[
                    episode["source_row"].clone(),
                    episode["confirmed_copy"].clone(),
                    damaged,
                ],
            )
            .await;
            let request: qc::SubmitEnrichedSqlRequest =
                serde_json::from_value(native[offset + 4].request.clone()).unwrap();
            let ready = sc.expect_execute(request.clone()).await;
            explain(&sc, ready.execution_decision_id.as_deref().unwrap(), 1).await;
            let epoch = episode["confirmed_copy"]["last_modified_epoch"]
                .as_i64()
                .unwrap()
                + 1000;
            assert!(
                sc.confirm(&ready.request_id, built_at(epoch).runtime(16003))
                    .await
            );
            let mut repeat = request;
            for table in &mut repeat.tables {
                if Some(&table.name) == repeat.target_table.as_ref() {
                    table.last_modified_epoch = Some(epoch);
                }
            }
            let skipped = sc.expect_skip(repeat).await;
            assert_eq!(skipped.execution_runtime_ms, Some(16003), "{case}");
            explain(&sc, skipped.execution_decision_id.as_deref().unwrap(), 0).await;
        }
    }
}

#[tokio::test]
async fn old_pending_copy_requires_valid_publication_and_rechecks_its_original_source() {
    let native = native_copy_requests();
    for episode in copy_fixture()["episodes"].as_array().unwrap() {
        let offset = episode["native_offset"].as_u64().unwrap() as usize;
        for case in [
            "retry_missing_epoch",
            "failed_clone",
            "source_pending",
            "source_replaced",
        ] {
            let server = support::start_server().await;
            let sc = Scenario::connect(&server).await;
            support::legacy::import_complete_history(
                &server,
                &[
                    episode["source_row"].clone(),
                    episode["pending_copy"].clone(),
                ],
            )
            .await;
            let request: qc::SubmitEnrichedSqlRequest =
                serde_json::from_value(native[offset + 4].request.clone()).unwrap();
            sc.expect_execute(request.clone()).await;
            let mut outcome: qc::ConfirmExecutionRequest =
                serde_json::from_value(episode["copy_outcome"].clone()).unwrap();
            match case {
                "retry_missing_epoch" => {
                    let mut invalid = outcome.clone();
                    invalid.last_modified_epoch = None;
                    let error = sc.try_confirm(invalid).await.unwrap_err();
                    assert_eq!(error.code(), tonic::Code::Internal);
                    sc.expect_execute(request.clone()).await;
                }
                "failed_clone" => outcome.failed_to_clone = true,
                "source_pending" | "source_replaced" => {
                    let mut source: qc::SubmitEnrichedSqlRequest =
                        serde_json::from_value(native[offset].request.clone()).unwrap();
                    source.sql.push_str(" where id > 0");
                    let id = sc.execute_id(source).await;
                    if case == "source_replaced" {
                        let epoch = episode["source_row"]["last_modified_epoch"]
                            .as_i64()
                            .unwrap()
                            + 1000;
                        assert!(sc.confirm(&id, built_at(epoch)).await);
                    }
                }
                _ => unreachable!(),
            }
            assert!(sc.try_confirm(outcome.clone()).await.unwrap().success);
            // A duplicate receipt is rejected without republishing history.
            assert_eq!(
                sc.try_confirm(outcome).await.unwrap_err().code(),
                tonic::Code::Internal
            );
            if case == "retry_missing_epoch" {
                let skipped = sc.expect_skip(request).await;
                explain(&sc, skipped.execution_decision_id.as_deref().unwrap(), 0).await;
            } else {
                let ready = sc.expect_execute(request).await;
                explain(&sc, ready.execution_decision_id.as_deref().unwrap(), 1).await;
            }
        }
    }
}
