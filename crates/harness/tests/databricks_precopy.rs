//! Native #134 causal pre-copy requests, unchanged except correlated transport IDs.
mod support;

use dbt_state_harness::diff::GoldenEntry;
use dbt_state_proto::query_cache as qc;
use support::scenario::Scenario;

fn entries() -> Vec<GoldenEntry> {
    include_str!("fixtures/databricks_precopy_native.jsonl")
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

async fn source(sc: &Scenario, rows: &[GoldenEntry]) {
    let req: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(rows[0].request.clone()).unwrap();
    let id = sc.execute_id(req).await;
    let mut confirm: qc::ConfirmExecutionRequest =
        serde_json::from_value(rows[1].request.clone()).unwrap();
    confirm.request_id = id;
    assert!(
        qc::execution_client::ExecutionClient::new(sc.channel())
            .confirm_execution(support::captured_request(confirm, &rows[1]))
            .await
            .unwrap()
            .into_inner()
            .success
    );
}

/// Preview policy refuses even a source that qualifies for the isolated copy path.
#[tokio::test]
async fn preview_defaults_to_no_databricks_copy_grants() {
    let rows = entries();
    let server = support::start_server().await;
    let sc = Scenario::connect(&server)
        .await
        .org(support::captured_org(&rows[0]));
    source(&sc, &rows).await;
    let req: qc::CloneRequest = serde_json::from_value(rows[2].request.clone()).unwrap();
    let response = sc.register_clone(req).await.unwrap();
    assert!(
        response.ready_to_clone.is_none(),
        "preview must not grant copy DDL"
    );
    assert!(
        response.response.is_none(),
        "deprecated copy grant must also be absent"
    );
    let refusal = response.unable_to_clone.expect("normal no-copy response");
    assert_eq!(
        refusal.explained_decision.unwrap().clone_rejection_reason,
        Some(10)
    );

    // The caller can continue via ordinary SubmitEnrichedSQL after refusal.
    // The absent target has no copied result and must be built normally.
    let mut submit: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(rows[4].request.clone()).unwrap();
    submit
        .tables
        .retain(|table| Some(&table.name) != submit.target_table.as_ref());
    assert_eq!(sc.verdict(submit).await, "ready_to_execute");
}

#[tokio::test]
async fn ordinary_databricks_full_submit_never_automatically_copies() {
    let rows = entries();
    // Even direct-copy opt-in cannot expand ordinary-submit copy policy.
    for direct_copy in [false, true] {
        let server = if direct_copy {
            support::start_server_databricks_direct_copy().await
        } else {
            support::start_server().await
        };
        let sc = Scenario::connect(&server)
            .await
            .org(support::captured_org(&rows[0]));
        let mut submit: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(rows[0].request.clone()).unwrap();
        submit.execution_type = qc::ModelExecutionType::Full as i32;
        let id = sc.execute_id(submit.clone()).await;
        let mut confirm: qc::ConfirmExecutionRequest =
            serde_json::from_value(rows[1].request.clone()).unwrap();
        confirm.request_id = id;
        assert!(sc.try_confirm(confirm).await.unwrap().success);
        submit.target_table = Some(rows[2].request["target_table"].as_str().unwrap().into());
        assert_eq!(sc.verdict(submit).await, "ready_to_execute");
    }
}

#[tokio::test]
async fn native_default_format_precopy_returns_exact_hosted_ddl() {
    for rows in entries().chunks(8) {
        let server = support::start_server_databricks_direct_copy().await;
        let sc = Scenario::connect(&server)
            .await
            .org(support::captured_org(&rows[0]));
        source(&sc, rows).await;
        let req: qc::CloneRequest = serde_json::from_value(rows[2].request.clone()).unwrap();
        assert!(req.clone_source_table_type.is_none());
        let response = sc.register_clone(req).await.unwrap();
        let mut actual = serde_json::to_value(response).unwrap();
        // IDs are generated independently by the local service. Keep every
        // decision, DDL byte, null and deprecated mirror from the capture.
        for path in ["ready_to_clone", "response"] {
            let (actual, expected) = if path == "response" {
                (
                    &mut actual[path]["ready_to_clone_v1"],
                    &rows[2].response[path]["ready_to_clone_v1"],
                )
            } else {
                (&mut actual[path], &rows[2].response[path])
            };
            actual["request_id"] = expected["request_id"].clone();
        }
        assert_eq!(actual, rows[2].response);
    }
}

/// Safety controls are local requirements; they make no claim about unknown
/// hosted rejection codes. A pending or unproven source cannot authorize DDL.
#[tokio::test]
async fn direct_precopy_requires_current_source_in_exact_scope() {
    let rows = entries();
    let server = support::start_server_databricks_direct_copy().await;
    let sc = Scenario::connect(&server)
        .await
        .org(support::captured_org(&rows[0]));
    let request: qc::CloneRequest = serde_json::from_value(rows[2].request.clone()).unwrap();
    assert!(sc
        .register_clone(request.clone())
        .await
        .unwrap()
        .ready_to_clone
        .is_none());
    let source_request: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(rows[0].request.clone()).unwrap();
    let pending = sc.execute_id(source_request.clone()).await;
    assert!(sc
        .register_clone(request.clone())
        .await
        .unwrap()
        .ready_to_clone
        .is_none());
    let mut confirmation: qc::ConfirmExecutionRequest =
        serde_json::from_value(rows[1].request.clone()).unwrap();
    confirmation.request_id = pending;
    assert!(sc.try_confirm(confirmation.clone()).await.unwrap().success);
    assert!(sc
        .register_clone(request.clone())
        .await
        .unwrap()
        .ready_to_clone
        .is_some());
    for case in [
        "missing_namespace",
        "empty_namespace",
        "wrong_namespace",
        "wrong_uid",
        "missing_uid",
        "wrong_epoch",
        "unknown_epoch",
        "wrong_kind",
        "foreign_family",
        "chain_limit",
    ] {
        let mut req = request.clone();
        match case {
            "missing_namespace" => req.table_namespace = None,
            "empty_namespace" => req.table_namespace = Some(String::new()),
            "wrong_namespace" => req.table_namespace = Some("another-connection".into()),
            "wrong_uid" => {
                req.labels.insert(
                    "dbt_node_unique_id".into(),
                    "model.other.copy_incremental".into(),
                );
            }
            "missing_uid" => {
                req.labels.remove("dbt_node_unique_id");
            }
            "wrong_epoch" => req.clone_source_last_modified_epoch = Some(1),
            "unknown_epoch" => req.clone_source_last_modified_epoch = None,
            "wrong_kind" => req.execution_type = 7,
            "foreign_family" => req.dialect = "duckdb".into(),
            "chain_limit" => req.clone_chain_depth_limit = Some(0),
            _ => unreachable!(),
        }
        assert!(
            sc.register_clone(req)
                .await
                .unwrap()
                .ready_to_clone
                .is_none(),
            "{case}"
        );
    }
    assert!(sc
        .org("other-org")
        .register_clone(request.clone())
        .await
        .unwrap()
        .ready_to_clone
        .is_none());
    // A newer incompatible result hides the older valid source. It is not a
    // candidate search through history for whichever source happens to match.
    let mut replacement = source_request;
    replacement.sql.push_str(" where id > 0");
    replacement.dbt_node_state.as_mut().unwrap().node_unique_id =
        "model.other.copy_incremental".into();
    let id = sc.execute_id(replacement).await;
    confirmation.request_id = id;
    confirmation.last_modified_epoch = confirmation.last_modified_epoch.map(|epoch| epoch + 1000);
    assert!(sc.try_confirm(confirmation).await.unwrap().success);
    assert!(sc
        .register_clone(request)
        .await
        .unwrap()
        .ready_to_clone
        .is_none());
}

#[tokio::test]
async fn native_confirmed_copy_adopts_exact_source_identity_and_inputs() {
    let rows = entries();
    let server = support::start_server_databricks_direct_copy().await;
    let sc = Scenario::connect(&server)
        .await
        .org(support::captured_org(&rows[0]));
    source(&sc, &rows).await;
    let req: qc::CloneRequest = serde_json::from_value(rows[2].request.clone()).unwrap();
    let ready = sc
        .register_clone(req)
        .await
        .unwrap()
        .ready_to_clone
        .unwrap();
    let adopter: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(rows[4].request.clone()).unwrap();
    sc.expect_execute(adopter.clone()).await;
    let mut confirmation: qc::ConfirmExecutionRequest =
        serde_json::from_value(rows[3].request.clone()).unwrap();
    confirmation.request_id = ready.request_id;
    assert!(sc.try_confirm(confirmation).await.unwrap().success);
    let response = sc.submit(adopter).await;
    let mut actual = serde_json::to_value(response).unwrap();
    actual["response"]["skip_execution"]["execution_decision_id"] =
        rows[4].response["response"]["skip_execution"]["execution_decision_id"].clone();
    assert_eq!(actual, rows[4].response);
}

#[tokio::test]
async fn earlier_submitted_pending_source_blocks_registration() {
    let rows = entries();
    let server = support::start_server_databricks_direct_copy().await;
    let sc = Scenario::connect(&server)
        .await
        .org(support::captured_org(&rows[0]));
    let mut earlier: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(rows[0].request.clone()).unwrap();
    earlier.sql.push_str(" where id > 0");
    let _earlier_pending = sc.execute_id(earlier).await;
    source(&sc, &rows).await;
    // Submission order cannot prove completion order. The earlier pending
    // write may replace the current source while the warehouse copies it.
    let req: qc::CloneRequest = serde_json::from_value(rows[2].request.clone()).unwrap();
    assert!(sc
        .register_clone(req)
        .await
        .unwrap()
        .ready_to_clone
        .is_none());
}

#[tokio::test]
async fn template_mode_without_signatures_cannot_establish_direct_adoption() {
    let rows = entries();
    let server = support::start_server_databricks_direct_copy().await;
    let sc = Scenario::connect(&server)
        .await
        .org(support::captured_org(&rows[0]));
    let mut request: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(rows[0].request.clone()).unwrap();
    request.compare_unrendered_code = true;
    request.dbt_node_state.as_mut().unwrap().node_body_hash = None;
    request.dbt_node_state.as_mut().unwrap().node_configs_hash = None;
    let id = sc.execute_id(request).await;
    let mut confirm: qc::ConfirmExecutionRequest =
        serde_json::from_value(rows[1].request.clone()).unwrap();
    confirm.request_id = id;
    assert!(sc.try_confirm(confirm).await.unwrap().success);
    let copy: qc::CloneRequest = serde_json::from_value(rows[2].request.clone()).unwrap();
    let ready = sc
        .register_clone(copy)
        .await
        .unwrap()
        .ready_to_clone
        .unwrap();
    let mut confirm: qc::ConfirmExecutionRequest =
        serde_json::from_value(rows[3].request.clone()).unwrap();
    confirm.request_id = ready.request_id;
    assert!(sc.try_confirm(confirm).await.unwrap().success);
    let mut adopter: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(rows[4].request.clone()).unwrap();
    adopter.compare_unrendered_code = true;
    adopter.dbt_node_state.as_mut().unwrap().node_body_hash = None;
    adopter.dbt_node_state.as_mut().unwrap().node_configs_hash = None;
    sc.expect_execute(adopter).await;
}

fn assert_native_response(actual: impl serde::Serialize, expected: &serde_json::Value) {
    fn remap(actual: &mut serde_json::Value, expected: &serde_json::Value) {
        if let (Some(actual), Some(expected)) = (actual.as_object_mut(), expected.as_object()) {
            for (key, value) in actual {
                if let Some(want) = expected.get(key) {
                    if ["request_id", "execution_decision_id"].contains(&key.as_str())
                        && value.as_str().is_some_and(|s| !s.is_empty())
                        && want.as_str().is_some_and(|s| !s.is_empty())
                    {
                        *value = want.clone();
                    } else {
                        remap(value, want);
                    }
                }
            }
        }
    }
    let mut actual = serde_json::to_value(actual).unwrap();
    remap(&mut actual, expected);
    assert_eq!(&actual, expected);
}

#[tokio::test]
async fn native_merge_and_snapshot_copy_adoption_repeat_and_changed_input() {
    for rows in entries().chunks(8) {
        let server = support::start_server_databricks_direct_copy().await;
        let sc = Scenario::connect(&server)
            .await
            .org(support::captured_org(&rows[0]));
        source(&sc, rows).await;
        let request: qc::CloneRequest = serde_json::from_value(rows[2].request.clone()).unwrap();
        let ready = sc
            .register_clone(request)
            .await
            .unwrap()
            .ready_to_clone
            .unwrap();
        let mut confirm: qc::ConfirmExecutionRequest =
            serde_json::from_value(rows[3].request.clone()).unwrap();
        confirm.request_id = ready.request_id;
        assert!(sc.try_confirm(confirm).await.unwrap().success);
        for row in &rows[4..7] {
            let request: qc::SubmitEnrichedSqlRequest =
                serde_json::from_value(row.request.clone()).unwrap();
            let response = sc.submit(request.clone()).await;
            assert_native_response(&response, &row.response);
            if let Some(qc::submit_sql_response::Response::ReadyToExecute(ready)) =
                response.response
            {
                let mut confirmation: qc::ConfirmExecutionRequest =
                    serde_json::from_value(rows[7].request.clone()).unwrap();
                confirmation.request_id = ready.request_id;
                let epoch = confirmation.last_modified_epoch;
                assert!(sc.try_confirm(confirmation).await.unwrap().success);
                let mut repeated = request;
                for table in &mut repeated.tables {
                    if Some(&table.name) == repeated.target_table.as_ref() {
                        table.last_modified_epoch = epoch;
                    }
                }
                sc.expect_skip(repeated).await;
            }
        }
    }
}

#[tokio::test]
async fn adoption_uses_source_input_epoch_even_when_input_changed_before_copy() {
    let rows = entries();
    let server = support::start_server_databricks_direct_copy().await;
    let sc = Scenario::connect(&server)
        .await
        .org(support::captured_org(&rows[0]));
    source(&sc, &rows).await;
    let req: qc::CloneRequest = serde_json::from_value(rows[2].request.clone()).unwrap();
    let ready = sc
        .register_clone(req)
        .await
        .unwrap()
        .ready_to_clone
        .unwrap();
    let mut confirmation: qc::ConfirmExecutionRequest =
        serde_json::from_value(rows[3].request.clone()).unwrap();
    confirmation.request_id = ready.request_id;
    assert!(sc.try_confirm(confirmation.clone()).await.unwrap().success);
    let adopter: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(rows[4].request.clone()).unwrap();
    sc.expect_skip(adopter.clone()).await;
    let mut changed = adopter.clone();
    changed
        .tables
        .iter_mut()
        .find(|t| Some(&t.name) != changed.target_table.as_ref())
        .unwrap()
        .last_modified_epoch = confirmation.last_modified_epoch.map(|epoch| epoch - 1000);
    // It changed AFTER the source read but BEFORE the copy. The copy epoch
    // must not silently replace the immutable source's input epoch baseline.
    sc.expect_execute(changed).await;
    for case in [
        "sql",
        "config",
        "uid",
        "binding",
        "own_epoch",
        "unknown_input",
        "template",
        "template_without_signatures",
    ] {
        let mut request = adopter.clone();
        match case {
            "sql" => request.sql.push_str(" where id > 0"),
            "config" => {
                request
                    .semantic_extras
                    .insert("unique_key".into(), "\"other\"".into());
            }
            "uid" => {
                request.dbt_node_state.as_mut().unwrap().node_unique_id =
                    "model.other.copy_incremental".into()
            }
            "binding" => {
                request.sql = request.sql.replace("`inputs`", "`other_inputs`");
                for table in &mut request.tables {
                    table.name = table.name.replace("`inputs`", "`other_inputs`");
                }
            }
            "own_epoch" => {
                request
                    .tables
                    .iter_mut()
                    .find(|t| Some(&t.name) == request.target_table.as_ref())
                    .unwrap()
                    .last_modified_epoch = confirmation.last_modified_epoch.map(|epoch| epoch + 1)
            }
            "unknown_input" => {
                request
                    .tables
                    .iter_mut()
                    .find(|t| Some(&t.name) != request.target_table.as_ref())
                    .unwrap()
                    .last_modified_epoch = None
            }
            "template" => request.compare_unrendered_code = true,
            "template_without_signatures" => {
                request.compare_unrendered_code = true;
                let state = request.dbt_node_state.as_mut().unwrap();
                state.node_body_hash = None;
                state.node_configs_hash = None;
            }
            _ => unreachable!(),
        }
        sc.expect_execute(request).await;
    }
}

#[tokio::test]
async fn source_replacement_between_registration_and_confirmation_cannot_be_adopted() {
    for publish_source in [false, true] {
        let rows = entries();
        let server = support::start_server_databricks_direct_copy().await;
        let sc = Scenario::connect(&server)
            .await
            .org(support::captured_org(&rows[0]));
        source(&sc, &rows).await;
        let req: qc::CloneRequest = serde_json::from_value(rows[2].request.clone()).unwrap();
        let ready = sc
            .register_clone(req)
            .await
            .unwrap()
            .ready_to_clone
            .unwrap();
        let mut replacement: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(rows[0].request.clone()).unwrap();
        replacement.sql.push_str(" where id > 0");
        let source_id = sc.execute_id(replacement).await;
        let mut source_confirm: qc::ConfirmExecutionRequest =
            serde_json::from_value(rows[1].request.clone()).unwrap();
        source_confirm.request_id = source_id;
        source_confirm.last_modified_epoch =
            source_confirm.last_modified_epoch.map(|epoch| epoch + 1);
        if publish_source {
            assert!(sc.try_confirm(source_confirm).await.unwrap().success);
        }
        let mut confirmation: qc::ConfirmExecutionRequest =
            serde_json::from_value(rows[3].request.clone()).unwrap();
        confirmation.request_id = ready.request_id;
        assert!(sc.try_confirm(confirmation).await.unwrap().success);
        let adopter: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(rows[4].request.clone()).unwrap();
        sc.expect_execute(adopter).await;
    }
}

#[tokio::test]
async fn changed_producer_before_copy_cannot_be_hidden_by_copy_age_or_lag() {
    let rows = entries();
    let server = support::start_server_databricks_direct_copy().await;
    let sc = Scenario::connect(&server)
        .await
        .org(support::captured_org(&rows[0]));
    source(&sc, &rows).await;
    let adopter: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(rows[4].request.clone()).unwrap();
    let input = adopter
        .tables
        .iter()
        .find(|t| Some(&t.name) != adopter.target_table.as_ref())
        .unwrap();
    let mut producer = adopter.clone();
    producer.target_table = Some(input.name.clone());
    producer.tables.clear();
    producer.sql = "select 1 as id".into();
    producer.execution_type = 1;
    producer.dbt_node_state.as_mut().unwrap().node_unique_id = "model.other.raw_producer".into();
    let producer_id = sc.execute_id(producer).await;
    let mut producer_confirm: qc::ConfirmExecutionRequest =
        serde_json::from_value(rows[1].request.clone()).unwrap();
    producer_confirm.request_id = producer_id;
    producer_confirm.last_modified_epoch = input.last_modified_epoch;
    assert!(sc.try_confirm(producer_confirm).await.unwrap().success);
    let req: qc::CloneRequest = serde_json::from_value(rows[2].request.clone()).unwrap();
    let ready = sc
        .register_clone(req)
        .await
        .unwrap()
        .ready_to_clone
        .unwrap();
    let mut confirmation: qc::ConfirmExecutionRequest =
        serde_json::from_value(rows[3].request.clone()).unwrap();
    confirmation.request_id = ready.request_id;
    assert!(sc.try_confirm(confirmation).await.unwrap().success);
    let mut adopter = adopter;
    adopter.freshness_tolerance_seconds = 1_000_000_000;
    sc.expect_execute(adopter).await;
}

#[tokio::test]
async fn failed_clone_clears_adoption_and_unknown_source_inputs_do_not_establish_it() {
    for case in [
        "failed_clone",
        "unknown_source_epoch",
        "unqualified_sql",
        "incompatible_source_type",
    ] {
        let rows = entries();
        let server = support::start_server_databricks_direct_copy().await;
        let sc = Scenario::connect(&server)
            .await
            .org(support::captured_org(&rows[0]));
        let mut source_request: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(rows[0].request.clone()).unwrap();
        if case == "unknown_source_epoch" {
            source_request.tables[0].last_modified_epoch = None;
        }
        if case == "unqualified_sql" {
            source_request.sql = "select * from inputs".into();
        }
        let id = sc.execute_id(source_request).await;
        let mut source_confirm: qc::ConfirmExecutionRequest =
            serde_json::from_value(rows[1].request.clone()).unwrap();
        source_confirm.request_id = id;
        if case == "incompatible_source_type" {
            source_confirm.table_type = Some("VIEW".into());
        }
        assert!(sc.try_confirm(source_confirm).await.unwrap().success);
        let request: qc::CloneRequest = serde_json::from_value(rows[2].request.clone()).unwrap();
        let response = sc.register_clone(request).await.unwrap();
        if case == "incompatible_source_type" {
            assert!(response.ready_to_clone.is_none());
            continue;
        }
        let mut confirmation: qc::ConfirmExecutionRequest =
            serde_json::from_value(rows[3].request.clone()).unwrap();
        confirmation.request_id = response.ready_to_clone.unwrap().request_id;
        confirmation.failed_to_clone = case == "failed_clone";
        assert!(sc.try_confirm(confirmation).await.unwrap().success);
        let mut adopter: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(rows[4].request.clone()).unwrap();
        if case == "unqualified_sql" {
            adopter.sql = "select * from inputs".into();
        }
        sc.expect_execute(adopter).await;
    }
}
