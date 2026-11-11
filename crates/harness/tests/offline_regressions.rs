//! Correctness contracts established without warehouse or hosted-service access.
//! These test the public gRPC boundary and durable Postgres state.
use dbt_state_proto::query_cache as qc;

#[path = "support.rs"]
mod support;
use support::legacy::{self, Rows};
use support::scenario::{
    clone_request, data_test, model, outcome, seed, test_results, ExecutionType, Policy, Scenario,
};
use support::{rows, start_server};

/// Complete cached test results carrying a nested, non-ASCII value, so a
/// round trip must be lossless.
fn results(failures: i64) -> qc::Struct {
    let mut results = test_results(failures);
    results.fields.insert(
        "nested".into(),
        serde_json::from_value(serde_json::json!(
            {"kind": {"list_value": {"values": [{"kind": {"string_value": "é"}}]}}}
        ))
        .unwrap(),
    );
    results
}

// Models here are bound in DB.S under project `p`, with their own target present
// at epoch 100; data tests are `model.p.test-id` in the same scope.

#[tokio::test]
async fn overwritten_history_and_execution_types_cannot_be_resurrected() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let node = model("DB.S.T")
        .catalog("DB")
        .schema("S")
        .own_epoch(Some(100))
        .project("p");
    let a = node.clone().sql("select 1");
    sc.build_with(a.clone(), outcome().epoch(100)).await;
    let b = node.sql("select 2");
    sc.build_with(b, outcome().epoch(200)).await;
    let reverted = a.clone().own_epoch(Some(200));
    assert_eq!(sc.verdict(reverted).await, "ready_to_execute");
    let view = a.clone().execution_type(ExecutionType::View);
    sc.build_with(view, outcome().epoch(300)).await;
    assert_eq!(sc.verdict(a).await, "ready_to_execute");
}

#[tokio::test]
async fn missing_changed_and_cross_target_objects_do_not_skip() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let req = model("DB.S.T")
        .catalog("DB")
        .schema("S")
        .own_epoch(Some(100))
        .project("p")
        .sql("select 1")
        .namespace("ns");
    sc.build_with(req.clone(), outcome().epoch(100)).await;
    assert_eq!(sc.verdict(req.clone()).await, "skip_execution");
    for changed in [
        req.clone().no_tables(),
        req.clone().table("DB.S.T", None),
        req.clone().own_epoch(Some(200)),
    ] {
        assert_eq!(sc.verdict(changed).await, "ready_to_execute");
    }
    let ignored = req
        .clone()
        .own_epoch(Some(200))
        .ignore_external_modifications();
    assert_eq!(sc.verdict(ignored).await, "skip_execution");
    let other = req
        .clone()
        .target("DB.DEV.T")
        .no_tables()
        .allow_clones(false);
    assert_eq!(sc.verdict(other).await, "ready_to_execute");
    let req = req.namespace("different-ns");
    assert_eq!(sc.verdict(req).await, "ready_to_execute");
}

#[tokio::test]
async fn data_test_identity_logic_and_results_round_trip() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let req = data_test("model.p.test-id")
        .catalog("DB")
        .schema("S")
        .project("p")
        .sql("select 7 as failures");
    let cached = results(7);
    sc.build_with(req.clone(), outcome().results(cached.clone()))
        .await;
    let r = sc.expect_skip(req.clone()).await;
    assert_eq!(r.execution_results, Some(cached));
    let changed = req.clone().sql("select 0 as failures");
    assert_eq!(sc.verdict(changed).await, "ready_to_execute");
    let changed = req.clone().extra("severity", "\"warn\"");
    assert_eq!(sc.verdict(changed).await, "ready_to_execute");
    let other_project = req.clone().project("other");
    assert_eq!(sc.verdict(other_project).await, "ready_to_execute");
    // Older clients publish the unique id only in labels.
    let unlabelled = req.without_node_state();
    let first = unlabelled
        .clone()
        .label("dbt_node_unique_id", "test.python.first");
    sc.build_with(first.clone(), outcome().results(results(0)))
        .await;
    assert_eq!(sc.verdict(first.clone()).await, "skip_execution");
    let other = first.label("dbt_node_unique_id", "test.python.other");
    assert_eq!(sc.verdict(other).await, "ready_to_execute");
    assert_eq!(sc.verdict(unlabelled).await, "ready_to_execute");
}

#[tokio::test]
async fn unknown_freshness_and_distinct_schema_inputs_are_not_hidden() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let req = model("DB.S.T")
        .catalog("DB")
        .schema("S")
        .own_epoch(Some(100))
        .project("p")
        .sql("select * from DB.A.U join DB.B.U using(id)")
        .upstream("DB.A.U", 1000)
        .upstream("DB.B.U", 100);
    sc.build_with(req.clone(), outcome().epoch(100)).await;
    let req = req.upstream("DB.B.U", 500);
    assert_eq!(sc.verdict(req.clone()).await, "ready_to_execute");
    let req = req.table("DB.B.U", None).policy(Policy::All);
    assert_eq!(sc.verdict(req).await, "ready_to_execute");
}

#[tokio::test]
async fn dependency_context_and_volatile_policy_prevent_unsupported_reuse() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let node = model("DB.S.T")
        .catalog("DB")
        .schema("S")
        .own_epoch(Some(100))
        .project("p")
        .sql("select * from V");
    let req = node.clone().dependency("DB.S.V", "select 1");
    sc.build_with(req.clone(), outcome().epoch(100)).await;
    assert_eq!(sc.verdict(req.clone()).await, "skip_execution");
    let changed = node.dependency("DB.S.V", "select 2");
    assert_eq!(sc.verdict(changed).await, "ready_to_execute");
    let changed = req.clone().schema("OTHER");
    assert_eq!(sc.verdict(changed).await, "ready_to_execute");
    let req = req.tolerate_nondeterminism(false);
    assert_eq!(sc.verdict(req).await, "ready_to_execute");
}

#[tokio::test]
async fn seeds_match_configuration_current_object_and_latest_build() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let req = seed("DB.S.SEED").values("bytes-A").own_epoch(Some(100));
    sc.build_values(req.clone(), outcome().epoch(100)).await;
    assert_eq!(sc.values_verdict(req.clone()).await, "skip_execution");
    let changed = req.clone().extra("column_types", "{\"id\":\"varchar\"}");
    assert_eq!(sc.values_verdict(changed).await, "ready_to_execute");
    let missing = req.clone().own_epoch(None);
    assert_eq!(sc.values_verdict(missing).await, "ready_to_execute");
    let rebuilt = req.clone().values("bytes-B");
    sc.build_values(rebuilt, outcome().epoch(200)).await;
    let reverted = req.own_epoch(Some(200));
    assert_eq!(sc.values_verdict(reverted).await, "ready_to_execute");
}

/// #81 (crafted d3 / d3e): a model resubmitted without a target, after an
/// equivalent build under its own name, must not get a Result copy of that
/// build into an empty relation name. Hosted fails `""` with InvalidArgument
/// (and unset with Internal); this server answers InvalidArgument for both.
/// Data tests, which carry no target, are unaffected.
#[tokio::test]
async fn model_submit_without_target_is_invalid_argument_not_a_result_copy() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let req = model("DB.S.M")
        .catalog("DB")
        .schema("S")
        .own_epoch(Some(100))
        .project("p")
        .namespace("ns")
        .sql("select * from DB.RAW.SRC")
        .upstream("DB.RAW.SRC", 50);
    sc.build_with(req.clone(), outcome().epoch(100)).await;
    for (case, target) in [("unset", None), ("empty", Some(String::new()))] {
        for execution_type in [ExecutionType::Full, ExecutionType::View] {
            let mut bad: qc::SubmitEnrichedSqlRequest =
                req.clone().execution_type(execution_type).into();
            bad.target_table = target.clone();
            let status = sc
                .try_submit(bad)
                .await
                .expect_err("a non-test submit without a target must be rejected");
            assert_eq!(
                status.code(),
                tonic::Code::InvalidArgument,
                "{case} target, {execution_type:?}: {status:?}"
            );
        }
    }
    // The rejected submits recorded nothing: the model still skips.
    assert_eq!(sc.verdict(req).await, "skip_execution");
    // A data test's target is legitimately unset or empty.
    let test = data_test("test.p.no_target")
        .catalog("DB")
        .schema("S")
        .project("p")
        .sql("select 0 as failures");
    sc.build_with(test.clone(), outcome().results(test_results(0)))
        .await;
    assert_eq!(sc.verdict(test.clone()).await, "skip_execution");
    let mut empty: qc::SubmitEnrichedSqlRequest = test.into();
    empty.target_table = Some(String::new());
    assert_eq!(sc.verdict(empty).await, "skip_execution");
}

#[tokio::test]
async fn unsupported_history_queries_are_errors_not_empty_successes() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    // SelectorService.GetStateSelection is implemented, but it must still reject
    // a malformed request (here: a default request with an empty project_id)
    // with INVALID_ARGUMENT rather than silently returning an empty success.
    let result = sc.try_select(qc::SelectorRequest::default()).await;
    assert_eq!(
        result.unwrap_err().code(),
        tonic::Code::InvalidArgument,
        "empty project_id must be rejected, not an empty success"
    );
    // ResolveDeferredRelations is now IMPLEMENTED (see deferral.rs). A default
    // request (no project_id / target / ids) resolves nothing — it returns an
    // empty map gracefully rather than erroring, matching the hosted behavior
    // for an unresolvable scope.
    let resolved = sc
        .resolve_deferred(qc::ResolveDeferredRelationsRequest::default())
        .await;
    assert!(
        resolved.is_empty(),
        "an unresolvable scope yields an empty map, not an error"
    );
}

#[tokio::test]
async fn batch_recording_preserves_results_and_shares_submit_fingerprints() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let req = data_test("model.p.test-id")
        .catalog("DB")
        .schema("S")
        .project("p")
        .sql("select 7 as failures");
    let cached = results(i64::MAX);
    assert_eq!(
        sc.record([req.record(outcome().results(cached.clone()))])
            .await
            .unwrap(),
        1
    );
    let r = sc.expect_skip(req).await;
    assert_eq!(r.execution_results, Some(cached));

    let seed = seed("DB.S.SEED")
        .values("data")
        .own_epoch(Some(100))
        .extra("column_types", "{\"id\":\"integer\"}");
    sc.record([seed.record(outcome().epoch(100))])
        .await
        .unwrap();
    assert_eq!(sc.values_verdict(seed).await, "skip_execution");
}

#[tokio::test]
async fn non_finite_protobuf_results_and_duplicate_confirmation_are_lossless() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let req = data_test("model.p.test-id")
        .catalog("DB")
        .schema("S")
        .project("p")
        .sql("select 0 as failures");
    let request_id = sc.execute_id(req.clone()).await;
    let mut cached = results(0);
    cached.fields.insert(
        "non_finite".into(),
        qc::Value {
            kind: Some(qc::value::Kind::DoubleValue(f64::INFINITY)),
        },
    );
    for value in [cached.clone(), results(9)] {
        assert!(sc.confirm(&request_id, outcome().results(value)).await);
    }
    let r = sc.expect_skip(req).await;
    assert_eq!(r.execution_results, Some(cached));
}

#[tokio::test]
async fn missing_or_incomplete_test_results_require_execution() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let req = data_test("model.p.test")
        .catalog("DB")
        .schema("S")
        .project("p")
        .sql("select 0 as failures");
    for payload in [outcome(), outcome().results(qc::Struct::default())] {
        sc.build_with(req.clone(), payload).await;
        assert_eq!(sc.verdict(req.clone()).await, "ready_to_execute");
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
    sc.build_with(req.clone(), outcome().results(warning.clone()))
        .await;
    let r = sc.expect_skip(req).await;
    assert_eq!(r.execution_results, Some(warning));
}

#[tokio::test]
async fn incomplete_or_ambiguous_dependency_evidence_requires_execution() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let node = model("DB.S.T")
        .catalog("DB")
        .schema("S")
        .own_epoch(Some(100))
        .project("p")
        .sql("select * from U");
    let req = node.clone().upstream("DB.S.U", 100);
    sc.build_with(req.clone(), outcome().epoch(100)).await;
    let missing = node.policy(Policy::All);
    assert_eq!(sc.verdict(missing).await, "ready_to_execute");
    // The same upstream reported twice is ambiguous evidence.
    let duplicated = req.with(|r| {
        let again = r.tables.last().cloned().expect("upstream");
        r.tables.push(again);
    });
    assert_eq!(sc.verdict(duplicated).await, "ready_to_execute");
}

#[tokio::test]
async fn corrupted_history_errors_and_old_fingerprints_cold_miss() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let req = model("DB.S.T")
        .catalog("DB")
        .schema("S")
        .own_epoch(Some(100))
        .project("p")
        .sql("select 1");
    sc.build_with(req.clone(), outcome().epoch(100)).await;
    legacy::corrupt_input_tables(&server).await;
    assert_eq!(
        sc.try_submit(req.clone()).await.unwrap_err().code(),
        tonic::Code::Internal
    );
    legacy::corrupt_execution_results(&server).await;
    assert_eq!(
        sc.try_submit(req.clone()).await.unwrap_err().code(),
        tonic::Code::Internal
    );
    legacy::rewrite_sql_hash(&server, Rows::All, "legacy-hash").await;
    assert_eq!(sc.verdict(req).await, "ready_to_execute");
}

#[tokio::test]
async fn clone_errors_do_not_leave_pending_executions() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    for (dialect, kind) in [("snowflake", "VIEW"), ("unknown", "TABLE")] {
        let err = sc
            .register_clone(
                clone_request("DB.S.SOURCE", "DB.S.T")
                    .dialect(dialect)
                    .execution_type(ExecutionType::Unspecified)
                    .source_table_type(kind),
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
    }
    assert_eq!(rows::execution_count(&server).await, 0);
}

#[tokio::test]
async fn ordering_is_irrelevant_but_incomplete_view_definitions_cannot_reuse() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let req = model("DB.S.T")
        .catalog("DB")
        .schema("S")
        .own_epoch(Some(100))
        .project("p")
        .sql("select * from V union all select * from W")
        .dependency("DB.S.V", "select 1")
        .dependency("DB.S.W", "select 2")
        .upstream("DB.S.A", 100)
        .upstream("DB.S.B", 100);
    sc.build_with(req.clone(), outcome().epoch(100)).await;
    let req = req.with(|r| {
        r.query_dependencies.reverse();
        r.tables.reverse();
    });
    assert_eq!(sc.verdict(req.clone()).await, "skip_execution");
    let req = req.with(|r| r.query_dependencies[0].query.clear());
    sc.build_with(req.clone(), outcome().epoch(100)).await;
    assert_eq!(sc.verdict(req).await, "ready_to_execute");
}

/// A node kind change with unchanged identity (#83, #90; VERIFIED LIVE
/// 2026-11-10/11, explain_execution_type_change.jsonl,
/// explain_snapshot_history.jsonl entry 58,
/// explain_incremental_append_merge_history.jsonl entries 74-75):
/// - with no build of the submit's type, the rebuild is EXECUTION_TYPE_MISMATCH
///   (2 / 2) with the generic rebuild description;
/// - with one, under a newer build of another type (here a RecordExecutions
///   full refresh, as client 2.0.11 sends one), the submit is judged against
///   it: its Target relation was modified after it (6 / 6), even though an
///   older matching build exists;
/// - and the older build is never reused, even when its Target relation is
///   unchanged since.
#[tokio::test]
async fn execution_type_change_is_judged_against_the_latest_build_of_that_type() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let decision = |r: qc::ReadyToExecuteResponse| {
        let d = r.explained_decision.expect("explained decision");
        (
            d.skip_rejection_reason,
            d.clone_rejection_reason,
            d.decision_description,
        )
    };
    let rebuild = "model was executed because either its query didn't match or its upstream data is out of date";
    let modified = "model was executed because the table was modified after the cached execution";

    let merge = model("DB.S.T")
        .catalog("DB")
        .schema("S")
        .project("p")
        .sql("select 1")
        .execution_type(ExecutionType::Merge)
        .upstream("DB.S.A", 50);
    sc.build_with(merge.clone().own_epoch(Some(100)), outcome().epoch(100))
        .await;
    // `--full-refresh`: recorded as FULL, with no submit.
    let full = merge.clone().execution_type(ExecutionType::Full);
    let refreshed = full.clone().own_epoch(Some(100));
    assert_eq!(
        sc.record([refreshed.record(outcome().epoch(300))])
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        decision(sc.expect_execute(merge.clone().own_epoch(Some(250))).await),
        (Some(6), Some(6), modified.to_string()),
        "MERGE after a recorded full refresh"
    );

    // No DELETE_INSERT build exists: the execution type change itself.
    let delete_insert = merge.clone().execution_type(ExecutionType::DeleteInsert);
    assert_eq!(
        decision(sc.expect_execute(delete_insert.own_epoch(Some(300))).await),
        (Some(2), Some(2), rebuild.to_string()),
        "DELETE_INSERT over FULL and MERGE builds"
    );

    // The MERGE build of the first submit would be reusable as recorded at
    // epoch 100, but only the latest row is ever reused.
    let other = model("DB.S.U")
        .catalog("DB")
        .schema("S")
        .project("p")
        .sql("select 2")
        .execution_type(ExecutionType::Merge)
        .upstream("DB.S.A", 50);
    sc.build_with(other.clone().own_epoch(Some(100)), outcome().epoch(100))
        .await;
    let other_full = other.clone().execution_type(ExecutionType::Full);
    assert_eq!(
        sc.record([other_full.own_epoch(Some(100)).record(outcome().epoch(100))])
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        decision(sc.expect_execute(other.own_epoch(Some(100))).await),
        (Some(2), Some(2), rebuild.to_string()),
        "an older build of the submit's type is not reused"
    );
}

/// The reason #40 split out of an identity mismatch so Explain could render
/// nothing for it (#93) keeps the wire answer the server gave before the
/// split: a rebuild while an OLDER build of the target would still be
/// reusable reports LATEST_QUERY_HASH_NOT_MATCH / TARGET_TABLE_MATCH, and
/// `is_stale` as that build would be reused (VERIFIED LIVE 2026-11-10,
/// lag_copy_candidates `ob*`, pinned in `lag_tolerance.rs`). #40 changes
/// Explain only.
#[tokio::test]
async fn split_identity_reasons_keep_the_older_match_wire_answer() {
    use support::demo::{self, clone_probe, clone_probe_sql, transient_at, DEV_SCHEMA};
    use support::demo::{PROD_SCHEMA, RAW_ORDERS};
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let codes = |r: qc::ReadyToExecuteResponse| {
        let d = r.explained_decision.expect("explained decision");
        (
            d.skip_rejection_reason,
            d.clone_rejection_reason,
            d.is_stale,
        )
    };

    // A RegisterClone copy the workload cannot adopt (other logic) is the
    // latest row; the older ordinary build of the same logic is fresh.
    let prod = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"CLONE_PROBE\"";
    let dev = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"CLONE_PROBE\"";
    let (raw, prod_epoch, dev_epoch, copy_epoch) = (1_000, 2_000, 3_000, 4_000);
    sc.build_with(
        clone_probe(prod, PROD_SCHEMA)
            .sql(&clone_probe_sql(PROD_SCHEMA, ", upper(customer_id) as c"))
            .own_epoch(Some(prod_epoch))
            .upstream(RAW_ORDERS, raw),
        transient_at(prod_epoch),
    )
    .await;
    let dev_model = clone_probe(dev, DEV_SCHEMA).upstream(RAW_ORDERS, raw);
    sc.build_with(
        dev_model.clone().own_epoch(Some(dev_epoch)),
        transient_at(dev_epoch),
    )
    .await;
    let copied = sc
        .register_clone(demo::copy(prod, dev).source_epoch(prod_epoch))
        .await
        .unwrap()
        .ready_to_clone
        .expect("ready_to_clone");
    assert!(
        sc.confirm(&copied.request_id, transient_at(copy_epoch))
            .await
    );
    assert_eq!(
        codes(
            sc.expect_execute(dev_model.own_epoch(Some(copy_epoch)))
                .await
        ),
        (Some(8), Some(7), false),
        "unadoptable copy over a reusable older build"
    );
}
