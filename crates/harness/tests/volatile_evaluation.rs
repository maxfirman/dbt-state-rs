//! Volatile evaluation (#67, #192) through the gRPC protocol.
//!
//! A submit that requests Volatile evaluation (`tolerate_nondeterminism =
//! false`, sent by current clients for `evaluate_volatile_sql: true`) has each
//! Volatile function its Warehouse has a captured rule for rendered to a value.
//! The value is returned in `transformed_nodes_by_query` on `ready_to_execute`,
//! and `dbt state explain` prints it from the persisted decision. The Warehouse
//! still executes the original SQL: the value is telemetry and never enters
//! identity or Reuse qualification.
//!
//! Evidence: the captured Databricks build of a `rand()` model,
//! `fixtures/databricks_submit_copy_controls.jsonl` entries 85 (submit) and 86
//! (Explain); the captured Snowflake first builds of a `current_timestamp()`
//! model, `golden/fixtures/clone_fallback_volatile.jsonl` entries 0 (prod) and
//! 2 (dev) (#193); the captured consumer of a volatile model,
//! `golden/fixtures/explain_volatile_evaluation.jsonl` #191 cell 7 (#196); the
//! captured Snowflake table over a volatile view and data test, the same
//! fixture's entries 10, 35 and 45, and 16, 24, 37 and 46 (#195).
mod support;

use std::collections::BTreeMap;

use dbt_state_harness::diff::GoldenEntry;
use dbt_state_proto::query_cache as qc;
use support::scenario::{
    built_at, captured_submit, hosted_volatile_values, variant, volatile_map_shape,
    volatile_values, volatile_values_of, Scenario, Submit, VolatileValues, OWN_EPOCH,
};

const CONTROLS: &str = include_str!("fixtures/databricks_submit_copy_controls.jsonl");
/// The captured submit whose SQL calls `rand()` with evaluation requested.
const RAND_SUBMIT: usize = 85;
/// Hosted's Explain of that decision.
const RAND_EXPLAIN: usize = 86;
/// The submit before it: the same model without `rand()`.
const DETERMINISTIC_SUBMIT: usize = 83;

fn entries(fixture: &str) -> Vec<GoldenEntry> {
    fixture
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

/// The request of captured submit `entry` of `fixture`.
fn request(fixture: &str, entry: usize) -> serde_json::Value {
    entries(fixture)[entry].request.clone()
}

/// The Target relation a captured `request` sends.
fn target(request: &serde_json::Value) -> String {
    request["target_table"].as_str().unwrap().to_owned()
}

/// The epoch the captured submit reports for its own Target relation.
fn captured_target_epoch() -> i64 {
    let req = captured_submit(&request(CONTROLS, RAND_SUBMIT)).build();
    req.tables
        .iter()
        .find(|t| Some(&t.name) == req.target_table.as_ref())
        .and_then(|t| t.last_modified_epoch)
        .unwrap()
}

/// Build the deterministic model over the captured Target relation, so a
/// later submit of changed SQL is a query-changed rebuild of an existing table.
async fn deterministic_history(sc: &Scenario) {
    let deterministic = request(CONTROLS, DETERMINISTIC_SUBMIT)["sql"]
        .as_str()
        .unwrap()
        .to_owned();
    sc.build(
        captured_submit(&request(CONTROLS, RAND_SUBMIT)).sql(&deterministic),
        captured_target_epoch(),
    )
    .await;
}

/// The rendered values a `ready_to_execute` returned, as
/// target → function → value.
fn rendered(ready: &qc::ReadyToExecuteResponse) -> VolatileValues {
    volatile_values_of(&ready.transformed_nodes_by_query)
}

/// Whether `value` has the captured `RAND()` (Databricks) and `RANDOM()`
/// (Snowflake) format: a plain decimal in [0, 1), without an exponent
/// (`0.2867523899434695`, `0.2496468898543448`).
fn is_captured_unit_random(value: &str) -> bool {
    let digits = value.strip_prefix("0.").unwrap_or_default();
    !digits.is_empty()
        && digits.bytes().all(|b| b.is_ascii_digit())
        && value.parse::<f64>().is_ok_and(|v| (0.0..1.0).contains(&v))
}

/// Replay the captured controls through the RAND submit (entry 85) at their
/// recorded times. Returns the local response to entry 85.
async fn replay_through_rand_submit(sc: &Scenario) -> qc::ReadyToExecuteResponse {
    let mut responses = replay(sc, &entries(CONTROLS), RAND_SUBMIT).await;
    support::scenario::into_execute(responses.remove(&RAND_SUBMIT).unwrap())
}

/// Replay the submits and confirms of `entries` through entry `through`, at
/// their recorded times, correlating each confirm with its local request id.
/// Returns the local response to each submit, by entry.
async fn replay(
    sc: &Scenario,
    entries: &[GoldenEntry],
    through: usize,
) -> std::collections::HashMap<usize, qc::SubmitSqlResponse> {
    let mut sql = qc::sql_client::SqlClient::new(sc.channel());
    let mut exec = qc::execution_client::ExecutionClient::new(sc.channel());
    let mut local_ids = std::collections::HashMap::new();
    let mut responses = std::collections::HashMap::new();
    for (index, entry) in entries.iter().enumerate().take(through + 1) {
        match entry.method.as_str() {
            "SubmitEnrichedSQL" => {
                let req = captured_submit(&entry.request).build();
                let response = sql
                    .submit_enriched_sql(support::captured_request(req, entry))
                    .await
                    .unwrap()
                    .into_inner();
                let hosted = entry.response["response"]["ready_to_execute"]["request_id"]
                    .as_str()
                    .or(entry.response["response"]["ready_to_clone"]["request_id"].as_str());
                if let (Some(hosted), Some(local)) = (hosted, ready_request_id(&response)) {
                    local_ids.insert(hosted.to_owned(), local);
                }
                responses.insert(index, response);
            }
            "ConfirmExecution" => {
                let mut req: qc::ConfirmExecutionRequest =
                    serde_json::from_value(entry.request.clone()).unwrap();
                req.request_id = local_ids[&req.request_id].clone();
                exec.confirm_execution(support::captured_request(req, entry))
                    .await
                    .unwrap();
            }
            _ => {}
        }
    }
    responses
}

fn ready_request_id(response: &qc::SubmitSqlResponse) -> Option<String> {
    let value = serde_json::to_value(response).unwrap();
    ["ready_to_execute", "ready_to_clone"].iter().find_map(|k| {
        value["response"][k]["request_id"]
            .as_str()
            .map(str::to_owned)
    })
}

async fn explain(sc: &Scenario, id: &str) -> serde_json::Value {
    serde_json::to_value(sc.explain(&[id]).await).unwrap()
}

/// Every line of an Explain response, depth first.
fn texts(explain: &serde_json::Value) -> Vec<String> {
    fn walk(lines: &serde_json::Value, out: &mut Vec<String>) {
        for line in lines.as_array().into_iter().flatten() {
            out.push(line["text"].as_str().unwrap().to_owned());
            walk(&line["children"], out);
        }
    }
    let mut out = Vec::new();
    for message in explain["messages"].as_array().unwrap() {
        walk(&message["explain_lines"], &mut out);
    }
    out
}

/// Whether an Explain response prints any Volatile evaluation line.
fn prints_volatile_lines(explain: &serde_json::Value) -> bool {
    texts(explain).iter().any(|t| {
        t.contains("rendered with")
            || t == "non-deterministic functions detected in query"
            || t.contains("is not deterministic")
    })
}

/// Hosted's entry 86 Explain, for the local decision `id` whose `RAND()`
/// rendered `value`.
fn hosted_rand_explain(id: &str, value: &str) -> serde_json::Value {
    let hosted = serde_json::to_string(&entries(CONTROLS)[RAND_EXPLAIN].response)
        .unwrap()
        .replace("0.2867523899434695", value)
        .replace("01a1e094-30e7-7588-b0dd-086c25a0df43", id);
    serde_json::from_str(&hosted).unwrap()
}

#[tokio::test]
async fn databricks_rand_build_returns_its_rendered_value_keyed_by_the_target_as_sent() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await.org("rand");
    deterministic_history(&sc).await;

    let ready = sc
        .expect_execute(captured_submit(&request(CONTROLS, RAND_SUBMIT)))
        .await;

    let values = rendered(&ready);
    assert_eq!(
        values.keys().collect::<Vec<_>>(),
        [&target(&request(CONTROLS, RAND_SUBMIT))],
        "keyed by the Target relation as sent"
    );
    let functions = &values[&target(&request(CONTROLS, RAND_SUBMIT))];
    assert_eq!(functions.keys().collect::<Vec<_>>(), ["RAND()"]);
    assert!(
        is_captured_unit_random(&functions["RAND()"]),
        "captured float format, got {:?}",
        functions["RAND()"]
    );
}

#[tokio::test]
async fn databricks_rand_explain_matches_hosted_with_the_returned_value_after_a_restart() {
    let database = std::sync::Arc::new(support::TestDatabase::new("volatile_restart").await);
    let server = support::start_server_on_database(database.clone()).await;
    let sc = Scenario::connect(&server).await;
    let ready = replay_through_rand_submit(&sc).await;
    let id = ready.execution_decision_id.clone().unwrap();
    let value = rendered(&ready)[&target(&request(CONTROLS, RAND_SUBMIT))]["RAND()"].clone();

    assert_eq!(explain(&sc, &id).await, hosted_rand_explain(&id, &value));

    drop(sc);
    drop(server);
    let server = support::start_server_on_database(database.clone()).await;
    let sc = Scenario::connect(&server).await;
    assert_eq!(
        explain(&sc, &id).await,
        hosted_rand_explain(&id, &value),
        "the persisted value prints after a restart"
    );
}

/// Build a deterministic version of `model` over its Target relation with
/// `history`, then submit `model` with `sql` (with evaluation requested
/// unless `tolerate`) as a query-changed rebuild. Returns the rebuild and
/// its Explain.
async fn rebuild_with(
    sc: &Scenario,
    history: impl AsyncFnOnce(&Scenario),
    model: Submit,
    sql: &str,
    tolerate: bool,
) -> (qc::ReadyToExecuteResponse, serde_json::Value) {
    history(sc).await;
    let ready = sc
        .expect_execute(model.sql(sql).tolerate_nondeterminism(tolerate))
        .await;
    let explained = explain(sc, ready.execution_decision_id.as_deref().unwrap()).await;
    (ready, explained)
}

#[tokio::test]
async fn submits_without_a_captured_volatile_call_render_nothing() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let input = "cdo_demo_db.ds_20261112a134ef_prod.p153_80615b233ac140e48993d85c06bfc823_inputs";
    let cases = [
        (
            "deterministic control",
            format!("select *, 1 AS noise from {input}"),
            false,
        ),
        (
            "evaluation off",
            format!("select *, rand() AS noise from {input}"),
            true,
        ),
        (
            "uncaptured volatile function",
            format!("select *, uuid() AS noise from {input}"),
            false,
        ),
        (
            "uncaptured clock",
            format!("select *, current_timestamp() AS noise from {input}"),
            false,
        ),
        (
            "seeded rand",
            format!("select *, rand(42) AS noise from {input}"),
            false,
        ),
        (
            "line comment",
            format!("select * from {input} -- rand()"),
            false,
        ),
        (
            "block comment",
            format!("select * /* rand() */ from {input}"),
            false,
        ),
        (
            "string literal",
            format!("select *, 'rand()' AS noise from {input}"),
            false,
        ),
        (
            "quoted identifier",
            format!("select *, `rand`() AS noise from {input}"),
            false,
        ),
    ];
    // Positive control for the same harness: the captured call renders.
    let (ready, explained) = rebuild_with(
        &sc.org("control"),
        deterministic_history,
        captured_submit(&request(CONTROLS, RAND_SUBMIT)),
        &format!("select *, rand() AS noise from {input}"),
        false,
    )
    .await;
    assert!(!ready.transformed_nodes_by_query.is_empty());
    assert!(prints_volatile_lines(&explained));
    for (case, sql, tolerate) in cases {
        let (ready, explained) = rebuild_with(
            &sc.org(case),
            deterministic_history,
            captured_submit(&request(CONTROLS, RAND_SUBMIT)),
            &sql,
            tolerate,
        )
        .await;
        assert!(
            ready.transformed_nodes_by_query.is_empty(),
            "{case}: {:?}",
            ready.transformed_nodes_by_query
        );
        assert!(
            texts(&explained).contains(&"the model query has changed".to_owned()),
            "{case}: the query analysis is printed"
        );
        assert!(!prints_volatile_lines(&explained), "{case}: {explained}");
    }
}

#[tokio::test]
async fn another_organizations_decision_renders_no_values() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let owner = sc.org("owner");
    deterministic_history(&owner).await;
    let ready = owner
        .expect_execute(captured_submit(&request(CONTROLS, RAND_SUBMIT)))
        .await;
    let id = ready.execution_decision_id.unwrap();
    assert!(prints_volatile_lines(&explain(&owner, &id).await));

    let other = explain(&sc.org("other"), &id).await;
    assert!(other["messages"].as_array().unwrap().is_empty(), "{other}");
    assert!(!prints_volatile_lines(&other));
}

#[tokio::test]
async fn a_decision_recorded_before_the_migration_prints_no_volatile_lines() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await.org("legacy");
    deterministic_history(&sc).await;
    let ready = sc
        .expect_execute(captured_submit(&request(CONTROLS, RAND_SUBMIT)))
        .await;
    let id = ready.execution_decision_id.unwrap();
    let with_values = explain(&sc, &id).await;
    assert!(prints_volatile_lines(&with_values));

    support::legacy::forget_rendered_volatile_values(&server.state, "legacy", &id).await;

    let legacy = explain(&sc, &id).await;
    assert!(!prints_volatile_lines(&legacy), "{legacy}");
    let mut expected = texts(&with_values);
    expected.retain(|t| {
        !t.contains("rendered with")
            && t != "non-deterministic functions detected in query"
            && !t.contains("is not deterministic")
    });
    assert_eq!(
        texts(&legacy),
        expected,
        "the rest of the tree is unchanged"
    );
}

/// The persisted execution row of `request_id` without the columns that
/// differ per request: its identity, fingerprints and evidence.
async fn persisted_execution(server: &support::TestServer, request_id: &str) -> serde_json::Value {
    let mut row = support::rows::complete_execution(&server.state, request_id).await;
    let object = row.as_object_mut().unwrap();
    for per_request in [
        "id",
        "request_id",
        "execution_decision_id",
        "created_at",
        "display_submitted_at_ms",
    ] {
        object.remove(per_request);
    }
    row
}

#[tokio::test]
async fn a_volatile_model_is_never_reused_or_copied_and_its_values_enter_no_fingerprint() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await.org("volatile");
    let model = captured_submit(&request(CONTROLS, RAND_SUBMIT));

    // Two captured query-changed rebuilds, each over a deterministic build.
    deterministic_history(&sc).await;
    let first = sc.expect_execute(model.clone()).await;
    deterministic_history(&sc).await;
    let second = sc.expect_execute(model.clone()).await;
    let value = |ready: &qc::ReadyToExecuteResponse| {
        rendered(ready)[&target(&request(CONTROLS, RAND_SUBMIT))]["RAND()"].clone()
    };
    assert_ne!(value(&first), value(&second), "each decision renders anew");
    assert_eq!(
        persisted_execution(&server, &first.request_id).await,
        persisted_execution(&server, &second.request_id).await,
        "rendered values enter no persisted identity or fingerprint"
    );

    assert!(
        sc.confirm(&second.request_id, built_at(captured_target_epoch()))
            .await
    );
    assert_eq!(
        variant(&sc.submit(model.clone()).await),
        "ready_to_execute",
        "a rerun is rebuilt"
    );

    let elsewhere = model
        .clone()
        .target("`cdo_demo_db`.`elsewhere`.`p153_80615b233ac140e48993d85c06bfc823_full`")
        .schema("elsewhere")
        .no_tables()
        .table(
            "`cdo_demo_db`.`ds_20261112a134ef_prod`.`p153_80615b233ac140e48993d85c06bfc823_inputs`",
            Some(1794768588000),
        );
    assert_eq!(
        variant(&sc.submit(elsewhere).await),
        "ready_to_execute",
        "never copied into another schema of the namespace"
    );

    // Control: the same history reuses the model when evaluation is off.
    let deterministic = sc.org("deterministic");
    let model = model.tolerate_nondeterminism(true);
    deterministic
        .build(model.clone(), captured_target_epoch())
        .await;
    assert_eq!(
        variant(&deterministic.submit(model).await),
        "skip_execution"
    );
}

// ── Snowflake CURRENT_TIMESTAMP() (#193) ────────────────────────────────────

const SNOWFLAKE_FIRST_BUILDS: &str =
    include_str!("../../../golden/fixtures/clone_fallback_volatile.jsonl");
/// The captured prod and dev first builds of a model calling
/// `current_timestamp()` with evaluation requested.
const SNOWFLAKE_PROD_SUBMIT: usize = 0;
const SNOWFLAKE_DEV_SUBMIT: usize = 2;

#[tokio::test]
async fn snowflake_current_timestamp_build_renders_the_replay_clock_in_utc_keyed_as_captured() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let mut sql = qc::sql_client::SqlClient::new(sc.channel());
    let entries = entries(SNOWFLAKE_FIRST_BUILDS);
    // Each submit's recorded time (the replay clock), in UTC.
    for (entry, at) in [
        (SNOWFLAKE_PROD_SUBMIT, "2026-10-12 21:52:38.707000"),
        (SNOWFLAKE_DEV_SUBMIT, "2026-10-12 21:54:20.470000"),
    ] {
        let entry = &entries[entry];
        let response = sql
            .submit_enriched_sql(support::captured_request(
                captured_submit(&entry.request).build(),
                entry,
            ))
            .await
            .unwrap()
            .into_inner();
        let values = rendered(&support::scenario::into_execute(response));
        let hosted = hosted_volatile_values(&entry.response);

        assert_eq!(
            volatile_map_shape(&values),
            volatile_map_shape(&hosted),
            "hosted's key shape"
        );
        let target = entry.request["target_table"].as_str().unwrap();
        assert_eq!(values[target]["CURRENT_TIMESTAMP()"], at);
    }
}

/// The captured model's SQL with `columns` in place of its column list.
fn snowflake_sql(columns: &str) -> String {
    format!("\nselect {columns} from CDO_DEMO_DB.prod_state_demo.ce_up_215158")
}

/// The epoch the captured build confirmed for its Target relation (entry 1).
const SNOWFLAKE_BUILT_AT: i64 = 1791841959783;

/// Build a deterministic version of the captured model over its Target
/// relation, so a later submit of other SQL is a query-changed rebuild.
async fn snowflake_history(sc: &Scenario) {
    sc.build(
        captured_submit(&request(SNOWFLAKE_FIRST_BUILDS, SNOWFLAKE_PROD_SUBMIT))
            .own_epoch(Some(OWN_EPOCH))
            .sql(&snowflake_sql("id, amount")),
        SNOWFLAKE_BUILT_AT,
    )
    .await;
}

#[tokio::test]
async fn snowflake_submits_without_a_captured_volatile_call_render_nothing() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let cases = [
        (
            "deterministic control",
            snowflake_sql("id, amount, 1 as loaded_at"),
            false,
        ),
        (
            "uncaptured precision argument",
            snowflake_sql("id, amount, current_timestamp(3) as loaded_at"),
            false,
        ),
        (
            "a function hosted renders nothing for",
            snowflake_sql("id, amount, seq4() as loaded_at"),
            false,
        ),
        (
            "zipf with a constant generator",
            snowflake_sql("id, amount, zipf(1, 10, 1234) as loaded_at"),
            false,
        ),
        (
            "uncaptured seed",
            snowflake_sql("id, amount, randstr(8, 99) as loaded_at"),
            false,
        ),
        (
            "uncaptured normal arguments",
            snowflake_sql("id, amount, normal(0, 1, 7) as loaded_at"),
            false,
        ),
        (
            "uniform with a constant generator",
            snowflake_sql("id, amount, uniform(1, 100, 1234) as loaded_at"),
            false,
        ),
        (
            "uncaptured parenless current_time",
            snowflake_sql("id, amount, current_time as loaded_at"),
            false,
        ),
        (
            "uncaptured parenless localtime",
            snowflake_sql("id, amount, localtime as loaded_at"),
            false,
        ),
        (
            "a column named like a function that needs parentheses",
            snowflake_sql("id, amount, random as loaded_at"),
            false,
        ),
        (
            "line comment",
            snowflake_sql("id, amount, 1 as loaded_at") + " -- current_timestamp()",
            false,
        ),
        (
            "block comment",
            snowflake_sql("id, amount, 1 /* current_timestamp() */ as loaded_at"),
            false,
        ),
        (
            "string literal",
            snowflake_sql("id, amount, 'current_timestamp()' as loaded_at"),
            false,
        ),
        (
            "quoted identifier",
            snowflake_sql("id, amount, \"current_timestamp\"() as loaded_at"),
            false,
        ),
        (
            "qualified user function",
            snowflake_sql("id, amount, analytics.current_timestamp() as loaded_at"),
            false,
        ),
    ];
    // Positive control for the same harness: the captured call renders.
    let (ready, explained) = rebuild_with(
        &sc.org("control"),
        snowflake_history,
        captured_submit(&request(SNOWFLAKE_FIRST_BUILDS, SNOWFLAKE_PROD_SUBMIT))
            .own_epoch(Some(OWN_EPOCH)),
        &snowflake_sql("id, amount, current_timestamp() as loaded_at"),
        false,
    )
    .await;
    assert!(!ready.transformed_nodes_by_query.is_empty());
    assert!(prints_volatile_lines(&explained));
    for (case, sql, tolerate) in cases {
        let (ready, explained) = rebuild_with(
            &sc.org(case),
            snowflake_history,
            captured_submit(&request(SNOWFLAKE_FIRST_BUILDS, SNOWFLAKE_PROD_SUBMIT))
                .own_epoch(Some(OWN_EPOCH)),
            &sql,
            tolerate,
        )
        .await;
        assert!(
            ready.transformed_nodes_by_query.is_empty(),
            "{case}: {:?}",
            ready.transformed_nodes_by_query
        );
        assert!(
            texts(&explained).contains(&"the model query has changed".to_owned()),
            "{case}: the query analysis is printed"
        );
        assert!(!prints_volatile_lines(&explained), "{case}: {explained}");
    }
}

/// A query-changed rebuild of the captured Snowflake model calling
/// `current_timestamp()`; returns its decision id.
async fn snowflake_volatile_rebuild(sc: &Scenario) -> String {
    let (ready, explained) = rebuild_with(
        sc,
        snowflake_history,
        captured_submit(&request(SNOWFLAKE_FIRST_BUILDS, SNOWFLAKE_PROD_SUBMIT))
            .own_epoch(Some(OWN_EPOCH)),
        &snowflake_sql("id, amount, current_timestamp() as loaded_at"),
        false,
    )
    .await;
    assert!(prints_volatile_lines(&explained));
    ready.execution_decision_id.unwrap()
}

#[tokio::test]
async fn snowflake_another_organizations_decision_renders_no_values() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let id = snowflake_volatile_rebuild(&sc.org("owner")).await;

    let other = explain(&sc.org("other"), &id).await;
    assert!(other["messages"].as_array().unwrap().is_empty(), "{other}");
}

#[tokio::test]
async fn snowflake_decision_recorded_before_the_migration_prints_no_volatile_lines() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await.org("legacy");
    let id = snowflake_volatile_rebuild(&sc).await;
    let with_values = explain(&sc, &id).await;

    support::legacy::forget_rendered_volatile_values(&server.state, "legacy", &id).await;

    let legacy = explain(&sc, &id).await;
    assert!(!prints_volatile_lines(&legacy), "{legacy}");
    let mut expected = texts(&with_values);
    expected.retain(|t| {
        !t.contains("rendered with")
            && t != "non-deterministic functions detected in query"
            && !t.contains("is not deterministic")
    });
    assert_eq!(
        texts(&legacy),
        expected,
        "the rest of the tree is unchanged"
    );
}

#[tokio::test]
async fn snowflake_decision_recorded_before_the_evaluation_flag_prints_no_volatile_lines() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await.org("legacy flag");
    let id = snowflake_volatile_rebuild(&sc).await;
    let with_flag = explain(&sc, &id).await;

    support::legacy::forget_volatile_evaluation_flag(&server.state, "legacy flag", &id).await;

    let legacy = explain(&sc, &id).await;
    assert!(!prints_volatile_lines(&legacy), "{legacy}");
    let mut expected = texts(&with_flag);
    expected.retain(|t| {
        !t.contains("rendered with")
            && t != "non-deterministic functions detected in query"
            && !t.contains("is not deterministic")
    });
    assert_eq!(
        texts(&legacy),
        expected,
        "the rest of the tree is unchanged"
    );
}

#[tokio::test]
async fn snowflake_volatile_model_is_never_reused_or_copied_and_its_values_enter_no_fingerprint() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await.org("volatile");
    let entries = entries(SNOWFLAKE_FIRST_BUILDS);
    let model = captured_submit(&request(SNOWFLAKE_FIRST_BUILDS, SNOWFLAKE_PROD_SUBMIT))
        .own_epoch(Some(OWN_EPOCH));
    let value = |ready: &qc::ReadyToExecuteResponse| {
        rendered(ready)[&target(&request(SNOWFLAKE_FIRST_BUILDS, SNOWFLAKE_PROD_SUBMIT))]
            ["CURRENT_TIMESTAMP()"]
            .clone()
    };

    let first = sc.expect_execute(model.clone()).await;
    let second = sc.expect_execute(model.clone()).await;
    assert!(!value(&first).is_empty() && !value(&second).is_empty());
    assert_eq!(
        persisted_execution(&server, &first.request_id).await,
        persisted_execution(&server, &second.request_id).await,
        "rendered values enter no persisted identity or fingerprint"
    );

    assert!(
        sc.confirm(&first.request_id, built_at(SNOWFLAKE_BUILT_AT))
            .await
    );
    let rerun = sc.expect_execute(model.clone()).await;
    assert!(!value(&rerun).is_empty(), "a rerun is rebuilt and rendered");

    // The captured dev build of the same model, in another schema of the
    // namespace: hosted built it, and so does this server.
    assert_eq!(
        variant(
            &sc.submit(captured_submit(&entries[SNOWFLAKE_DEV_SUBMIT].request))
                .await
        ),
        "ready_to_execute",
        "never copied into another schema of the namespace"
    );

    // Control: the same history reuses the model when evaluation is off.
    let deterministic = sc.org("deterministic");
    let model = model.tolerate_nondeterminism(true);
    deterministic.build(model.clone(), SNOWFLAKE_BUILT_AT).await;
    assert_eq!(
        variant(&deterministic.submit(model).await),
        "skip_execution"
    );
}

// ── Snowflake model cells 1–4 and 8 (#194) ──────────────────────────────────
//
// Evidence: the #191 capture batch, `golden/fixtures/explain_volatile_evaluation.jsonl`
// (run 1: every function in one model, reruns, the deterministic control and
// evaluation off) and `explain_volatile_evaluation_functions.jsonl` (run 2:
// one function form per model). Both were captured with dbt 2.0.11.

const RUN_1: &str = include_str!("../../../golden/fixtures/explain_volatile_evaluation.jsonl");
const RUN_2: &str =
    include_str!("../../../golden/fixtures/explain_volatile_evaluation_functions.jsonl");

/// Whether `value` is a lowercase version 4 UUID (`8f162c59-9bda-412e-b5de-d0d22af57bf9`).
fn is_lowercase_v4_uuid(value: &str) -> bool {
    uuid::Uuid::parse_str(value).is_ok_and(|u| u.get_version_num() == 4)
        && value.len() == 36
        && value == value.to_ascii_lowercase()
}

#[tokio::test]
async fn snowflake_every_captured_function_renders_hosted_key_and_value_format() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let mut sql = qc::sql_client::SqlClient::new(sc.channel());
    let run_1 = entries(RUN_1);
    let run_2 = entries(RUN_2);
    // Each submit is decided at its recorded time (the replay clock). The
    // clock values are that instant in UTC, in hosted's format per function.
    let clock: &[(&GoldenEntry, &[(&str, &str)])] = &[
        (
            &run_1[6],
            &[
                ("CURRENT_TIMESTAMP()", "2026-11-17 16:48:51.890000"),
                ("SYSDATE()", "2026-11-17 16:48:51.890000+00:00"),
                ("CURRENT_TIME", "16:48:51.890000"),
                ("CURRENT_DATE", "2026-11-17"),
            ],
        ),
        (
            &run_2[13],
            &[("CURRENT_TIMESTAMP()", "2026-11-17 17:05:56.620000")],
        ),
        (
            &run_2[14],
            &[("CURRENT_TIMESTAMP", "2026-11-17 17:05:56.712000")],
        ),
        (&run_2[15], &[("CURRENT_DATE", "2026-11-17")]),
        (&run_2[16], &[]),
        (&run_2[17], &[("CURRENT_TIME", "17:05:56.783000")]),
        (&run_2[18], &[]),
        (
            &run_2[19],
            &[("CURRENT_TIMESTAMP()", "2026-11-17 17:05:56.805000")],
        ),
        (&run_2[20], &[("CURRENT_TIME", "17:05:56.812000")]),
        (&run_2[21], &[]),
        (&run_2[22], &[]),
        (
            &run_2[23],
            &[("CURRENT_TIMESTAMP()", "2026-11-17 17:05:56.855000")],
        ),
        (
            &run_2[24],
            &[("CURRENT_TIMESTAMP()", "2026-11-17 17:05:56.862000")],
        ),
    ];
    for (entry, clock_values) in clock {
        let request = captured_submit(&entry.request).build();
        let sql_text = request.sql.clone();
        let response = sql
            .submit_enriched_sql(support::captured_request(request, entry))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(variant(&response), "ready_to_execute", "{sql_text}");
        let local = volatile_values(&response);
        let hosted = hosted_volatile_values(&entry.response);
        assert_eq!(
            volatile_map_shape(&local),
            volatile_map_shape(&hosted),
            "{sql_text}"
        );
        for (query, functions) in &local {
            for (function, value) in functions {
                let hosted_value = &hosted[query][function];
                match function.as_str() {
                    "RANDOM()" | "UNIFORM(1, 100, RANDOM())" => {
                        assert!(is_captured_unit_random(value), "{function}: {value:?}")
                    }
                    "UUID_STRING()" => {
                        assert!(is_lowercase_v4_uuid(value), "{function}: {value:?}");
                        assert!(is_lowercase_v4_uuid(hosted_value));
                    }
                    // Stable across every captured render.
                    "RANDSTR(8, 1234)" | "NORMAL(0, 1, 1234)" => {
                        assert_eq!(value, hosted_value, "{function}")
                    }
                    _ => {
                        let expected = clock_values
                            .iter()
                            .find(|(f, _)| f == function)
                            .unwrap_or_else(|| panic!("{function} is a clock value"));
                        assert_eq!(value, expected.1, "{function}");
                    }
                }
            }
        }
    }
}

/// A decision payload's fields other than its ids and map, which differ per
/// run.
fn codes(payload: &serde_json::Value) -> serde_json::Value {
    let mut payload = payload.clone();
    let object = payload.as_object_mut().unwrap();
    for per_run in [
        "request_id",
        "execution_decision_id",
        "transformed_nodes_by_query",
    ] {
        object.remove(per_run);
    }
    payload
}

/// The local and hosted response to captured submit `entry`: variant, codes
/// and description, and the map's keys and function texts.
fn assert_matches_hosted(local: &qc::SubmitSqlResponse, entry: &GoldenEntry, case: &str) {
    let local_json = serde_json::to_value(local).unwrap();
    let (variant, hosted) = entry.response["response"]
        .as_object()
        .unwrap()
        .iter()
        .next()
        .unwrap();
    assert_eq!(
        support::scenario::variant(local),
        variant,
        "{case}: {local_json}"
    );
    assert_eq!(
        codes(&local_json["response"][variant]),
        codes(hosted),
        "{case}: {local_json}"
    );
    assert_eq!(
        volatile_map_shape(&volatile_values(local)),
        volatile_map_shape(&hosted_volatile_values(&entry.response)),
        "{case}"
    );
}

#[tokio::test]
async fn snowflake_model_cells_return_hosted_codes_and_maps_on_builds_and_reuses() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let run_1 = entries(RUN_1);
    let responses = replay(&sc, &run_1, 50).await;
    for (entry, case) in [
        (5, "cell 3 deterministic control, first build: empty map"),
        (6, "cell 1 every function, first build"),
        (8, "cell 4 evaluation off, first build: rendered"),
        (7, "a view with evaluation off, first build: rendered"),
        (
            31,
            "cell 3 deterministic control rerun with evaluation on: reused",
        ),
        (32, "a view with evaluation off, rerun: reused and rendered"),
        (33, "cell 2 rerun with evaluation on: rebuilt and rendered"),
        (34, "cell 4 evaluation off, rerun: reused and rendered"),
        (
            39,
            "cell 7 consumer within Lag tolerance: reused, empty map",
        ),
        (
            50,
            "cell 4 evaluation switched off over an evaluation-on build: reused and rendered",
        ),
    ] {
        assert_matches_hosted(&responses[&entry], &run_1[entry], case);
    }
    // Reuse is still refused for SQL whose query dependency definition calls
    // a Volatile function with evaluation requested (cell 5 consumer rerun).
    assert_eq!(variant(&responses[&35]), "ready_to_execute");
}

#[tokio::test]
async fn snowflake_evaluation_off_renders_a_rebuild_but_prints_no_uncaptured_explain_lines() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await.org("evaluation off");
    let (ready, explained) = rebuild_with(
        &sc,
        snowflake_history,
        captured_submit(&request(SNOWFLAKE_FIRST_BUILDS, SNOWFLAKE_PROD_SUBMIT))
            .own_epoch(Some(OWN_EPOCH)),
        &snowflake_sql("id, amount, current_timestamp() as loaded_at"),
        true,
    )
    .await;

    assert_eq!(
        volatile_map_shape(&rendered(&ready)),
        [(
            target(&request(SNOWFLAKE_FIRST_BUILDS, SNOWFLAKE_PROD_SUBMIT)),
            vec!["CURRENT_TIMESTAMP()".to_owned()]
        )],
        "rendered with evaluation off, as on hosted's evaluation-off builds"
    );
    // Hosted's Explain of a query-changed rebuild with evaluation off is
    // captured for a table without upstreams only (`ro`, #206); this one
    // reads an upstream table, so the query analysis prints without
    // volatile lines.
    assert!(
        texts(&explained).contains(&"the model query has changed".to_owned()),
        "{explained}"
    );
    assert!(!prints_volatile_lines(&explained), "{explained}");
}

/// Hosted's Explain `explain` of captured submit `submit`, for the local
/// decision `local_id` that rendered `local_values`: hosted's decision id and
/// rendered values replaced by the local ones.
fn hosted_explain_for(
    explain: &GoldenEntry,
    submit: &GoldenEntry,
    local_id: &str,
    local_values: &VolatileValues,
) -> serde_json::Value {
    let hosted_id = submit.response["response"]
        .as_object()
        .and_then(|branches| branches.values().next())
        .and_then(|payload| payload["execution_decision_id"].as_str())
        .unwrap();
    let mut hosted = serde_json::to_string(&explain.response)
        .unwrap()
        .replace(hosted_id, local_id);
    for (query, functions) in hosted_volatile_values(&submit.response) {
        for (function, value) in functions {
            let ours = &local_values[&query][&function];
            hosted = hosted.replace(
                &format!("{function} -> rendered with '{value}'"),
                &format!("{function} -> rendered with '{ours}'"),
            );
        }
    }
    serde_json::from_str(&hosted).unwrap()
}

fn decision_id(response: &qc::SubmitSqlResponse) -> String {
    let value = serde_json::to_value(response).unwrap();
    value["response"]
        .as_object()
        .unwrap()
        .values()
        .next()
        .unwrap()["execution_decision_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

#[tokio::test]
async fn snowflake_model_cells_explain_as_hosted_including_after_a_restart() {
    let database = std::sync::Arc::new(support::TestDatabase::new("volatile_cells").await);
    let server = support::start_server_on_database(database.clone()).await;
    let sc = Scenario::connect(&server).await;
    let run_1 = entries(RUN_1);
    let responses = replay(&sc, &run_1, 50).await;
    // (submit, its captured Explain, case)
    let cases = [
        (6, 19, "cell 1 first build: no volatile lines"),
        (
            8,
            20,
            "cell 4 evaluation off, first build: no volatile lines",
        ),
        (5, 21, "cell 3 first build"),
        (7, 22, "a view with evaluation off, first build"),
        (33, 41, "cell 2 rerun with evaluation on"),
        (34, 42, "cell 4 evaluation off, reused"),
        (31, 43, "cell 3 deterministic control, reused"),
        (32, 44, "a view with evaluation off, reused"),
        (50, 51, "cell 4 evaluation switched off, reused"),
    ];
    let expected: Vec<_> = cases
        .iter()
        .map(|(submit, explained, _)| {
            hosted_explain_for(
                &run_1[*explained],
                &run_1[*submit],
                &decision_id(&responses[submit]),
                &volatile_values(&responses[submit]),
            )
        })
        .collect();
    for ((submit, _, case), expected) in cases.iter().zip(&expected) {
        let id = decision_id(&responses[submit]);
        assert_eq!(&explain(&sc, &id).await, expected, "{case}");
    }

    drop(sc);
    drop(server);
    let server = support::start_server_on_database(database.clone()).await;
    let sc = Scenario::connect(&server).await;
    for ((submit, _, case), expected) in cases.iter().zip(&expected) {
        let id = decision_id(&responses[submit]);
        assert_eq!(
            &explain(&sc, &id).await,
            expected,
            "{case}: the persisted values print after a restart"
        );
    }
}
// ── Snowflake consumer of a volatile model (#196) ───────────────────────────
//
// #191 cell 7, `golden/fixtures/explain_volatile_evaluation.jsonl`: `vm` calls
// every Volatile function with evaluation requested, and `vk` is a table over
// `vm` with a Lag tolerance of an hour and evaluation off. A rebuild of `vm`
// with unchanged SQL is a data change for `vk` (ADR 0007): hosted SKIPs `vk`
// within Lag tolerance, so its identity holds no rendered value.

const VOLATILE_CELLS: &str =
    include_str!("../../../golden/fixtures/explain_volatile_evaluation.jsonl");
const VM_BUILD: usize = 6;
const VM_REBUILD: usize = 33;
const VK_BUILD: usize = 15;
const VK_BUILD_EXPLAIN: usize = 25;
const VK_RERUN: usize = 39;
const VK_RERUN_EXPLAIN: usize = 47;
const HOUR_MS: i64 = 3_600_000;

fn volatile_cells() -> Vec<GoldenEntry> {
    VOLATILE_CELLS
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

/// When the client sent the captured `entry` (`x-submitted-at-epoch`): the
/// earliest time hosted can have decided it. The proxy's `recorded_at` is
/// later, after hosted answered, and moves "last updated: now" past a second.
fn submitted_at(entry: &GoldenEntry) -> i64 {
    entry
        .metadata
        .iter()
        .find(|(key, _)| key == "x-submitted-at-epoch")
        .and_then(|(_, value)| value.parse().ok())
        .unwrap()
}

/// Submit the captured `submit` when the client sent it, expect a build, and
/// confirm it with its captured confirmation when that one was sent.
async fn build_captured(
    sc: &Scenario,
    entries: &[GoldenEntry],
    submit: usize,
) -> qc::ReadyToExecuteResponse {
    let entry = &entries[submit];
    let ready = sc
        .at(submitted_at(entry))
        .expect_execute(captured_submit(&entry.request))
        .await;
    let hosted_id = &entry.response["response"]["ready_to_execute"]["request_id"];
    let confirmation = entries
        .iter()
        .find(|e| e.method == "ConfirmExecution" && &e.request["request_id"] == hosted_id)
        .unwrap();
    let mut confirm: qc::ConfirmExecutionRequest =
        serde_json::from_value(confirmation.request.clone()).unwrap();
    confirm.request_id = ready.request_id.clone();
    assert!(
        sc.at(submitted_at(confirmation))
            .try_confirm(confirm)
            .await
            .unwrap()
            .success
    );
    ready
}

/// Hosted's captured Explain `entry`, for the local decision `id`.
fn hosted_explain(entries: &[GoldenEntry], entry: usize, id: &str) -> serde_json::Value {
    let hosted = &entries[entry].response;
    let hosted_id = hosted["messages"][0]["execution_decision_id"]
        .as_str()
        .unwrap();
    serde_json::from_str(
        &serde_json::to_string(hosted)
            .unwrap()
            .replace(hosted_id, id),
    )
    .unwrap()
}

/// Cell 7's first builds of `vm` and `vk`. Returns `vk`'s.
async fn first_builds(sc: &Scenario, entries: &[GoldenEntry]) -> qc::ReadyToExecuteResponse {
    build_captured(sc, entries, VM_BUILD).await;
    build_captured(sc, entries, VK_BUILD).await
}

/// The captured decision of a submit `entry`, as hosted explained it.
fn hosted_decision(entries: &[GoldenEntry], entry: usize, variant: &str) -> serde_json::Value {
    entries[entry].response["response"][variant]["explained_decision"].clone()
}

/// Cell 7 up to `vk`'s rerun: both first builds, then a rebuild of `vm`.
/// Returns `vk`'s first build.
async fn consumer_history(sc: &Scenario, entries: &[GoldenEntry]) -> qc::ReadyToExecuteResponse {
    let vk = first_builds(sc, entries).await;
    let vm = build_captured(sc, entries, VM_REBUILD).await;
    assert!(
        !vm.transformed_nodes_by_query.is_empty(),
        "the volatile upstream is rebuilt and rendered"
    );
    vk
}

#[tokio::test]
async fn snowflake_consumer_of_a_volatile_model_skips_within_lag_tolerance_after_its_rebuild() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await.org("cell7");
    let entries = volatile_cells();

    let vk = consumer_history(&sc, &entries).await;
    assert_eq!(
        serde_json::to_value(&vk.explained_decision).unwrap(),
        hosted_decision(&entries, VK_BUILD, "ready_to_execute")
    );
    assert!(vk.transformed_nodes_by_query.is_empty());
    let id = vk.execution_decision_id.unwrap();
    assert_eq!(
        explain(&sc, &id).await,
        hosted_explain(&entries, VK_BUILD_EXPLAIN, &id)
    );

    let rerun = &entries[VK_RERUN];
    let skip = sc
        .at(submitted_at(rerun))
        .expect_skip(captured_submit(&rerun.request))
        .await;
    assert_eq!(
        serde_json::to_value(&skip.explained_decision).unwrap(),
        hosted_decision(&entries, VK_RERUN, "skip_execution"),
        "a SKIP within Lag tolerance, reported stale"
    );
    assert!(skip.transformed_nodes_by_query.is_empty());
    assert_eq!(
        skip.execution_runtime_ms,
        rerun.response["response"]["skip_execution"]["execution_runtime_ms"].as_i64()
    );
    let id = skip.execution_decision_id.unwrap();
    assert_eq!(
        explain(&sc, &id).await,
        hosted_explain(&entries, VK_RERUN_EXPLAIN, &id)
    );
}

/// One Explain line of the first message, by its exact text.
fn line<'a>(explain: &'a serde_json::Value, text: &str) -> Option<&'a serde_json::Value> {
    fn find<'a>(lines: &'a serde_json::Value, text: &str) -> Option<&'a serde_json::Value> {
        lines.as_array()?.iter().find_map(|line| {
            (line["text"] == text)
                .then_some(line)
                .or_else(|| find(&line["children"], text))
        })
    }
    find(&explain["messages"][0]["explain_lines"], text)
}

#[tokio::test]
async fn snowflake_consumer_of_a_volatile_model_rebuilds_after_its_rebuild_outside_lag_tolerance() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await.org("cell7-late");
    let entries = volatile_cells();
    consumer_history(&sc, &entries).await;

    // The captured rerun, an hour later than captured: `vk` was built more
    // than its Lag tolerance ago, and `vm` has changed since.
    let rerun = &entries[VK_RERUN];
    let ready = sc
        .at(submitted_at(rerun) + HOUR_MS)
        .expect_execute(captured_submit(&rerun.request))
        .await;
    assert_eq!(
        ready.explained_decision.as_ref().unwrap().decision_description,
        "model was executed because either its query didn't match or its upstream data is out of date"
    );
    assert!(ready.transformed_nodes_by_query.is_empty());
    let explained = explain(&sc, ready.execution_decision_id.as_deref().unwrap()).await;
    assert!(!prints_volatile_lines(&explained), "{explained}");
    for (text, marker, badge) in [
        ("the model query has not changed", Some(1), Some(5)),
        ("upstream model queries have not changed", Some(1), None),
        ("upstream data is outdated", Some(2), None),
    ] {
        let found = line(&explained, text).unwrap_or_else(|| panic!("{text}: {explained}"));
        assert_eq!(
            (found["marker"].as_i64(), found["badge"].as_i64()),
            (marker, badge),
            "{text}"
        );
    }
    // A data-only rebuild with no equivalent elsewhere prints no Lag
    // tolerance line (hosted, `explain_rebuild_tolerance_line.jsonl` entry 3).
    assert!(
        !texts(&explained)
            .iter()
            .any(|t| t.contains("lag tolerance")),
        "{explained}"
    );
}

#[tokio::test]
async fn snowflake_consumer_of_an_unchanged_volatile_model_skips_outside_lag_tolerance() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await.org("cell7-control");
    let entries = volatile_cells();
    first_builds(&sc, &entries).await;

    // Control for the test above: the same late rerun without `vm`'s
    // rebuild, so `vm` reports the epoch `vk` was built over.
    let rerun = &entries[VK_RERUN];
    let vm = entries[VM_BUILD].request["target_table"].as_str().unwrap();
    let vm_built = entries[VK_BUILD].request["tables"][0]["last_modified_epoch"]
        .as_i64()
        .unwrap();
    let skip = sc
        .at(submitted_at(rerun) + HOUR_MS)
        .expect_skip(captured_submit(&rerun.request).upstream(vm, vm_built))
        .await;
    assert_eq!(
        skip.explained_decision.unwrap().decision_description,
        "model was a no-op because both its query and its upstream data are up to date"
    );
}

// ── Snowflake volatile view dependency and data test (#195) ─────────────────

const SNOWFLAKE_EVALUATION: &str =
    include_str!("../../../golden/fixtures/explain_volatile_evaluation.jsonl");
/// `vd`, a table over the view `vv` that calls `current_timestamp()`, with
/// evaluation requested: first build and unchanged rerun.
const VD_FIRST_BUILD: usize = 10;
const VD_RERUN: usize = 35;
/// Hosted's Explain of the `vd` rerun.
const VD_RERUN_EXPLAIN: usize = 45;
/// `vt`, a data test calling `current_timestamp()` with evaluation
/// requested: first execution and unchanged rerun, and their Explains.
const VT_FIRST: usize = 16;
const VT_RERUN: usize = 37;
const VT_FIRST_EXPLAIN: usize = 24;
const VT_RERUN_EXPLAIN: usize = 46;

fn snowflake_evaluation() -> Vec<GoldenEntry> {
    SNOWFLAKE_EVALUATION
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[tokio::test]
async fn snowflake_volatile_data_test_and_dependency_render_keyed_as_captured() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let entries = snowflake_evaluation();
    for (case, entry) in [("data test", VT_FIRST), ("view dependency", VD_FIRST_BUILD)] {
        let entry = &entries[entry];
        let ready = sc
            .org(case)
            .expect_execute(captured_submit(&entry.request))
            .await;
        let values = rendered(&ready);
        assert_eq!(
            volatile_map_shape(&values),
            volatile_map_shape(&hosted_volatile_values(&entry.response)),
            "{case}: hosted's key shape"
        );
    }
}

/// Replay the captured run through `through` (inclusive) at the recorded
/// times, correlating each confirm with its local request id. Returns each
/// local `ready_to_execute` by its submit's entry index.
async fn replay_snowflake_evaluation(
    sc: &Scenario,
    through: usize,
) -> BTreeMap<usize, qc::ReadyToExecuteResponse> {
    let entries = snowflake_evaluation();
    let mut sql = qc::sql_client::SqlClient::new(sc.channel());
    let mut exec = qc::execution_client::ExecutionClient::new(sc.channel());
    let mut local_ids = std::collections::HashMap::new();
    let mut executed = BTreeMap::new();
    for (index, entry) in entries[..=through].iter().enumerate() {
        match entry.method.as_str() {
            "SubmitEnrichedSQL" => {
                let response = sql
                    .submit_enriched_sql(support::captured_request(
                        captured_submit(&entry.request).build(),
                        entry,
                    ))
                    .await
                    .unwrap()
                    .into_inner();
                let hosted = entry.response["response"]["ready_to_execute"]["request_id"].as_str();
                if let (Some(hosted), Some(local)) = (hosted, ready_request_id(&response)) {
                    local_ids.insert(hosted.to_owned(), local);
                }
                if variant(&response) == "ready_to_execute" {
                    executed.insert(index, support::scenario::into_execute(response));
                }
            }
            "ConfirmExecution" => {
                let mut req: qc::ConfirmExecutionRequest =
                    serde_json::from_value(entry.request.clone()).unwrap();
                req.request_id = local_ids[&req.request_id].clone();
                exec.confirm_execution(support::captured_request(req, entry))
                    .await
                    .unwrap();
            }
            _ => {}
        }
    }
    executed
}

/// `explain` with each `last updated: …` line masked. Outside Databricks a
/// data test's upstream listing prints the raw epoch, not hosted's relative
/// time; the conformance replay masks the same slot (`«rel_time»`).
fn masked_relative_times(mut explain: serde_json::Value) -> serde_json::Value {
    fn walk(lines: &mut serde_json::Value) {
        for line in lines.as_array_mut().into_iter().flatten() {
            if line["text"]
                .as_str()
                .is_some_and(|t| t.starts_with("last updated:"))
            {
                line["text"] = "last updated: «rel_time»".into();
            }
            walk(&mut line["children"]);
        }
    }
    for message in explain["messages"].as_array_mut().unwrap() {
        walk(&mut message["explain_lines"]);
    }
    explain
}

/// Replay the captured run through the last of `cases` (submit entry,
/// hosted Explain entry), then require each local Explain to equal hosted's
/// for the values the local decision rendered, before and after a restart.
async fn assert_explains_match_hosted_after_a_restart(database: &str, cases: &[(usize, usize)]) {
    let database = std::sync::Arc::new(support::TestDatabase::new(database).await);
    let server = support::start_server_on_database(database.clone()).await;
    let entries = snowflake_evaluation();
    let through = cases.iter().map(|&(submit, _)| submit).max().unwrap();
    let executed = replay_snowflake_evaluation(&Scenario::connect(&server).await, through).await;
    let expected: Vec<_> = cases
        .iter()
        .map(|&(submit, explain)| {
            let local = &executed[&submit];
            masked_relative_times(hosted_explain_for(
                &entries[explain],
                &entries[submit],
                local.execution_decision_id.as_deref().unwrap(),
                &rendered(local),
            ))
        })
        .collect();
    let explained = |server: support::TestServer| {
        let executed = &executed;
        async move {
            let sc = Scenario::connect(&server).await;
            let mut out = Vec::new();
            for &(submit, _) in cases {
                let id = executed[&submit].execution_decision_id.as_deref().unwrap();
                out.push(masked_relative_times(explain(&sc, id).await));
            }
            out
        }
    };

    assert_eq!(explained(server).await, expected);

    let server = support::start_server_on_database(database.clone()).await;
    assert_eq!(
        explained(server).await,
        expected,
        "the persisted values print after a restart"
    );
}

#[tokio::test]
async fn snowflake_volatile_data_test_explain_matches_hosted_on_both_runs_after_a_restart() {
    assert_explains_match_hosted_after_a_restart(
        "volatile_test_restart",
        &[(VT_FIRST, VT_FIRST_EXPLAIN), (VT_RERUN, VT_RERUN_EXPLAIN)],
    )
    .await;
}

#[tokio::test]
async fn snowflake_volatile_dependency_explain_matches_hosted_after_a_restart() {
    assert_explains_match_hosted_after_a_restart(
        "volatile_dependency_restart",
        &[(VD_RERUN, VD_RERUN_EXPLAIN)],
    )
    .await;
}

#[tokio::test]
async fn snowflake_volatile_dependency_and_data_test_are_never_reused_or_copied() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let entries = snowflake_evaluation();
    let request = |entry: usize| captured_submit(&entries[entry].request);
    let vv = entries[VD_FIRST_BUILD].request["query_dependencies"][0]["name"]
        .as_str()
        .unwrap()
        .to_owned();
    let vd_built_at = entries[VD_RERUN].request["tables"][0]["last_modified_epoch"]
        .as_i64()
        .unwrap();
    // The captured confirm of the first `vt` execution: passed, 0 failures.
    let vt_results: qc::Struct =
        serde_json::from_value(entries[VT_FIRST + 1].request["execution_results"].clone()).unwrap();
    let vt_passed = || support::scenario::outcome().results(vt_results.clone());
    // `vd` built in the other schema of its namespace over that schema's
    // `vv`, as a dev Target.
    let vd_elsewhere = |tolerate: bool| {
        let dev = serde_json::to_string(&entries[VD_FIRST_BUILD].request)
            .unwrap()
            .replace("PROD_STATE_DEMO", "DEV_STATE_DEMO")
            .replace("prod_state_demo", "dev_state_demo");
        captured_submit(&serde_json::from_str(&dev).unwrap()).tolerate_nondeterminism(tolerate)
    };

    let pending = sc.org("pending");
    let first = pending.expect_execute(request(VD_FIRST_BUILD)).await;
    let second = pending.expect_execute(request(VD_FIRST_BUILD)).await;
    assert!(!rendered(&first).is_empty() && !rendered(&second).is_empty());
    assert_eq!(
        persisted_execution(&server, &first.request_id).await,
        persisted_execution(&server, &second.request_id).await,
        "rendered values enter no persisted identity or fingerprint"
    );

    let volatile = sc.org("volatile");
    volatile.build(request(VD_FIRST_BUILD), vd_built_at).await;
    let rerun = volatile.expect_execute(request(VD_RERUN)).await;
    assert_eq!(
        volatile_map_shape(&rendered(&rerun)),
        [(vv.clone(), vec!["CURRENT_TIMESTAMP()".to_owned()])],
        "the unchanged table over the volatile view is rebuilt and rendered"
    );
    assert!(
        volatile
            .confirm(&rerun.request_id, built_at(vd_built_at))
            .await
    );
    assert_eq!(
        variant(&volatile.submit(vd_elsewhere(false)).await),
        "ready_to_execute",
        "never copied into another schema of the namespace"
    );
    let test = volatile.expect_execute(request(VT_FIRST)).await;
    assert!(volatile.confirm(&test.request_id, vt_passed()).await);
    let test_rerun = volatile.expect_execute(request(VT_RERUN)).await;
    assert_eq!(
        volatile_map_shape(&rendered(&test_rerun)),
        [(
            "__target_query__".to_owned(),
            vec!["CURRENT_TIMESTAMP()".to_owned()]
        )],
        "the unchanged data test is executed again and rendered"
    );

    // Control: the same history is reused, and `vd` copied, with evaluation off.
    let tolerated = sc.org("tolerated");
    tolerated
        .build(
            request(VD_FIRST_BUILD).tolerate_nondeterminism(true),
            vd_built_at,
        )
        .await;
    assert_eq!(
        variant(
            &tolerated
                .submit(request(VD_RERUN).tolerate_nondeterminism(true))
                .await
        ),
        "skip_execution"
    );
    assert_eq!(
        variant(&tolerated.submit(vd_elsewhere(true)).await),
        "ready_to_clone"
    );
    tolerated
        .build_with(request(VT_FIRST).tolerate_nondeterminism(true), vt_passed())
        .await;
    assert_eq!(
        variant(
            &tolerated
                .submit(request(VT_RERUN).tolerate_nondeterminism(true))
                .await
        ),
        "skip_execution"
    );
}

#[tokio::test]
async fn dependency_and_data_test_sql_without_a_captured_call_renders_nothing() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let entries = snowflake_evaluation();
    let vv_columns = "1 as id, current_timestamp() as ts, 'vv_164840c243b8' as v";
    let vt_condition = "current_timestamp() < '2000-01-01'::timestamp_ltz";
    let cases = [
        (
            "line comment",
            "1 as id, 1 as ts -- current_timestamp()",
            "1 = 1 -- current_timestamp()",
        ),
        (
            "block comment",
            "1 as id, 1 /* current_timestamp() */ as ts",
            "1 /* current_timestamp() */ = 1",
        ),
        (
            "string literal",
            "1 as id, 'current_timestamp()' as ts",
            "'current_timestamp()' = v",
        ),
        (
            "quoted identifier",
            "1 as id, \"current_timestamp\"() as ts",
            "\"current_timestamp\"() < '2000-01-01'::timestamp_ltz",
        ),
    ];
    let with = |entry: usize, find: &str, replace: &str| {
        let (find, replace) = (find.to_owned(), replace.to_owned());
        captured_submit(&entries[entry].request).with(move |r| {
            r.sql = r.sql.replace(&find, &replace);
            for d in &mut r.query_dependencies {
                d.query = d.query.replace(&find, &replace);
            }
        })
    };
    for (case, dependency, condition) in cases {
        for (node, submit) in [
            (
                "view dependency",
                with(VD_FIRST_BUILD, vv_columns, dependency),
            ),
            ("data test", with(VT_FIRST, vt_condition, condition)),
        ] {
            let ready = sc
                .org(&format!("{case} {node}"))
                .expect_execute(submit)
                .await;
            assert!(
                rendered(&ready).is_empty(),
                "{case} {node}: {:?}",
                rendered(&ready)
            );
        }
    }
    // Positive control for the same edits: the captured calls render.
    for (node, submit) in [
        (
            "view dependency",
            with(VD_FIRST_BUILD, vv_columns, vv_columns),
        ),
        ("data test", with(VT_FIRST, vt_condition, vt_condition)),
    ] {
        let ready = sc
            .org(&format!("control {node}"))
            .expect_execute(submit)
            .await;
        assert!(!rendered(&ready).is_empty(), "{node}");
    }

    // Databricks has no capture of a dependency or a data test: only a Target
    // relation's own `rand()` renders.
    let model = captured_submit(&request(CONTROLS, RAND_SUBMIT));
    let databricks_dependency = model
        .clone()
        .sql(&format!(
            "select * from {}",
            "`cdo_demo_db`.`ds_20261112a134ef_prod`.`v`"
        ))
        .dependency(
            "`cdo_demo_db`.`ds_20261112a134ef_prod`.`v`",
            "select rand() as noise",
        );
    let databricks_test = support::scenario::data_test("test.databricks.rand")
        .dialect("databricks")
        .sql("select rand() as noise where false")
        .tolerate_nondeterminism(false);
    for (node, submit) in [
        ("Databricks dependency", databricks_dependency),
        ("Databricks data test", databricks_test),
    ] {
        let ready = sc.org(node).expect_execute(submit).await;
        assert!(
            rendered(&ready).is_empty(),
            "{node}: {:?}",
            rendered(&ready)
        );
    }
}

// ── Uncaptured cells render nothing (#67 review) ────────────────────────────
//
// Values are returned only in the cells hosted was captured rendering:
// Snowflake FULL models (own SQL, evaluation on or off; a query dependency,
// evaluation on), views with evaluation off, and data tests without a Target
// relation with evaluation on; Databricks the query-changed rebuild of a FULL
// model (entries 85–86). Every other cell keeps an empty map and prints no
// volatile Explain lines.

/// The captured `vd` request `entry` with its own SQL also calling
/// `current_timestamp()`.
fn vd_with_own_volatile_sql(entries: &[GoldenEntry], entry: usize) -> serde_json::Value {
    let mut request = entries[entry].request.clone();
    let sql = request["sql"].as_str().unwrap().replacen(
        "select id,",
        "select current_timestamp() as own_ts, id,",
        1,
    );
    request["sql"] = sql.into();
    request
}

#[tokio::test]
async fn snowflake_evaluation_off_renders_nothing_for_a_data_test_or_a_query_dependency() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let entries = snowflake_evaluation();
    // Positive control: a model's own SQL renders with evaluation off (run 1
    // entry 8).
    let control = sc
        .org("control")
        .expect_execute(captured_submit(&entries[8].request))
        .await;
    assert!(!rendered(&control).is_empty());

    for (case, entry) in [("data test", VT_FIRST), ("view dependency", VD_FIRST_BUILD)] {
        let ready = sc
            .org(case)
            .expect_execute(captured_submit(&entries[entry].request).tolerate_nondeterminism(true))
            .await;
        assert!(
            rendered(&ready).is_empty(),
            "{case}: {:?}",
            rendered(&ready)
        );
    }
}

#[tokio::test]
async fn snowflake_values_in_own_sql_and_a_dependency_or_a_data_test_over_a_volatile_view_render_nothing(
) {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let entries = snowflake_evaluation();
    let vv = entries[VD_FIRST_BUILD].request["query_dependencies"][0]["name"]
        .as_str()
        .unwrap()
        .to_owned();
    let vv_sql = entries[VD_FIRST_BUILD].request["query_dependencies"][0]["query"]
        .as_str()
        .unwrap()
        .to_owned();
    let vd_built_at = entries[VD_RERUN].request["tables"][0]["last_modified_epoch"]
        .as_i64()
        .unwrap();

    // A model calling a Volatile function in its own SQL and in a dependency:
    // first build, and an unchanged rerun whose Explain would carry the lines.
    let both = sc.org("both");
    let first = both
        .expect_execute(captured_submit(&vd_with_own_volatile_sql(
            &entries,
            VD_FIRST_BUILD,
        )))
        .await;
    assert!(rendered(&first).is_empty(), "{:?}", rendered(&first));
    assert!(both.confirm(&first.request_id, built_at(vd_built_at)).await);
    let rerun = both
        .expect_execute(captured_submit(&vd_with_own_volatile_sql(
            &entries, VD_RERUN,
        )))
        .await;
    assert!(rendered(&rerun).is_empty(), "{:?}", rendered(&rerun));
    let explained = explain(&both, rerun.execution_decision_id.as_deref().unwrap()).await;
    assert!(!prints_volatile_lines(&explained), "{explained}");

    // A data test over the volatile view, with deterministic and with
    // volatile SQL of its own.
    let over_view = |own: &str| {
        captured_submit(&entries[VT_FIRST].request)
            .sql(&format!(
                "\nselect id, v from CDO_DEMO_DB.prod_state_demo.vv_164840c243b8\nwhere {own}"
            ))
            .no_tables()
            .dependency(&vv, &vv_sql)
    };
    for (case, test) in [
        ("data test over a volatile view", over_view("id > 100")),
        (
            "volatile data test over a volatile view",
            over_view("id > 100 and current_timestamp() < '2000-01-01'::timestamp_ltz"),
        ),
    ] {
        let sc = sc.org(case);
        let ready = sc.expect_execute(test).await;
        assert!(
            rendered(&ready).is_empty(),
            "{case}: {:?}",
            rendered(&ready)
        );
        let explained = explain(&sc, ready.execution_decision_id.as_deref().unwrap()).await;
        assert!(!prints_volatile_lines(&explained), "{case}: {explained}");
    }
}

#[tokio::test]
async fn databricks_renders_only_the_captured_query_changed_rebuild() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await.org("databricks cells");
    let model = captured_submit(&request(CONTROLS, RAND_SUBMIT));

    let first = sc.expect_execute(model.clone()).await;
    assert!(
        rendered(&first).is_empty(),
        "first build: {:?}",
        rendered(&first)
    );
    assert!(
        sc.confirm(&first.request_id, built_at(captured_target_epoch()))
            .await
    );
    let rerun = sc.expect_execute(model.clone()).await;
    assert!(
        rendered(&rerun).is_empty(),
        "unchanged rerun (cell 2, #198): {:?}",
        rendered(&rerun)
    );
    let explained = explain(&sc, rerun.execution_decision_id.as_deref().unwrap()).await;
    assert!(!prints_volatile_lines(&explained), "{explained}");

    // Positive control: the captured query-changed rebuild renders.
    let control = sc.org("databricks control");
    deterministic_history(&control).await;
    assert!(!rendered(&control.expect_execute(model).await).is_empty());
}

#[tokio::test]
async fn volatile_sql_of_an_uncaptured_execution_type_renders_nothing() {
    use support::scenario::ExecutionType;
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let uncaptured = [
        ExecutionType::Append,
        ExecutionType::Merge,
        ExecutionType::InsertOverwrite,
        ExecutionType::DeleteInsert,
        ExecutionType::Microbatch,
        ExecutionType::Snapshot,
        ExecutionType::DbtCustom,
    ];

    // Snowflake: the captured FULL first build renders; the same build of
    // another execution type, or a view with evaluation on, does not.
    let snowflake = captured_submit(&request(SNOWFLAKE_FIRST_BUILDS, SNOWFLAKE_PROD_SUBMIT));
    assert!(!rendered(
        &sc.org("snowflake control")
            .expect_execute(snowflake.clone())
            .await
    )
    .is_empty());
    for execution_type in uncaptured.into_iter().chain([ExecutionType::View]) {
        let case = format!("snowflake {execution_type:?}");
        let ready = sc
            .org(&case)
            .expect_execute(snowflake.clone().execution_type(execution_type))
            .await;
        assert!(
            rendered(&ready).is_empty(),
            "{case}: {:?}",
            rendered(&ready)
        );
    }
    // A Snowflake data test with a Target relation (store_failures).
    let entries = snowflake_evaluation();
    let stored = captured_submit(&entries[VT_FIRST].request)
        .target("\"CDO_DEMO_DB\".\"PROD_STATE_DEMO_DBT_TEST__AUDIT\".\"VT_164840C243B8\"");
    let ready = sc
        .org("snowflake stored failures")
        .expect_execute(stored)
        .await;
    assert!(rendered(&ready).is_empty(), "{:?}", rendered(&ready));

    // Databricks: the captured query-changed FULL rebuild renders; the same
    // rebuild of another execution type does not.
    let input = "cdo_demo_db.ds_20261112a134ef_prod.p153_80615b233ac140e48993d85c06bfc823_inputs";
    for execution_type in [
        ExecutionType::Full,
        ExecutionType::View,
        ExecutionType::Merge,
        ExecutionType::Snapshot,
    ] {
        let case = format!("databricks {execution_type:?}");
        let sc = sc.org(&case);
        let model = captured_submit(&request(CONTROLS, RAND_SUBMIT)).execution_type(execution_type);
        sc.build(
            model
                .clone()
                .sql(&format!("select *, 1 AS noise from {input}")),
            captured_target_epoch(),
        )
        .await;
        let ready = sc.expect_execute(model).await;
        assert_eq!(
            rendered(&ready).is_empty(),
            execution_type != ExecutionType::Full,
            "{case}: {:?}",
            rendered(&ready)
        );
    }
}

// ── Snowflake rebuild reasons and Explain variants (#206) ───────────────────
//
// Evidence: the #202 capture, `golden/fixtures/explain_volatile_cells.jsonl`
// (dbt 2.0.11; `docs/reports/snowflake-volatile-cells-2026-11-19.md`). Round 2
// reruns each node; every case below is a `ready_to_execute` 6/6 whose map
// replays, and whose Explain is compared with hosted's.
mod rebuild_reasons_206 {
    use super::*;

    const CELLS: &str = include_str!("../../../golden/fixtures/explain_volatile_cells.jsonl");
    /// Round 2 submits, and hosted's Explain of each.
    /// `rq`: query-changed FULL rebuild, evaluation on.
    const RQ: (usize, usize) = (142, 192);
    /// `rx`: rebuild after an External modification, evaluation on.
    const RX: (usize, usize) = (137, 193);
    /// `ro`: query-changed FULL rebuild, evaluation off.
    const RO: (usize, usize) = (133, 194);
    /// `rt`: a volatile data test after its logic changed (`id > 100` ->
    /// `id > 101`), evaluation on.
    const RT: (usize, usize) = (148, 204);
    /// `eq`: unchanged rerun of a table over a deterministic query
    /// dependency (the view `vwd`), evaluation on.
    const EQ: (usize, usize) = (147, 197);
    /// `eu`: unchanged rerun of a table over an upstream table (`vb`),
    /// evaluation on.
    const EU: (usize, usize) = (166, 195);
    /// `edu`: unchanged rerun of a table over the volatile view `vw` (a
    /// query dependency) and an upstream table (`vb`), evaluation on.
    const EDU: (usize, usize) = (168, 199);

    /// Replay the capture through its last round 2 submit (`edu`, entry 168)
    /// at the recorded times on a fresh database, then require each case's
    /// local response to match hosted's codes and map shape, and its Explain
    /// to equal hosted's for the values the local decision rendered, before
    /// and after a restart. `last updated` lines are masked: outside
    /// Databricks they print the raw epoch, as the conformance replay masks.
    async fn assert_rebuilds_match_hosted(database: &str, cases: &[((usize, usize), &str)]) {
        let database = std::sync::Arc::new(support::TestDatabase::new(database).await);
        let server = support::start_server_on_database(database.clone()).await;
        let entries = entries(CELLS);
        let responses = replay(&Scenario::connect(&server).await, &entries, 168).await;
        let expected: Vec<_> = cases
            .iter()
            .map(|&((submit, explained), case)| {
                assert_matches_hosted(&responses[&submit], &entries[submit], case);
                masked_relative_times(hosted_explain_for(
                    &entries[explained],
                    &entries[submit],
                    &decision_id(&responses[&submit]),
                    &volatile_values(&responses[&submit]),
                ))
            })
            .collect();
        let explained = |server: support::TestServer| {
            let responses = &responses;
            async move {
                let sc = Scenario::connect(&server).await;
                let mut out = Vec::new();
                for &((submit, _), _) in cases {
                    let id = decision_id(&responses[&submit]);
                    out.push(masked_relative_times(explain(&sc, &id).await));
                }
                out
            }
        };
        let cases: Vec<_> = cases.iter().map(|(_, case)| *case).collect();
        let local = explained(server).await;
        for ((local, expected), case) in local.iter().zip(&expected).zip(&cases) {
            assert_eq!(local, expected, "{case}");
        }
        let server = support::start_server_on_database(database.clone()).await;
        let local = explained(server).await;
        for ((local, expected), case) in local.iter().zip(&expected).zip(&cases) {
            assert_eq!(local, expected, "{case}: after a restart");
        }
    }

    #[tokio::test]
    async fn snowflake_query_changed_and_externally_modified_rebuilds_render_and_explain_as_captured(
    ) {
        assert_rebuilds_match_hosted(
            "volatile_206_rq_rx",
            &[
                (RQ, "rq: query-changed rebuild, evaluation on"),
                (
                    RX,
                    "rx: rebuild after an External modification, evaluation on",
                ),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn snowflake_query_changed_rebuild_with_evaluation_off_lists_its_values_without_a_tolerance_line(
    ) {
        assert_rebuilds_match_hosted(
            "volatile_206_ro",
            &[(RO, "ro: query-changed rebuild, evaluation off")],
        )
        .await;
    }

    #[tokio::test]
    async fn snowflake_volatile_data_test_logic_change_prints_the_volatile_query_analysis() {
        assert_rebuilds_match_hosted(
            "volatile_206_rt",
            &[(RT, "rt: volatile data test logic change, evaluation on")],
        )
        .await;
    }

    #[tokio::test]
    async fn snowflake_rerun_over_a_deterministic_query_dependency_explains_as_captured() {
        assert_rebuilds_match_hosted(
            "volatile_206_eq",
            &[(EQ, "eq: rerun over a deterministic query dependency")],
        )
        .await;
    }

    #[tokio::test]
    async fn snowflake_rerun_over_an_upstream_table_explains_as_captured() {
        assert_rebuilds_match_hosted(
            "volatile_206_eu",
            &[(EU, "eu: rerun over an upstream table")],
        )
        .await;
    }

    #[tokio::test]
    async fn snowflake_rerun_over_a_volatile_dependency_and_an_upstream_table_explains_as_captured()
    {
        assert_rebuilds_match_hosted(
            "volatile_206_edu",
            &[(
                EDU,
                "edu: rerun over a volatile view dependency and an upstream table",
            )],
        )
        .await;
    }

    /// Hosted's rerun trees over an upstream table were captured with the
    /// upstream unchanged since the build. A rerun after the upstream was
    /// rebuilt is uncaptured: it keeps its map and renders no tree.
    #[tokio::test]
    async fn snowflake_reruns_over_a_changed_upstream_table_render_no_tree() {
        let server = support::start_server().await;
        let sc = Scenario::connect(&server).await;
        let entries = entries(CELLS);
        // Round 1: every first build and its confirm.
        replay(&sc, &entries, 108).await;
        for (case, (submit, _)) in [("eu", EU), ("edu", EDU)] {
            let vb = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"VB_1431544D67B8\"";
            let rebuilt = captured_submit(&entries[submit].request).with(move |r| {
                for table in r.tables.iter_mut().filter(|t| t.name == vb) {
                    table.last_modified_epoch = Some(1795098900000);
                }
            });
            let ready = sc.expect_execute(rebuilt).await;
            assert_eq!(
                volatile_map_shape(&rendered(&ready)),
                volatile_map_shape(&hosted_volatile_values(&entries[submit].response)),
                "{case}: the map is unchanged"
            );
            let explained = explain(&sc, ready.execution_decision_id.as_deref().unwrap()).await;
            assert_eq!(
                explained["messages"][0]["explain_lines"],
                serde_json::json!([]),
                "{case}: {explained}"
            );
        }
    }
}
