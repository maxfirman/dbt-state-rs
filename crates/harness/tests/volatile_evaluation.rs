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

use dbt_state_harness::diff::GoldenEntry;
use dbt_state_proto::query_cache as qc;
use support::scenario::{
    built_at, captured_submit, hosted_volatile_values, variant, volatile_map_shape,
    volatile_values, volatile_values_of, Scenario, Submit, VolatileValues, OWN_EPOCH,
};

const CONTROLS: &str = include_str!("fixtures/databricks_submit_copy_controls.jsonl");
/// The #191 Snowflake capture batch, run 1 (dbt 2.0.11): every function in
/// one model, reruns, the deterministic control, evaluation off, a table
/// over a volatile view, a data test and a consumer of a volatile model.
const RUN_1: &str = include_str!("../../../golden/fixtures/explain_volatile_evaluation.jsonl");
/// The #191 capture batch, run 2: one function form per model.
const RUN_2: &str =
    include_str!("../../../golden/fixtures/explain_volatile_evaluation_functions.jsonl");
/// The #202 Snowflake capture batch of the cells #191 left open (dbt 2.0.11;
/// entry indices in `docs/reports/snowflake-volatile-cells-2026-11-19.md`).
const CELLS: &str = include_str!("../../../golden/fixtures/explain_volatile_cells.jsonl");
/// The #198 Databricks capture (dbt 2.0.11): a `rand()` table's first build
/// and unchanged rerun, a non-storing `rand()` data test's first execution
/// and rerun, deterministic controls and a consumer (`docs/databricks-volatile.md`).
const DATABRICKS_EVALUATION: &str =
    include_str!("../../../golden/fixtures/databricks_volatile_evaluation.jsonl");
const DATABRICKS_PRODUCER: &str =
    include_str!("../../../golden/fixtures/databricks_volatile_producer.jsonl");
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

/// Replay the submits and confirms of `entries` through entry `through`, at
/// their recorded times, correlating each confirm with its local request id.
/// Returns the local response to each submit, by entry.
async fn replay(
    sc: &Scenario,
    entries: &[GoldenEntry],
    through: usize,
) -> std::collections::HashMap<usize, qc::SubmitSqlResponse> {
    replay_where(sc, entries, through, |_| true).await
}

/// [`replay`] of only the submits `submits` admits, and their confirms. A
/// confirm of a submit that was replayed but not answered with a request id
/// panics.
async fn replay_where(
    sc: &Scenario,
    entries: &[GoldenEntry],
    through: usize,
    submits: impl Fn(&GoldenEntry) -> bool,
) -> std::collections::HashMap<usize, qc::SubmitSqlResponse> {
    let mut sql = qc::sql_client::SqlClient::new(sc.channel());
    let mut exec = qc::execution_client::ExecutionClient::new(sc.channel());
    let mut local_ids = std::collections::HashMap::new();
    let mut left_out = std::collections::HashSet::new();
    let mut responses = std::collections::HashMap::new();
    for (index, entry) in entries.iter().enumerate().take(through + 1) {
        let hosted = entry.response["response"]["ready_to_execute"]["request_id"]
            .as_str()
            .or(entry.response["response"]["ready_to_clone"]["request_id"].as_str());
        match entry.method.as_str() {
            "SubmitEnrichedSQL" if !submits(entry) => left_out.extend(hosted),
            "SubmitEnrichedSQL" => {
                let req = captured_submit(&entry.request).build();
                let request = if entry.request["dialect"] == "databricks" {
                    support::captured_request_at(
                        req,
                        entry,
                        support::databricks_decision_time(entry).unwrap(),
                    )
                } else {
                    support::captured_request(req, entry)
                };
                let response = sql.submit_enriched_sql(request).await.unwrap().into_inner();
                if let (Some(hosted), Some(local)) = (hosted, ready_request_id(&response)) {
                    local_ids.insert(hosted.to_owned(), local);
                }
                responses.insert(index, response);
            }
            "ConfirmExecution" => {
                let mut req: qc::ConfirmExecutionRequest =
                    serde_json::from_value(entry.request.clone()).unwrap();
                if left_out.contains(req.request_id.as_str()) {
                    continue;
                }
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
    assert_replayed_explains_match_hosted(
        "volatile_restart",
        CONTROLS,
        &[(RAND_SUBMIT, RAND_EXPLAIN, "RAND() query-changed rebuild")],
        std::convert::identity,
    )
    .await;
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
            snowflake_sql("id, amount, current_timestamp(6) as loaded_at"),
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
            "uniform with uncaptured constant arguments",
            snowflake_sql("id, amount, uniform(1, 10, 1234) as loaded_at"),
            false,
        ),
        (
            "uncaptured time precision argument",
            snowflake_sql("id, amount, current_time(3) as loaded_at"),
            false,
        ),
        (
            "uncaptured randstr seed",
            snowflake_sql("id, amount, randstr(5, 42) as loaded_at"),
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

/// One local decision whose Explain must equal hosted's.
struct HostedExplain {
    case: String,
    /// The local decision.
    id: String,
    /// When the client asked hosted for its Explain (`x-submitted-at-epoch`).
    at: i64,
    /// Hosted's Explain, for the local decision and the values it rendered.
    expected: serde_json::Value,
}

impl HostedExplain {
    /// Hosted's Explain `explain` of captured submit `submit`, for the local
    /// decision `id` that rendered `values`.
    fn of(
        case: impl Into<String>,
        explain: &GoldenEntry,
        submit: &GoldenEntry,
        id: &str,
        values: &VolatileValues,
    ) -> Self {
        // An Explain of several decisions (the Databricks capture asks for
        // four at once) is compared by the message of this one.
        let mut expected = hosted_explain_for(explain, submit, id, values);
        let messages = expected["messages"].as_array_mut().unwrap();
        messages.retain(|message| message["execution_decision_id"] == id);
        assert_eq!(messages.len(), 1, "one hosted message for decision {id}");
        Self {
            case: case.into(),
            id: id.to_owned(),
            at: submitted_at(explain),
            expected,
        }
    }

    /// [`HostedExplain::of`] the local `response` to `submit`.
    fn of_response(
        case: impl Into<String>,
        explain: &GoldenEntry,
        submit: &GoldenEntry,
        response: &qc::SubmitSqlResponse,
    ) -> Self {
        let id = decision_id(response);
        Self::of(case, explain, submit, &id, &volatile_values(response))
    }
}

/// Require each local Explain, asked at hosted's Explain time and seen
/// through `seen`, to equal hosted's on `server`, then on a server restarted
/// on `database`: the persisted values print after a restart.
async fn assert_explains_match_hosted(
    database: &std::sync::Arc<support::TestDatabase>,
    server: support::TestServer,
    explains: &[HostedExplain],
    seen: fn(serde_json::Value) -> serde_json::Value,
) {
    async fn check(
        server: &support::TestServer,
        explains: &[HostedExplain],
        seen: fn(serde_json::Value) -> serde_json::Value,
        when: &str,
    ) {
        let sc = Scenario::connect(server).await;
        for hosted in explains {
            assert_eq!(
                seen(explain(&sc.at(hosted.at), &hosted.id).await),
                seen(hosted.expected.clone()),
                "{}{when}",
                hosted.case
            );
        }
    }
    check(&server, explains, seen, "").await;
    drop(server);
    let server = support::start_server_on_database(database.clone()).await;
    check(&server, explains, seen, ": after a restart").await;
}

/// Replay `fixture` through the last submit of `cases` (submit entry, hosted
/// Explain entry, case) on a fresh `database`, then
/// [`assert_explains_match_hosted`].
async fn assert_replayed_explains_match_hosted(
    database: &str,
    fixture: &str,
    cases: &[(usize, usize, &str)],
    seen: fn(serde_json::Value) -> serde_json::Value,
) {
    let database = std::sync::Arc::new(support::TestDatabase::new(database).await);
    let server = support::start_server_on_database(database.clone()).await;
    let entries = entries(fixture);
    let through = cases.iter().map(|&(submit, _, _)| submit).max().unwrap();
    let responses = replay(&Scenario::connect(&server).await, &entries, through).await;
    let explains: Vec<_> = cases
        .iter()
        .map(|&(submit, explain, case)| {
            HostedExplain::of_response(
                case,
                &entries[explain],
                &entries[submit],
                &responses[&submit],
            )
        })
        .collect();
    assert_explains_match_hosted(&database, server, &explains, seen).await;
}

#[tokio::test]
async fn snowflake_model_cells_explain_as_hosted_including_after_a_restart() {
    // (submit, its captured Explain, case)
    assert_replayed_explains_match_hosted(
        "volatile_cells",
        RUN_1,
        &[
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
        ],
        std::convert::identity,
    )
    .await;
}

// ── Snowflake consumer of a volatile model (#196) ───────────────────────────
//
// #191 cell 7, `golden/fixtures/explain_volatile_evaluation.jsonl`: `vm` calls
// every Volatile function with evaluation requested, and `vk` is a table over
// `vm` with a Lag tolerance of an hour and evaluation off. A rebuild of `vm`
// with unchanged SQL is a data change for `vk` (ADR 0007): hosted SKIPs `vk`
// within Lag tolerance, so its identity holds no rendered value.

const VM_BUILD: usize = 6;
const VM_REBUILD: usize = 33;
const VK_BUILD: usize = 15;
const VK_BUILD_EXPLAIN: usize = 25;
const VK_RERUN: usize = 39;
const VK_RERUN_EXPLAIN: usize = 47;
const HOUR_MS: i64 = 3_600_000;

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
    let entries = entries(RUN_1);

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
    let entries = entries(RUN_1);
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
    let entries = entries(RUN_1);
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

#[tokio::test]
async fn snowflake_volatile_data_test_and_dependency_render_keyed_as_captured() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let entries = entries(RUN_1);
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

/// `explain` with the text of each line `masked` selects replaced by
/// `replacement`, as the conformance replay masks a slot.
fn mask_lines(
    mut explain: serde_json::Value,
    masked: fn(&str) -> bool,
    replacement: &str,
) -> serde_json::Value {
    fn walk(lines: &mut serde_json::Value, masked: fn(&str) -> bool, replacement: &str) {
        for line in lines.as_array_mut().into_iter().flatten() {
            if line["text"].as_str().is_some_and(masked) {
                line["text"] = replacement.into();
            }
            walk(&mut line["children"], masked, replacement);
        }
    }
    for message in explain["messages"].as_array_mut().unwrap() {
        walk(&mut message["explain_lines"], masked, replacement);
    }
    explain
}

/// `explain` with each `last updated: …` line masked (`«rel_time»`). A data
/// test's upstream listing, like a copy's, prints each upstream's raw epoch
/// (`last updated: 1795098737210 (UTC)`, `explain::clone_upstream_line`), not
/// hosted's relative time (`2 minutes ago, 2026-11-19 14:32:17 UTC`). The
/// conformance replay masks the same slot.
fn masked_relative_times(explain: serde_json::Value) -> serde_json::Value {
    mask_lines(
        explain,
        |text| text.starts_with("last updated:"),
        "last updated: «rel_time»",
    )
}

#[tokio::test]
async fn snowflake_volatile_data_test_explain_matches_hosted_on_both_runs_after_a_restart() {
    assert_replayed_explains_match_hosted(
        "volatile_test_restart",
        RUN_1,
        &[
            (VT_FIRST, VT_FIRST_EXPLAIN, "vt first execution"),
            (VT_RERUN, VT_RERUN_EXPLAIN, "vt rerun"),
        ],
        masked_relative_times,
    )
    .await;
}

#[tokio::test]
async fn snowflake_volatile_dependency_explain_matches_hosted_after_a_restart() {
    assert_replayed_explains_match_hosted(
        "volatile_dependency_restart",
        RUN_1,
        &[(VD_RERUN, VD_RERUN_EXPLAIN, "vd rerun")],
        masked_relative_times,
    )
    .await;
}

#[tokio::test]
async fn snowflake_volatile_dependency_and_data_test_are_never_reused_or_copied() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let entries = entries(RUN_1);
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
    let entries = entries(RUN_1);
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

    // Databricks query dependencies and tests storing failures are uncaptured.
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
        .target("`cdo_demo_db`.`ds_20261112a134ef_prod`.`audit`")
        .sql("select rand() as noise where false")
        .tolerate_nondeterminism(false);
    for (node, submit) in [
        ("Databricks dependency", databricks_dependency),
        ("Databricks stored-failure test", databricks_test),
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
// Snowflake FULL models (own SQL or a query dependency, evaluation on or off;
// both, evaluation on), views (evaluation off; own SQL, evaluation on), and
// data tests without a Target relation (own SQL, evaluation on or off; a
// query dependency, evaluation on); Databricks, evaluation on and its own
// SQL only, a FULL model's first build, query-changed rebuild and unchanged
// rerun, and a data test's first execution and unchanged rerun (controls
// entries 85–86; #198). Every other cell keeps an empty map and prints no
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
async fn snowflake_evaluation_off_renders_a_data_test_and_a_query_dependency_keyed_as_captured() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let entries = entries(RUN_1);
    let vv = entries[VD_FIRST_BUILD].request["query_dependencies"][0]["name"]
        .as_str()
        .unwrap()
        .to_owned();
    // Captured with evaluation off in #202 (`explain_volatile_cells.jsonl`
    // entries 60 and 58); the same cells over the #191 nodes.
    for (case, entry, key) in [
        ("data test", VT_FIRST, "__target_query__".to_owned()),
        ("view dependency", VD_FIRST_BUILD, vv),
    ] {
        let ready = sc
            .org(case)
            .expect_execute(captured_submit(&entries[entry].request).tolerate_nondeterminism(true))
            .await;
        assert_eq!(
            volatile_map_shape(&rendered(&ready)),
            [(key, vec!["CURRENT_TIMESTAMP()".to_owned()])],
            "{case}"
        );
    }
}

#[tokio::test]
async fn snowflake_volatile_data_test_over_a_volatile_view_renders_nothing() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await.org("both in a data test");
    let entries = entries(RUN_1);
    let vv = entries[VD_FIRST_BUILD].request["query_dependencies"][0]["name"]
        .as_str()
        .unwrap()
        .to_owned();
    let vv_sql = entries[VD_FIRST_BUILD].request["query_dependencies"][0]["query"]
        .as_str()
        .unwrap()
        .to_owned();
    // A data test with values in its own SQL and in a query dependency is
    // uncaptured.
    let test = captured_submit(&entries[VT_FIRST].request)
        .sql(
            "\nselect id, v from CDO_DEMO_DB.prod_state_demo.vv_164840c243b8\nwhere id > 100 \
             and current_timestamp() < '2000-01-01'::timestamp_ltz",
        )
        .no_tables()
        .dependency(&vv, &vv_sql);
    let ready = sc.expect_execute(test).await;
    assert!(rendered(&ready).is_empty(), "{:?}", rendered(&ready));
    let explained = explain(&sc, ready.execution_decision_id.as_deref().unwrap()).await;
    assert!(!prints_volatile_lines(&explained), "{explained}");
}

// ── Databricks cells (#198) ─────────────────────────────────────────────────
//
// Evidence: `golden/fixtures/databricks_volatile_evaluation.jsonl` (dbt
// 2.0.11, `docs/databricks-volatile.md`). Run 1: a deterministic control
// table (entry 4), a `rand()` table (5), a table over it with an hour's Lag
// tolerance and evaluation off (9) and a `rand()` data test without a Target
// relation (10), explained in entry 15. Run 2 reruns them (20, 21, 26 and
// 22), explained in entry 28.

/// Run 1 of the Databricks capture: the deterministic control table's,
/// the `rand()` table's and the consumer's first builds, the `rand()` data
/// test's first execution, and their Explain.
const DB_CONTROL_FIRST: usize = 4;
const DB_TABLE_FIRST: usize = 5;
const DB_CONSUMER_FIRST: usize = 9;
const DB_TEST_FIRST: usize = 10;
const DB_RUN_1_EXPLAIN: usize = 15;
/// Run 2: the reruns, and their Explain (the last entry of the capture).
const DB_CONTROL_RERUN: usize = 20;
const DB_TABLE_RERUN: usize = 21;
const DB_TEST_RERUN: usize = 22;
const DB_CONSUMER_RERUN: usize = 26;
const DB_RUN_2_EXPLAIN: usize = 28;

#[tokio::test]
async fn databricks_cells_return_hosted_codes_and_maps() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let entries = entries(DATABRICKS_EVALUATION);
    let responses = replay(&sc, &entries, DB_RUN_2_EXPLAIN).await;
    for (entry, case) in [
        (
            DB_CONTROL_FIRST,
            "deterministic control, first build: empty map",
        ),
        (DB_TABLE_FIRST, "rand() table, first build"),
        (DB_CONSUMER_FIRST, "consumer, first build: empty map"),
        (DB_TEST_FIRST, "rand() data test, first execution"),
        (DB_CONTROL_RERUN, "deterministic control, rerun: reused"),
        (
            DB_TABLE_RERUN,
            "rand() table, unchanged rerun refused for its volatile SQL alone",
        ),
        (
            DB_TEST_RERUN,
            "rand() data test, unchanged rerun refused for its volatile SQL alone",
        ),
        (
            DB_CONSUMER_RERUN,
            "consumer within Lag tolerance after its producer's rebuild: reused, empty map",
        ),
    ] {
        assert_matches_hosted(&responses[&entry], &entries[entry], case);
    }
    for (entry, query) in [
        (DB_TABLE_FIRST, target(&entries[DB_TABLE_FIRST].request)),
        (DB_TEST_FIRST, "__target_query__".to_owned()),
        (DB_TABLE_RERUN, target(&entries[DB_TABLE_RERUN].request)),
        (DB_TEST_RERUN, "__target_query__".to_owned()),
    ] {
        let value = &volatile_values(&responses[&entry])[&query]["RAND()"];
        assert!(is_captured_unit_random(value), "entry {entry}: {value}");
    }
}

#[tokio::test]
async fn databricks_cells_explain_as_hosted_including_after_a_restart() {
    // (submit, its captured Explain, case). Each captured Explain answers
    // four decisions; each is compared with its own message.
    assert_replayed_explains_match_hosted(
        "databricks_volatile_cells",
        DATABRICKS_EVALUATION,
        &[
            (
                DB_CONTROL_FIRST,
                DB_RUN_1_EXPLAIN,
                "deterministic control, first build",
            ),
            (
                DB_TABLE_FIRST,
                DB_RUN_1_EXPLAIN,
                "rand() table, first build",
            ),
            (DB_CONSUMER_FIRST, DB_RUN_1_EXPLAIN, "consumer, first build"),
            (
                DB_TEST_FIRST,
                DB_RUN_1_EXPLAIN,
                "rand() data test, first execution",
            ),
            (
                DB_CONTROL_RERUN,
                DB_RUN_2_EXPLAIN,
                "deterministic control, reused",
            ),
            (DB_TABLE_RERUN, DB_RUN_2_EXPLAIN, "rand() table, rerun"),
            (DB_TEST_RERUN, DB_RUN_2_EXPLAIN, "rand() data test, rerun"),
            (
                DB_CONSUMER_RERUN,
                DB_RUN_2_EXPLAIN,
                "consumer, reused within Lag tolerance",
            ),
        ],
        std::convert::identity,
    )
    .await;
}

/// The four promoted #213 histories also match after reopening State.
#[tokio::test]
async fn databricks_priority_cells_match_hosted_including_after_a_restart() {
    for (name, fixture, count) in [
        (
            "databricks_volatile_priority",
            include_str!("../../../golden/fixtures/databricks_volatile_priority.jsonl"),
            27,
        ),
        (
            "databricks_volatile_function_builds",
            include_str!("../../../golden/fixtures/databricks_volatile_function_builds.jsonl"),
            6,
        ),
        (
            "databricks_volatile_checked_reuse",
            include_str!("../../../golden/fixtures/databricks_volatile_checked_reuse.jsonl"),
            4,
        ),
        ("databricks_volatile_producer", DATABRICKS_PRODUCER, 14),
    ] {
        let database = std::sync::Arc::new(support::TestDatabase::new(name).await);
        let server = support::start_server_on_database(database.clone()).await;
        let capture = entries(fixture);
        let responses = replay(
            &Scenario::connect(&server).await,
            &capture,
            capture.len() - 1,
        )
        .await;
        let mut explains = Vec::new();
        for (index, submit) in capture.iter().enumerate() {
            let Some(response) = responses.get(&index) else {
                continue;
            };
            let case = format!("{name} entry {index}");
            assert_matches_hosted(response, submit, &case);
            let id = support::hosted_submit_edid(&submit.response).unwrap();
            let explain = capture
                .iter()
                .find(|entry| {
                    entry.method == "GetExplainMessages"
                        && entry.response["messages"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|message| message["execution_decision_id"] == id)
                })
                .expect("native Explain for each ordinary submit");
            explains.push(HostedExplain::of_response(case, explain, submit, response));
        }
        assert_eq!(explains.len(), count);
        assert_explains_match_hosted(&database, server, &explains, std::convert::identity).await;
    }
}

#[tokio::test]
async fn databricks_checked_reuse_refuses_unproven_sql() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let submit = captured_submit(&request(DATABRICKS_EVALUATION, DB_TABLE_FIRST));
    for sql in [
        "select rand()",
        "select rand(0)",
        "select current_date()",
        "select current_timestamp",
        "select rand(42), uuid()",
        "select example.rand(42)",
        "select rand(42) from values (1)",
    ] {
        let org = sc.org(sql);
        let request = submit.clone().sql(sql);
        org.build(request.clone(), OWN_EPOCH).await;
        org.expect_execute(request.own_epoch(Some(OWN_EPOCH))).await;
    }
}

// ── Databricks producer history (#213) ──────────────────────────────────────
//
// Evidence: `golden/fixtures/databricks_volatile_producer.jsonl`
// (`docs/reports/databricks-volatile-priority-2026-11-21.md`). The managed
// input's first build (entry 3, confirmed in 5) and the FULL `rand()` model
// over it (6). The rerun phase ends with its Explain (26). After an External
// modification of the input, the model rebuilds (30, confirmed in 31) and both
// consumers rebuild (strict 32, tolerant 33). On the next rerun the model
// rebuilds again (42, confirmed in 43) and the tolerant consumer reuses (44).

const INPUT_FIRST: usize = 3;
const INPUT_FIRST_CONFIRM: usize = 5;
const PRODUCER_FIRST: usize = 6;
const PRODUCER_FIRST_CONFIRM: usize = 8;
const PRODUCER_RERUN_EXPLAIN: usize = 26;
const PRODUCER_AFTER_INPUT_EDIT: usize = 30;
const PRODUCER_AFTER_INPUT_EDIT_CONFIRM: usize = 31;
const STRICT_AFTER_INPUT_EDIT: usize = 32;
const TOLERANT_AFTER_INPUT_EDIT: usize = 33;
const PRODUCER_AFTER_TRANSITION: usize = 42;
const PRODUCER_AFTER_TRANSITION_CONFIRM: usize = 43;
const TOLERANT_AFTER_TRANSITION: usize = 44;

#[tokio::test]
async fn databricks_producer_revision_requires_a_confirmed_source_and_cannot_revert() {
    let capture = entries(DATABRICKS_PRODUCER);
    let producer = target(&capture[PRODUCER_AFTER_INPUT_EDIT].request);
    let source = target(&capture[INPUT_FIRST].request);
    let first_edit_epoch = capture[PRODUCER_AFTER_INPUT_EDIT].request["tables"][0]
        ["last_modified_epoch"]
        .as_i64()
        .unwrap();
    let consumer = captured_submit(&capture[TOLERANT_AFTER_INPUT_EDIT].request);
    for (pending_source, ambiguous_target) in [(true, false), (false, false), (false, true)] {
        let server = support::start_server().await;
        let sc = Scenario::connect(&server).await;
        replay(&sc, &capture, PRODUCER_RERUN_EXPLAIN).await;
        if pending_source {
            sc.at(submitted_at(&capture[PRODUCER_AFTER_INPUT_EDIT]) - 1)
                .expect_execute(
                    captured_submit(&capture[INPUT_FIRST].request)
                        .sql("select 1 as id, 9 as revision")
                        .own_epoch(Some(
                            capture[PRODUCER_FIRST].request["tables"][0]["last_modified_epoch"]
                                .as_i64()
                                .unwrap(),
                        )),
                )
                .await;
        }
        let built = capture[PRODUCER_AFTER_INPUT_EDIT_CONFIRM].request["last_modified_epoch"]
            .as_i64()
            .unwrap();
        sc.at(submitted_at(&capture[PRODUCER_AFTER_INPUT_EDIT]))
            .build(
                captured_submit(&capture[PRODUCER_AFTER_INPUT_EDIT].request),
                built,
            )
            .await;
        if pending_source {
            // The source epoch can belong to an unconfirmed write. The
            // consumer sees drift and can Reuse within Lag tolerance.
            let skip = sc
                .at(submitted_at(&capture[TOLERANT_AFTER_INPUT_EDIT]))
                .expect_skip(consumer.clone())
                .await;
            assert!(skip.explained_decision.unwrap().is_stale);
        } else {
            // Leave the consumer on its first result across both edits.
            // A later edit of the already external input cannot restore the
            // producer identity the consumer recorded before either edit.
            let built_again = capture[PRODUCER_AFTER_TRANSITION_CONFIRM].request
                ["last_modified_epoch"]
                .as_i64()
                .unwrap();
            let mut submit = captured_submit(&capture[PRODUCER_AFTER_TRANSITION].request)
                .upstream(&source, first_edit_epoch + 1_000);
            if ambiguous_target {
                submit = submit.tolerate_nondeterminism(true).table(&producer, None);
            }
            sc.at(submitted_at(&capture[PRODUCER_AFTER_TRANSITION]))
                .build(submit, built_again)
                .await;
            let sc = sc.at(submitted_at(&capture[TOLERANT_AFTER_TRANSITION]));
            let ready = sc
                .expect_execute(consumer.clone().upstream(&producer, built_again))
                .await;
            let tree = explain(&sc, ready.execution_decision_id.as_deref().unwrap()).await;
            assert!(texts(&tree)
                .iter()
                .any(|line| line == "an upstream model query has changed"));
        }
    }
}

#[tokio::test]
async fn databricks_record_preserves_the_producer_revision_in_and_after_the_batch() {
    let capture = entries(DATABRICKS_PRODUCER);
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    replay(&sc, &capture, PRODUCER_RERUN_EXPLAIN).await;

    let producer = target(&capture[PRODUCER_AFTER_INPUT_EDIT].request);
    let first_build = capture[PRODUCER_AFTER_INPUT_EDIT_CONFIRM].request["last_modified_epoch"]
        .as_i64()
        .unwrap();
    sc.at(submitted_at(&capture[PRODUCER_AFTER_INPUT_EDIT]))
        .build(
            captured_submit(&capture[PRODUCER_AFTER_INPUT_EDIT].request),
            first_build,
        )
        .await;

    let recorded_build = first_build + 1_000;
    let recorded_consumer = recorded_build + 1_000;
    let strict = captured_submit(&capture[STRICT_AFTER_INPUT_EDIT].request)
        .upstream(&producer, recorded_build);
    let sc = sc.at(submitted_at(&capture[TOLERANT_AFTER_TRANSITION]));
    assert_eq!(
        sc.record([
            captured_submit(&capture[PRODUCER_AFTER_TRANSITION].request)
                .record(built_at(recorded_build)),
            strict.record(built_at(recorded_consumer)),
        ])
        .await
        .unwrap(),
        2
    );

    // The untouched consumer still sees a producer change. The consumer
    // recorded in the batch already holds the revised producer.
    let tolerant = captured_submit(&capture[TOLERANT_AFTER_INPUT_EDIT].request)
        .upstream(&producer, recorded_build);
    sc.expect_execute(tolerant).await;
    let skip = sc
        .expect_skip(strict.own_epoch(Some(recorded_consumer)))
        .await;
    assert!(!skip.explained_decision.unwrap().is_stale);
}

/// The FULL `rand()` model's reruns after its sole input was modified
/// externally, or after its Target relation was modified or dropped, were
/// captured for one plain projection over one Delta input
/// (`PRODUCER_AFTER_INPUT_EDIT`; `databricks_volatile_priority.jsonl`
/// entries 86 and 93). The same reruns of a join, of a WHERE clause or over
/// an input confirmed in another table format are uncaptured: an empty map
/// and no volatile Explain lines. The captured projection is the positive
/// control in each situation; a dropped Target relation renders its map but
/// prints only the absent-table analysis.
#[tokio::test]
async fn databricks_changed_reruns_render_only_the_captured_projection_over_a_delta_input() {
    use support::scenario::outcome;
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let capture = entries(DATABRICKS_PRODUCER);
    let epoch = |entry: usize| {
        capture[entry].request["last_modified_epoch"]
            .as_i64()
            .unwrap()
    };
    let input = captured_submit(&capture[INPUT_FIRST].request);
    let source = target(&capture[INPUT_FIRST].request);
    let input_built = epoch(INPUT_FIRST_CONFIRM);
    let built = epoch(PRODUCER_FIRST_CONFIRM);
    let producer = captured_submit(&capture[PRODUCER_FIRST].request);
    let projection = capture[PRODUCER_FIRST].request["sql"].as_str().unwrap();
    let join = format!(
        "\nselect a.id, a.revision, rand() as value, a.run_id from {source} a \
         join {source} b on a.id = b.id"
    );
    let filtered = format!("{projection} where revision > 0");
    // The captured confirmation of the input reported no table type.
    let delta = outcome().epoch(input_built).runtime(10);
    let parquet = delta.clone().table_type("PARQUET");

    for (shape, sql, input_confirmed, renders) in [
        ("captured projection", projection, &delta, true),
        ("join", join.as_str(), &delta, false),
        ("WHERE clause", filtered.as_str(), &delta, false),
        ("non-Delta input", projection, &parquet, false),
    ] {
        let producer = producer.clone().sql(sql);
        for (situation, rerun, lines) in [
            (
                "input modified externally",
                producer
                    .clone()
                    .upstream(&source, input_built + 1_000)
                    .own_epoch(Some(built)),
                renders,
            ),
            (
                "Target relation modified",
                producer.clone().own_epoch(Some(built + 1_000)),
                renders,
            ),
            (
                "Target relation dropped",
                producer.clone().own_epoch(None),
                false,
            ),
        ] {
            let case = format!("{shape}, {situation}");
            let org = sc.org(&case);
            org.build_with(input.clone(), input_confirmed.clone()).await;
            org.build(producer.clone(), built).await;
            let ready = org.expect_execute(rerun).await;
            assert_eq!(
                ready
                    .explained_decision
                    .as_ref()
                    .unwrap()
                    .skip_rejection_reason,
                Some(qc::RejectionReason::NoSuitableMatchFound as i32),
                "{case}: refused at Reuse qualification"
            );
            assert_eq!(
                !rendered(&ready).is_empty(),
                renders,
                "{case}: {:?}",
                rendered(&ready)
            );
            let explained = explain(&org, ready.execution_decision_id.as_deref().unwrap()).await;
            assert_eq!(
                prints_volatile_lines(&explained),
                lines,
                "{case}: {explained}"
            );
        }
    }
}

/// The rebuild after an External modification of the producer's input reads
/// that input's State as evidence. Corrupt recorded evidence on it fails the
/// submit, as it fails a direct copy source lookup, instead of establishing a
/// producer revision.
#[tokio::test]
async fn databricks_producer_input_with_corrupt_state_fails_the_submit() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let capture = entries(DATABRICKS_PRODUCER);
    replay(&sc, &capture, PRODUCER_RERUN_EXPLAIN).await;
    let source = target(&capture[INPUT_FIRST].request);
    assert_eq!(
        support::legacy::corrupt_input_tables_of(
            &server,
            support::legacy::Rows::ConfirmedTarget(&source)
        )
        .await,
        1
    );
    let err = sc
        .at(submitted_at(&capture[PRODUCER_AFTER_INPUT_EDIT]))
        .try_submit(captured_submit(&capture[PRODUCER_AFTER_INPUT_EDIT].request))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Internal);
}

/// Neighbouring uncaptured Databricks cells keep an empty map.
#[tokio::test]
async fn databricks_uncaptured_cells_render_nothing() {
    use support::scenario::{outcome, test_results, ExecutionType};
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let entries = entries(DATABRICKS_EVALUATION);
    let table = captured_submit(&entries[DB_TABLE_FIRST].request);
    let test = captured_submit(&entries[DB_TEST_FIRST].request);
    let assert_empty = |case: &str, ready: &qc::ReadyToExecuteResponse| {
        assert!(rendered(ready).is_empty(), "{case}: {:?}", rendered(ready));
    };

    // First builds of uncaptured node kinds or with uncaptured functions.
    for (case, submit) in [
        (
            "data test, evaluation off",
            test.clone().tolerate_nondeterminism(true),
        ),
        ("view", table.clone().execution_type(ExecutionType::View)),
        (
            "append incremental over an upstream table",
            table
                .clone()
                .execution_type(ExecutionType::Append)
                .upstream("catalog.schema.input", OWN_EPOCH),
        ),
        (
            "append incremental with UUID()",
            table
                .clone()
                .execution_type(ExecutionType::Append)
                .sql("select uuid()"),
        ),
        (
            "mixed recognized call forms",
            table.clone().sql("select rand(), uuid()"),
        ),
        (
            "recognized and uncaptured RAND forms",
            table.clone().sql("select rand(), rand(17)"),
        ),
        (
            "uncaptured alias with evaluation off",
            table
                .clone()
                .tolerate_nondeterminism(true)
                .sql("select now()"),
        ),
        (
            "incremental with uncaptured RAND alias",
            table
                .clone()
                .execution_type(ExecutionType::Append)
                .sql("select random()"),
        ),
        (
            "table with an uncaptured RAND seed",
            table.clone().sql("select rand(17)"),
        ),
        (
            "snapshot",
            table.clone().execution_type(ExecutionType::Snapshot),
        ),
    ] {
        assert_empty(case, &sc.org(case).expect_execute(submit).await);
    }

    // A data test's query-changed rebuild.
    let case = "data test, query changed";
    let org = sc.org(case);
    org.build_with(test.clone(), outcome().runtime(10).results(test_results(0)))
        .await;
    let changed = test.clone().sql(
        &entries[DB_TEST_FIRST].request["sql"]
            .as_str()
            .unwrap()
            .replace("100", "200"),
    );
    assert_empty(case, &org.expect_execute(changed).await);

    // A table's rerun refused at Reuse qualification under
    // compare_unrendered_code: Databricks keeps the refusal, and it is not
    // a refusal of volatile SQL compared by its rendered SQL alone.
    let case = "table rerun under compare_unrendered_code";
    let org = sc.org(case);
    let rerun = captured_submit(&entries[DB_TABLE_RERUN].request).compare_unrendered_code();
    let request = rerun.clone().build();
    let epoch = request
        .tables
        .iter()
        .find(|t| Some(&t.name) == request.target_table.as_ref())
        .and_then(|t| t.last_modified_epoch)
        .unwrap();
    org.build(rerun.clone(), epoch).await;
    let ready = org.expect_execute(rerun).await;
    assert_eq!(
        ready
            .explained_decision
            .as_ref()
            .unwrap()
            .skip_rejection_reason,
        Some(qc::RejectionReason::NoSuitableMatchFound as i32),
        "{case}: refused at Reuse qualification"
    );
    assert_empty(case, &ready);
}

/// The Databricks reruns refused for their volatile SQL alone were captured
/// as unchanged reruns for this graph: the table has no upstreams,
/// the data test over unchanged inputs (`DB_TABLE_RERUN`, `DB_TEST_RERUN`).
/// The same refusal after the table was dropped or modified externally, or
/// after the data test's upstream changed, is uncaptured: an empty map and
/// no volatile Explain lines. Each positive control is the captured rerun
/// over the same history.
#[tokio::test]
async fn databricks_refused_reruns_render_only_unchanged() {
    use support::scenario::{outcome, test_results};
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let entries = entries(DATABRICKS_EVALUATION);
    let table = captured_submit(&entries[DB_TABLE_FIRST].request);
    let table_rerun = captured_submit(&entries[DB_TABLE_RERUN].request);
    let test = captured_submit(&entries[DB_TEST_FIRST].request);
    let test_rerun = captured_submit(&entries[DB_TEST_RERUN].request);
    let built = {
        let request = table_rerun.clone().build();
        request
            .tables
            .iter()
            .find(|t| Some(&t.name) == request.target_table.as_ref())
            .and_then(|t| t.last_modified_epoch)
            .unwrap()
    };
    let upstream = entries[DB_TEST_RERUN].request["tables"][0]["name"]
        .as_str()
        .unwrap()
        .to_owned();
    let upstream_epoch = entries[DB_TEST_RERUN].request["tables"][0]["last_modified_epoch"]
        .as_i64()
        .unwrap();
    let complete = || outcome().runtime(10).results(test_results(0));

    for (case, rerun, renders) in [
        ("table, unchanged rerun", table_rerun.clone(), true),
        (
            "table, rerun after the table was dropped",
            table_rerun.clone().own_epoch(None),
            false,
        ),
        (
            "table, rerun after an External modification",
            table_rerun.clone().own_epoch(Some(built + 1_000)),
            false,
        ),
    ] {
        let org = sc.org(case);
        org.build(table.clone(), built).await;
        let ready = org.expect_execute(rerun).await;
        assert_refused_rerun(&org, case, &ready, renders).await;
    }

    for (case, history, rerun, renders) in [
        (
            "data test, unchanged rerun",
            complete(),
            test_rerun.clone(),
            true,
        ),
        (
            "data test, rerun after its upstream changed",
            complete(),
            test_rerun
                .clone()
                .upstream(&upstream, upstream_epoch + 1_000),
            false,
        ),
        (
            "data test, rerun over incomplete cached results",
            outcome().runtime(10),
            test_rerun.clone(),
            false,
        ),
    ] {
        let org = sc.org(case);
        org.build_with(test.clone(), history).await;
        let ready = org.expect_execute(rerun).await;
        assert_refused_rerun(&org, case, &ready, renders).await;
    }
}

/// `ready` is a rerun refused at Reuse qualification that renders `RAND()`
/// and prints volatile Explain lines when `renders`, and neither otherwise.
async fn assert_refused_rerun(
    sc: &Scenario,
    case: &str,
    ready: &qc::ReadyToExecuteResponse,
    renders: bool,
) {
    assert_eq!(
        ready
            .explained_decision
            .as_ref()
            .unwrap()
            .skip_rejection_reason,
        Some(qc::RejectionReason::NoSuitableMatchFound as i32),
        "{case}: refused at Reuse qualification"
    );
    assert_eq!(
        !rendered(ready).is_empty(),
        renders,
        "{case}: {:?}",
        rendered(ready)
    );
    let explained = explain(sc, ready.execution_decision_id.as_deref().unwrap()).await;
    assert_eq!(
        prints_volatile_lines(&explained),
        renders,
        "{case}: {explained}"
    );
}

#[tokio::test]
async fn volatile_sql_of_an_uncaptured_execution_type_renders_nothing() {
    use support::scenario::ExecutionType;
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    // Snowflake: the captured FULL first build renders, and so does an APPEND
    // incremental's, a snapshot's and a custom node's (#205), a view's with
    // evaluation on (#204) and a MERGE incremental's (#211); the same build
    // of another incremental strategy does not.
    let snowflake = captured_submit(&request(SNOWFLAKE_FIRST_BUILDS, SNOWFLAKE_PROD_SUBMIT));
    for execution_type in [
        ExecutionType::Full,
        ExecutionType::Append,
        ExecutionType::Merge,
        ExecutionType::Snapshot,
        ExecutionType::DbtCustom,
        ExecutionType::View,
    ] {
        let case = format!("snowflake control {execution_type:?}");
        let ready = sc
            .org(&case)
            .expect_execute(snowflake.clone().execution_type(execution_type))
            .await;
        assert!(!rendered(&ready).is_empty(), "{case}");
    }
    for execution_type in [
        ExecutionType::InsertOverwrite,
        ExecutionType::DeleteInsert,
        ExecutionType::Microbatch,
    ] {
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
    let entries = entries(RUN_1);
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

// ── Snowflake call forms and functions outside the detector set (#203) ──────
//
// Evidence: the #202 capture batch, `golden/fixtures/explain_volatile_cells.jsonl`
// (dbt 2.0.11), FULL tables without upstreams, evaluation on. First builds
// are entries 20–36, reruns 128–139, and their Explains 80–87 and 177–184.
// `fct` calls a parenless `current_time`, `flt` a parenless `localtime`, `fts`
// `current_timestamp(3)`; `fra` and `frb` call `uniform`, `randstr` and
// `normal` with a `random()` generator or a constant one; `fcu`, `fcx` and
// `fcq` call functions outside the detector set (`current_user()` …).

/// The nodes of the call-form cells, by the tag their SQL selects.
const CALL_FORM_TAGS: &[&str] = &[
    "fct_", "flt_", "fts_", "fra_", "frb_", "fcu_", "fcx_", "fcq_",
];
/// (submit entry, hosted Explain entry) of every call-form decision.
const CALL_FORM_EXPLAINS: &[(usize, usize)] = &[
    (23, 80),
    (28, 81),
    (21, 82),
    (33, 83),
    (36, 84),
    (25, 85),
    (31, 86),
    (20, 87),
    (139, 177),
    (138, 178),
    (135, 179),
    (134, 180),
    (131, 181),
    (129, 182),
    (136, 183),
    (128, 184),
];

/// Replay the call-form nodes' submits and confirms at their recorded times.
/// Returns the local response to each submit, by its entry in the fixture.
async fn replay_call_forms(
    sc: &Scenario,
    entries: &[GoldenEntry],
) -> std::collections::HashMap<usize, qc::SubmitSqlResponse> {
    replay_where(sc, entries, entries.len() - 1, |entry| {
        let sql = entry.request["sql"].as_str().unwrap();
        CALL_FORM_TAGS.iter().any(|tag| sql.contains(tag))
    })
    .await
}

#[tokio::test]
async fn snowflake_captured_call_forms_render_hosted_keys_and_value_formats() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let entries = entries(CELLS);
    let responses = replay_call_forms(&sc, &entries).await;
    // Each submit is decided at its recorded time (the replay clock), so a
    // clock value is that instant in UTC in hosted's format. A random value
    // is a unit decimal, and a constant-seeded value is hosted's captured
    // value (the same on both runs).
    enum Expected {
        Exactly(&'static str),
        UnitRandom,
    }
    use Expected::{Exactly, UnitRandom};
    /// Each function text and its expected value.
    type Values = &'static [(&'static str, Expected)];
    let cases: &[(usize, &str, Values)] = &[
        (
            23,
            "fct first build",
            &[("CURRENT_TIME", Exactly("14:32:11.092000"))],
        ),
        (
            139,
            "fct rerun",
            &[("CURRENT_TIME", Exactly("14:33:51.183000"))],
        ),
        (
            28,
            "flt first build",
            &[("CURRENT_TIME", Exactly("14:32:11.162000"))],
        ),
        (
            138,
            "flt rerun",
            &[("CURRENT_TIME", Exactly("14:33:51.177000"))],
        ),
        (
            21,
            "fts first build",
            &[(
                "CURRENT_TIMESTAMP(3)",
                Exactly("2026-11-19 14:32:11.078000"),
            )],
        ),
        (
            135,
            "fts rerun",
            &[(
                "CURRENT_TIMESTAMP(3)",
                Exactly("2026-11-19 14:33:51.156000"),
            )],
        ),
        (
            33,
            "fra first build",
            &[
                ("UNIFORM(1, 10, RANDOM())", UnitRandom),
                ("RANDOM()", UnitRandom),
                ("RANDSTR(5, 1234)", Exactly("9XCha")),
                ("NORMAL(5, 2, 42)", Exactly("4.711819340844143")),
            ],
        ),
        (
            134,
            "fra rerun",
            &[
                ("UNIFORM(1, 10, RANDOM())", UnitRandom),
                ("RANDOM()", UnitRandom),
                ("RANDSTR(5, 1234)", Exactly("9XCha")),
                ("NORMAL(5, 2, 42)", Exactly("4.711819340844143")),
            ],
        ),
        (
            36,
            "frb first build",
            &[
                ("UNIFORM(1, 100, 1234)", Exactly("96")),
                ("RANDSTR(8, RANDOM())", UnitRandom),
                ("RANDOM()", UnitRandom),
                ("NORMAL(0, 1, RANDOM())", UnitRandom),
            ],
        ),
        (
            131,
            "frb rerun",
            &[
                ("UNIFORM(1, 100, 1234)", Exactly("96")),
                ("RANDSTR(8, RANDOM())", UnitRandom),
                ("RANDOM()", UnitRandom),
                ("NORMAL(0, 1, RANDOM())", UnitRandom),
            ],
        ),
    ];
    for (entry, case, expected) in cases {
        let response = &responses[entry];
        assert_matches_hosted(response, &entries[*entry], case);
        let local = volatile_values(response);
        let [(_, functions)] = local.iter().collect::<Vec<_>>()[..] else {
            panic!("{case}: one query, {local:?}");
        };
        assert_eq!(functions.len(), expected.len(), "{case}: {functions:?}");
        for (function, value) in expected.iter() {
            let rendered = &functions[*function];
            match value {
                Exactly(value) => assert_eq!(rendered, value, "{case}: {function}"),
                UnitRandom => assert!(
                    is_captured_unit_random(rendered),
                    "{case}: {function}: {rendered:?}"
                ),
            }
        }
    }
}

#[tokio::test]
async fn snowflake_functions_outside_the_detector_set_render_nothing_and_are_reused_as_captured() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let entries = entries(CELLS);
    let responses = replay_call_forms(&sc, &entries).await;
    for (entry, case) in [
        (25, "fcu current_user(), first build"),
        (31, "fcx current_role() …, first build"),
        (20, "fcq current_session() …, first build"),
        (129, "fcu rerun: reused"),
        (136, "fcx rerun: reused"),
        (128, "fcq rerun: reused"),
    ] {
        assert_matches_hosted(&responses[&entry], &entries[entry], case);
        assert!(volatile_values(&responses[&entry]).is_empty(), "{case}");
    }
}

#[tokio::test]
async fn snowflake_call_form_explains_match_hosted_after_a_restart() {
    let database = std::sync::Arc::new(support::TestDatabase::new("volatile_call_forms").await);
    let server = support::start_server_on_database(database.clone()).await;
    let entries = entries(CELLS);
    let responses = replay_call_forms(&Scenario::connect(&server).await, &entries).await;
    let explains: Vec<_> = CALL_FORM_EXPLAINS
        .iter()
        .map(|&(submit, explain)| {
            HostedExplain::of_response(
                format!("submit {submit}"),
                &entries[explain],
                &entries[submit],
                &responses[&submit],
            )
        })
        .collect();
    assert_explains_match_hosted(&database, server, &explains, std::convert::identity).await;
}

// ── Snowflake execution types (#205) ────────────────────────────────────────
//
// Evidence: the #202 capture, `golden/fixtures/explain_volatile_cells.jsonl`
// (dbt 2.0.11). Each node calls `current_timestamp()` in its own SQL with
// evaluation requested: `xi` an APPEND incremental over the table `vb`, `xs`
// a check snapshot of `vb`, `xc` a custom materialization without upstreams,
// and `xt` a `store_failures` data test (sent without a Target relation).
// Round 1 is each node's first build, round 2 its unchanged rerun.

/// (first build, rerun, rerun Explain) submit entries of `xi`, `xs`, `xc`.
const XI: (usize, usize, usize) = (71, 167, 189);
const XS: (usize, usize, usize) = (69, 165, 190);
const XC: (usize, usize, usize) = (24, 143, 191);
/// `xt`: first execution and rerun, with their Explains.
const XT: (usize, usize) = (61, 145);
const XT_EXPLAINS: (usize, usize) = (107, 203);

#[tokio::test]
async fn snowflake_incremental_snapshot_and_custom_render_their_own_values_on_both_runs() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let cells = entries(CELLS);
    let responses = replay(&sc, &cells, XI.1).await;
    for (entry, case) in [
        (XI.0, "xi APPEND incremental, first build"),
        (XI.1, "xi APPEND incremental, incremental run"),
        (XS.0, "xs snapshot, first build"),
        (XS.1, "xs snapshot, rerun"),
        (XC.0, "xc custom materialization, first build"),
        (XC.1, "xc custom materialization, rerun"),
        (XT.0, "xt store_failures data test, first execution"),
        (XT.1, "xt store_failures data test, rerun"),
    ] {
        assert_matches_hosted(&responses[&entry], &cells[entry], case);
    }
}

#[tokio::test]
async fn snowflake_custom_rerun_explains_as_hosted_after_a_restart() {
    assert_replayed_explains_match_hosted(
        "volatile_custom_restart",
        CELLS,
        &[(XC.1, XC.2, "xc rerun")],
        std::convert::identity,
    )
    .await;
}

#[tokio::test]
async fn snowflake_incremental_run_explains_as_hosted_after_a_restart() {
    assert_replayed_explains_match_hosted(
        "volatile_incremental_restart",
        CELLS,
        &[(XI.1, XI.2, "xi incremental run")],
        std::convert::identity,
    )
    .await;
}

#[tokio::test]
async fn snowflake_snapshot_rerun_explains_as_hosted_in_snapshot_wording_after_a_restart() {
    assert_replayed_explains_match_hosted(
        "volatile_snapshot_restart",
        CELLS,
        &[(XS.1, XS.2, "xs rerun")],
        std::convert::identity,
    )
    .await;
}

/// The `store_failures` data test, sent without a Target relation, keeps the
/// #191 cell 6 trees on both runs; its rerun is explained without an anchor.
#[tokio::test]
async fn snowflake_store_failures_data_test_explains_as_hosted_on_both_runs_after_a_restart() {
    assert_replayed_explains_match_hosted(
        "volatile_store_failures_restart",
        CELLS,
        &[
            (XT.0, XT_EXPLAINS.0, "xt first execution"),
            (XT.1, XT_EXPLAINS.1, "xt rerun"),
        ],
        masked_relative_times,
    )
    .await;
}

/// Uncaptured cells of these execution types keep an empty map and print no
/// volatile lines: evaluation off, and a query-changed rebuild. A rerun
/// whose upstream drifted is still refused at Reuse qualification and
/// rendered, but its tree is uncaptured and renders nothing.
#[tokio::test]
async fn snowflake_uncaptured_incremental_snapshot_and_custom_cells_render_nothing() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let cells = entries(CELLS);
    let submit = |entry: usize| captured_submit(&cells[entry].request);
    // The epoch each rerun reports for its own Target relation: the build.
    let own_epoch = |entry: usize| {
        let req = submit(entry).build();
        req.tables
            .iter()
            .find(|t| Some(&t.name) == req.target_table.as_ref())
            .and_then(|t| t.last_modified_epoch)
            .unwrap()
    };
    for (node, (first, rerun, _)) in [("xi", XI), ("xs", XS), ("xc", XC)] {
        let off = sc.org(&format!("{node} evaluation off"));
        let built = off
            .expect_execute(submit(first).tolerate_nondeterminism(true))
            .await;
        assert!(
            rendered(&built).is_empty(),
            "{node} evaluation off: first build"
        );
        assert!(
            off.confirm(&built.request_id, built_at(own_epoch(rerun)))
                .await
        );
        let reused = off
            .submit(submit(rerun).tolerate_nondeterminism(true))
            .await;
        assert_eq!(variant(&reused), "skip_execution", "{node} evaluation off");
        assert!(
            volatile_values(&reused).is_empty(),
            "{node} evaluation off: rerun"
        );

        let changed = sc.org(&format!("{node} query changed"));
        changed.build(submit(first), own_epoch(rerun)).await;
        let rebuilt = changed
            .expect_execute(submit(rerun).with(|r| r.sql = r.sql.replace(" as ts", " as ts2")))
            .await;
        assert!(rendered(&rebuilt).is_empty(), "{node} query changed");
        let explained = explain(&changed, rebuilt.execution_decision_id.as_deref().unwrap()).await;
        assert!(!prints_volatile_lines(&explained), "{node}: {explained}");
    }

    for (node, (first, rerun, _)) in [("xi", XI), ("xs", XS)] {
        let upstream = |drift: i64| {
            submit(rerun).with(move |r| {
                let vb = r
                    .tables
                    .iter_mut()
                    .find(|t| t.name.contains("VB_"))
                    .unwrap();
                vb.last_modified_epoch = vb.last_modified_epoch.map(|e| e + drift);
            })
        };
        for (case, drift, tree) in [
            ("fresh upstream", 0, true),
            ("drifted upstream", 600_000, false),
        ] {
            let sc = sc.org(&format!("{node} {case}"));
            sc.build(submit(first), own_epoch(rerun)).await;
            let rebuilt = sc.expect_execute(upstream(drift)).await;
            assert!(
                !rendered(&rebuilt).is_empty(),
                "{node} {case}: refused and rendered"
            );
            let explained = explain(&sc, rebuilt.execution_decision_id.as_deref().unwrap()).await;
            assert_eq!(
                prints_volatile_lines(&explained),
                tree,
                "{node} {case}: {explained}"
            );
            assert_eq!(
                texts(&explained).is_empty(),
                !tree,
                "{node} {case}: {explained}"
            );
        }
    }
}

// ── Snowflake rebuild reasons and Explain variants (#206) ───────────────────
//
// Evidence: the #202 capture, `golden/fixtures/explain_volatile_cells.jsonl`
// (dbt 2.0.11; `docs/reports/snowflake-volatile-cells-2026-11-19.md`). Round 2
// reruns each node; every case below is a `ready_to_execute` 6/6 whose map
// replays, and whose Explain is compared with hosted's.
mod rebuild_reasons {
    use super::*;

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
    /// and after a restart. `last updated` lines are masked for the data
    /// test `rt`, whose upstream listing prints raw epochs
    /// ([`masked_relative_times`]); the models' lines match hosted's.
    async fn assert_rebuilds_match_hosted(database: &str, cases: &[((usize, usize), &str)]) {
        let database = std::sync::Arc::new(support::TestDatabase::new(database).await);
        let server = support::start_server_on_database(database.clone()).await;
        let entries = entries(CELLS);
        let responses = replay(&Scenario::connect(&server).await, &entries, 168).await;
        let explains: Vec<_> = cases
            .iter()
            .map(|&((submit, explained), case)| {
                assert_matches_hosted(&responses[&submit], &entries[submit], case);
                HostedExplain::of_response(
                    case,
                    &entries[explained],
                    &entries[submit],
                    &responses[&submit],
                )
            })
            .collect();
        assert_explains_match_hosted(&database, server, &explains, masked_relative_times).await;
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

    /// Snowflake build cells render only for the captured decision reasons:
    /// a first build, a rerun refused at Reuse qualification and a
    /// query-changed rebuild (evaluation on), and a first build and a
    /// query-changed rebuild (evaluation off). A rebuild for any other reason
    /// keeps an empty map.
    #[tokio::test]
    async fn snowflake_rebuilds_for_uncaptured_reasons_keep_an_empty_map() {
        use support::scenario::ExecutionType;
        let server = support::start_server().await;
        let sc = Scenario::connect(&server).await;
        let model = captured_submit(&request(SNOWFLAKE_FIRST_BUILDS, SNOWFLAKE_PROD_SUBMIT))
            .own_epoch(Some(SNOWFLAKE_BUILT_AT));
        let upstream = model.clone().build().tables[0].name.clone();
        let upstream_epoch = model.clone().build().tables[0].last_modified_epoch.unwrap();
        let off = model.clone().tolerate_nondeterminism(true);
        let cases = [
            (
                "evaluation off, upstream data changed",
                off.clone(),
                off.clone().upstream(&upstream, upstream_epoch + 60_000),
            ),
            (
                "evaluation off, External modification",
                off.clone(),
                off.clone().own_epoch(Some(SNOWFLAKE_BUILT_AT + 60_000)),
            ),
            (
                "evaluation on, built as a view before",
                model.clone().execution_type(ExecutionType::View),
                model.clone(),
            ),
        ];
        for (case, history, rebuild) in cases {
            let sc = sc.org(case);
            sc.build(history, SNOWFLAKE_BUILT_AT).await;
            let ready = sc.expect_execute(rebuild).await;
            assert!(
                rendered(&ready).is_empty(),
                "{case}: {:?}",
                rendered(&ready)
            );
        }

        // Controls over the same history: the captured reasons render.
        let controls = [
            (
                "evaluation off, query changed",
                off.clone(),
                off.clone()
                    .sql(&snowflake_sql("id, current_timestamp() as loaded_at")),
            ),
            (
                "evaluation on, unchanged rerun",
                model.clone(),
                model.clone(),
            ),
            (
                "evaluation on, External modification",
                model.clone(),
                model.clone().own_epoch(Some(SNOWFLAKE_BUILT_AT + 60_000)),
            ),
            (
                "evaluation on, query changed",
                model.clone(),
                model
                    .clone()
                    .sql(&snowflake_sql("id, current_timestamp() as loaded_at")),
            ),
        ];
        for (case, history, rebuild) in controls {
            let sc = sc.org(&format!("control {case}"));
            sc.build(history, SNOWFLAKE_BUILT_AT).await;
            let ready = sc.expect_execute(rebuild).await;
            assert!(!rendered(&ready).is_empty(), "{case}");
        }
    }

    // ── compare_unrendered_code (#206; maintainer decision 1 on #201) ───────
    //
    // `ecu`: a table calling `current_timestamp()` with evaluation on and
    // `compare_unrendered_code: true`, unchanged rerun. Hosted reuses it
    // (entry 140, its runtime 12089 from the round 1 confirm, entry 54) and
    // explains it by its unrendered code with no volatile lines (entry 198).
    // Only that mode with an unchanged template lifts the refusal of volatile
    // SQL at Reuse qualification; everywhere else it stays.

    /// `ecu`: round 1 build (entry 35) and confirm (54); round 2 rerun (140)
    /// and hosted's Explain of it (198).
    const ECU_BUILD: usize = 35;
    const ECU_BUILT_AT: i64 = 1795098737983;
    const ECU: (usize, usize) = (140, 198);
    /// The `rt` data test's round 1 build (evaluation on).
    const RT_BUILD: usize = 62;

    /// The skip rejection reason hosted reports for a rebuild refused at
    /// Reuse qualification.
    const NO_SUITABLE_MATCH_FOUND: Option<i32> =
        Some(qc::RejectionReason::NoSuitableMatchFound as i32);

    #[tokio::test]
    async fn snowflake_volatile_rerun_compared_by_unrendered_code_skips_and_explains_as_hosted() {
        assert_rebuilds_match_hosted(
            "volatile_206_ecu",
            &[(
                ECU,
                "ecu: unchanged rerun under compare_unrendered_code, evaluation on",
            )],
        )
        .await;
    }

    /// The reuse is only lifted for an unchanged template under
    /// `compare_unrendered_code` on Snowflake models: each case rebuilds,
    /// refused at Reuse qualification or for another identity, while its
    /// control over the same history reuses.
    #[tokio::test]
    async fn snowflake_volatile_sql_is_still_refused_outside_an_unchanged_unrendered_comparison() {
        use support::scenario::{outcome, test_results};
        let server = support::start_server().await;
        let sc = Scenario::connect(&server).await;
        let entries = entries(CELLS);
        let built = captured_submit(&entries[ECU_BUILD].request);
        let rerun = captured_submit(&entries[ECU.0].request);
        let off = |s: Submit| s.with(|r| r.compare_unrendered_code = false);
        let databricks = |s: Submit| s.dialect("databricks");
        let cases = [
            (
                "compare_unrendered_code off",
                off(built.clone()),
                off(rerun.clone()),
                NO_SUITABLE_MATCH_FOUND,
            ),
            (
                "template changed, same SQL",
                built.clone(),
                rerun
                    .clone()
                    .node_state(|n| n.node_body_hash = Some("another body".into())),
                NO_SUITABLE_MATCH_FOUND,
            ),
            (
                "template and SQL changed",
                built.clone(),
                rerun
                    .clone()
                    .node_state(|n| n.node_body_hash = Some("another body".into()))
                    .sql("\nselect 2 as id, current_timestamp() as ts, 'ecu_1431544d67b8' as v"),
                None,
            ),
            (
                "Databricks, which has no checked detector",
                databricks(built.clone()),
                databricks(rerun.clone()),
                NO_SUITABLE_MATCH_FOUND,
            ),
        ];
        for (case, history, rebuild, reason) in cases {
            let sc = sc.org(case);
            sc.build(history.clone(), ECU_BUILT_AT).await;
            let ready = sc.expect_execute(rebuild.clone()).await;
            if reason.is_some() {
                assert_eq!(
                    ready.explained_decision.unwrap().skip_rejection_reason,
                    reason,
                    "{case}"
                );
            }
            // Control: the same history and rerun reuse with evaluation off.
            let sc = sc.org(&format!("control {case}"));
            sc.build(history.tolerate_nondeterminism(true), ECU_BUILT_AT)
                .await;
            let control = rebuild.tolerate_nondeterminism(true);
            if reason.is_some() {
                sc.expect_skip(control).await;
            }
        }

        // A volatile data test under compare_unrendered_code: uncaptured,
        // still refused (its control reuses with evaluation off).
        let test = captured_submit(&entries[RT_BUILD].request).compare_unrendered_code();
        for (case, test) in [
            ("data test, evaluation on", test.clone()),
            ("control data test", test.tolerate_nondeterminism(true)),
        ] {
            let sc = sc.org(case);
            sc.build_with(test.clone(), outcome().runtime(10).results(test_results(0)))
                .await;
            let reused = variant(&sc.submit(test).await) == "skip_execution";
            assert_eq!(reused, case.starts_with("control"), "{case}");
        }
    }

    /// Submit `rerun` over `history` built in a fresh organization `case`;
    /// require a rebuild refused at Reuse qualification, and return it with
    /// its Explain.
    async fn refused_rerun(
        sc: &Scenario,
        case: &str,
        history: Submit,
        rerun: Submit,
    ) -> (qc::ReadyToExecuteResponse, serde_json::Value) {
        use support::scenario::{outcome, test_results};
        let sc = sc.org(case);
        if history.clone().build().execution_type == qc::ModelExecutionType::DbtDataTest as i32 {
            sc.build_with(history, outcome().runtime(10).results(test_results(0)))
                .await;
        } else {
            sc.build(history, ECU_BUILT_AT).await;
        }
        let ready = sc.expect_execute(rerun).await;
        assert_eq!(
            ready
                .explained_decision
                .as_ref()
                .unwrap()
                .skip_rejection_reason,
            NO_SUITABLE_MATCH_FOUND,
            "{case}"
        );
        let explained = explain(&sc, ready.execution_decision_id.as_deref().unwrap()).await;
        (ready, explained)
    }

    /// Under `compare_unrendered_code`, a rerun whose template changed but
    /// whose rendered SQL did not is still refused at Reuse qualification
    /// for its volatile SQL. Hosted was not captured in that cell: the map
    /// stays empty, and Explain prints no volatile lines and, as for any
    /// qualification refusal of a table that rendered nothing, no tree. The
    /// same rerun compared by its rendered SQL is the captured cell 2
    /// refusal, and renders.
    #[tokio::test]
    async fn snowflake_rerun_refused_under_compare_unrendered_code_keeps_an_empty_map() {
        let server = support::start_server().await;
        let sc = Scenario::connect(&server).await;
        let entries = entries(CELLS);
        let built = captured_submit(&entries[ECU_BUILD].request);
        let changed_template =
            |s: Submit| s.node_state(|n| n.node_body_hash = Some("another body".into()));
        let rerun = changed_template(captured_submit(&entries[ECU.0].request));

        let (ready, explained) =
            refused_rerun(&sc, "template changed", built.clone(), rerun.clone()).await;
        assert!(
            rendered(&ready).is_empty(),
            "uncaptured: {:?}",
            rendered(&ready)
        );
        assert!(!prints_volatile_lines(&explained), "{explained}");
        assert_eq!(
            texts(&explained),
            Vec::<String>::new(),
            "no tree: {explained}"
        );

        // Control: compared by its rendered SQL, the captured refusal renders.
        let off = |s: Submit| s.with(|r| r.compare_unrendered_code = false);
        let (ready, explained) = refused_rerun(&sc, "control", off(built), off(rerun)).await;
        assert!(!rendered(&ready).is_empty());
        assert!(prints_volatile_lines(&explained), "{explained}");
    }

    /// The captured qualification refusal is of volatile SQL alone. A
    /// volatile table or data test refused for an uncaptured cause as well
    /// (a query dependency without a definition; under
    /// `compare_unrendered_code`, a missing template body) keeps an empty map
    /// and prints no volatile lines.
    #[tokio::test]
    async fn snowflake_reruns_refused_for_an_uncaptured_qualification_cause_keep_an_empty_map() {
        let server = support::start_server().await;
        let sc = Scenario::connect(&server).await;
        let entries = entries(CELLS);
        let off = |s: Submit| s.with(|r| r.compare_unrendered_code = false);
        let table = off(captured_submit(&entries[ECU_BUILD].request));
        let table_rerun = off(captured_submit(&entries[ECU.0].request));
        let test = captured_submit(&entries[RT_BUILD].request);
        let undefined =
            |s: Submit| s.dependency("\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"NODEF\"", "");
        let no_body = |s: Submit| s.node_state(|n| n.node_body_hash = None);
        let cases = [
            (
                "table, a query dependency without a definition",
                undefined(table.clone()),
                undefined(table_rerun.clone()),
            ),
            (
                "data test, a query dependency without a definition",
                undefined(test.clone()),
                undefined(test.clone()),
            ),
            (
                "table under compare_unrendered_code, no template body",
                no_body(captured_submit(&entries[ECU_BUILD].request)),
                no_body(captured_submit(&entries[ECU.0].request)),
            ),
        ];
        for (case, history, rerun) in cases {
            let (ready, explained) = refused_rerun(&sc, case, history, rerun).await;
            assert!(
                rendered(&ready).is_empty(),
                "{case}: {:?}",
                rendered(&ready)
            );
            assert!(!prints_volatile_lines(&explained), "{case}: {explained}");
        }

        // Controls: the same nodes refused for their volatile SQL alone render.
        for (case, history, rerun) in [
            ("control table", table.clone(), table_rerun),
            ("control data test", test.clone(), test),
        ] {
            let (ready, _) = refused_rerun(&sc, case, history, rerun).await;
            assert!(!rendered(&ready).is_empty(), "{case}");
        }
    }

    /// The lifted refusal changes no identity or fingerprint: the build the
    /// rerun reuses is persisted as with evaluation off.
    #[tokio::test]
    async fn snowflake_unrendered_comparison_reuse_enters_no_identity_or_fingerprint() {
        let server = support::start_server().await;
        let sc = Scenario::connect(&server).await;
        let entries = entries(CELLS);
        let built = captured_submit(&entries[ECU_BUILD].request);
        let on = sc.org("evaluation on").execute_id(built.clone()).await;
        let off = sc
            .org("evaluation off")
            .execute_id(built.tolerate_nondeterminism(true))
            .await;
        let mut on = persisted_execution(&server, &on).await;
        let mut off = persisted_execution(&server, &off).await;
        for row in [&mut on, &mut off] {
            row.as_object_mut().unwrap().remove("org_id");
        }
        assert_eq!(on, off);
    }
}

// ── Snowflake combination cells (#204) ──────────────────────────────────────
//
// Evidence: the #202 capture batch, `golden/fixtures/explain_volatile_cells.jsonl`
// (dbt 2.0.11; entry indices in docs/reports/snowflake-volatile-cells-2026-11-19.md).
// `cmb` is a table calling `uuid_string()` over the view `vw` that calls
// `current_timestamp()`, with evaluation on; `ctv` a data test over `vw`
// with evaluation on; `cto` a data test calling `current_timestamp()` with
// evaluation off; `cdo` a table over `vw` with evaluation off; `cvo` a view
// calling `current_timestamp()` with evaluation on.

/// The last combination submit of the run (`cmb`'s rerun).
const COMBINATIONS_THROUGH: usize = 159;
/// `cto`'s rerun SKIP. It echoes the runtime of `rt` (entry 65), the
/// later-confirmed run of the same SQL under another unique id: a Snowflake
/// data test reuses the latest run of its history key (#200).
const CTO_SKIP: usize = 146;
const RT_CONFIRM: usize = 65;

#[tokio::test]
async fn snowflake_combination_cells_return_hosted_codes_and_maps() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let entries = entries(CELLS);
    let responses = replay(&sc, &entries, COMBINATIONS_THROUGH).await;
    for (entry, case) in [
        (59, "cmb first build: the dependency's and its own values"),
        (159, "cmb rerun: the dependency's and its own values"),
        (52, "ctv first execution: the dependency's values"),
        (144, "ctv rerun: the dependency's values"),
        (60, "cto first execution, evaluation off: its own values"),
        (
            58,
            "cdo first build, evaluation off: the dependency's values",
        ),
        (158, "cdo reuse, evaluation off: the dependency's values"),
        (19, "cvo view first build, evaluation on: its own values"),
        (141, "cvo view rerun, evaluation on: its own values"),
    ] {
        assert_matches_hosted(&responses[&entry], &entries[entry], case);
    }

    let skip = support::scenario::into_skip(responses[&CTO_SKIP].clone());
    let hosted = &entries[CTO_SKIP].response["response"]["skip_execution"];
    assert_eq!(
        codes(&serde_json::to_value(&skip).unwrap()),
        codes(hosted),
        "cto reuse, evaluation off"
    );
    assert_eq!(
        volatile_map_shape(&volatile_values(&responses[&CTO_SKIP])),
        volatile_map_shape(&hosted_volatile_values(&entries[CTO_SKIP].response)),
        "cto reuse, evaluation off: its own values"
    );
    assert_eq!(
        skip.execution_runtime_ms,
        entries[RT_CONFIRM].request["execution_runtime_ms"].as_i64(),
        "the later-confirmed run of the same SQL, rt's"
    );
}

#[tokio::test]
async fn snowflake_combination_cells_explain_as_hosted_including_after_a_restart() {
    // (submit, its captured Explain, case)
    assert_replayed_explains_match_hosted(
        "volatile_combinations",
        CELLS,
        &[
            (59, 89, "cmb first build: no volatile lines"),
            (159, 186, "cmb rerun: only its own value"),
            (
                52,
                105,
                "ctv first execution: a bare data freshness analysis",
            ),
            (144, 201, "ctv rerun: a bare data freshness analysis"),
            (
                60,
                106,
                "cto first execution, evaluation off: its own value",
            ),
            (
                CTO_SKIP,
                202,
                "cto reuse, evaluation off: tolerated by config",
            ),
            (58, 90, "cdo first build: no volatile lines"),
            (
                158,
                187,
                "cdo reuse, evaluation off: the dependency's value",
            ),
            (19, 91, "cvo view first build: no volatile lines"),
            (141, 188, "cvo view rerun: its own value"),
        ],
        masked_relative_times,
    )
    .await;
}

#[tokio::test]
async fn snowflake_own_and_dependency_values_render_with_evaluation_on_and_enter_no_fingerprint() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let entries = entries(RUN_1);
    let vd = target(&entries[VD_FIRST_BUILD].request);
    let vv = entries[VD_FIRST_BUILD].request["query_dependencies"][0]["name"]
        .as_str()
        .unwrap()
        .to_owned();
    let vd_built_at = entries[VD_RERUN].request["tables"][0]["last_modified_epoch"]
        .as_i64()
        .unwrap();
    // `vd` (a table over the volatile view `vv`) also calling
    // `current_timestamp()` itself: `cmb`'s cell over the #191 nodes.
    let both = |entry: usize| captured_submit(&vd_with_own_volatile_sql(&entries, entry));
    let both_keys = vec![
        (vd.clone(), vec!["CURRENT_TIMESTAMP()".to_owned()]),
        (vv.clone(), vec!["CURRENT_TIMESTAMP()".to_owned()]),
    ];

    let pending = sc.org("pending both");
    let first = pending.expect_execute(both(VD_FIRST_BUILD)).await;
    let second = pending.expect_execute(both(VD_FIRST_BUILD)).await;
    assert_eq!(volatile_map_shape(&rendered(&first)), both_keys);
    assert_eq!(
        persisted_execution(&server, &first.request_id).await,
        persisted_execution(&server, &second.request_id).await,
        "rendered values enter no persisted identity or fingerprint"
    );

    let rerun_sc = sc.org("both");
    rerun_sc.build(both(VD_FIRST_BUILD), vd_built_at).await;
    let rerun = rerun_sc.expect_execute(both(VD_RERUN)).await;
    let values = rendered(&rerun);
    assert_eq!(volatile_map_shape(&values), both_keys);
    let explained = explain(&rerun_sc, rerun.execution_decision_id.as_deref().unwrap()).await;
    let printed: Vec<_> = texts(&explained)
        .into_iter()
        .filter(|t| t.contains(" -> rendered with "))
        .collect();
    assert_eq!(
        printed,
        [format!(
            "CURRENT_TIMESTAMP() -> rendered with '{}'",
            values[&vd]["CURRENT_TIMESTAMP()"]
        )],
        "only its own value prints: {explained}"
    );
    for text in [
        "upstream model queries have not changed",
        "upstream data is up to date",
    ] {
        assert!(line(&explained, text).is_some(), "{text}: {explained}");
    }

    // Evaluation off is uncaptured for values in both: nothing renders, on
    // the build or the reuse.
    let off = sc.org("both off");
    let ready = off
        .expect_execute(both(VD_FIRST_BUILD).tolerate_nondeterminism(true))
        .await;
    assert!(rendered(&ready).is_empty(), "{:?}", rendered(&ready));
    assert!(off.confirm(&ready.request_id, built_at(vd_built_at)).await);
    let reused = off
        .expect_skip(both(VD_RERUN).tolerate_nondeterminism(true))
        .await;
    assert!(
        reused.transformed_nodes_by_query.is_empty(),
        "{:?}",
        reused.transformed_nodes_by_query
    );
}

#[tokio::test]
async fn snowflake_uncaptured_data_tests_over_a_volatile_view_render_nothing() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let entries = entries(CELLS);
    // `ctv`, the data test over the volatile view (entry 52).
    let ctv = || captured_submit(&entries[52].request);
    for (case, test) in [
        ("evaluation off", ctv().tolerate_nondeterminism(true)),
        (
            "with a Target relation",
            ctv()
                .target("\"CDO_DEMO_DB\".\"PROD_STATE_DEMO_DBT_TEST__AUDIT\".\"CTV_1431544D67B8\""),
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
    // Positive control: the captured cell renders the dependency's key.
    let ready = sc.org("control").expect_execute(ctv()).await;
    assert_eq!(
        volatile_map_shape(&rendered(&ready)),
        volatile_map_shape(&hosted_volatile_values(&entries[52].response))
    );
}

// ── Snowflake copy and zero Lag tolerance (#207) ────────────────────────────
//
// The #202 capture, `golden/fixtures/explain_volatile_cells.jsonl`. `pc` calls
// `current_timestamp()` with evaluation off. It is built in prod (entries 29,
// 47), then submitted in dev, where hosted copies the prod build (207, 208)
// and renders the value under the dev Target relation at the dev decision
// time; entry 209 explains the copy. `lz` is a table with zero Lag tolerance,
// evaluation off, over `eu`, which calls `current_timestamp()` with evaluation
// on and so is rebuilt on every run: `lz` is built (77, 78) and rebuilt on its
// rerun (172, 174), explained by entries 104 and 200.

const PC_PROD_BUILD: usize = 29;
const PC_DEV_COPY: usize = 207;
const PC_DEV_CONFIRM: usize = 208;
const PC_COPY_EXPLAIN: usize = 209;
const EU_BUILD: usize = 70;
const EU_REBUILD: usize = 166;
const LZ_BUILD: usize = 77;
const LZ_REBUILD: usize = 172;
const LZ_BUILD_EXPLAIN: usize = 104;
const LZ_REBUILD_EXPLAIN: usize = 200;

/// `pc`'s captured prod build, then its dev submit at the captured time,
/// which must be a Result copy.
async fn pc_copy(sc: &Scenario, entries: &[GoldenEntry]) -> qc::ReadyToCloneResponse {
    build_captured(sc, entries, PC_PROD_BUILD).await;
    let copy = &entries[PC_DEV_COPY];
    sc.at(submitted_at(copy))
        .expect_clone(captured_submit(&copy.request))
        .await
}

#[tokio::test]
async fn snowflake_evaluation_off_copy_renders_under_the_dev_target_at_the_dev_decision_time() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await.org("pc");
    let entries = entries(CELLS);
    let copy = pc_copy(&sc, &entries).await;

    let hosted = &entries[PC_DEV_COPY].response["response"]["ready_to_clone"];
    assert_eq!(
        serde_json::to_value(&copy.explained_decision).unwrap(),
        hosted["explained_decision"]
    );
    assert_eq!(
        serde_json::to_value(&copy.clone_sqls).unwrap(),
        hosted["clone_sqls"]
    );
    assert_eq!(
        copy.execution_runtime_ms,
        hosted["execution_runtime_ms"].as_i64()
    );
    let values = volatile_values_of(&copy.transformed_nodes_by_query);
    assert_eq!(
        volatile_map_shape(&values),
        volatile_map_shape(&hosted_volatile_values(&entries[PC_DEV_COPY].response)),
        "hosted's key shape: the dev Target relation"
    );
    // The replay clock at the dev submit (1795098919604 ms), not the prod
    // build's time.
    assert_eq!(
        values[&target(&entries[PC_DEV_COPY].request)]["CURRENT_TIMESTAMP()"],
        "2026-11-19 14:35:19.604000"
    );
}

#[tokio::test]
async fn snowflake_evaluation_off_copy_explain_matches_hosted_after_a_restart() {
    let database = std::sync::Arc::new(support::TestDatabase::new("volatile_copy_restart").await);
    let server = support::start_server_on_database(database.clone()).await;
    let sc = Scenario::connect(&server).await;
    let entries = entries(CELLS);
    let copy = pc_copy(&sc, &entries).await;
    let mut confirm: qc::ConfirmExecutionRequest =
        serde_json::from_value(entries[PC_DEV_CONFIRM].request.clone()).unwrap();
    confirm.request_id = copy.request_id.clone();
    assert!(
        sc.at(submitted_at(&entries[PC_DEV_CONFIRM]))
            .try_confirm(confirm)
            .await
            .unwrap()
            .success
    );
    let id = copy.execution_decision_id.clone().unwrap();
    let explains = [HostedExplain::of(
        "pc copy",
        &entries[PC_COPY_EXPLAIN],
        &entries[PC_DEV_COPY],
        &id,
        &volatile_values_of(&copy.transformed_nodes_by_query),
    )];
    assert_explains_match_hosted(&database, server, &explains, std::convert::identity).await;
}

/// `explain` with each `updated … after … last executed` line masked, as the
/// conformance replay masks it (`«gap»`). Hosted words `lz`'s 94.992 s gap
/// (entry 200: `eu` at 1795098837609, recorded at 1795098742617) as
/// `2 minutes`; this server rounds down to `a minute` outside Databricks
/// (#210).
fn masked_gaps(explain: serde_json::Value) -> serde_json::Value {
    mask_lines(
        explain,
        |text| text.starts_with("updated ") && text.ends_with(" last executed"),
        "«gap»",
    )
}

#[tokio::test]
async fn snowflake_zero_lag_tolerance_table_over_a_volatile_model_is_rebuilt_on_its_rerun() {
    let database = std::sync::Arc::new(support::TestDatabase::new("volatile_zero_lag").await);
    let server = support::start_server_on_database(database.clone()).await;
    let sc = Scenario::connect(&server).await;
    let entries = entries(CELLS);

    build_captured(&sc, &entries, EU_BUILD).await;
    let first = build_captured(&sc, &entries, LZ_BUILD).await;
    build_captured(&sc, &entries, EU_REBUILD).await;
    let rerun = build_captured(&sc, &entries, LZ_REBUILD).await;

    let mut explains = Vec::new();
    for (ready, submit, explained) in [
        (&first, LZ_BUILD, LZ_BUILD_EXPLAIN),
        (&rerun, LZ_REBUILD, LZ_REBUILD_EXPLAIN),
    ] {
        assert_eq!(
            serde_json::to_value(&ready.explained_decision).unwrap(),
            hosted_decision(&entries, submit, "ready_to_execute"),
            "entry {submit}"
        );
        assert!(
            ready.transformed_nodes_by_query.is_empty(),
            "entry {submit}"
        );
        explains.push(HostedExplain::of(
            format!("entry {explained}"),
            &entries[explained],
            &entries[submit],
            ready.execution_decision_id.as_deref().unwrap(),
            &rendered(ready),
        ));
    }
    drop(sc);
    assert_explains_match_hosted(&database, server, &explains, masked_gaps).await;
}

#[tokio::test]
async fn snowflake_uncaptured_copy_cells_render_nothing() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let evaluation = entries(RUN_1);
    let entries = entries(CELLS);
    let dev = captured_submit(&entries[PC_DEV_COPY].request);

    // `pc`'s copy over a dev Target relation that already exists.
    let over = sc.org("copy over");
    build_captured(&over, &entries, PC_PROD_BUILD).await;
    let response = over
        .at(submitted_at(&entries[PC_DEV_COPY]))
        .submit(dev.clone().own_epoch(Some(OWN_EPOCH)))
        .await;
    assert_eq!(variant(&response), "ready_to_clone");
    assert!(volatile_values(&response).is_empty(), "{response:?}");

    // A copy of `vd` (#191), whose values are in its query dependency `vv`,
    // into the other schema of its namespace, with evaluation off.
    let vd = captured_submit(&evaluation[VD_FIRST_BUILD].request).tolerate_nondeterminism(true);
    let vd_dev = serde_json::to_string(&evaluation[VD_FIRST_BUILD].request)
        .unwrap()
        .replace("PROD_STATE_DEMO", "DEV_STATE_DEMO")
        .replace("prod_state_demo", "dev_state_demo");
    let vd_dev =
        captured_submit(&serde_json::from_str(&vd_dev).unwrap()).tolerate_nondeterminism(true);
    let dependency = sc.org("copy over a volatile dependency");
    let built_at = evaluation[VD_RERUN].request["tables"][0]["last_modified_epoch"]
        .as_i64()
        .unwrap();
    dependency.build(vd, built_at).await;
    let response = dependency.submit(vd_dev).await;
    assert_eq!(variant(&response), "ready_to_clone");
    assert!(volatile_values(&response).is_empty(), "{response:?}");
}

// ── Snowflake constant generator and MERGE over upstream drift (#211) ───────
//
// Evidence: the first #211 capture,
// `golden/fixtures/explain_volatile_generator_merge.jsonl` (dbt 2.0.11;
// `docs/reports/snowflake-volatile-generator-merge-2026-11-24.md`). Every
// model requests evaluation. `gu` is a FULL table projecting
// `uniform(1, 100, 1234)` alone. `mv` is a keyed MERGE incremental with
// `current_timestamp()` and `lag_tolerance: 0s` over the source table `ms`,
// built, rerun unchanged, then rerun after only `ms`'s value changed.
mod generator_merge {
    use super::*;

    const GENERATOR_MERGE: &str =
        include_str!("../../../golden/fixtures/explain_volatile_generator_merge.jsonl");
    /// `mv`'s submits and hosted's Explain of each: first build, unchanged
    /// rerun, and the rerun after upstream data drift.
    const MV_FIRST: usize = 7;
    const MV_FIRST_EXPLAIN: usize = 19;
    const MV_RERUN: usize = 31;
    const MV_RERUN_EXPLAIN: usize = 37;
    const MV_DRIFT: usize = 41;
    const MV_DRIFT_EXPLAIN: usize = 45;
    /// `gu`'s first build, and its unchanged rerun, which hosted reuses (a
    /// Check-in finding).
    const GU_FIRST: usize = 9;
    const GU_RERUN: usize = 28;
    /// `gr` (`random()` alone): first build and rerun refused at Reuse
    /// qualification.
    const GR_FIRST: usize = 10;
    const GR_RERUN: usize = 27;
    /// The source table both MERGE models read, as the client reports it.
    const MS: &str = r#""CDO_DEMO_DB"."PROD_STATE_DEMO"."MS_000054C3E38D""#;

    /// `explain` with the `updated … after` and `last updated:` lines masked,
    /// as the conformance replay masks them.
    fn masked(explain: serde_json::Value) -> serde_json::Value {
        masked_relative_times(masked_gaps(explain))
    }

    #[tokio::test]
    async fn snowflake_merge_renders_and_explains_as_hosted_on_its_builds_and_after_upstream_drift()
    {
        let database = std::sync::Arc::new(support::TestDatabase::new("volatile_211_merge").await);
        let server = support::start_server_on_database(database.clone()).await;
        let entries = entries(GENERATOR_MERGE);
        let responses = replay(&Scenario::connect(&server).await, &entries, MV_DRIFT).await;
        let explains: Vec<_> = [
            (MV_FIRST, MV_FIRST_EXPLAIN, "mv first build"),
            (MV_RERUN, MV_RERUN_EXPLAIN, "mv unchanged rerun"),
            (
                MV_DRIFT,
                MV_DRIFT_EXPLAIN,
                "mv rerun after upstream data drift",
            ),
        ]
        .into_iter()
        .map(|(submit, explained, case)| {
            assert_matches_hosted(&responses[&submit], &entries[submit], case);
            HostedExplain::of_response(
                case,
                &entries[explained],
                &entries[submit],
                &responses[&submit],
            )
        })
        .collect();
        assert_explains_match_hosted(&database, server, &explains, masked).await;
    }

    /// Hosted reuses `gu` (entry 28). Matching it relaxes a Reuse refusal,
    /// which waits for the maintainer (#211, #201 check-in rule): this
    /// server still refuses it and renders hosted's map on the rebuild. The
    /// rebuild prints no tree: hosted never reports this query as changed
    /// (entry 35, an `empty_tree` replay gap).
    #[tokio::test]
    async fn snowflake_constant_generator_alone_is_still_refused_pending_check_in() {
        let server = support::start_server().await;
        let sc = Scenario::connect(&server).await;
        let entries = entries(GENERATOR_MERGE);
        let responses = replay(&sc, &entries, GU_RERUN).await;
        let rerun = &responses[&GU_RERUN];
        assert_eq!(variant(rerun), "ready_to_execute");
        assert_eq!(
            volatile_map_shape(&volatile_values(rerun)),
            volatile_map_shape(&hosted_volatile_values(&entries[GU_RERUN].response)),
        );
        let explained = explain(&sc, &decision_id(rerun)).await;
        assert!(texts(&explained).is_empty(), "{explained}");
    }

    /// The epoch `submit` reports for `relation`.
    fn epoch_of(submit: &Submit, relation: &str) -> i64 {
        let req = submit.clone().build();
        req.tables
            .iter()
            .find(|t| t.name == relation)
            .and_then(|t| t.last_modified_epoch)
            .unwrap()
    }

    /// Uncaptured neighbours of `mv`'s reruns keep the conservative form:
    /// refused and explained with no tree. Hosted rendered `mv`'s map with
    /// its Target relation physically as built and one source whose epoch
    /// is known, at or past the epoch recorded at the refused build
    /// (entries 31 and 41); every other target and upstream state renders
    /// nothing, even when the submit ignores External modifications. Within
    /// a Lag tolerance the drift is no rebuild reason, so its tree is
    /// uncaptured, though its map renders.
    #[tokio::test]
    async fn snowflake_merge_rerun_over_uncaptured_upstream_evidence_prints_no_tree() {
        let server = support::start_server().await;
        let sc = Scenario::connect(&server).await;
        let entries = entries(GENERATOR_MERGE);
        let first = captured_submit(&entries[MV_FIRST].request);
        let unchanged = captured_submit(&entries[MV_RERUN].request);
        let drifted = captured_submit(&entries[MV_DRIFT].request);
        let target = entries[MV_DRIFT].request["target_table"]
            .as_str()
            .unwrap()
            .to_owned();
        let built_epoch = epoch_of(&drifted, &target);
        let source_epoch = epoch_of(&unchanged, MS);
        // `submit` with `relation` reported at `epoch`.
        let reporting = |submit: &Submit, relation: String, epoch: Option<i64>| {
            submit.clone().with(move |r| {
                for t in &mut r.tables {
                    if t.name == relation {
                        t.last_modified_epoch = epoch;
                    }
                }
            })
        };
        for (case, rerun, renders) in [
            (
                "drift within an hour's Lag tolerance",
                drifted
                    .clone()
                    .with(|r| r.freshness_tolerance_seconds = 3600),
                true,
            ),
            (
                "an upstream of unknown epoch",
                reporting(&drifted, MS.to_owned(), None),
                false,
            ),
            (
                "the source's epoch moved back",
                reporting(&unchanged, MS.to_owned(), Some(source_epoch - 1)),
                false,
            ),
            (
                "its Target relation written since the build",
                reporting(&unchanged, target.clone(), Some(built_epoch + 1)),
                false,
            ),
            (
                "its Target relation written, External modifications ignored",
                reporting(&unchanged, target.clone(), Some(built_epoch + 1))
                    .ignore_external_modifications(),
                false,
            ),
        ] {
            let sc = sc.org(case);
            sc.build(first.clone(), built_epoch).await;
            let rebuilt = sc.expect_execute(rerun).await;
            assert_eq!(!rendered(&rebuilt).is_empty(), renders, "{case}");
            let explained = explain(&sc, rebuilt.execution_decision_id.as_deref().unwrap()).await;
            assert!(texts(&explained).is_empty(), "{case}: {explained}");
        }

        // The upstream is a dbt-built table, rebuilt between the two builds
        // with its producer unchanged: `gr` (a `random()` table, refused at
        // Reuse qualification on its rerun) built into `ms`'s relation.
        let sc = sc.org("a dbt-built upstream rebuilt");
        let as_ms = |entry: usize| {
            let json = serde_json::to_string(&entries[entry].request)
                .unwrap()
                .replace("GR_000054C3E38D", "MS_000054C3E38D")
                .replace("gr_000054c3e38d", "ms_000054c3e38d");
            captured_submit(&serde_json::from_str(&json).unwrap())
        };
        sc.build(as_ms(GR_FIRST), epoch_of(&first, MS)).await;
        sc.build(first.clone(), built_epoch).await;
        sc.build(as_ms(GR_RERUN), epoch_of(&drifted, MS)).await;
        let rebuilt = sc.expect_execute(drifted.clone()).await;
        assert!(rendered(&rebuilt).is_empty(), "a dbt-built upstream");
        let explained = explain(&sc, rebuilt.execution_decision_id.as_deref().unwrap()).await;
        assert!(texts(&explained).is_empty(), "{explained}");

        // A source-less MERGE, built and rerun unchanged.
        let sc = sc.org("source-less");
        let sourceless = |s: &Submit| s.clone().with(|r| r.tables.retain(|t| t.name != MS));
        sc.build(sourceless(&first), built_epoch).await;
        let rebuilt = sc.expect_execute(sourceless(&unchanged)).await;
        assert!(rendered(&rebuilt).is_empty(), "source-less");
        let explained = explain(&sc, rebuilt.execution_decision_id.as_deref().unwrap()).await;
        assert!(texts(&explained).is_empty(), "{explained}");
    }

    /// The isolated generator's empty tree is limited to `gu`'s shape. With
    /// `UNIFORM(1, 100, 1234)` in place of `UUID_STRING()` in `cmb`, a table
    /// over the volatile view `vw` (`explain_volatile_cells.jsonl` entries
    /// 59 and 159), the rerun renders both functions and keeps the rerun
    /// tree.
    #[tokio::test]
    async fn snowflake_generator_beside_a_volatile_dependency_keeps_the_rerun_tree() {
        const CMB: (usize, usize) = (59, 159);
        let server = support::start_server().await;
        let sc = Scenario::connect(&server).await;
        let mut cells = entries(CELLS);
        for entry in [CMB.0, CMB.1] {
            let sql = cells[entry].request["sql"].as_str().unwrap();
            assert!(sql.contains("uuid_string()"), "entry {entry}: {sql}");
            let sql = sql.replace("uuid_string()", "uniform(1, 100, 1234)");
            cells[entry].request["sql"] = sql.into();
        }
        let responses = replay(&sc, &cells, CMB.1).await;
        let rerun = &responses[&CMB.1];
        assert_eq!(variant(rerun), "ready_to_execute");
        let mut functions: Vec<_> = volatile_map_shape(&volatile_values(rerun))
            .into_iter()
            .flat_map(|(_, functions)| functions)
            .collect();
        functions.sort();
        assert_eq!(
            functions,
            ["CURRENT_TIMESTAMP()", "UNIFORM(1, 100, 1234)"],
            "{functions:?}"
        );
        let explained = explain(&sc, &decision_id(rerun)).await;
        assert!(
            texts(&explained).contains(&"the model query has changed".to_owned()),
            "{explained}"
        );
    }

    /// The empty tree is limited to `gu` as captured: its SQL calling only
    /// `uniform(1, 100, 1234)`, with its Target relation as built. Another
    /// Volatile call the map does not show, or a write to the Target
    /// relation since the build, keeps the rerun tree.
    #[tokio::test]
    async fn snowflake_generator_rerun_keeps_its_tree_outside_the_captured_form() {
        let server = support::start_server().await;
        let sc = Scenario::connect(&server).await;
        let entries = entries(GENERATOR_MERGE);
        let target = entries[GU_RERUN].request["target_table"]
            .as_str()
            .unwrap()
            .to_owned();
        let with_call = |entry: usize, call: &'static str| {
            captured_submit(&entries[entry].request).with(move |r| {
                assert!(r.sql.contains(" as u"), "{}", r.sql);
                r.sql = r.sql.replace(" as u", &format!(" as u, {call} as w"));
            })
        };
        let first = captured_submit(&entries[GU_FIRST].request);
        let rerun = captured_submit(&entries[GU_RERUN].request);
        let built_epoch = epoch_of(&rerun, &target);
        for (case, first, rerun) in [
            (
                "beside CURRENT_TIMESTAMP(6)",
                with_call(GU_FIRST, "current_timestamp(6)"),
                with_call(GU_RERUN, "current_timestamp(6)"),
            ),
            (
                "beside SEQ4()",
                with_call(GU_FIRST, "seq4()"),
                with_call(GU_RERUN, "seq4()"),
            ),
            (
                "its Target relation written since the build",
                first.clone(),
                rerun.clone().with({
                    let target = target.clone();
                    move |r| {
                        for t in &mut r.tables {
                            if t.name == target {
                                t.last_modified_epoch = Some(built_epoch + 1);
                            }
                        }
                    }
                }),
            ),
        ] {
            let sc = sc.org(case);
            sc.build(first, built_epoch).await;
            let rebuilt = sc.expect_execute(rerun).await;
            assert_eq!(
                volatile_map_shape(&rendered(&rebuilt)),
                [(target.clone(), vec!["UNIFORM(1, 100, 1234)".to_owned()])],
                "{case}"
            );
            let explained = explain(&sc, rebuilt.execution_decision_id.as_deref().unwrap()).await;
            assert!(
                texts(&explained).contains(&"the model query has changed".to_owned()),
                "{case}: {explained}"
            );
        }
    }

    /// A dbt-built `ms`, modified externally before `mv`'s build, is recorded
    /// with no producer like a source, but it has a confirmed State in scope:
    /// it is not the captured source, so `mv`'s rerun renders nothing and
    /// prints no tree.
    #[tokio::test]
    async fn snowflake_merge_over_an_externally_modified_dbt_built_input_renders_nothing() {
        let server = support::start_server().await;
        let sc = Scenario::connect(&server).await;
        let entries = entries(GENERATOR_MERGE);
        let first = captured_submit(&entries[MV_FIRST].request);
        let unchanged = captured_submit(&entries[MV_RERUN].request);
        let target = entries[MV_DRIFT].request["target_table"]
            .as_str()
            .unwrap()
            .to_owned();
        let as_ms = |entry: usize| {
            let json = serde_json::to_string(&entries[entry].request)
                .unwrap()
                .replace("GR_000054C3E38D", "MS_000054C3E38D")
                .replace("gr_000054c3e38d", "ms_000054c3e38d");
            captured_submit(&serde_json::from_str(&json).unwrap())
        };
        // `ms` is built by dbt, then written outside dbt before `mv` reads it.
        sc.build(as_ms(GR_FIRST), epoch_of(&first, MS) - 1).await;
        sc.build(first, epoch_of(&unchanged, &target)).await;
        let rebuilt = sc.expect_execute(unchanged).await;
        assert!(rendered(&rebuilt).is_empty(), "{:?}", rendered(&rebuilt));
        let explained = explain(&sc, rebuilt.execution_decision_id.as_deref().unwrap()).await;
        assert!(texts(&explained).is_empty(), "{explained}");
    }
}
