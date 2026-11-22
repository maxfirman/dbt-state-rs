//! Complete local gRPC responses and execution evidence before Result copy
//! selection was consolidated in #236. Each test starts an isolated schema,
//! each history has its own Organization, and all writes go through
//! SubmitEnrichedSQL and ConfirmExecution.
mod support;

use std::collections::HashMap;

use dbt_state_harness::diff::RpcOutcome;
use dbt_state_proto::query_cache as qc;
use qc::submit_sql_response::Response;
use serde_json::{json, Value};
use support::scenario::{
    built_at, duckdb_ctas_at, duckdb_model, environment_model, environment_upstream, into_clone,
    into_execute, into_skip, model, outcome, Outcome, Scenario, Submit,
};
use support::{normalize_ids, rows, TestServer};

const BASELINE: &str = include_str!("fixtures/result_copy_baseline.jsonl");

struct History<'a> {
    name: &'static str,
    server: &'a TestServer,
    client: Scenario,
    events: Vec<Value>,
    execution_steps: HashMap<i64, &'static str>,
}

impl<'a> History<'a> {
    async fn new(server: &'a TestServer, name: &'static str) -> Self {
        Self {
            name,
            server,
            client: Scenario::connect(server).await.org(name),
            events: Vec::new(),
            execution_steps: HashMap::new(),
        }
    }

    fn clock(&self) -> Scenario {
        self.client
            .at(1_790_000_000_000 + self.events.len() as i64 * 100)
    }

    async fn submit(&mut self, step: &'static str, submit: Submit) -> qc::SubmitSqlResponse {
        let reply = qc::sql_client::SqlClient::new(self.client.channel())
            .submit_enriched_sql(self.clock().request(submit.build()))
            .await;
        let response = reply.as_ref().expect(step).get_ref().clone();
        let captured = RpcOutcome::capture(reply).unwrap();
        let request_id = match response.response.as_ref().expect(step) {
            Response::ReadyToExecute(ready) => Some(ready.request_id.as_str()),
            Response::ReadyToClone(ready) => Some(ready.request_id.as_str()),
            Response::SkipExecution(_) => None,
        };
        let stored = if let Some(id) = request_id {
            let row = rows::complete_execution(self.server, id).await;
            assert_eq!(row["status"], "pending", "{step}");
            assert_eq!(row["org_id"], self.name, "{step}");
            assert!(self
                .execution_steps
                .insert(row["id"].as_i64().unwrap(), step)
                .is_none());
            self.evidence(row)
        } else {
            Value::Null
        };
        self.record(step, "SubmitEnrichedSQL", captured, stored);
        response
    }

    async fn confirm_request(&mut self, step: &str, req: qc::ConfirmExecutionRequest) -> bool {
        let id = req.request_id.clone();
        let reply = qc::execution_client::ExecutionClient::new(self.client.channel())
            .confirm_execution(self.clock().request(req))
            .await;
        let success = reply.as_ref().is_ok_and(|response| {
            assert_eq!(response.get_ref().request_id, id, "{step}");
            response.get_ref().success
        });
        let captured = RpcOutcome::capture(reply).unwrap();
        let row = rows::complete_execution(self.server, &id).await;
        self.record(step, "ConfirmExecution", captured, self.evidence(row));
        success
    }

    async fn confirm(&mut self, step: &str, id: &str, outcome: Outcome) {
        assert!(
            self.confirm_request(step, outcome.confirm(id)).await,
            "{step}"
        );
    }

    async fn build(&mut self, step: &'static str, submit: Submit, outcome: Outcome) -> String {
        let id = into_execute(self.submit(step, submit).await).request_id;
        self.confirm(step, &id, outcome).await;
        id
    }

    fn evidence(&self, mut row: Value) -> Value {
        // Execution IDs are opaque. Correlate them with the submit that
        // minted them so a changed source ID remains observable.
        let execution_step = |id: &mut Value| {
            if let Some(number) = id.as_i64() {
                *id = json!(self.execution_steps.get(&number).expect("known execution"));
            }
        };
        for column in ["id", "clone_source_execution_id"] {
            if let Some(id) = row.get_mut(column) {
                execution_step(id);
            }
        }
        for column in ["submit_copy_provenance", "direct_copy_provenance"] {
            if let Some(id) = row[column].get_mut("source_execution_id") {
                execution_step(id);
            }
        }
        // Pin the persisted identity, input evidence, publication and
        // physical outcome. UI metadata and wall-clock row creation vary
        // independently of the copy lifecycle.
        row.as_object_mut().unwrap().retain(|field, _| {
            matches!(
                field.as_str(),
                "id" | "target_table"
                    | "physical_target_key"
                    | "execution_type"
                    | "table_namespace"
                    | "warehouse_family"
                    | "status"
                    | "node_unique_id"
                    | "node_sql_hash"
                    | "node_namespace_logic_hash"
                    | "input_tables"
                    | "analyzed_copy_proof"
                    | "clone_source_table"
                    | "clone_source_inputs"
                    | "clone_source_execution_id"
                    | "submit_copy_provenance"
                    | "direct_copy_provenance"
                    | "last_modified_epoch"
                    | "table_type"
                    | "execution_runtime_ms"
            )
        });
        row
    }

    fn record(&mut self, step: &str, method: &str, captured: RpcOutcome, execution: Value) {
        self.events.push(json!({
            "history": self.name,
            "step": step,
            "method": method,
            "response": normalize_ids(captured.response),
            "status": captured.status,
            "response_metadata": captured.response_metadata,
            "execution": execution,
        }));
    }

    fn assert_baseline(self) {
        if std::env::var_os("DBT_STATE_PRINT_COPY_BASELINE").is_some() {
            for event in &self.events {
                println!("copy-baseline {event}");
            }
        }
        let expected: Vec<Value> = BASELINE
            .lines()
            .map(|line| serde_json::from_str(line).expect("valid baseline fixture"))
            .filter(|event: &Value| event["history"] == self.name)
            .collect();
        assert_eq!(self.events.len(), expected.len(), "{}", self.name);
        for (event, expected) in self.events.iter().zip(expected) {
            assert_eq!(
                event, &expected,
                "{} {} {}",
                self.name, event["method"], event["step"]
            );
        }
    }
}

fn databricks_model(schema: &str) -> Submit {
    model(&format!("`db`.`{schema}`.`result`"))
        .dialect("databricks")
        .catalog("db")
        .schema(schema)
        .namespace("copy-warehouse")
        .unique_id("model.project.result")
        .body(&"a".repeat(32))
        .node_state(|state| {
            state.resource_type = "model".into();
            state.node_configs_hash = Some("b".repeat(32));
        })
        .own_epoch(None)
        .sql("SELECT * FROM db.raw.input")
        .upstream("db.raw.input", 100)
        .extra("lookback", "1")
        .extra("on_schema_change", "\"ignore\"")
        .allow_clones(true)
        .clone_chain_depth_limit(1)
}

fn delta(epoch: i64, runtime: i64) -> Outcome {
    outcome().epoch(epoch).table_type("DELTA").runtime(runtime)
}

#[tokio::test]
async fn databricks_pending_newest_preserves_the_older_build_through_confirmation() {
    let server = support::start_server().await;
    let mut history = History::new(&server, "databricks-pending-newest").await;
    let queries = |schema| {
        databricks_model(schema)
            .sql("SELECT a.id AS input_id, b.id AS other_id FROM db.raw.input AS a(id) LEFT JOIN db.raw.other AS b(id) ON TRUE")
            .upstream("db.raw.other", 101)
    };

    // Give one genuine input a recorded Upstream producer.
    history
        .build(
            "input producer",
            databricks_model("raw")
                .target("db.raw.input")
                .unique_id("model.project.input")
                .no_tables()
                .sql("SELECT 1 AS id"),
            delta(100, 11),
        )
        .await;

    let old = queries("older").with(|req| req.tables.reverse());
    let older_id = history
        .build("older source", old.allow_clones(false), delta(200, 20))
        .await;
    let newer = queries("newer");
    history
        .build(
            "newer source",
            newer.clone().allow_clones(false),
            delta(300, 30),
        )
        .await;

    // Verify both sources qualify, and that the newer source wins without
    // the barrier, using a separate absent Target relation.
    let control = into_clone(history.submit("no pending write", queries("control")).await);
    assert_eq!(control.clone_source, "`db`.`newer`.`result`");
    assert_eq!(control.clone_required_last_modified_epoch, Some(300));
    into_execute(
        history
            .submit(
                "pending source write",
                newer
                    .own_epoch(Some(300))
                    .sql("SELECT a.id FROM db.raw.input AS a WHERE a.id = 2"),
            )
            .await,
    );

    let destination = queries("destination");
    let grant = into_clone(history.submit("destination", destination.clone()).await);
    assert_eq!(grant.clone_source, "`db`.`older`.`result`");
    assert_eq!(grant.clone_required_last_modified_epoch, Some(200));
    assert_eq!(grant.execution_runtime_ms, Some(20));

    let older = rows::complete_execution(&server, &older_id).await;
    let pending = rows::complete_execution(&server, &grant.request_id).await;
    let proof = &pending["submit_copy_provenance"];
    assert_eq!(proof["source_execution_id"], older["id"]);
    assert_eq!(proof["source_epoch"], older["last_modified_epoch"]);
    assert_eq!(proof["source_physical_key"], older["physical_target_key"]);
    assert_eq!(proof["published"], false);
    // The older source observed the same inputs in the opposite order
    // from the destination and the newer source.
    assert_eq!(pending["input_tables"], older["input_tables"]);
    assert!(pending["input_tables"][1]["producer"].is_string());

    history
        .confirm("publish destination", &grant.request_id, delta(400, 20))
        .await;
    let confirmed = rows::complete_execution(&server, &grant.request_id).await;
    let mut published = proof.clone();
    published["published"] = true.into();
    assert_eq!(confirmed["submit_copy_provenance"], published);
    assert_eq!(confirmed["input_tables"], older["input_tables"]);

    let reused = into_skip(
        history
            .submit("unchanged destination", destination.own_epoch(Some(400)))
            .await,
    );
    assert_eq!(reused.execution_runtime_ms, Some(20));
    history.assert_baseline();
}

/// Public gRPC counterparts of the two Snowflake cases in submit_decision.rs.
#[tokio::test]
async fn snowflake_continues_on_stale_and_stops_at_a_different_input() {
    let server = support::start_server().await;
    let up = environment_upstream("prod");
    let named =
        |table: &str| environment_model("prod").target(&format!("\"DB\".\"PROD\".\"{table}\""));
    let current = || environment_model("prod").upstream(&up, 2_000);

    let mut history = History::new(&server, "snowflake-continues").await;
    history
        .build("stale source", named("STALE"), built_at(9_000))
        .await;
    history
        .build(
            "fresh source",
            named("FRESH")
                .upstream(&up, 2_000)
                .clone_chain_depth_limit(0),
            built_at(3_000),
        )
        .await;
    let grant = into_clone(history.submit("destination", current()).await);
    assert_eq!(grant.clone_source, "\"DB\".\"PROD\".\"FRESH\"");
    history
        .confirm("publish destination", &grant.request_id, built_at(10_000))
        .await;
    into_skip(
        history
            .submit("unchanged destination", current().own_epoch(Some(10_000)))
            .await,
    );
    history.assert_baseline();

    let mut history = History::new(&server, "snowflake-stops").await;
    history
        .build(
            "other-input source",
            environment_model("dev"),
            built_at(5_000),
        )
        .await;
    let stale = into_clone(history.submit("same-input source", named("STALE")).await);
    history
        .confirm(
            "publish same-input source",
            &stale.request_id,
            built_at(9_000),
        )
        .await;
    let control = into_clone(
        history
            .submit("unchanged input control", environment_model("prod"))
            .await,
    );
    assert_eq!(control.clone_source, "\"DB\".\"PROD\".\"STALE\"");
    into_execute(history.submit("changed input", current()).await);
    history.assert_baseline();
}

/// The local #237 differing-input lifecycle tracked for policy review in #264.
#[tokio::test]
async fn snowflake_differing_input_copy_and_follow_up() {
    let server = support::start_server().await;
    let mut history = History::new(&server, "snowflake-differing-input").await;
    let upstream = |schema| {
        environment_model(schema)
            .target(&environment_upstream(schema))
            .unique_id("model.p.up")
            .no_tables()
            .sql(&format!("select '{schema}' as v"))
    };
    let downstream = |schema, epoch| {
        environment_model(schema)
            .sql(&format!("select v from DB.{schema}.up"))
            .upstream(&environment_upstream(schema), epoch)
    };
    history
        .build("prod upstream", upstream("prod"), built_at(500))
        .await;
    history
        .build("dev upstream", upstream("dev"), built_at(600))
        .await;
    history
        .build("prod downstream", downstream("prod", 500), built_at(2_000))
        .await;

    let dev = downstream("dev", 600);
    let grant = into_clone(history.submit("dev downstream", dev.clone()).await);
    assert_eq!(grant.clone_source, "\"DB\".\"PROD\".\"M\"");
    history
        .confirm("confirm dev copy", &grant.request_id, built_at(3_000))
        .await;
    into_skip(
        history
            .submit("unchanged dev downstream", dev.own_epoch(Some(3_000)))
            .await,
    );
    history.assert_baseline();
}

#[tokio::test]
async fn databricks_replacing_the_selected_source_invalidates_publication() {
    let server = support::start_server().await;
    let mut history = History::new(&server, "databricks-source-replacement").await;
    let source = databricks_model("prod");
    history
        .build("source", source.clone(), delta(200, 20))
        .await;
    let destination = databricks_model("dev");
    let grant = into_clone(history.submit("destination", destination.clone()).await);
    history
        .build(
            "replacement",
            source
                .own_epoch(Some(200))
                .sql("SELECT id FROM db.raw.input WHERE id = 2"),
            delta(300, 30),
        )
        .await;
    history
        .confirm(
            "confirm after replacement",
            &grant.request_id,
            delta(400, 20),
        )
        .await;
    into_execute(
        history
            .submit(
                "destination after replacement",
                destination.own_epoch(Some(400)),
            )
            .await,
    );
    history.assert_baseline();
}

#[tokio::test]
async fn databricks_failed_copy_publishes_only_the_fallback_build() {
    let server = support::start_server().await;
    let mut history = History::new(&server, "databricks-failed-copy").await;
    history
        .build("source", databricks_model("prod"), delta(200, 20))
        .await;
    let destination = databricks_model("dev");
    let grant = into_clone(history.submit("destination", destination.clone()).await);
    let fallback = delta(300, 154).failed_to_clone().confirm(&grant.request_id);
    let mut missing_outcome = fallback.clone();
    missing_outcome.last_modified_epoch = None;
    assert!(
        !history
            .confirm_request("missing physical outcome", missing_outcome)
            .await
    );
    assert!(
        history
            .confirm_request("publish fallback build", fallback)
            .await
    );
    let unchanged = destination.own_epoch(Some(300));
    let reuse = into_skip(
        history
            .submit("unchanged fallback", unchanged.clone())
            .await,
    );
    assert_eq!(reuse.execution_runtime_ms, Some(154));
    assert!(
        !history
            .confirm_request(
                "duplicate confirmation",
                delta(300, 9999).confirm(&grant.request_id)
            )
            .await
    );
    let reuse = into_skip(history.submit("unchanged after duplicate", unchanged).await);
    assert_eq!(reuse.execution_runtime_ms, Some(154));
    history.assert_baseline();
}

/// The physical ancestry remains two hops even after an ancestor rebuilds.
#[tokio::test]
async fn duckdb_copy_chain_limit_survives_ancestor_rebuild() {
    let server = support::start_server().await;
    let mut history = History::new(&server, "duckdb-chain-limit").await;
    let graph = |schema: &str, step: u8| {
        duckdb_model(schema)
            .target(&format!("\"db\".\"{schema}\".\"result{step}\""))
            .sql(&format!(
                "select id, amount + 1 as amount from db.{schema}.upper_view"
            ))
            .dependency_in(
                &format!("db.{schema}.upper_view"),
                &format!(
                    "CREATE VIEW {schema}.upper_view AS SELECT id, amount FROM {schema}.lower_view"
                ),
                "db",
                schema,
            )
            .dependency_in(
                &format!("db.{schema}.lower_view"),
                "CREATE VIEW lower_view AS SELECT id, amount FROM raw.input",
                "db",
                schema,
            )
    };
    history
        .build("source", graph("prod", 0), duckdb_ctas_at(20))
        .await;
    let one = into_clone(history.submit("first hop", graph("dev", 1)).await);
    assert_eq!(one.clone_source, "\"db\".\"prod\".\"result0\"");
    history
        .confirm("publish first hop", &one.request_id, duckdb_ctas_at(30))
        .await;
    let two = into_clone(history.submit("second hop", graph("prod", 2)).await);
    assert_eq!(two.clone_source, "\"db\".\"dev\".\"result1\"");
    history
        .confirm("publish second hop", &two.request_id, duckdb_ctas_at(40))
        .await;
    history
        .build(
            "rebuild ancestor",
            graph("dev", 1).allow_clones(false),
            duckdb_ctas_at(35),
        )
        .await;
    history
        .build("third hop refused", graph("dev", 3), duckdb_ctas_at(50))
        .await;
    history.assert_baseline();
}
