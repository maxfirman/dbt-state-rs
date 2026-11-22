//! Audit: in `oidc` mode, every decision and execution recorded in State
//! history names the Principal whose request made it, so a security reviewer
//! can trace who changed State history.
//!
//! The Token broker records each Principal it issues a token to, keyed by
//! the Upstream IdP issuer and subject, with its kind and a display email
//! when known. Decision and execution rows carry the token's opaque
//! Principal id (`sub`), which names that record. `none` mode records no
//! Principal.

#[path = "support.rs"]
mod support;

use dbt_state_auth::Role;
use serde_json::json;
use support::broker_client::{claims, client_credentials, json};
use support::fake_idp::{FakeIdp, UpstreamMachine};
use support::oidc::{TestAuth, TestAuthBuilder};
use support::rows::{self, StoredPrincipal};
use support::scenario::{
    data_test, into_execute, into_skip, into_speculative_skip, model, outcome, seed, test_results,
    Scenario,
};
use support::{
    demo, fault, legacy, start_server, start_server_oidc, start_server_on_database,
    start_server_with_broker, TestDatabase,
};

const TARGET: &str = "\"DB\".\"S\".\"T\"";
const ORGANIZATIONS: &[(&str, bool)] = &[("analytics", false), ("research", false)];

/// The CI machine's app registration at the Upstream IdP.
const CI: &str = "ci-pipeline";
const CI_SECRET: &str = "ci-secret-Zx81qv";

fn ci_machine() -> UpstreamMachine {
    UpstreamMachine::new(CI, CI_SECRET).claim("roles", json!(["dbt-state.ci"]))
}

/// CI machines with the `dbt-state.ci` app role are developers in
/// `analytics`.
fn deployment(idp: &FakeIdp) -> TestAuthBuilder {
    TestAuth::builder(ORGANIZATIONS).upstream(idp).rule(
        "machine",
        "roles",
        "dbt-state.ci",
        "analytics",
        "developer",
    )
}

/// The `id_token` the broker at `broker` issues the CI machine.
async fn ci_login(broker: &str) -> String {
    let response = client_credentials(broker, CI, CI_SECRET).await;
    assert_eq!(response.status(), 200);
    json(response).await["id_token"]
        .as_str()
        .expect("an id_token")
        .to_owned()
}

/// A CI machine signs in through the broker, builds a model and confirms
/// it: the decision and the execution both name the machine's Principal,
/// and the broker recorded that Principal by its Upstream IdP issuer and
/// subject.
#[tokio::test]
async fn a_machine_s_decision_and_execution_record_its_principal() {
    let idp = FakeIdp::start().await;
    idp.machine(ci_machine());
    let auth = deployment(&idp).build();
    let server = start_server_with_broker(&auth).await;

    let token = ci_login(&server.broker_url()).await;
    let principal = claims(&token)["sub"].as_str().unwrap().to_owned();
    let sc = Scenario::connect(&server)
        .await
        .bearer(&token)
        .org("analytics");
    let built = into_execute(sc.submit(model(TARGET)).await);
    assert!(sc.confirm(&built.request_id, outcome().epoch(100)).await);

    let decisions = rows::decisions(&server, 1).await;
    assert_eq!(decisions.len(), 1, "{decisions:?}");
    assert_eq!(decisions[0].org_id, "analytics");
    assert_eq!(decisions[0].decision, "build");
    assert_eq!(decisions[0].principal_id.as_deref(), Some(&*principal));
    let executions = rows::executions(&server).await;
    assert_eq!(executions.len(), 1, "{executions:?}");
    assert!(executions[0].is_confirmed());
    assert_eq!(executions[0].org_id, "analytics");
    assert_eq!(executions[0].principal_id.as_deref(), Some(&*principal));

    // Signing in again is the same Principal record.
    ci_login(&server.broker_url()).await;
    assert_eq!(
        rows::principals(&server).await,
        [StoredPrincipal {
            id: principal,
            issuer: idp.issuer(),
            subject: ci_machine().subject().to_owned(),
            kind: "machine".into(),
            email: None,
        }]
    );
}

/// A broker that cannot record a Principal issues it no token, so every
/// Principal on a decision it enabled has a record. The client sees the
/// broker as temporarily unavailable, as when the Upstream IdP is down.
#[tokio::test]
async fn a_principal_the_broker_cannot_record_gets_no_token() {
    let idp = FakeIdp::start().await;
    idp.machine(ci_machine());
    let auth = deployment(&idp).build();
    let server = start_server_with_broker(&auth).await;
    fault::fail_principal_records(&server).await;

    let response = client_credentials(&server.broker_url(), CI, CI_SECRET).await;
    assert_eq!(response.status(), 503);
    let body = json(response).await;
    assert_eq!(body["error"], "temporarily_unavailable");
    assert!(!body.contains_key("id_token"), "{body:?}");
    assert!(rows::principals(&server).await.is_empty());
}

/// Records State history through every RPC that writes it: a build and its
/// confirmation, a reuse, a seed build, a RecordExecutions batch, a
/// RegisterClone copy, and a data test's build and speculative reuse.
/// Returns the number of decisions and execution rows it recorded.
async fn record_every_kind(sc: &Scenario) -> (usize, usize) {
    let built = into_execute(sc.submit(model(TARGET)).await);
    assert!(sc.confirm(&built.request_id, outcome().epoch(100)).await);
    into_skip(sc.submit(model(TARGET)).await);
    let seed = into_execute(
        sc.submit_values(seed("\"DB\".\"S\".\"SEED\"").values("bytes"))
            .await,
    );
    assert!(sc.confirm(&seed.request_id, outcome().epoch(110)).await);
    let recorded = model("\"DB\".\"S\".\"RECORDED\"").record(outcome().epoch(120));
    assert_eq!(sc.record([recorded]).await.expect("RecordExecutions"), 1);
    let copy = sc
        .register_clone(demo::copy(TARGET, "\"DB\".\"DEV\".\"T\"").source_epoch(100))
        .await
        .expect("RegisterClone")
        .ready_to_clone
        .expect("ready_to_clone");
    assert!(sc.confirm(&copy.request_id, outcome().epoch(130)).await);
    let test = data_test("test.p.audited").namespace("ns").project("p");
    sc.build_with(test.clone(), outcome().results(test_results(0)))
        .await;
    into_speculative_skip(sc.speculative(test).await);
    // Decisions: build, reuse, seed, copy, test build, speculative reuse.
    // Executions: build, seed, recorded, copy, test build.
    (6, 5)
}

/// Every decision and execution row of the server, once `decisions` are
/// captured: the Principal each names.
async fn recorded_principals(
    server: &support::TestServer,
    decisions: usize,
) -> (Vec<Option<String>>, Vec<Option<String>>) {
    (
        rows::decisions(server, decisions)
            .await
            .into_iter()
            .map(|row| row.principal_id)
            .collect(),
        rows::executions(server)
            .await
            .into_iter()
            .map(|row| row.principal_id)
            .collect(),
    )
}

/// Every RPC that writes State history records the token's Principal, also
/// when the Principal has no record: a decision is never refused or lost
/// because its Principal is unknown to this database.
#[tokio::test]
async fn every_state_history_write_records_the_token_s_principal() {
    let auth = TestAuth::new(ORGANIZATIONS);
    let server = start_server_oidc(&auth).await;
    let token = auth
        .signer()
        .machine("principal-ci")
        .grant("analytics", Role::Developer)
        .token();
    let sc = Scenario::connect(&server)
        .await
        .bearer(&token)
        .org("analytics");

    let (decisions, executions) = record_every_kind(&sc).await;

    let principal = Some("principal-ci".to_owned());
    assert_eq!(
        recorded_principals(&server, decisions).await,
        (
            vec![principal.clone(); decisions],
            vec![principal; executions]
        )
    );
    assert!(rows::principals(&server).await.is_empty());
}

/// `none` mode authenticates no one: its decisions and executions record no
/// Principal.
#[tokio::test]
async fn none_mode_records_no_principal() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await.org("analytics");

    let (decisions, executions) = record_every_kind(&sc).await;

    assert_eq!(
        recorded_principals(&server, decisions).await,
        (vec![None; decisions], vec![None; executions])
    );
    assert!(rows::principals(&server).await.is_empty());
}

/// Upgrading a deployment from before Principals were recorded keeps its
/// State history, with no Principal on any existing decision or execution,
/// and drops the reserved, never-written actor, role and role-binding
/// tables, even when something wrote to them.
#[tokio::test]
async fn existing_decisions_and_executions_record_no_principal_after_upgrade() {
    let database = std::sync::Arc::new(TestDatabase::new("principals_upgrade").await);
    legacy::record_history_before_principals(&database, "acme").await;

    let server = start_server_on_database(database).await;

    let decisions = rows::decisions(&server, 1).await;
    assert_eq!(decisions.len(), 1, "{decisions:?}");
    assert_eq!(decisions[0].principal_id, None);
    let executions = rows::executions(&server).await;
    assert_eq!(executions.len(), 1, "{executions:?}");
    assert_eq!(executions[0].principal_id, None);
    let tables = rows::tables(&server).await;
    let reserved: Vec<&String> = tables
        .iter()
        .filter(|table| ["actors", "roles", "role_bindings"].contains(&table.as_str()))
        .collect();
    assert!(tables.iter().any(|table| table == "principals"));
    assert!(reserved.is_empty(), "{reserved:?}");
}
