//! Bounded synthetic safety, not hosted conformance or Databricks SQL execution.
mod generated_model;
mod support;
use dbt_state_proto::query_cache as qc;
use generated_model::*;
use serde_json::{json, Value};
use std::io::Write;
use std::path::PathBuf;
use support::scenario::built_at;

const TEMPLATE: &str =
    include_str!("../../../golden/fixtures/databricks_freshness_actual_epochs.jsonl");
#[derive(Clone, Copy, Debug)]
enum Mutant {
    None,
    WrongSkipUpdate,
    WrongSkipK,
    WrongSkipLiteral,
    AlwaysBuild,
}
struct Journal {
    file: std::fs::File,
}
impl Journal {
    fn new(name: &str, plan: &Value) -> Self {
        let root = std::env::var_os("DBT_STATE_DATABRICKS_GENERATED_ARTIFACTS")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join("dbt-state-databricks-generated"));
        let dir = root.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        // Persist the complete input before even starting the public server.
        std::fs::write(
            dir.join("plan.json"),
            serde_json::to_vec_pretty(plan).unwrap(),
        )
        .unwrap();
        Self {
            file: std::fs::File::create(dir.join("events.jsonl")).unwrap(),
        }
    }
    fn add(&mut self, event: Value) {
        serde_json::to_writer(&mut self.file, &event).unwrap();
        writeln!(self.file).unwrap();
        self.file.flush().unwrap();
    }
}
fn rpc<T: serde::Serialize>(
    journal: &mut Journal,
    method: &str,
    result: Result<tonic::Response<T>, tonic::Status>,
) -> T {
    match result {
        Ok(response) => {
            let body = response.into_inner();
            journal.add(json!({"rpc":method,"status":"OK","response":body}));
            body
        }
        Err(status) => {
            journal.add(json!({"rpc":method,"status":format!("{:?}", status.code()),"message":status.message()}));
            panic!("{method} transport/status failure: {status}");
        }
    }
}
fn pins() -> Value {
    static PINS: std::sync::LazyLock<Value> = std::sync::LazyLock::new(|| {
        json!({
            "hosted_template_sha256": digest(TEMPLATE.as_bytes()),
            "runner_sha256": digest(include_bytes!("databricks_generated.rs")),
            "model_sha256": digest(include_bytes!("generated_model/mod.rs")),
            "literal_template_source_sha256": digest(include_bytes!("../../../experiments/databricks_baseline.py")),
            "source_template_source_sha256": digest(include_bytes!("../../../experiments/databricks_target_lifecycle.py")),
            "test_binary_sha256": digest(&std::fs::read(std::env::current_exe().unwrap()).unwrap()),
            "package_version": env!("CARGO_PKG_VERSION")
        })
    });
    PINS.clone()
}
fn digest(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(bytes))
}
fn template() -> qc::SubmitEnrichedSqlRequest {
    TEMPLATE
        .lines()
        .map(|l| serde_json::from_str::<Value>(l).unwrap())
        .find(|r| r["method"] == "SubmitEnrichedSQL")
        .map(|r| serde_json::from_value(r["request"].clone()).unwrap())
        .unwrap()
}
fn request(
    query: &Query,
    source: &str,
    target: &str,
    namespace: &str,
    epoch: i64,
    r_epoch: Option<i64>,
) -> qc::SubmitEnrichedSqlRequest {
    let mut req = template();
    req.sql = render(query, source);
    req.target_table = Some(target.into());
    req.default_schema = Some("generated".into());
    req.table_namespace = Some(namespace.into());
    req.tables.clear();
    if matches!(query, Query::Source(_)) {
        req.tables.push(qc::TableModifiedInfo {
            name: source.into(),
            last_modified_epoch: Some(epoch),
        });
    }
    if let Some(epoch) = r_epoch {
        req.tables.push(qc::TableModifiedInfo {
            name: target.into(),
            last_modified_epoch: Some(epoch),
        });
    }
    let node = req.dbt_node_state.as_mut().unwrap();
    node.node_unique_id = "model.generated.result".into();
    node.project_name = "generated".into();
    node.profile_name = "generated".into();
    node.node_hash = format!("generated-{}", digest(req.sql.as_bytes()));
    node.node_body_hash = Some(node.node_hash.clone());
    node.node_configs_hash = Some("generated-delta-ignore-v1".into());
    node.node_contract_hash = Some("generated-no-contract-v1".into());
    req.labels = [
        ("dbt_node_unique_id".into(), node.node_unique_id.clone()),
        ("dbt_node_name".into(), "result".into()),
        ("dbt_node_fqn".into(), "generated.result".into()),
    ]
    .into();
    admit(&req, source, target, query);
    req
}
fn admit(r: &qc::SubmitEnrichedSqlRequest, source: &str, target: &str, q: &Query) {
    assert_eq!(r.dialect, "databricks");
    assert_eq!(r.execution_type, 1);
    assert_eq!(r.freshness_tolerance_seconds, 0);
    assert_eq!(r.stale_upstream_policy, 0);
    assert!(!r.compare_unrendered_code && !r.ignore_external_modifications);
    assert!(
        r.tolerate_nondeterminism
            && r.allow_clones == Some(true)
            && r.is_defer_to_profile
            && r.defer_enabled
    );
    let config = [
        ("file_format".to_string(), "\"delta\"".to_string()),
        ("on_schema_change".into(), "\"ignore\"".into()),
        ("lookback".into(), "1".into()),
    ]
    .into();
    assert_eq!(r.semantic_extras, config);
    assert_eq!(r.default_catalog, "cdo_demo_db");
    assert_eq!(r.default_schema.as_deref(), Some("generated"));
    let node = r.dbt_node_state.as_ref().unwrap();
    assert_eq!(node.node_unique_id, "model.generated.result");
    assert_eq!(node.project_name, "generated");
    assert_eq!(node.resource_type, "model");
    assert!(node.node_hash.starts_with("generated-"));
    assert!(r
        .tables
        .iter()
        .all(|t| t.name == source || t.name == target));
    assert!(r.tables.iter().filter(|t| t.name == target).count() <= 1);
    assert_eq!(r.sql, render(q, source));
    assert_eq!(r.target_table.as_deref(), Some(target));
    assert!(r.table_namespace.as_ref().is_some_and(|ns| !ns.is_empty()));
    assert!(r
        .tables
        .iter()
        .all(|t| t.last_modified_epoch.is_some_and(|e| e > 0)));
    assert!(r.query_dependencies.is_empty() && r.lenient_dependencies.is_empty());
    assert_eq!(
        r.tables.iter().filter(|t| t.name == source).count(),
        usize::from(matches!(q, Query::Source(_)))
    );
}
fn wire<T>(body: T, org: &str, clock: i64) -> tonic::Request<T> {
    let mut r = support::request_with_org(body, org);
    r.metadata_mut().insert(
        "x-dbt-state-replay-now-ms",
        clock.to_string().parse().unwrap(),
    );
    r
}
async fn explain(channel: tonic::transport::Channel, org: &str, id: String, journal: &mut Journal) {
    // Capture is asynchronous; wait only for this public decision to appear.
    for attempt in 0..30 {
        let request = qc::GetExplainMessagesRequest {
            execution_decision_ids: vec![id.clone()],
        };
        journal.add(json!({"rpc":"GetExplainMessages","request":request}));
        let response = rpc(
            journal,
            "GetExplainMessages",
            qc::explain_client::ExplainClient::new(channel.clone())
                .get_explain_messages(support::request_with_org(request, org))
                .await,
        );
        journal.add(json!({"rpc":"GetExplainMessages","response":response}));
        if !response.messages.is_empty() {
            return;
        }
        assert!(attempt < 29, "Explain must retain generated decision");
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}
async fn run(seed: u64, literal: bool, mutant: Mutant) -> Result<(), &'static str> {
    let nonce = uuid::Uuid::new_v4().simple().to_string();
    let name = format!("seed{seed}-literal{literal}-{mutant:?}-{nonce}");
    let org = format!("generated-{nonce}-{seed}");
    let ns = format!("generated-ns-{nonce}");
    let source = "cdo_demo_db.generated.inputs";
    let target = "cdo_demo_db.generated.result";
    let steps = plan(seed, literal);
    let mut journal = Journal::new(
        &name,
        &json!({"trace_version":1,"mode":"generated-modeled-safety","hosted_verified":false,"actual_client":false,"real_warehouse_execution":false,"generated_request_metadata":true,"seed":seed,"scope_nonce":nonce,"org":org,"namespace":ns,"source":source,"target":target,"mutant":format!("{mutant:?}"),"operations":steps,"source_pins":pins(),"synthetic_initial_epoch":1_800_000_000_000_i64}),
    );
    let server = support::start_server_require_org().await;
    let channel = support::channel(server.addr).await;
    let mut source_rows = initial();
    let mut query = if literal {
        Query::Literal(10)
    } else {
        Query::Source(0)
    };
    let mut r = absent();
    let mut epoch = 1_800_000_000_000_i64;
    let mut source_epoch = epoch;
    let mut target_epoch = None;
    let mut builds = 0;
    let mut skips = 0;
    for (index, step) in steps.iter().enumerate() {
        epoch += 10_000;
        if apply(&mut source_rows, &mut query, &step.op) {
            source_epoch = epoch;
        }
        let f = reference(&source_rows, &query); // Unconditionally computed before the response.
        let req = request(&query, source, target, &ns, source_epoch, target_epoch);
        journal.add(json!({"index":index,"op":step,"source_rows":source_rows,"request":req,"synthetic_clock":epoch+1000,"source_epoch":source_epoch,"target_epoch":target_epoch}));
        let raw = rpc(
            &mut journal,
            "SubmitEnrichedSQL",
            qc::sql_client::SqlClient::new(channel.clone())
                .submit_enriched_sql(wire(req, &org, epoch + 1000))
                .await,
        );
        let (raw_decision, decision_id, request_id) = match raw.response.as_ref() {
            Some(qc::submit_sql_response::Response::ReadyToExecute(x)) => (
                "BUILD",
                x.execution_decision_id.clone().expect("build decision id"),
                Some(x.request_id.clone()),
            ),
            Some(qc::submit_sql_response::Response::SkipExecution(x)) => {
                assert!(
                    !x.explained_decision
                        .as_ref()
                        .expect("skip explanation")
                        .is_stale
                );
                (
                    "SKIP",
                    x.execution_decision_id.clone().expect("skip decision id"),
                    None,
                )
            }
            _ => panic!("unsupported FULL response: {raw:?}"),
        };
        let inject = match mutant {
            Mutant::WrongSkipUpdate => matches!(step.op, Op::Update(17)),
            Mutant::WrongSkipK => matches!(step.op, Op::K(1)),
            Mutant::WrongSkipLiteral => matches!(step.op, Op::Literal(17)),
            Mutant::AlwaysBuild => index == 1,
            Mutant::None => false,
        };
        let effective = if inject {
            if matches!(mutant, Mutant::AlwaysBuild) {
                "BUILD"
            } else {
                "SKIP"
            }
        } else {
            raw_decision
        };
        journal.add(json!({"index":index,"raw_response":raw,"raw_decision":raw_decision,"effective_decision":effective,"mutated":inject}));
        if effective == "BUILD" {
            r = execute(&source_rows, &query);
            builds += 1;
            // Mutant-only BUILD of raw SKIP has no server pending ID. Never forge one.
            if let Some(id) = request_id {
                epoch += 1000;
                let confirmation = built_at(epoch).confirm(&id);
                journal.add(json!({"rpc":"ConfirmExecution","request":confirmation,"synthetic_clock":epoch+1000}));
                let response = rpc(
                    &mut journal,
                    "ConfirmExecution",
                    qc::execution_client::ExecutionClient::new(channel.clone())
                        .confirm_execution(wire(confirmation, &org, epoch + 1000))
                        .await,
                );
                journal.add(json!({"rpc":"ConfirmExecution","response":response}));
                assert!(response.success);
                target_epoch = Some(epoch);
            }
        } else {
            skips += 1;
        }
        explain(channel.clone(), &org, decision_id, &mut journal).await;
        // Rows first: a wrong SKIP must fail independently from expected decisions.
        let result = check(&r, &f).and_then(|()| contract(effective, &step.required));
        journal.add(json!({"index":index,"R":r,"F":f,"rows_equal":check(&r,&f).is_ok(),"violation":result.err(),"synthetic_epoch":epoch}));
        result?;
    }
    journal.add(json!({"complete":true,"builds":builds,"skips":skips,"observations":steps.len()}));
    Ok(())
}
#[tokio::test]
async fn four_seed_bounded_full_freshness_safety_and_latest_reversion() {
    for seed in 0..4 {
        run(seed, false, Mutant::None).await.unwrap();
        run(seed, true, Mutant::None).await.unwrap();
    }
}
#[tokio::test]
async fn public_boundary_mutants_fail_for_exact_named_reasons() {
    for (literal, mutant) in [
        (false, Mutant::WrongSkipUpdate),
        (false, Mutant::WrongSkipK),
        (true, Mutant::WrongSkipLiteral),
    ] {
        assert_eq!(run(0, literal, mutant).await, Err("wrong-skip:rows"));
    }
    assert_eq!(
        run(0, false, Mutant::AlwaysBuild).await,
        Err("required-reuse:decision")
    );
}
#[test]
fn typed_multiset_checker_rejects_loss_null_type_order_and_absence() {
    let f = reference(&initial(), &Query::Source(0));
    assert_eq!(
        f.columns,
        vec![
            ("id".into(), "BIGINT".into()),
            ("amount".into(), "INT".into()),
            ("tag".into(), "STRING".into())
        ]
    );
    assert_eq!(f.modeled_properties, vec!["MANAGED", "delta"]);
    let mut r = f.clone();
    r.rows.remove(0);
    assert_eq!(check(&r, &f), Err("wrong-skip:rows"));
    let mut r = f.clone();
    r.rows[2].1 = Some(0);
    assert_eq!(check(&r, &f), Err("wrong-skip:rows"));
    let mut r = f.clone();
    r.columns[1].1 = "BIGINT".into();
    assert_eq!(check(&r, &f), Err("wrong-skip:schema"));
    let mut r = f.clone();
    r.columns.swap(0, 1);
    assert_eq!(check(&r, &f), Err("wrong-skip:schema"));
    assert_eq!(check(&absent(), &f), Err("wrong-skip:existence"));
    let mut r = f.clone();
    r.modeled_properties.clear();
    assert_eq!(check(&r, &f), Err("wrong-skip:modeled-properties"));
    let mut source = initial();
    let snapshot = execute(&source, &Query::Source(0));
    source[0].1 = Some(17);
    assert_eq!(snapshot.rows[0].1, Some(10));
    assert_eq!(
        check(&snapshot, &reference(&source, &Query::Source(0))),
        Err("wrong-skip:rows")
    );
    let mut reordered = f.clone();
    reordered.rows.reverse();
    assert_eq!(check(&reordered, &f), Ok(()));
    assert_eq!(check(&execute(&initial(), &Query::Source(0)), &f), Ok(()));
    assert_eq!(contract("BUILD", "SKIP"), Err("required-reuse:decision"));
}
#[test]
fn restricted_emitter_and_model_match_worked_symbolic_intent() {
    assert_eq!(
        render(&Query::Source(0), "c.s.i"),
        "select id, amount, tag from c.s.i"
    );
    assert_eq!(
        render(&Query::Source(3), "c.s.i"),
        "select id, cast(amount+3 as int) amount, tag from c.s.i"
    );
    assert_eq!(render(&Query::Literal(17),"ignored"),"select cast(id as bigint) id, cast(amount as int) amount, cast(tag as string) tag from values (1,17,'one'),(1,17,'one'),(2,cast(null as int),cast(null as string)) as declared(id,amount,tag)");
    let expected = vec![
        (1, Some(13), Some("one".into())),
        (1, Some(13), Some("one".into())),
        (2, None, None),
    ];
    assert_eq!(execute(&initial(), &Query::Source(3)).rows, expected);
    assert_eq!(reference(&initial(), &Query::Source(3)).rows, expected);
    assert_eq!(plan(2, false).len(), 44);
    assert_eq!(plan(2, true).len(), 6);
}

#[tokio::test]
async fn source_full_history_stays_in_org_named_namespace_and_relation_scope() {
    let nonce = uuid::Uuid::new_v4().simple().to_string();
    let org_a = format!("scope-a-{nonce}");
    let org_b = format!("scope-b-{nonce}");
    let ns_a = format!("namespace-a-{nonce}");
    let ns_b = format!("namespace-b-{nonce}");
    let source = "cdo_demo_db.generated.inputs";
    let target = "cdo_demo_db.generated.result";
    let epoch = 1_800_000_000_000_i64;
    let base = request(
        &Query::Source(0),
        source,
        target,
        &ns_a,
        epoch,
        Some(epoch + 1000),
    );
    let mut journal = Journal::new(
        &format!("scope-floor-{nonce}"),
        &json!({"trace_version":1,"generated_request_metadata":true,"mode":"generated-modeled-safety","hosted_verified":false,"actual_client":false,"real_warehouse_execution":false,"scope_nonce":nonce,"base_request":base,"operations":["org-a cold/build/reuse","org-b same request cold/build/reuse","namespace-b only namespace changed cold/build/reuse","relation-b only target changed cold/build/reuse"],"wrong_initial_rows":[[1,99,"one"]],"source_pins":pins()}),
    );
    let server = support::start_server_require_org().await;
    let channel = support::channel(server.addr).await;
    // Other scopes report exactly A's confirmed target epoch. Changing an
    // epoch here could hide a namespace leak behind External modification.
    let mut warmed_a = base.clone();
    warmed_a
        .tables
        .iter_mut()
        .find(|t| t.name == target)
        .unwrap()
        .last_modified_epoch = Some(epoch + 10_000);
    let mut ns_req = warmed_a.clone();
    ns_req.table_namespace = Some(ns_b);
    let mut relation_req = warmed_a.clone();
    relation_req.target_table = Some("cdo_demo_db.generated.other".into());
    relation_req
        .tables
        .iter_mut()
        .find(|t| t.name == target)
        .unwrap()
        .name = "cdo_demo_db.generated.other".into();
    for (cell_index, (cell, org, mut req)) in [
        ("org-a", org_a.clone(), base.clone()),
        ("org-b", org_b, warmed_a.clone()),
        ("namespace-b", org_a.clone(), ns_req),
        ("relation-b", org_a, relation_req),
    ]
    .into_iter()
    .enumerate()
    {
        // Starting physical target exists with positive known epoch but wrong
        // contents in this scope. A cross-scope SKIP must fail the data oracle.
        if cell == "org-b" {
            assert_eq!(req, warmed_a);
        }
        if cell == "namespace-b" {
            let mut same = req.clone();
            same.table_namespace = warmed_a.table_namespace.clone();
            assert_eq!(same, warmed_a);
        }
        let completion_epoch = epoch + 10_000 + cell_index as i64 * 10_000;
        let mut r = execute(&initial(), &Query::Literal(99));
        let f = reference(&initial(), &Query::Source(0));
        assert_eq!(check(&r, &f), Err("wrong-skip:rows"));
        for (index, required) in ["BUILD", "SKIP"].into_iter().enumerate() {
            journal.add(
                json!({"cell":cell,"org":org,"request":req,"required":required,"R_before":r,"F":f,"synthetic_clock":completion_epoch+1000+index as i64*5000}),
            );
            let response = rpc(
                &mut journal,
                "SubmitEnrichedSQL",
                qc::sql_client::SqlClient::new(channel.clone())
                    .submit_enriched_sql(wire(
                        req.clone(),
                        &org,
                        completion_epoch + 1000 + index as i64 * 5000,
                    ))
                    .await,
            );
            journal.add(json!({"cell":cell,"raw_response":response}));
            let (decision, id) = match response.response.as_ref() {
                Some(qc::submit_sql_response::Response::ReadyToExecute(ready)) => {
                    r = execute(&initial(), &Query::Source(0));
                    let confirm = built_at(completion_epoch).confirm(&ready.request_id);
                    journal.add(json!({"rpc":"ConfirmExecution","request":confirm,"synthetic_clock":completion_epoch+2000}));
                    let confirmed = rpc(
                        &mut journal,
                        "ConfirmExecution",
                        qc::execution_client::ExecutionClient::new(channel.clone())
                            .confirm_execution(wire(confirm, &org, completion_epoch + 2000))
                            .await,
                    );
                    journal.add(json!({"rpc":"ConfirmExecution","response":confirmed}));
                    assert!(confirmed.success);
                    let target = req.target_table.as_ref().unwrap();
                    req.tables
                        .iter_mut()
                        .find(|t| &t.name == target)
                        .unwrap()
                        .last_modified_epoch = Some(completion_epoch);
                    ("BUILD", ready.execution_decision_id.clone().unwrap())
                }
                Some(qc::submit_sql_response::Response::SkipExecution(skip)) => {
                    assert!(!skip.explained_decision.as_ref().unwrap().is_stale);
                    ("SKIP", skip.execution_decision_id.clone().unwrap())
                }
                _ => panic!("scope FULL unsupported: {response:?}"),
            };
            explain(channel.clone(), &org, id, &mut journal).await;
            let result = check(&r, &f).and_then(|()| contract(decision, required));
            journal.add(json!({"cell":cell,"index":index,"R":r,"F":f,"violation":result.err()}));
            result.unwrap();
        }
    }
}
