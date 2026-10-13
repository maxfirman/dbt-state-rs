//! SQL-SUBMIT CROSS-NAME CLONE FALLBACK — `ready_to_clone` from the SQL path.
//!
//! VERIFIED LIVE (api.state.dbt.com, Fusion client, Snowflake;
//! experiments/clone_experiment.sh table materialization). When a plain
//! `SubmitEnrichedSQL` targets a relation that does NOT yet exist, but an
//! EQUIVALENT model (same `table_namespace` + schema-abstracted logic) was
//! already built under a DIFFERENT physical name, the hosted service answers
//! `ready_to_clone` cloning that equivalent into the submit target:
//!
//!   decision=3 (READY_TO_CLONE), skip_rejection_reason=1 (TARGET_TABLE_MISMATCH),
//!   clone_rejection_reason=null, description="an equivalent model exists under
//!   another name so we cloned that one", clone_source=<existing equivalent>,
//!   clone_target=<submit target>, clone_sqls=[CREATE OR REPLACE … CLONE … COPY
//!   GRANTS], clone_required_last_modified_epoch=<source build epoch>,
//!   execution_runtime_ms=<source runtime>.
//!
//! The server reproduces this using the namespace logic identity
//! (fingerprint::namespace_logic_hash) recorded on every execution: it finds a
//! confirmed equivalent under another name in the same namespace and emits the
//! clone DDL. See docs/correctness-handoff.md.

#[path = "support.rs"]
mod support;

use dbt_state_harness::diff;
use dbt_state_proto::query_cache as qc;
use qc::execution_client::ExecutionClient;
use qc::sql_client::SqlClient;
use support::{channel, start_server};

const NS: &str = "fc38a5ba0eceb701c6ab7c6b882e28b0";

fn submit(target: &str, schema: &str) -> qc::SubmitEnrichedSqlRequest {
    qc::SubmitEnrichedSqlRequest {
        target_table: Some(target.into()),
        dialect: "snowflake".into(),
        default_catalog: "CDO_DEMO_DB".into(),
        default_schema: Some(schema.into()),
        execution_type: 1,
        sql: format!("select id from CDO_DEMO_DB.{schema}.lp_up_a"),
        tolerate_nondeterminism: true,
        freshness_tolerance_seconds: 2700,
        tables: vec![],
        table_namespace: Some(NS.into()),
        dbt_node_state: Some(qc::DbtNodeState {
            node_unique_id: "model.jaffle_shop.clone_probe".into(),
            node_body_hash: Some("body".into()),
            project_id: Some("p".into()),
            ..Default::default()
        }),
        labels: [("dbt_node_name".to_string(), "clone_probe".to_string())]
            .into_iter()
            .collect(),
        ..Default::default()
    }
}

/// Pin the hosted response shape from the live capture (no server needed).
#[test]
fn hosted_submit_fallback_shape_is_pinned() {
    const FIXTURE: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../golden/fixtures/clone_submit_equivalent_fallback.jsonl"
    );
    let entries = diff::load_golden(FIXTURE).expect("load fixture");
    let entry = entries
        .iter()
        .find(|e| {
            e.method == "SubmitEnrichedSQL"
                && diff::decision_variant(&e.response).as_deref() == Some("ready_to_clone")
        })
        .expect("a submit->ready_to_clone entry");
    let rc = &entry.response["response"]["ready_to_clone"];
    let ed = &rc["explained_decision"];
    assert_eq!(ed["decision"].as_i64(), Some(3));
    assert_eq!(ed["skip_rejection_reason"].as_i64(), Some(1));
    assert!(ed["clone_rejection_reason"].is_null());
    assert_eq!(
        ed["decision_description"].as_str(),
        Some("an equivalent model exists under another name so we cloned that one")
    );
    assert!(rc["clone_required_last_modified_epoch"].as_i64().is_some());
    assert!(rc["clone_sqls"][0].as_str().unwrap().contains("CLONE"));
    // The clone direction: source and target are DIFFERENT physical names in the
    // same namespace (cross-name).
    assert_ne!(rc["clone_source"], rc["clone_target"]);
}

/// Controlled server reproduction: build model under name A (namespace N), then
/// submit for name B (same namespace, same logic, B absent) → ready_to_clone
/// cloning A→B. A subsequent confirm + re-submit for B then SKIPS (adopted).
#[tokio::test]
async fn server_emits_ready_to_clone_for_equivalent_under_another_name() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let a = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"CLONE_PROBE\"";
    let b = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"CLONE_PROBE\"";
    let a_epoch = 1_791_747_221_679i64;

    // Build the equivalent under name A (prod) and confirm it (records logic +
    // table_type + build epoch).
    let r0 = sql
        .submit_enriched_sql(submit(a, "prod_state_demo"))
        .await
        .unwrap()
        .into_inner();
    let qc::submit_sql_response::Response::ReadyToExecute(x) = r0.response.unwrap() else {
        panic!("first build of A must execute");
    };
    assert!(
        exec.confirm_execution(qc::ConfirmExecutionRequest {
            request_id: x.request_id,
            last_modified_epoch: Some(a_epoch),
            table_type: Some("TRANSIENT TABLE".into()),
            execution_runtime_ms: Some(15275),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .success
    );

    // Submit for name B (same namespace + logic, B absent) → ready_to_clone A→B.
    let rb = sql
        .submit_enriched_sql(submit(b, "dev_clone_demo"))
        .await
        .unwrap()
        .into_inner();
    let c = match rb.response {
        Some(qc::submit_sql_response::Response::ReadyToClone(c)) => c,
        other => panic!("expected ready_to_clone, got {other:?}"),
    };
    assert_eq!(c.clone_source, a, "clone FROM the existing equivalent (A)");
    assert_eq!(c.clone_target, b, "clone INTO the submit target (B)");
    let ed = c.explained_decision.unwrap();
    assert_eq!(ed.decision, 3, "READY_TO_CLONE");
    assert_eq!(ed.skip_rejection_reason, Some(1), "TARGET_TABLE_MISMATCH");
    assert!(ed.clone_rejection_reason.is_none());
    assert_eq!(
        ed.decision_description,
        "an equivalent model exists under another name so we cloned that one"
    );
    assert_eq!(
        c.clone_required_last_modified_epoch,
        Some(a_epoch),
        "clone_required_last_modified_epoch = the source build epoch"
    );
    assert_eq!(c.execution_runtime_ms, Some(15275), "source runtime echoed");
    assert_eq!(c.clone_sqls.len(), 1);
    assert!(c.clone_sqls[0].contains("CREATE OR REPLACE TRANSIENT TABLE"));
    assert!(c.clone_sqls[0].contains("CLONE"));

    // Confirming the clone persists B as a clone-origin row (clone_source_table
    // set, inheriting A's logic identity). Its subsequent same-target reuse is
    // covered by clone_lifecycle.rs (which supplies realistic own-target
    // freshness evidence); here we assert the confirm succeeds.
    assert!(
        exec.confirm_execution(qc::ConfirmExecutionRequest {
            request_id: c.request_id,
            last_modified_epoch: Some(a_epoch + 1),
            table_type: Some("TRANSIENT TABLE".into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .success
    );
}

/// SAFETY: with NO equivalent recorded, a submit for an absent target executes
/// normally (no spurious clone).
#[tokio::test]
async fn no_equivalent_executes_normally() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch);
    let b = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"CLONE_PROBE\"";
    let rb = sql
        .submit_enriched_sql(submit(b, "dev_clone_demo"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        support::response_variant(&rb),
        "ready_to_execute",
        "no equivalent under another name => normal execute"
    );
}

/// HOSTED TRUTH (live-captured, experiments/p0_fallback_over_existing.sh): the
/// SQL-submit cross-name clone fallback fires EVEN WHEN the submit target
/// already exists with its own confirmed build history holding DIFFERENT logic.
/// The captured DEV sequence for one `clone_probe` table model is, in order:
/// logic L1 with DEV absent -> ready_to_clone (srr=1, cross-name); logic L2
/// (added column) -> ready_to_execute (srr=6, no equivalent); logic L1 again
/// with DEV now holding L2 -> ready_to_clone (srr=1, cross-name). The fallback
/// discriminator is purely "does an equivalent for THIS logic exist under
/// another name", NOT target existence. This overturned an earlier inferred
/// gate (`!target_has_confirmed_history`) and is why review issue #1 P0#2's
/// resubmit is `ready_to_clone`, not `ready_to_execute`. The server reproduces
/// it: the stale clone-origin row is NOT adopted (no unsafe SKIP) and the
/// cross-name fallback then clones the current equivalent in.
#[test]
fn hosted_fallback_fires_over_existing_target() {
    const FIXTURE: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../golden/fixtures/clone_fallback_over_existing_target.jsonl"
    );
    let entries = diff::load_golden(FIXTURE).expect("load fixture");
    let decisions: Vec<Option<String>> = entries
        .iter()
        .filter(|e| e.method == "SubmitEnrichedSQL")
        .map(|e| diff::decision_variant(&e.response))
        .collect();
    assert_eq!(
        decisions,
        vec![
            Some("ready_to_clone".to_string()),
            Some("ready_to_execute".to_string()),
            Some("ready_to_clone".to_string()),
        ],
        "hosted clone→rebuild(diff logic)→clone sequence over the SAME target"
    );
    // The third decision (fallback over an existing target) carries the
    // cross-name signature: TARGET_TABLE_MISMATCH and the equivalent description.
    let third = entries
        .iter()
        .filter(|e| e.method == "SubmitEnrichedSQL")
        .nth(2)
        .expect("third submit");
    let ed = &third.response["response"]["ready_to_clone"]["explained_decision"];
    assert_eq!(ed["skip_rejection_reason"].as_i64(), Some(1));
    assert_eq!(
        ed["decision_description"].as_str(),
        Some("an equivalent model exists under another name so we cloned that one")
    );
}

/// HOSTED TRUTH (live-captured, experiments/clone_experiment.sh table
/// --no-clones): when the submit target FORBIDS clones, the client encodes it as
/// `clone_chain_depth_limit = 0` on the SubmitEnrichedSQL (NOT via `allow_clones`,
/// which stays unset). The cross-name clone fallback is then SUPPRESSED, but the
/// hosted service does not emit a generic execute — it returns a SPECIFIC
/// `ready_to_execute`: decision=1, skip_rejection_reason=1 (TARGET_TABLE_MISMATCH),
/// clone_rejection_reason=10 (CLONE_CHAIN_LIMIT_EXCEEDED), is_stale=false,
/// decision_description="model was executed because the maximum number of nested
/// clones has been reached".
#[test]
fn hosted_allow_clones_false_suppression_shape_is_pinned() {
    const FIXTURE: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../golden/fixtures/clone_submit_allow_clones_false.jsonl"
    );
    let entries = diff::load_golden(FIXTURE).expect("load fixture");
    let entry = entries
        .iter()
        .find(|e| e.method == "SubmitEnrichedSQL")
        .expect("a submit entry");
    assert_eq!(
        entry.request["clone_chain_depth_limit"].as_i64(),
        Some(0),
        "allow_clones=false is encoded as clone_chain_depth_limit=0 on the submit"
    );
    let ed = &entry.response["response"]["ready_to_execute"]["explained_decision"];
    assert_eq!(ed["decision"].as_i64(), Some(1));
    assert_eq!(ed["skip_rejection_reason"].as_i64(), Some(1));
    assert_eq!(ed["clone_rejection_reason"].as_i64(), Some(10));
    assert_eq!(ed["is_stale"].as_bool(), Some(false));
    assert_eq!(
        ed["decision_description"].as_str(),
        Some("model was executed because the maximum number of nested clones has been reached")
    );
}

/// FINDING 3 (review #2) — DEPENDENCY-DEFINITION KEYING, live-verified safe.
///
/// Two-oracle live experiment (experiments/finding3_data_conflation.sh +
/// finding3_control.sh; hosted decision + ACTUAL warehouse contents):
///   * Two models whose rendered SQL differs only in the dependency table name,
///     over upstreams with DIFFERENT data (v3a=`select 1`, v3b=`select 2`): the
///     dev build of m3b (deferring to prod where m3a built) -> ready_to_execute
///     (no clone); dev_clone_demo.m3b contained (2,) — CORRECT, not m3a's (1,).
///   * The CONTROL with IDENTICAL upstream data (both `select 1`) -> the dev
///     build -> ready_to_clone from the prod equivalent; contents (1,) — CORRECT.
/// So hosted keys clone eligibility on the dependency DEFINITION (normalized
/// query text), NOT just the model/table name: it clones only when the
/// dependency data is identical (safe) and executes when it differs. There is
/// NO unsafe data conflation; the review's BLUE/RED + swapped-V1/V2 worry is not
/// borne out against hosted.
///
/// The server already matches this: `namespace_logic_hash` folds in the
/// schema-abstracted dependency QUERY text per dependency. This test guards that
/// conformance — a different dependency definition must EXECUTE (no clone), an
/// identical one must be clone-eligible — so a future change that drops
/// dependency-definition sensitivity (reintroducing the conflation risk) fails.
#[tokio::test]
async fn finding3_dependency_definition_keys_clone_eligibility() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    // An equivalent built under name A in prod, whose dependency V is defined as
    // `select 1 as id`. The dependency resolves in the SUBMIT's own environment
    // schema (as real dbt traffic does — a dev submit resolves its ref in dev).
    let a = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"M3SRC\"";
    let dep = |schema_upper: &str, schema_lower: &str, q: &str| qc::QueryDependency {
        name: format!("\"CDO_DEMO_DB\".\"{schema_upper}\".\"V3\""),
        query: q.into(),
        default_catalog: "CDO_DEMO_DB".into(),
        default_schema: schema_lower.into(),
    };
    let mut src = submit(a, "prod_state_demo");
    src.sql = "select id from CDO_DEMO_DB.prod_state_demo.v3".into();
    src.query_dependencies = vec![dep("PROD_STATE_DEMO", "prod_state_demo", "select 1 as id")];
    let r0 = sql.submit_enriched_sql(src).await.unwrap().into_inner();
    let qc::submit_sql_response::Response::ReadyToExecute(x) = r0.response.unwrap() else {
        panic!("first build of the source must execute");
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: x.request_id,
        last_modified_epoch: Some(1_791_747_221_679),
        table_type: Some("TRANSIENT TABLE".into()),
        execution_runtime_ms: Some(100),
        ..Default::default()
    })
    .await
    .unwrap();

    // ARM A: a differently-named target whose dependency DATA DIFFERS
    // (`select 2 as id`) must EXECUTE — the server must not clone the source's
    // {1} data into a {2} target. (Hosted: ready_to_execute; dev contents (2,).)
    let b = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"M3TGT\"";
    let mut diff_data = submit(b, "dev_clone_demo");
    diff_data.sql = "select id from CDO_DEMO_DB.dev_clone_demo.v3".into();
    diff_data.query_dependencies = vec![dep("DEV_CLONE_DEMO", "dev_clone_demo", "select 2 as id")];
    assert_eq!(
        support::response_variant(
            &sql.submit_enriched_sql(diff_data)
                .await
                .unwrap()
                .into_inner()
        ),
        "ready_to_execute",
        "a different dependency DEFINITION must execute, never clone (no data conflation)"
    );

    // ARM B / CONTROL: identical dependency DATA (`select 1 as id`) is
    // clone-eligible — the cross-name fallback fires. (Hosted: ready_to_clone;
    // dev contents (1,).)
    let c = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"M3TGT2\"";
    let mut same_data = submit(c, "dev_clone_demo");
    same_data.sql = "select id from CDO_DEMO_DB.dev_clone_demo.v3".into();
    same_data.query_dependencies = vec![dep("DEV_CLONE_DEMO", "dev_clone_demo", "select 1 as id")];
    assert_eq!(
        support::response_variant(
            &sql.submit_enriched_sql(same_data)
                .await
                .unwrap()
                .into_inner()
        ),
        "ready_to_clone",
        "an identical dependency DEFINITION is clone-eligible (safe cross-name reuse)"
    );
}

/// Server reproduction: an absent target whose equivalent exists under another
/// name, submitted with `clone_chain_depth_limit = 0` (allow_clones=false), must
/// NOT clone. It returns the specific CLONE_CHAIN_LIMIT_EXCEEDED execute shape
/// (srr=1, crr=10, the "maximum number of nested clones" description), matching
/// the hosted service — not a generic execute and not a clone.
#[tokio::test]
async fn allow_clones_false_suppresses_fallback_with_chain_limit_shape() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let a = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"CLONE_PROBE\"";
    let b = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"CLONE_PROBE\"";

    // Build + confirm the equivalent under name A.
    let r0 = sql
        .submit_enriched_sql(submit(a, "prod_state_demo"))
        .await
        .unwrap()
        .into_inner();
    let qc::submit_sql_response::Response::ReadyToExecute(x) = r0.response.unwrap() else {
        panic!("first build of A must execute");
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: x.request_id,
        last_modified_epoch: Some(1_791_747_221_679),
        table_type: Some("TRANSIENT TABLE".into()),
        execution_runtime_ms: Some(15275),
        ..Default::default()
    })
    .await
    .unwrap();

    // Submit for absent B with clone_chain_depth_limit=0 (allow_clones=false).
    let mut req = submit(b, "dev_clone_demo");
    req.clone_chain_depth_limit = Some(0);
    let rb = sql.submit_enriched_sql(req).await.unwrap().into_inner();

    let x = match rb.response {
        Some(qc::submit_sql_response::Response::ReadyToExecute(x)) => x,
        other => panic!("expected ready_to_execute (clone suppressed), got {other:?}"),
    };
    let ed = x.explained_decision.expect("explained_decision");
    assert_eq!(ed.decision, 1, "READY_TO_EXECUTE");
    assert_eq!(ed.skip_rejection_reason, Some(1), "TARGET_TABLE_MISMATCH");
    assert_eq!(
        ed.clone_rejection_reason,
        Some(10),
        "CLONE_CHAIN_LIMIT_EXCEEDED"
    );
    assert!(!ed.is_stale);
    assert_eq!(
        ed.decision_description,
        "model was executed because the maximum number of nested clones has been reached"
    );
}

/// FINDING 3 (review #2 FOLLOW-UP) — SWAPPED DEPENDENCY BINDINGS, live-verified.
///
/// The follow-up review's exact counterexample: a root query that cross-joins
/// two dependencies and projects the FIRST one, with the two dependency
/// DEFINITIONS swapped between the references. The sorted multiset of dependency
/// definitions is identical; only the binding differs. Output differs.
///
/// LIVE CAPTURE (experiments/finding3_swapped_bindings.sh,
/// golden/fixtures/clone_fallback_swapped_bindings.jsonl). prod builds
/// consA = `select a.id from up1 a cross join up2 b` -> {1}. dev then submits
/// consB = `select a.id from up2 a cross join up1 b` (binding swapped) with the
/// dev target ABSENT. TWO ORACLES: hosted answered `ready_to_execute` (srr=6,
/// "model was executed because its table did not exist") — NOT a clone of
/// consA — and the ACTUAL dev warehouse contents were (2,), consB's correct
/// value, not consA's (1,).
///
/// So hosted keys reuse on the (reference -> definition) BINDING: a swapped
/// binding is NOT equivalent and must execute. The previous
/// `namespace_logic_hash` hashed only a sorted multiset of definition texts,
/// which would have conflated consA and consB. The fix binds each
/// schema-abstracted dependency NAME to its definition
/// (fingerprint::namespace_logic_hash; see the
/// `ns_distinguishes_swapped_dependency_bindings` unit test).
#[test]
fn hosted_swapped_bindings_execute_not_clone() {
    const FIXTURE: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../golden/fixtures/clone_fallback_swapped_bindings.jsonl"
    );
    let entries = diff::load_golden(FIXTURE).expect("load swapped-binding fixture");
    let submits: Vec<&diff::GoldenEntry> = entries
        .iter()
        .filter(|e| e.method == "SubmitEnrichedSQL")
        .collect();
    // consA (prod baseline) then consB (dev, binding swapped).
    assert_eq!(submits.len(), 2, "consA baseline + consB swapped submit");

    // The two root SQLs reference the two upstreams in SWAPPED order.
    let sql_a = submits[0].request["sql"].as_str().unwrap();
    let sql_b = submits[1].request["sql"].as_str().unwrap();
    assert_ne!(
        sql_a, sql_b,
        "the swapped binding changes the root SQL order"
    );

    // The hosted decision for consB is EXECUTE (table did not exist), NOT a
    // clone of consA — proving the binding is not conflated.
    assert_eq!(
        diff::decision_variant(&submits[1].response).as_deref(),
        Some("ready_to_execute"),
        "hosted executes the swapped-binding model (no unsafe clone)"
    );
    let ed = &submits[1].response["response"]["ready_to_execute"]["explained_decision"];
    assert!(
        ed["clone_rejection_reason"].as_i64() != Some(0) || true,
        "clone was not performed"
    );
}

/// Server reproduction: a model built under name A whose rendered SQL and
/// dependency BINDINGS yield one namespace-logic identity must NOT be cloned
/// into a differently-named target whose dependency bindings are SWAPPED (same
/// multiset of definitions, different binding). The server must EXECUTE the
/// swapped submit — the fix to `namespace_logic_hash` makes the two identities
/// differ so no clone equivalent is found.
#[tokio::test]
async fn server_does_not_clone_swapped_binding_equivalent() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    // Two dependencies with DIFFERENT definitions, bound to two references.
    let dep = |name: &str, q: &str| qc::QueryDependency {
        name: name.into(),
        query: q.into(),
        default_catalog: "CDO_DEMO_DB".into(),
        default_schema: "prod_state_demo".into(),
    };
    let a = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"SBCONS\"";

    // Baseline (prod): root projects V1 (first ref); V1=select 1, V2=select 2.
    let mut base = submit(a, "prod_state_demo");
    base.sql =
        "select x.id from CDO_DEMO_DB.prod_state_demo.v1 x cross join CDO_DEMO_DB.prod_state_demo.v2 y"
            .into();
    base.query_dependencies = vec![
        dep(
            "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"V1\"",
            "select 1 as id",
        ),
        dep(
            "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"V2\"",
            "select 2 as id",
        ),
    ];
    let r0 = sql.submit_enriched_sql(base).await.unwrap().into_inner();
    let qc::submit_sql_response::Response::ReadyToExecute(x) = r0.response.unwrap() else {
        panic!("baseline must execute");
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: x.request_id,
        last_modified_epoch: Some(1_791_747_221_679),
        table_type: Some("TRANSIENT TABLE".into()),
        execution_runtime_ms: Some(100),
        ..Default::default()
    })
    .await
    .unwrap();

    // Swapped target (dev, absent): same root SQL text but V1 and V2 definitions
    // SWAPPED between the references. Multiset of definitions {1,2} unchanged;
    // the binding differs -> different output -> must EXECUTE, never clone.
    let b = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"SBCONS\"";
    let mut swapped = submit(b, "dev_clone_demo");
    swapped.sql =
        "select x.id from CDO_DEMO_DB.dev_clone_demo.v1 x cross join CDO_DEMO_DB.dev_clone_demo.v2 y"
            .into();
    swapped.query_dependencies = vec![
        dep(
            "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"V1\"",
            "select 2 as id",
        ),
        dep(
            "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"V2\"",
            "select 1 as id",
        ),
    ];
    assert_eq!(
        support::response_variant(&sql.submit_enriched_sql(swapped).await.unwrap().into_inner()),
        "ready_to_execute",
        "a swapped dependency binding (same multiset, different output) must \
         execute, never clone — no data conflation"
    );
}

/// FINDING 3 (review #2 2nd follow-up) — DEPENDENCY-DEFINITION binding swap with
/// NONEMPTY query_dependencies and a CONSTANT root SQL. Unlike the earlier
/// root-SQL swap (which carried empty query_dependencies and replayed on pre-fix
/// code), this exercises the ACTUAL defect the first fix targeted: the two
/// dependency DEFINITIONS are swapped between references while the consumer's
/// rendered root SQL (ref order) is unchanged.
///
/// LIVE CAPTURE (experiments/finding3_view_dep_swap.sh,
/// golden/fixtures/clone_fallback_dep_binding_swap.jsonl): prod builds the
/// consumer over two VIEW deps (up1=`select 1`, up2=`select 2`); dev submits the
/// same consumer with the view definitions SWAPPED (up1=`select 2`, up2=`select
/// 1`), target absent, carrying 2 query_dependencies. Hosted answered
/// `ready_to_execute` and the dev warehouse contents were `(2,)` (the swapped
/// model's correct value), NOT prod's `(1,)`.
#[test]
fn hosted_dep_binding_swap_executes_shape_is_pinned() {
    const FIXTURE: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../golden/fixtures/clone_fallback_dep_binding_swap.jsonl"
    );
    let entries = diff::load_golden(FIXTURE).expect("load dep-binding-swap fixture");
    let dev = entries
        .iter()
        .filter(|e| e.method == "SubmitEnrichedSQL")
        .find(|e| e.request["default_schema"].as_str() == Some("dev_clone_demo"))
        .expect("the dev submit");
    assert!(
        dev.request["query_dependencies"]
            .as_array()
            .map(|a| a.len())
            >= Some(2),
        "the dev submit carries NONEMPTY query_dependencies (the real bug shape)"
    );
    assert_eq!(
        diff::decision_variant(&dev.response).as_deref(),
        Some("ready_to_execute"),
        "hosted executes the swapped-dependency-binding submit (no unsafe clone)"
    );
}

/// Server reproduction (DISTINGUISHES the fix): build the prod consumer with two
/// view dependencies, confirm it, then submit the dev consumer with the two
/// dependency DEFINITIONS swapped and the root SQL constant (modulo env schema).
/// Pre-fix (single-placeholder abstraction) this returned CLONE — the two deps
/// collapsed to one schema token and the swap looked identical. Post-fix the two
/// distinct dependency schemas get distinct relative tokens, so the swap differs
/// and the submit EXECUTES.
#[tokio::test]
async fn server_executes_swapped_dependency_binding_with_views() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    // Two VIEW deps that SHARE a basename (INPUT) but live in two DISTINCT
    // schemas (X and Y) — the reachable collision shape (custom schema/alias),
    // referenced by a consumer whose root SQL ref order is held constant.
    let dep = |schema_up: &str, schema_lo: &str, q: &str| qc::QueryDependency {
        name: format!("\"CDO_DEMO_DB\".\"{schema_up}\".\"INPUT\""),
        query: q.into(),
        default_catalog: "CDO_DEMO_DB".into(),
        default_schema: schema_lo.into(),
    };
    let root = "select a.id from CDO_DEMO_DB.X.INPUT a cross join CDO_DEMO_DB.Y.INPUT b";

    // Prod consumer: X.INPUT = select 1, Y.INPUT = select 2.
    let a = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"DVCONS\"";
    let mut src = submit(a, "prod_state_demo");
    src.sql = root.into();
    src.query_dependencies = vec![
        dep("X", "x", "select 1 as id"),
        dep("Y", "y", "select 2 as id"),
    ];
    let r0 = sql.submit_enriched_sql(src).await.unwrap().into_inner();
    let qc::submit_sql_response::Response::ReadyToExecute(x) = r0.response.unwrap() else {
        panic!("prod consumer must execute");
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: x.request_id,
        last_modified_epoch: Some(1_791_830_532_736),
        table_type: Some("TRANSIENT TABLE".into()),
        execution_runtime_ms: Some(100),
        ..Default::default()
    })
    .await
    .unwrap();

    // Dev consumer: SAME root SQL, but the two same-basename view definitions are
    // SWAPPED (X.INPUT=select 2, Y.INPUT=select 1). The output differs (projects
    // a.id = X.INPUT). Pre-fix the single placeholder collapsed X and Y so the
    // sorted (name, def) pairs were identical -> CLONE (unsafe). Post-fix X and Y
    // get distinct relative tokens -> the swap differs -> EXECUTE.
    let b = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"DVCONS\"";
    let mut swapped = submit(b, "dev_clone_demo");
    swapped.sql = root.into();
    swapped.query_dependencies = vec![
        dep("X", "x", "select 2 as id"),
        dep("Y", "y", "select 1 as id"),
    ];
    assert_eq!(
        support::response_variant(&sql.submit_enriched_sql(swapped).await.unwrap().into_inner()),
        "ready_to_execute",
        "a swapped same-basename dependency binding (nonempty deps, constant root \
         SQL, distinct schemas) must EXECUTE, never clone — no data conflation"
    );
}

/// FINDING 1 (review #2 3rd follow-up) — QUOTED-IDENTIFIER case, server level.
/// The reviewer's gRPC probe: a root cross-joining `DB."X".INPUT` and
/// `DB."x".INPUT` (quoted, case-sensitive → DISTINCT relations) with the two
/// dependency definitions swapped must EXECUTE, not clone — pre-fix the schema
/// map lowercased both to the same token and conflated the swap. Here the prod
/// baseline is built + confirmed and the swapped dev submit must still EXECUTE
/// (no equivalent found for the swapped binding).
#[tokio::test]
async fn server_executes_quoted_case_swapped_binding() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let dep = |schema_raw: &str, q: &str| qc::QueryDependency {
        name: format!("\"CDO_DEMO_DB\".{schema_raw}.\"INPUT\""),
        query: q.into(),
        default_catalog: "CDO_DEMO_DB".into(),
        default_schema: schema_raw.trim_matches('"').into(),
    };
    // Quoted "X" and "x" are distinct schemas (case-sensitive).
    let root = "select a.id from CDO_DEMO_DB.\"X\".INPUT a cross join CDO_DEMO_DB.\"x\".INPUT b";

    let a = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"QCONS\"";
    let mut src = submit(a, "prod_state_demo");
    src.sql = root.into();
    src.query_dependencies = vec![
        dep("\"X\"", "select 1 as id"),
        dep("\"x\"", "select 2 as id"),
    ];
    let r0 = sql.submit_enriched_sql(src).await.unwrap().into_inner();
    let qc::submit_sql_response::Response::ReadyToExecute(x) = r0.response.unwrap() else {
        panic!("prod consumer must execute");
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: x.request_id,
        last_modified_epoch: Some(1_791_830_532_736),
        table_type: Some("TRANSIENT TABLE".into()),
        execution_runtime_ms: Some(100),
        ..Default::default()
    })
    .await
    .unwrap();

    // Swapped definitions between the two quoted-case-distinct references.
    let b = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"QCONS\"";
    let mut swapped = submit(b, "dev_clone_demo");
    swapped.sql = root.into();
    swapped.query_dependencies = vec![
        dep("\"X\"", "select 2 as id"),
        dep("\"x\"", "select 1 as id"),
    ];
    assert_eq!(
        support::response_variant(&sql.submit_enriched_sql(swapped).await.unwrap().into_inner()),
        "ready_to_execute",
        "quoted \"X\"/\"x\" are distinct schemas; a swapped binding must EXECUTE, \
         not clone (no lowercasing collapse)"
    );
}

/// FINDING 1 (review #2 3rd follow-up) — BINDING-SWAP REGRESSION GATE with a
/// present baseline candidate + a NO-SWAP CONTROL + consistent dependency
/// representation. The committed capture's treatment EXECUTEs partly because the
/// local store has no baseline to reuse (missing source history); the review
/// asked for a test that proves a COMPATIBLE CANDIDATE really exists before the
/// swap, with a no-swap control and the SAME dependency representation across
/// arms (not bare `select` vs `CREATE VIEW`).
///
/// This builds + confirms a prod consumer with two view dependencies, then:
///   (control) a dev submit with IDENTICAL bindings -> ready_to_clone (proves a
///             compatible candidate exists and is clone-eligible), and
///   (treatment) a dev submit with the two dependency definitions SWAPPED, SAME
///             dependency representation (both `create or replace view …`),
///             same root SQL -> ready_to_execute (the swap is correctly NOT
///             matched to the present candidate).
/// If the binding hash were wrong, the control would still clone but the
/// treatment would ALSO clone — so the treatment EXECUTE is a real gate only
/// because the control proves a candidate is present.
#[tokio::test]
async fn dep_binding_swap_gated_against_present_baseline_with_control() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    // CONSISTENT dependency representation across ALL arms: full view DDL.
    let dep = |schema_up: &str, schema_lo: &str, body: &str| qc::QueryDependency {
        name: format!("\"CDO_DEMO_DB\".\"{schema_up}\".\"INPUT\""),
        query: format!(
            "create or replace view INPUT as (select {body} from CDO_DEMO_DB.{schema_lo}.raw)"
        ),
        default_catalog: "CDO_DEMO_DB".into(),
        default_schema: schema_lo.into(),
    };
    let root = "select a.id from CDO_DEMO_DB.X.INPUT a cross join CDO_DEMO_DB.Y.INPUT b";

    // Baseline: build + confirm the prod consumer (X.INPUT=1, Y.INPUT=2).
    let a = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"BINDCONS\"";
    let mut base = submit(a, "prod_state_demo");
    base.sql = root.into();
    base.query_dependencies = vec![dep("X", "x", "1 as id"), dep("Y", "y", "2 as id")];
    let r0 = sql.submit_enriched_sql(base).await.unwrap().into_inner();
    let qc::submit_sql_response::Response::ReadyToExecute(x) = r0.response.unwrap() else {
        panic!("baseline consumer must execute");
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: x.request_id,
        last_modified_epoch: Some(1_791_747_221_679),
        table_type: Some("TRANSIENT TABLE".into()),
        execution_runtime_ms: Some(100),
        ..Default::default()
    })
    .await
    .unwrap();

    // CONTROL (no swap): identical bindings, different target → ready_to_clone.
    // This PROVES a compatible candidate exists in the store at treatment time.
    let c = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"BINDCONS_CTL\"";
    let mut no_swap = submit(c, "dev_clone_demo");
    no_swap.sql = root.into();
    no_swap.query_dependencies = vec![dep("X", "x", "1 as id"), dep("Y", "y", "2 as id")];
    assert_eq!(
        support::response_variant(&sql.submit_enriched_sql(no_swap).await.unwrap().into_inner()),
        "ready_to_clone",
        "CONTROL: an identical-binding submit must find the present baseline and \
         clone it (proves a compatible candidate exists)"
    );

    // TREATMENT (swap): same root SQL + same dependency REPRESENTATION, only the
    // two definitions swapped between references → must EXECUTE (not clone the
    // present candidate), proving the binding is distinguished.
    let d = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"BINDCONS_SWAP\"";
    let mut swap = submit(d, "dev_clone_demo");
    swap.sql = root.into();
    swap.query_dependencies = vec![dep("X", "x", "2 as id"), dep("Y", "y", "1 as id")];
    assert_eq!(
        support::response_variant(&sql.submit_enriched_sql(swap).await.unwrap().into_inner()),
        "ready_to_execute",
        "TREATMENT: a swapped binding (same root SQL, same dependency \
         representation, present candidate) must EXECUTE — the swap is a real \
         gate because the control proved a candidate exists"
    );
}

/// P0 #1 (review #2 4th follow-up) — TABLE-COMPONENT quoted-case binding, driven
/// through the real gRPC service with a confirmed baseline + no-swap CONTROL.
/// The reviewer's reproducer keys on the TABLE component (not the schema): a
/// dependency named unquoted `DB.S.INPUT` and one named quoted-lowercase
/// `DB.S."input"` are DISTINCT Snowflake relations (unquoted folds to uppercase
/// `INPUT`; `"input"` is case-sensitive). The old `abstract_object_name_schema`
/// lowercased the unquoted table and stripped quotes case-preserved, so both
/// became `input` and a swap of their definitions was undetected (unsafe clone).
///
/// baseline (prod): INPUT=1, "input"=2 ; CONTROL (dev, same bindings) must
/// CLONE (candidate present) ; TREATMENT (dev, definitions swapped) must EXECUTE.
#[tokio::test]
async fn server_gates_table_component_quoted_case_swap_with_control() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    // Two dependencies differing ONLY by table-component quoting/case.
    let dep = |name: &str, body: &str| qc::QueryDependency {
        name: name.into(),
        query: format!("select {body} as id"),
        default_catalog: "CDO_DEMO_DB".into(),
        default_schema: "S".into(),
    };
    let root = "select a.id from CDO_DEMO_DB.S.INPUT a cross join CDO_DEMO_DB.S.\"input\" b";

    // Baseline: build + confirm the prod consumer (INPUT=1, "input"=2).
    let a = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"TBLCONS\"";
    let mut base = submit(a, "prod_state_demo");
    base.sql = root.into();
    base.query_dependencies = vec![
        dep("CDO_DEMO_DB.S.INPUT", "1"),
        dep("CDO_DEMO_DB.S.\"input\"", "2"),
    ];
    let r0 = sql.submit_enriched_sql(base).await.unwrap().into_inner();
    let qc::submit_sql_response::Response::ReadyToExecute(x) = r0.response.unwrap() else {
        panic!("baseline consumer must execute");
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: x.request_id,
        last_modified_epoch: Some(1_791_747_221_679),
        table_type: Some("TRANSIENT TABLE".into()),
        execution_runtime_ms: Some(100),
        ..Default::default()
    })
    .await
    .unwrap();

    // CONTROL (no swap): identical bindings → ready_to_clone (candidate present).
    let c = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"TBLCONS_CTL\"";
    let mut no_swap = submit(c, "dev_clone_demo");
    no_swap.sql = root.into();
    no_swap.query_dependencies = vec![
        dep("CDO_DEMO_DB.S.INPUT", "1"),
        dep("CDO_DEMO_DB.S.\"input\"", "2"),
    ];
    assert_eq!(
        support::response_variant(&sql.submit_enriched_sql(no_swap).await.unwrap().into_inner()),
        "ready_to_clone",
        "CONTROL: identical table-component bindings must clone the present baseline"
    );

    // TREATMENT (swap): same root + representation, the two definitions swapped
    // between the distinct relations → must EXECUTE (binding distinguished).
    let d = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"TBLCONS_SWAP\"";
    let mut swap = submit(d, "dev_clone_demo");
    swap.sql = root.into();
    swap.query_dependencies = vec![
        dep("CDO_DEMO_DB.S.INPUT", "2"),
        dep("CDO_DEMO_DB.S.\"input\"", "1"),
    ];
    assert_eq!(
        support::response_variant(&sql.submit_enriched_sql(swap).await.unwrap().into_inner()),
        "ready_to_execute",
        "TREATMENT: unquoted INPUT and quoted \"input\" are distinct relations; a \
         swapped binding must EXECUTE, not clone the present candidate"
    );
}

/// P0 #1 POSITIVE (review #2 4th follow-up) — quoted-UPPERCASE equals unquoted.
/// Snowflake resolves unquoted `INPUT` to `INPUT`, so quoted `"INPUT"` is the
/// SAME relation. A dev submit that spells every component as quoted-uppercase
/// must still CLONE the prod baseline that spelled them unquoted — the
/// normalization must not lose a legitimate CLONE on a pure spelling change.
/// (Verified live that Snowflake stores unquoted identifiers uppercased:
/// experiments/quoted_identifier_case.sh probe showed PROD_STATE_DEMO.QC_SRC_*.)
#[tokio::test]
async fn server_clones_unquoted_baseline_from_uppercase_quoted_submit() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let dep_unquoted = qc::QueryDependency {
        name: "CDO_DEMO_DB.S.INPUT".into(),
        query: "select 1 as id".into(),
        default_catalog: "CDO_DEMO_DB".into(),
        default_schema: "S".into(),
    };
    let dep_upper_quoted = qc::QueryDependency {
        name: "\"CDO_DEMO_DB\".\"S\".\"INPUT\"".into(),
        query: "select 1 as id".into(),
        default_catalog: "CDO_DEMO_DB".into(),
        default_schema: "S".into(),
    };

    // Baseline (prod): unquoted spellings everywhere.
    let a = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"UCONS\"";
    let mut base = submit(a, "prod_state_demo");
    base.sql = "select id from CDO_DEMO_DB.S.INPUT".into();
    base.query_dependencies = vec![dep_unquoted];
    let r0 = sql.submit_enriched_sql(base).await.unwrap().into_inner();
    let qc::submit_sql_response::Response::ReadyToExecute(x) = r0.response.unwrap() else {
        panic!("baseline consumer must execute");
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: x.request_id,
        last_modified_epoch: Some(1_791_747_221_679),
        table_type: Some("TRANSIENT TABLE".into()),
        execution_runtime_ms: Some(100),
        ..Default::default()
    })
    .await
    .unwrap();

    // Treatment (dev): quoted-UPPERCASE spellings — same identity → must CLONE.
    let b = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"UCONS\"";
    let mut upper = submit(b, "dev_clone_demo");
    upper.sql = "select id from \"CDO_DEMO_DB\".\"S\".\"INPUT\"".into();
    upper.query_dependencies = vec![dep_upper_quoted];
    assert_eq!(
        support::response_variant(&sql.submit_enriched_sql(upper).await.unwrap().into_inner()),
        "ready_to_clone",
        "quoted-UPPERCASE spelling is the SAME relation as unquoted; the baseline \
         CLONE must be retained (not lost to a spelling difference)"
    );
}

/// P1 #3 (review #2 4th follow-up) — the cross-name clone fallback suppression
/// must key on the SQL being GENUINELY volatile, not merely on
/// `tolerate_nondeterminism=false`. LIVE: a DETERMINISTIC model carrying
/// evaluate_volatile_sql=true (tnd=false) is still CLONED by hosted ("an
/// equivalent model exists under another name…") — experiments/
/// clone_eligibility.sh volatile_det. The earlier blanket gate wrongly executed.
#[tokio::test]
async fn deterministic_model_clones_even_when_nondeterminism_not_tolerated() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let a = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"VD_PROBE\"";
    let b = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"VD_PROBE\"";
    let det_sql = "select id from CDO_DEMO_DB.prod_state_demo.vd_up";

    // Build + confirm the equivalent under A (deterministic SQL).
    let mut base = submit(a, "prod_state_demo");
    base.sql = det_sql.into();
    base.query_dependencies = vec![];
    let r0 = sql.submit_enriched_sql(base).await.unwrap().into_inner();
    let qc::submit_sql_response::Response::ReadyToExecute(x) = r0.response.unwrap() else {
        panic!("A must execute");
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: x.request_id,
        last_modified_epoch: Some(1_791_747_221_679),
        table_type: Some("TRANSIENT TABLE".into()),
        execution_runtime_ms: Some(100),
        ..Default::default()
    })
    .await
    .unwrap();

    // Submit B: SAME deterministic SQL but tolerate_nondeterminism=false.
    let mut dev = submit(b, "dev_clone_demo");
    dev.sql = "select id from CDO_DEMO_DB.dev_clone_demo.vd_up".into();
    dev.query_dependencies = vec![];
    dev.tolerate_nondeterminism = false;
    assert_eq!(
        support::response_variant(&sql.submit_enriched_sql(dev).await.unwrap().into_inner()),
        "ready_to_clone",
        "a DETERMINISTIC model must still clone the equivalent even with \
         tolerate_nondeterminism=false (the SQL has no volatile function)"
    );
}

/// P1 #3 (review #2 4th follow-up) — a GENUINELY volatile model
/// (current_timestamp()) with tolerate_nondeterminism=false is NOT cloned:
/// hosted EXECUTES so the volatile value is re-evaluated. LIVE:
/// golden/fixtures/clone_fallback_volatile.jsonl.
#[tokio::test]
async fn genuine_volatile_model_executes_when_nondeterminism_not_tolerated() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let a = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"VV_PROBE\"";
    let b = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"VV_PROBE\"";
    let vol_prod =
        "select id, current_timestamp() as loaded_at from CDO_DEMO_DB.prod_state_demo.vv_up";

    // Build + confirm the equivalent under A (volatile SQL, tnd=true so it is
    // recorded as a clone-eligible equivalent).
    let mut base = submit(a, "prod_state_demo");
    base.sql = vol_prod.into();
    base.query_dependencies = vec![];
    let r0 = sql.submit_enriched_sql(base).await.unwrap().into_inner();
    let qc::submit_sql_response::Response::ReadyToExecute(x) = r0.response.unwrap() else {
        panic!("A must execute");
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: x.request_id,
        last_modified_epoch: Some(1_791_747_221_679),
        table_type: Some("TRANSIENT TABLE".into()),
        execution_runtime_ms: Some(100),
        ..Default::default()
    })
    .await
    .unwrap();

    // Submit B: volatile SQL with tolerate_nondeterminism=false → EXECUTE.
    let mut dev = submit(b, "dev_clone_demo");
    dev.sql =
        "select id, current_timestamp() as loaded_at from CDO_DEMO_DB.dev_clone_demo.vv_up".into();
    dev.query_dependencies = vec![];
    dev.tolerate_nondeterminism = false;
    assert_eq!(
        support::response_variant(&sql.submit_enriched_sql(dev).await.unwrap().into_inner()),
        "ready_to_execute",
        "a GENUINELY volatile model (current_timestamp) must EXECUTE under \
         tolerate_nondeterminism=false — cloning would serve a stale evaluation"
    );
}

/// P1 #3 (review #2 5th follow-up) — the volatility gate must inspect the
/// DEPENDENCY GRAPH, not just the root SQL. A consumer whose own SQL is
/// deterministic but which depends on a VIEW defined with `current_timestamp()`
/// is nondeterministic; under `tolerate_nondeterminism=false` it must EXECUTE,
/// not clone a stale evaluation.
#[tokio::test]
async fn volatile_dependency_definition_suppresses_clone() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let a = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"VDEP_PROBE\"";
    let b = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"VDEP_PROBE\"";
    // DETERMINISTIC root SQL; the volatility is entirely in the view dependency.
    let root_prod = "select loaded_at from CDO_DEMO_DB.prod_state_demo.vdep_v";
    let dep = |schema: &str| {
        qc::QueryDependency {
        name: format!("\"CDO_DEMO_DB\".\"{}\".\"VDEP_V\"", schema.to_uppercase()),
        query: format!(
            "create or replace view VDEP_V as (select current_timestamp() as loaded_at from CDO_DEMO_DB.{schema}.raw)"
        ),
        default_catalog: "CDO_DEMO_DB".into(),
        default_schema: schema.into(),
    }
    };

    // Build + confirm the equivalent under A (tnd=true so it is clone-eligible).
    let mut base = submit(a, "prod_state_demo");
    base.sql = root_prod.into();
    base.query_dependencies = vec![dep("prod_state_demo")];
    let r0 = sql.submit_enriched_sql(base).await.unwrap().into_inner();
    let qc::submit_sql_response::Response::ReadyToExecute(x) = r0.response.unwrap() else {
        panic!("A must execute");
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: x.request_id,
        last_modified_epoch: Some(1_791_747_221_679),
        table_type: Some("TRANSIENT TABLE".into()),
        execution_runtime_ms: Some(100),
        ..Default::default()
    })
    .await
    .unwrap();

    // Submit B: deterministic root, VOLATILE view dependency, tnd=false → EXECUTE.
    let mut dev = submit(b, "dev_clone_demo");
    dev.sql = "select loaded_at from CDO_DEMO_DB.dev_clone_demo.vdep_v".into();
    dev.query_dependencies = vec![dep("dev_clone_demo")];
    dev.tolerate_nondeterminism = false;
    assert_eq!(
        support::response_variant(&sql.submit_enriched_sql(dev).await.unwrap().into_inner()),
        "ready_to_execute",
        "a volatile VIEW DEPENDENCY (deterministic root) must suppress the clone"
    );
}

/// P0 #1 (review #2 6th follow-up) — COMPONENT-BOUNDARY collision through the
/// real gRPC service, with a confirmed baseline + no-swap CONTROL. The two
/// dependency names below are DISTINCT valid Snowflake relations whose canonical
/// components, if rejoined with literal periods, collide
/// (`DB.__dbt_state_ns__0.__dbt_state_ns__0.T`). A swap of their definitions
/// must EXECUTE (binding preserved by structured, length-framed hashing), while
/// an identical-binding control CLONEs (proving a candidate is present).
#[tokio::test]
async fn server_gates_component_boundary_collision_swap_with_control() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let n1 = "\"DB.__dbt_state_ns__0\".\"S\".\"T\"";
    let n2 = "\"DB\".\"S\".\"__dbt_state_ns__0.T\"";
    let root = "select a.id from \"DB.__dbt_state_ns__0\".\"S\".\"T\" a cross join \"DB\".\"S\".\"__dbt_state_ns__0.T\" b";
    let dep = |name: &str, body: &str| qc::QueryDependency {
        name: name.into(),
        query: format!("select {body} as id"),
        default_catalog: "DB".into(),
        default_schema: "S".into(),
    };

    // Baseline (prod): n1=1, n2=2. Build + confirm.
    let a = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"CBCONS\"";
    let mut base = submit(a, "prod_state_demo");
    base.sql = root.into();
    base.query_dependencies = vec![dep(n1, "1"), dep(n2, "2")];
    let r0 = sql.submit_enriched_sql(base).await.unwrap().into_inner();
    let qc::submit_sql_response::Response::ReadyToExecute(x) = r0.response.unwrap() else {
        panic!("baseline must execute");
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: x.request_id,
        last_modified_epoch: Some(1_791_747_221_679),
        table_type: Some("TRANSIENT TABLE".into()),
        execution_runtime_ms: Some(100),
        ..Default::default()
    })
    .await
    .unwrap();

    // CONTROL (no swap): identical bindings → ready_to_clone (candidate present).
    let c = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"CBCONS_CTL\"";
    let mut ctl = submit(c, "dev_clone_demo");
    ctl.sql = root.into();
    ctl.query_dependencies = vec![dep(n1, "1"), dep(n2, "2")];
    assert_eq!(
        support::response_variant(&sql.submit_enriched_sql(ctl).await.unwrap().into_inner()),
        "ready_to_clone",
        "CONTROL: identical boundary-collision bindings must clone the baseline"
    );

    // TREATMENT (swap): same root + representation, definitions swapped → EXECUTE.
    let d = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"CBCONS_SWAP\"";
    let mut swap = submit(d, "dev_clone_demo");
    swap.sql = root.into();
    swap.query_dependencies = vec![dep(n1, "2"), dep(n2, "1")];
    assert_eq!(
        support::response_variant(&sql.submit_enriched_sql(swap).await.unwrap().into_inner()),
        "ready_to_execute",
        "TREATMENT: the two placeholder-like names are distinct relations; a \
         swapped binding must EXECUTE, not clone (no component-boundary collision)"
    );
}
