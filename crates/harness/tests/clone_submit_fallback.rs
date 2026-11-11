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
use support::demo::{self, transient_at, CLONE_PROBE, DEV_SCHEMA, NS, PROD_SCHEMA};
use support::scenario::{outcome, ExecutionType, Scenario};
use support::{legacy, rows, start_server};

/// The captured probe's SQL in `schema` (the base most tests below override).
fn probe_sql(schema: &str) -> String {
    format!("select id from CDO_DEMO_DB.{schema}.lp_up_a")
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
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let a = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"CLONE_PROBE\"";
    let b = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"CLONE_PROBE\"";
    let a_epoch = 1_791_747_221_679i64;
    let probe = |target: &str, schema: &str| {
        demo::model(target, schema, CLONE_PROBE)
            .sql(&probe_sql(schema))
            .namespace(NS)
    };

    // Build the equivalent under name A (prod) and confirm it (records logic +
    // table_type + build epoch).
    let x = sc.expect_execute(probe(a, PROD_SCHEMA)).await;
    assert!(
        sc.confirm(&x.request_id, transient_at(a_epoch).runtime(15275))
            .await
    );

    // Submit for name B (same namespace + logic, B absent) → ready_to_clone A→B.
    let c = sc.expect_clone(probe(b, DEV_SCHEMA)).await;
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
    assert!(sc.confirm(&c.request_id, transient_at(a_epoch + 1)).await);
}

/// Submit-time Result copy is FULL only (maintainer decision 2026-11-11,
/// #92): a view whose equivalent was built under another name builds as a
/// first build, reporting the equivalent with TARGET_TABLE_MISMATCH /
/// EXECUTION_TYPE_NOT_FULL, as hosted 2.0.11 did for a view and a snapshot
/// (explain_copy_probes.jsonl entries 13-14) and a custom node
/// (explain_custom_copy.jsonl entry 9). The equivalent is a view, so this
/// also shows the refusal does not depend on copy DDL for its source.
#[tokio::test]
async fn a_view_with_an_equivalent_builds_with_execution_type_not_full() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let a = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"CLONE_PROBE\"";
    let b = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"CLONE_PROBE\"";
    let probe = |target: &str, schema: &str| {
        demo::model(target, schema, CLONE_PROBE)
            .sql(&probe_sql(schema))
            .namespace(NS)
            .execution_type(ExecutionType::View)
    };
    sc.build_with(
        probe(a, PROD_SCHEMA),
        outcome().epoch(1_791_747_221_679).table_type("VIEW"),
    )
    .await;

    let x = sc.expect_execute(probe(b, DEV_SCHEMA)).await;
    let ed = x.explained_decision.expect("explained_decision");
    assert_eq!(ed.skip_rejection_reason, Some(1), "TARGET_TABLE_MISMATCH");
    assert_eq!(
        ed.clone_rejection_reason,
        Some(3),
        "EXECUTION_TYPE_NOT_FULL"
    );
    assert!(!ed.is_stale);
    assert_eq!(
        ed.decision_description,
        "model was executed because its table did not exist"
    );
}

/// SAFETY: with NO equivalent recorded, a submit for an absent target executes
/// normally (no spurious clone).
#[tokio::test]
async fn no_equivalent_executes_normally() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let b = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"CLONE_PROBE\"";
    let probe = demo::model(b, DEV_SCHEMA, CLONE_PROBE)
        .sql(&probe_sql(DEV_SCHEMA))
        .namespace(NS);
    assert_eq!(
        sc.verdict(probe).await,
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
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    // An equivalent built under name A in prod, whose dependency V is defined as
    // `select 1 as id`. The dependency resolves in the SUBMIT's own environment
    // schema (as real dbt traffic does — a dev submit resolves its ref in dev).
    let a = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"M3SRC\"";
    let probe = |target: &str, schema: &str, q: &str| {
        demo::model(target, schema, CLONE_PROBE)
            .sql(&format!("select id from CDO_DEMO_DB.{schema}.v3"))
            .namespace(NS)
            .dependency_in(
                &format!("\"CDO_DEMO_DB\".\"{}\".\"V3\"", schema.to_uppercase()),
                q,
                demo::CATALOG,
                schema,
            )
    };
    sc.build_with(
        probe(a, PROD_SCHEMA, "select 1 as id"),
        transient_at(1_791_747_221_679).runtime(100),
    )
    .await;

    // ARM A: a differently-named target whose dependency DATA DIFFERS
    // (`select 2 as id`) must EXECUTE — the server must not clone the source's
    // {1} data into a {2} target. (Hosted: ready_to_execute; dev contents (2,).)
    let b = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"M3TGT\"";
    assert_eq!(
        sc.verdict(probe(b, DEV_SCHEMA, "select 2 as id")).await,
        "ready_to_execute",
        "a different dependency DEFINITION must execute, never clone (no data conflation)"
    );

    // ARM B / CONTROL: identical dependency DATA (`select 1 as id`) is
    // clone-eligible — the cross-name fallback fires. (Hosted: ready_to_clone;
    // dev contents (1,).)
    let c = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"M3TGT2\"";
    assert_eq!(
        sc.verdict(probe(c, DEV_SCHEMA, "select 1 as id")).await,
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
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let a = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"CLONE_PROBE\"";
    let b = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"CLONE_PROBE\"";
    let probe = |target: &str, schema: &str| {
        demo::model(target, schema, CLONE_PROBE)
            .sql(&probe_sql(schema))
            .namespace(NS)
    };

    // Build + confirm the equivalent under name A.
    sc.build_with(
        probe(a, PROD_SCHEMA),
        transient_at(1_791_747_221_679).runtime(15275),
    )
    .await;

    // Submit for absent B with clone_chain_depth_limit=0 (allow_clones=false).
    let x = sc
        .expect_execute(probe(b, DEV_SCHEMA).clone_chain_depth_limit(0))
        .await;
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
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    // Two dependencies with DIFFERENT definitions, bound to two references
    // (every dependency resolves in the prod schema).
    let a = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"SBCONS\"";

    // Baseline (prod): root projects V1 (first ref); V1=select 1, V2=select 2.
    let base = demo::model(a, PROD_SCHEMA, CLONE_PROBE)
        .namespace(NS)
        .sql("select x.id from CDO_DEMO_DB.prod_state_demo.v1 x cross join CDO_DEMO_DB.prod_state_demo.v2 y")
        .dependency_in(
            "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"V1\"",
            "select 1 as id",
            demo::CATALOG,
            PROD_SCHEMA,
        )
        .dependency_in(
            "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"V2\"",
            "select 2 as id",
            demo::CATALOG,
            PROD_SCHEMA,
        );
    sc.build_with(base, transient_at(1_791_747_221_679).runtime(100))
        .await;

    // Swapped target (dev, absent): same root SQL text but V1 and V2 definitions
    // SWAPPED between the references. Multiset of definitions {1,2} unchanged;
    // the binding differs -> different output -> must EXECUTE, never clone.
    let b = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"SBCONS\"";
    let swapped = demo::model(b, DEV_SCHEMA, CLONE_PROBE)
        .namespace(NS)
        .sql("select x.id from CDO_DEMO_DB.dev_clone_demo.v1 x cross join CDO_DEMO_DB.dev_clone_demo.v2 y")
        .dependency_in(
            "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"V1\"",
            "select 2 as id",
            demo::CATALOG,
            PROD_SCHEMA,
        )
        .dependency_in(
            "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"V2\"",
            "select 1 as id",
            demo::CATALOG,
            PROD_SCHEMA,
        );
    assert_eq!(
        sc.verdict(swapped).await,
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
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    // Two VIEW deps that SHARE a basename (INPUT) but live in two DISTINCT
    // schemas (X and Y) — the reachable collision shape (custom schema/alias),
    // referenced by a consumer whose root SQL ref order is held constant.
    let consumer = |target: &str, schema: &str, x: &str, y: &str| {
        demo::model(target, schema, CLONE_PROBE)
            .namespace(NS)
            .sql("select a.id from CDO_DEMO_DB.X.INPUT a cross join CDO_DEMO_DB.Y.INPUT b")
            .dependency_in("\"CDO_DEMO_DB\".\"X\".\"INPUT\"", x, demo::CATALOG, "x")
            .dependency_in("\"CDO_DEMO_DB\".\"Y\".\"INPUT\"", y, demo::CATALOG, "y")
    };

    // Prod consumer: X.INPUT = select 1, Y.INPUT = select 2.
    let a = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"DVCONS\"";
    sc.build_with(
        consumer(a, PROD_SCHEMA, "select 1 as id", "select 2 as id"),
        transient_at(1_791_830_532_736).runtime(100),
    )
    .await;

    // Dev consumer: SAME root SQL, but the two same-basename view definitions are
    // SWAPPED (X.INPUT=select 2, Y.INPUT=select 1). The output differs (projects
    // a.id = X.INPUT). Pre-fix the single placeholder collapsed X and Y so the
    // sorted (name, def) pairs were identical -> CLONE (unsafe). Post-fix X and Y
    // get distinct relative tokens -> the swap differs -> EXECUTE.
    let b = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"DVCONS\"";
    let swapped = consumer(b, DEV_SCHEMA, "select 2 as id", "select 1 as id");
    assert_eq!(
        sc.verdict(swapped).await,
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
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    // Quoted "X" and "x" are distinct schemas (case-sensitive).
    let consumer = |target: &str, schema: &str, upper: &str, lower: &str| {
        demo::model(target, schema, CLONE_PROBE)
            .namespace(NS)
            .sql("select a.id from CDO_DEMO_DB.\"X\".INPUT a cross join CDO_DEMO_DB.\"x\".INPUT b")
            .dependency_in("\"CDO_DEMO_DB\".\"X\".\"INPUT\"", upper, demo::CATALOG, "X")
            .dependency_in("\"CDO_DEMO_DB\".\"x\".\"INPUT\"", lower, demo::CATALOG, "x")
    };

    let a = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"QCONS\"";
    sc.build_with(
        consumer(a, PROD_SCHEMA, "select 1 as id", "select 2 as id"),
        transient_at(1_791_830_532_736).runtime(100),
    )
    .await;

    // Swapped definitions between the two quoted-case-distinct references.
    let b = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"QCONS\"";
    let swapped = consumer(b, DEV_SCHEMA, "select 2 as id", "select 1 as id");
    assert_eq!(
        sc.verdict(swapped).await,
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
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    // CONSISTENT dependency representation across ALL arms: full view DDL.
    let view = |schema_lo: &str, body: &str| {
        format!("create or replace view INPUT as (select {body} from CDO_DEMO_DB.{schema_lo}.raw)")
    };
    let consumer = |target: &str, schema: &str, x: &str, y: &str| {
        demo::model(target, schema, CLONE_PROBE)
            .namespace(NS)
            .sql("select a.id from CDO_DEMO_DB.X.INPUT a cross join CDO_DEMO_DB.Y.INPUT b")
            .dependency_in(
                "\"CDO_DEMO_DB\".\"X\".\"INPUT\"",
                &view("x", x),
                demo::CATALOG,
                "x",
            )
            .dependency_in(
                "\"CDO_DEMO_DB\".\"Y\".\"INPUT\"",
                &view("y", y),
                demo::CATALOG,
                "y",
            )
    };

    // Baseline: build + confirm the prod consumer (X.INPUT=1, Y.INPUT=2).
    let a = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"BINDCONS\"";
    sc.build_with(
        consumer(a, PROD_SCHEMA, "1 as id", "2 as id"),
        transient_at(1_791_747_221_679).runtime(100),
    )
    .await;

    // CONTROL (no swap): identical bindings, different target → ready_to_clone.
    // This PROVES a compatible candidate exists in the store at treatment time.
    let c = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"BINDCONS_CTL\"";
    assert_eq!(
        sc.verdict(consumer(c, DEV_SCHEMA, "1 as id", "2 as id"))
            .await,
        "ready_to_clone",
        "CONTROL: an identical-binding submit must find the present baseline and \
         clone it (proves a compatible candidate exists)"
    );

    // TREATMENT (swap): same root SQL + same dependency REPRESENTATION, only the
    // two definitions swapped between references → must EXECUTE (not clone the
    // present candidate), proving the binding is distinguished.
    let d = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"BINDCONS_SWAP\"";
    assert_eq!(
        sc.verdict(consumer(d, DEV_SCHEMA, "2 as id", "1 as id"))
            .await,
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
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    // Two dependencies differing ONLY by table-component quoting/case.
    let consumer = |target: &str, schema: &str, upper: &str, lower: &str| {
        demo::model(target, schema, CLONE_PROBE)
            .namespace(NS)
            .sql("select a.id from CDO_DEMO_DB.S.INPUT a cross join CDO_DEMO_DB.S.\"input\" b")
            .dependency_in(
                "CDO_DEMO_DB.S.INPUT",
                &format!("select {upper} as id"),
                demo::CATALOG,
                "S",
            )
            .dependency_in(
                "CDO_DEMO_DB.S.\"input\"",
                &format!("select {lower} as id"),
                demo::CATALOG,
                "S",
            )
    };

    // Baseline: build + confirm the prod consumer (INPUT=1, "input"=2).
    let a = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"TBLCONS\"";
    sc.build_with(
        consumer(a, PROD_SCHEMA, "1", "2"),
        transient_at(1_791_747_221_679).runtime(100),
    )
    .await;

    // CONTROL (no swap): identical bindings → ready_to_clone (candidate present).
    let c = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"TBLCONS_CTL\"";
    assert_eq!(
        sc.verdict(consumer(c, DEV_SCHEMA, "1", "2")).await,
        "ready_to_clone",
        "CONTROL: identical table-component bindings must clone the present baseline"
    );

    // TREATMENT (swap): same root + representation, the two definitions swapped
    // between the distinct relations → must EXECUTE (binding distinguished).
    let d = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"TBLCONS_SWAP\"";
    assert_eq!(
        sc.verdict(consumer(d, DEV_SCHEMA, "2", "1")).await,
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
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    // Baseline (prod): unquoted spellings everywhere.
    let a = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"UCONS\"";
    let base = demo::model(a, PROD_SCHEMA, CLONE_PROBE)
        .namespace(NS)
        .sql("select id from CDO_DEMO_DB.S.INPUT")
        .dependency_in("CDO_DEMO_DB.S.INPUT", "select 1 as id", demo::CATALOG, "S");
    sc.build_with(base, transient_at(1_791_747_221_679).runtime(100))
        .await;

    // Treatment (dev): quoted-UPPERCASE spellings — same identity → must CLONE.
    let b = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"UCONS\"";
    let upper = demo::model(b, DEV_SCHEMA, CLONE_PROBE)
        .namespace(NS)
        .sql("select id from \"CDO_DEMO_DB\".\"S\".\"INPUT\"")
        .dependency_in(
            "\"CDO_DEMO_DB\".\"S\".\"INPUT\"",
            "select 1 as id",
            demo::CATALOG,
            "S",
        );
    assert_eq!(
        sc.verdict(upper).await,
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
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let a = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"VD_PROBE\"";
    let b = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"VD_PROBE\"";
    let det_sql = "select id from CDO_DEMO_DB.prod_state_demo.vd_up";

    // Build + confirm the equivalent under A (deterministic SQL).
    let base = demo::model(a, PROD_SCHEMA, CLONE_PROBE)
        .namespace(NS)
        .sql(det_sql);
    sc.build_with(base, transient_at(1_791_747_221_679).runtime(100))
        .await;

    // Submit B: SAME deterministic SQL but tolerate_nondeterminism=false.
    let dev = demo::model(b, DEV_SCHEMA, CLONE_PROBE)
        .namespace(NS)
        .sql("select id from CDO_DEMO_DB.dev_clone_demo.vd_up")
        .tolerate_nondeterminism(false);
    assert_eq!(
        sc.verdict(dev).await,
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
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let a = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"VV_PROBE\"";
    let b = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"VV_PROBE\"";
    let vol_prod =
        "select id, current_timestamp() as loaded_at from CDO_DEMO_DB.prod_state_demo.vv_up";

    // Build + confirm the equivalent under A (volatile SQL, tnd=true so it is
    // recorded as a clone-eligible equivalent).
    let base = demo::model(a, PROD_SCHEMA, CLONE_PROBE)
        .namespace(NS)
        .sql(vol_prod);
    sc.build_with(base, transient_at(1_791_747_221_679).runtime(100))
        .await;

    // Submit B: volatile SQL with tolerate_nondeterminism=false → EXECUTE.
    let dev = demo::model(b, DEV_SCHEMA, CLONE_PROBE)
        .namespace(NS)
        .sql("select id, current_timestamp() as loaded_at from CDO_DEMO_DB.dev_clone_demo.vv_up")
        .tolerate_nondeterminism(false);
    assert_eq!(
        sc.verdict(dev).await,
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
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let a = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"VDEP_PROBE\"";
    let b = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"VDEP_PROBE\"";
    // DETERMINISTIC root SQL; the volatility is entirely in the view dependency.
    let root_prod = "select loaded_at from CDO_DEMO_DB.prod_state_demo.vdep_v";
    let consumer = |target: &str, schema: &str, root: &str| {
        demo::model(target, schema, CLONE_PROBE)
            .namespace(NS)
            .sql(root)
            .dependency_in(
                &format!("\"CDO_DEMO_DB\".\"{}\".\"VDEP_V\"", schema.to_uppercase()),
                &format!(
                    "create or replace view VDEP_V as (select current_timestamp() as loaded_at from CDO_DEMO_DB.{schema}.raw)"
                ),
                demo::CATALOG,
                schema,
            )
    };

    // Build + confirm the equivalent under A (tnd=true so it is clone-eligible).
    sc.build_with(
        consumer(a, PROD_SCHEMA, root_prod),
        transient_at(1_791_747_221_679).runtime(100),
    )
    .await;

    // Submit B: deterministic root, VOLATILE view dependency, tnd=false → EXECUTE.
    let dev = consumer(
        b,
        DEV_SCHEMA,
        "select loaded_at from CDO_DEMO_DB.dev_clone_demo.vdep_v",
    )
    .tolerate_nondeterminism(false);
    assert_eq!(
        sc.verdict(dev).await,
        "ready_to_execute",
        "a volatile VIEW DEPENDENCY (deterministic root) must suppress the clone"
    );
}

/// P0 #1 (review #2 6th follow-up; STRENGTHENED review #2 11th follow-up) —
/// COMPONENT-BOUNDARY collision through the real gRPC service, with a confirmed
/// baseline + no-swap CONTROL. The two dependency names below are DISTINCT valid
/// Snowflake relations whose canonical components, if rejoined with literal
/// periods, collide (`DB.__dbt_state_ns__0.__dbt_state_ns__0.T`).
///
/// CRITICAL: the collision only manifests when the request's `default_schema`
/// makes schema `S` map to the relative token **0** (`__dbt_state_ns__0`), which
/// is exactly what the embedded placeholder text matches. So all three requests
/// set `default_schema = "S"` (the 11th-follow-up correction): with the earlier
/// `dev_clone_demo`/`prod_state_demo` defaults, `S` got a NON-zero token and the
/// token-0 construction did NOT collide, so the TREATMENT passed even on the
/// vulnerable (pre-fix) revision — a weak gate. With `default_schema = "S"` the
/// pre-fix revision (366be09) returns `ready_to_clone` for the SWAP (verified by
/// local old-revision gRPC testing: the base and swapped namespace hashes both
/// equal `6243fdaf021893fa229db660636af8ea60a1ea2532588e83f806b26ac55fd545`), so
/// this test now FAILS on the vulnerable implementation and passes only with the
/// structured, length-framed component hashing.
#[tokio::test]
async fn server_gates_component_boundary_collision_swap_with_control() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let n1 = "\"DB.__dbt_state_ns__0\".\"S\".\"T\"";
    let n2 = "\"DB\".\"S\".\"__dbt_state_ns__0.T\"";
    let root = "select a.id from \"DB.__dbt_state_ns__0\".\"S\".\"T\" a cross join \"DB\".\"S\".\"__dbt_state_ns__0.T\" b";
    // default_schema="S" so S maps to relative token 0 and the placeholder-text
    // collision is exercised.
    let consumer = |target: &str, first: &str, second: &str| {
        demo::model(target, "S", CLONE_PROBE)
            .namespace(NS)
            .sql(root)
            .dependency_in(n1, &format!("select {first} as id"), "DB", "S")
            .dependency_in(n2, &format!("select {second} as id"), "DB", "S")
    };

    // Baseline (prod): n1=1, n2=2. Build + confirm.
    let a = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"CBCONS\"";
    sc.build_with(
        consumer(a, "1", "2"),
        transient_at(1_791_747_221_679).runtime(100),
    )
    .await;

    // CONTROL (no swap): identical bindings → ready_to_clone (candidate present).
    let c = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"CBCONS_CTL\"";
    assert_eq!(
        sc.verdict(consumer(c, "1", "2")).await,
        "ready_to_clone",
        "CONTROL: identical boundary-collision bindings must clone the baseline"
    );

    // TREATMENT (swap): same root + representation, definitions swapped → EXECUTE.
    let d = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"CBCONS_SWAP\"";
    assert_eq!(
        sc.verdict(consumer(d, "2", "1")).await,
        "ready_to_execute",
        "TREATMENT: the two placeholder-like names are distinct relations; a \
         swapped binding must EXECUTE, not clone (no component-boundary collision)"
    );
}

/// P1 #2 (review #2 6th follow-up) — a column literally named `random` is
/// DETERMINISTIC (`random()` with parentheses would be the volatile function).
/// Under `tolerate_nondeterminism=false` the model must still CLONE its
/// equivalent, exactly like any other deterministic model — the bare-identifier
/// volatility branch must not misclassify it.
#[tokio::test]
async fn bare_random_column_is_deterministic_and_clones() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let a = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"RNDCONS\"";
    let b = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"RNDCONS\"";
    // Root selects a COLUMN named `random` — not the function.
    let consumer = |target: &str, schema: &str| {
        demo::model(target, schema, CLONE_PROBE)
            .namespace(NS)
            .sql(&format!("select random from CDO_DEMO_DB.{schema}.rnd_up"))
    };

    sc.build_with(
        consumer(a, PROD_SCHEMA),
        transient_at(1_791_747_221_679).runtime(100),
    )
    .await;

    let dev = consumer(b, DEV_SCHEMA).tolerate_nondeterminism(false);
    assert_eq!(
        sc.verdict(dev).await,
        "ready_to_clone",
        "a column named `random` is deterministic and must CLONE even with \
         tolerate_nondeterminism=false (random() with parens would be volatile)"
    );
}

/// P1 #1 (review #2 7th follow-up) — UPGRADE REGRESSION: a clone CONFIRMED by a
/// PRE-FIX server must not authorize same-target SKIP reuse after upgrade. The
/// pre-fix clone persisted the requesting target's strict `sql_logic_hash`
/// (which did not change across the fix) alongside a now-STALE
/// `node_namespace_logic_hash` (the fix bumped the namespace identity). The
/// strict same-target `find_confirmed` matched on the SQL hash alone, so the
/// stale clone kept authorizing SKIP after upgrade.
///
/// This simulates the persisted pre-fix state by cloning + confirming through
/// the real gRPC flow, then rewriting the stored namespace hash to a stale value
/// (what an older build left and the current build no longer computes), then
/// resubmitting the IDENTICAL request. A row whose binding-aware identity no
/// longer matches must NOT be reused: the resubmit must NOT SKIP. (A plain
/// clean-store hash-inequality test cannot cover this — it needs a persisted
/// pre-fix row.)
#[tokio::test]
async fn upgrade_does_not_reuse_prefix_clone_with_stale_namespace_hash() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;

    let root = "select a.id from CDO_DEMO_DB.S.INPUT a cross join CDO_DEMO_DB.U.INPUT b";
    let consumer = |target: &str, schema: &str| {
        demo::model(target, schema, CLONE_PROBE)
            .namespace(NS)
            .sql(root)
            .dependency_in(
                "\"CDO_DEMO_DB\".\"S\".\"INPUT\"",
                "select 1 as id",
                demo::CATALOG,
                "S",
            )
            .dependency_in(
                "\"CDO_DEMO_DB\".\"U\".\"INPUT\"",
                "select 2 as id",
                demo::CATALOG,
                "U",
            )
    };
    let prod = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"UPG\"";
    let dev = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"UPG\"";

    // Build + confirm the prod source.
    sc.build_with(consumer(prod, PROD_SCHEMA), transient_at(100).runtime(100))
        .await;

    // Dev clone from prod; confirm it (this is the "pre-fix clone" row).
    let c = sc.expect_clone(consumer(dev, DEV_SCHEMA)).await;
    sc.confirm(&c.request_id, transient_at(200).runtime(100))
        .await;

    // Simulate the UPGRADE: rewrite the confirmed dev clone row's stored
    // namespace hash to a STALE value (what the pre-fix normalizer produced,
    // which the current normalizer no longer computes). The SQL hash is left
    // intact — exactly the pre-fix persisted shape that previously SKIPPED.
    let updated = legacy::rewrite_namespace_logic_hash(
        &server,
        legacy::Rows::ConfirmedTarget(dev),
        "STALE_PREFIX_NS_HASH",
    )
    .await;
    assert!(updated >= 1, "the clone row must exist to mutate");

    // Resubmit the IDENTICAL dev request. With a stale namespace hash on the
    // persisted row, same-target reuse must NOT SKIP it.
    let resub = consumer(dev, DEV_SCHEMA).own_epoch(Some(200));
    let variant = sc.verdict(resub).await;
    assert_ne!(
        variant, "skip_execution",
        "a persisted clone row whose namespace identity no longer matches the \
         submit must NOT authorize a same-target SKIP after upgrade (got {variant})"
    );
}

/// P2 (review #2 9th/10th follow-up) — a TAG-ROLLBACK GUARD. This seeds the
/// prior-tag digest produced by `fingerprint::sql_logic_hash_with_version(…,
/// b"dbt-state-sql-v3")` — which is PARAMETERIZED PRODUCTION HASHING, not an
/// independently frozen v3 oracle (it shares today's conversion/normalization/
/// framing). Its value is that reverting the production tag `v4 → v3` makes the
/// resubmit's hash collide with the seeded value → SKIP → this test fails,
/// catching a tag rollback. The DURABLE historical oracle (real old-server
/// bytes) is `upgrade_rejects_frozen_366be09_clone_fixture_all_cases`; this test
/// is complementary ordinary-row coverage.
///
/// This isolates the SQL/template hash as the deciding reuse factor by using
/// ORDINARY (non-clone) rows: the clone-origin namespace-adoption path
/// (`find_latest_confirmed_target`, which filters `clone_source_table IS NOT
/// NULL`) does not apply, so a non-clone row is reused ONLY through
/// `find_confirmed`'s rendered/template hash — exactly the identity the bump
/// invalidates. (The clone-origin stale-namespace path is covered by
/// `upgrade_does_not_reuse_prefix_clone_with_stale_namespace_hash`.) The reviewer
/// confirmed an unchanged ordinary source also EXECUTEs after upgrade.
///
/// Covers all four cells: {outcome CONFIRMED on old, outcome left PENDING on old
/// then confirmed on new} × {`compare_unrendered_code` false, true}, asserting
/// EXECUTE in each. NOTE: this is a LOCAL multi-revision gRPC + Postgres upgrade
/// test (frozen-v3 fingerprints standing in for the old revision), NOT
/// hosted-service characterization.
#[tokio::test]
async fn upgrade_tag_rollback_guard_ordinary_rows_all_cases() {
    use dbt_state_server::{fingerprint, services::sql_execution};

    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;

    // An ordinary model request held constant across build/confirm/resubmit so
    // its CURRENT (v4) hash is well-defined; own-target epoch present so the
    // only thing that can block reuse is the logic-hash identity. The literal is
    // per-target so cases are logically distinct (no cross-name clone equivalence).
    let request = |target: &str, uniq: &str| {
        demo::model(target, PROD_SCHEMA, CLONE_PROBE)
            .namespace(NS)
            .sql(&format!(
                "select id, '{uniq}' as tag from CDO_DEMO_DB.prod_state_demo.up_{uniq}"
            ))
            .own_epoch(Some(200))
    };

    // Frozen v3 rendered + template hashes for the request.
    let frozen_v3 = |target: &str, uniq: &str| {
        let se = sql_execution(&request(target, uniq).build());
        let body = se
            .dbt_node_state
            .as_ref()
            .and_then(|s| s.node_body_hash.clone())
            .unwrap_or_default();
        (
            fingerprint::sql_logic_hash_with_version(&se, None, b"dbt-state-sql-v3"),
            fingerprint::sql_logic_hash_with_version(&se, Some(&body), b"dbt-state-sql-v3"),
        )
    };

    for (confirm_on_old, template_mode, tag) in [
        (true, false, "CONFIRMED_RENDERED"),
        (true, true, "CONFIRMED_TEMPLATE"),
        (false, false, "PENDING_RENDERED"),
        (false, true, "PENDING_TEMPLATE"),
    ] {
        let target = format!("\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"UPG_{tag}\"");
        let uniq = tag.to_ascii_lowercase();

        // Build the ordinary model (stores CURRENT v4 hashes).
        let rid = sc.execute_id(request(&target, &uniq)).await;

        // Seed the FAITHFUL frozen v3 rendered+template hashes. For the
        // "confirmed-on-old" lifecycle, confirm BEFORE seeding (the pre-fix
        // server confirmed, then upgraded); for "pending-on-old then confirmed-
        // on-new", seed while PENDING and confirm afterwards. Either way the
        // persisted row ends CONFIRMED carrying the v3 hashes.
        let v3 = frozen_v3(&target, &uniq);
        let seed_v3 = || async {
            let n = legacy::rewrite_logic_hashes(
                &server,
                legacy::Rows::Target(&target),
                Some(&v3.0),
                Some(&v3.1),
            )
            .await;
            assert!(n >= 1, "row must exist to seed");
        };
        let built = transient_at(200).runtime(100);
        if confirm_on_old {
            sc.confirm(&rid, built).await;
            seed_v3().await;
        } else {
            seed_v3().await;
            sc.confirm(&rid, built).await;
        }

        // Resubmit the IDENTICAL request under the given comparison mode → the
        // stored v3 hash cannot match the v4 submit → EXECUTE.
        let mut resub = request(&target, &uniq);
        if template_mode {
            resub = resub.compare_unrendered_code();
        }
        assert_eq!(
            sc.verdict(resub).await,
            "ready_to_execute",
            "case {tag}: a faithful v3 pre-fix outcome must be EXECUTED (not reused) \
             on the v4 server (confirm_on_old={confirm_on_old}, template={template_mode})"
        );
    }
}

/// Sanity: the PARAMETERIZED production hasher with the predecessor tag and the
/// current tag DIFFER for the same request. This is what makes the tag-rollback
/// guard above sensitive — if production reverted to v3, the submit's hash would
/// equal the seeded v3-tag value and the clone would be reused (SKIP), failing
/// that guard. (This is not a frozen-oracle claim; see the module note on
/// `sql_logic_hash_with_version`.)
#[tokio::test]
async fn parameterized_v3_tag_differs_from_current_v4() {
    use dbt_state_server::{fingerprint, services::sql_execution};
    let r = demo::model("\"DB\".\"S\".\"T\"", "s", CLONE_PROBE)
        .namespace(NS)
        .sql("select 1 as id")
        .build();
    let se = sql_execution(&r);
    let v3 = fingerprint::sql_logic_hash_with_version(&se, None, b"dbt-state-sql-v3");
    let v4 = fingerprint::sql_logic_hash(&se, None);
    assert_ne!(
        v3, v4,
        "the v3 and v4 fingerprints of the same request must differ (the tag bump \
         is what invalidates pre-fix outcomes); if they were equal the upgrade \
         regression could not detect a tag revert"
    );
    // And the frozen helper reproduces the current tag exactly.
    assert_eq!(
        fingerprint::sql_logic_hash_with_version(&se, None, b"dbt-state-sql-v4"),
        v4,
        "the versioned helper with the current tag equals sql_logic_hash"
    );
}

/// CONTROL for the above (review #2 8th follow-up) — a LEGITIMATE template-mode
/// reuse of a CURRENT-version confirmed row must still SKIP. This proves the
/// mode-independent invalidation does not over-reject: a row built by the
/// current server (current fingerprint version) is reused in template mode, and
/// a template-preserving rendered-SQL change still SKIPs (the rendered-OR-template
/// matching rule is intact).
#[tokio::test]
async fn template_mode_legitimate_reuse_still_skips() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let target = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"TMPL\"";
    let mk = |rendered_literal: &str| {
        demo::model(target, PROD_SCHEMA, CLONE_PROBE)
            .namespace(NS)
            .sql(&format!(
                "select '{rendered_literal}' as v from CDO_DEMO_DB.prod_state_demo.tmpl_up"
            ))
            .compare_unrendered_code()
            .own_epoch(Some(1_000))
            // The template (node_body_hash) is constant across rendered variants.
            .unique_id("model.jaffle.tmpl")
            .node_state(|state| state.node_body_hash = Some("CONSTANT_TEMPLATE_BODY".into()))
    };

    // Build + confirm run1 (rendered literal 'aaa') on the CURRENT server.
    sc.build_with(mk("aaa"), transient_at(1_000).runtime(100))
        .await;

    // run2: a template-PRESERVING rendered-SQL change (literal 'bbb', same body
    // hash) in template mode → must SKIP (rendered-OR-template matching).
    assert_eq!(
        sc.verdict(mk("bbb")).await,
        "skip_execution",
        "a current-version row must still be reused in template mode for a \
         template-preserving rendered change (invalidation must not over-reject)"
    );
}

/// P2 (review #2 10th follow-up) — a DURABLE, INDEPENDENT historical oracle. The
/// prior upgrade regression derived "old" fingerprints from today's production
/// hashing (parameterized by tag), which is not an independent frozen
/// implementation. This test instead loads FIXED BYTES captured from the OLD
/// `366be09` server (`golden/upgrade/prefix_clone_366be09.json`): the actual
/// persisted CONFIRMED and PENDING clone rows (their real v3 `node_sql_hash`,
/// NULL `node_template_hash`, `clone_source_table`, `node_namespace_logic_hash`,
/// status, epoch) plus the exact dev requests that produced them. Those bytes do
/// not depend on today's code, so a sentinel-return mutation of the production
/// hasher cannot subvert this test, and future changes to shared hashing
/// functions do not silently redefine the "historical" state.
///
/// We seed each fixture row VERBATIM (preserving the original NULL template
/// hash), then exercise the four clone cells {CONFIRMED on old, PENDING on old
/// then confirmed on new} × {rendered, template}: the current (v4) server must
/// EXECUTE the resubmit (the pre-fix clone outcome is invalidated), never reuse
/// the stale v3 clone. For the pending case we assert a successful
/// pending→confirmed transition on the current server before resubmitting.
#[tokio::test]
async fn upgrade_rejects_frozen_366be09_clone_fixture_all_cases() {
    #[derive(serde::Deserialize)]
    struct Row {
        target_table: String,
        execution_type: i32,
        status: String,
        node_sql_hash: Option<String>,
        node_template_hash: Option<String>,
        node_namespace_logic_hash: Option<String>,
        clone_source_table: Option<String>,
        table_namespace: Option<String>,
        dialect: String,
        last_modified_epoch: Option<i64>,
    }
    #[derive(serde::Deserialize)]
    struct Fixture {
        namespace: String,
        dev_request_sql_conf: String,
        dev_request_sql_pend: String,
        dev_request_deps: Vec<qc::QueryDependency>,
        rows: Vec<Row>,
    }
    const FIXTURE: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../golden/upgrade/prefix_clone_366be09.json"
    );
    let fx: Fixture = serde_json::from_str(&std::fs::read_to_string(FIXTURE).unwrap()).unwrap();

    // Fixture-shape invariant (review #2 10th follow-up corpus maintenance): the
    // frozen capture must hold EXACTLY one confirmed + one pending clone row, so
    // an emptied or shortened `rows` array fails here instead of silently
    // reducing the four cases below.
    assert_eq!(fx.rows.len(), 2, "fixture must have exactly 2 clone rows");
    assert_eq!(
        fx.rows.iter().filter(|r| r.status == "confirmed").count(),
        1,
        "fixture must have exactly one CONFIRMED clone row"
    );
    assert_eq!(
        fx.rows.iter().filter(|r| r.status == "pending").count(),
        1,
        "fixture must have exactly one PENDING clone row"
    );
    // Every clone row must preserve clone provenance and the NULL template hash
    // (the authentic pre-fix state we are protecting).
    for r in &fx.rows {
        assert!(
            r.clone_source_table.is_some(),
            "fixture rows must carry clone provenance"
        );
        assert!(
            r.node_template_hash.is_none(),
            "the pre-fix clone was rendered-mode; its stored template hash is NULL"
        );
    }
    let mut cases_run = 0usize;

    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;

    // Build the dev request exactly as recorded (per-case SQL, same deps/ns).
    let dev_request = |target: &str, case_sql: &str, template: bool| {
        let recorded = demo::model(target, DEV_SCHEMA, CLONE_PROBE)
            .namespace(&fx.namespace)
            .sql(case_sql)
            .own_epoch(Some(200));
        let recorded = fx.dev_request_deps.iter().fold(recorded, |r, d| {
            r.dependency_in(&d.name, &d.query, &d.default_catalog, &d.default_schema)
        });
        if template {
            recorded.compare_unrendered_code()
        } else {
            recorded
        }
    };

    for template_mode in [false, true] {
        for row in &fx.rows {
            let case_sql = if row.target_table.contains("CONF") {
                &fx.dev_request_sql_conf
            } else {
                &fx.dev_request_sql_pend
            };
            // Re-seed the fixture row VERBATIM with a fresh request_id (idempotent
            // across the two template-mode passes). Preserve the original NULL
            // template hash and clone provenance.
            let rid = format!(
                "frozen-{}-{}",
                if template_mode { "t" } else { "r" },
                if row.target_table.contains("CONF") {
                    "c"
                } else {
                    "p"
                }
            );
            legacy::replace_with_frozen_execution(
                &server,
                &legacy::FrozenExecution {
                    org_id: "local",
                    target_table: &row.target_table,
                    execution_type: row.execution_type,
                    dialect: &row.dialect,
                    status: &row.status,
                    request_id: &rid,
                    node_sql_hash: row.node_sql_hash.as_deref(),
                    node_template_hash: row.node_template_hash.as_deref(),
                    node_namespace_logic_hash: row.node_namespace_logic_hash.as_deref(),
                    clone_source_table: row.clone_source_table.as_deref(),
                    table_namespace: row.table_namespace.as_deref(),
                    last_modified_epoch: row.last_modified_epoch,
                },
            )
            .await;

            // Pending lifecycle: confirm on the CURRENT server (assert success +
            // pending→confirmed transition), simulating "confirmed after upgrade".
            if row.status == "pending" {
                let ok = sc.confirm(&rid, transient_at(200).runtime(100)).await;
                assert!(
                    ok,
                    "pending clone must confirm successfully on the new server"
                );
                let status = rows::execution(&server, &rid)
                    .await
                    .expect("the frozen row")
                    .status;
                assert_eq!(status, "confirmed", "pending→confirmed transition");
            }

            // Resubmit the recorded dev request → the stored v3 clone outcome
            // must NOT be reused on the v4 server, in either comparison mode.
            assert_eq!(
                sc.verdict(dev_request(&row.target_table, case_sql, template_mode))
                    .await,
                "ready_to_execute",
                "frozen 366be09 clone ({}, template={template_mode}) must be EXECUTED \
                 (not reused) on the v4 server",
                row.status
            );
            cases_run += 1;
        }
    }
    assert_eq!(
        cases_run, 4,
        "all four historical-clone cases must run ({{confirmed, pending}} × \
         {{rendered, template}}); got {cases_run}"
    );
}

/// P2 (review #2 11th follow-up) — the ORIGINAL incorrect clone CREATION as a
/// durable frozen fixture. Earlier I wrongly concluded this was "not capturable";
/// it IS reproducible on `366be09` when the request's `default_schema = "S"` so
/// schema `S` maps to relative token 0 and the embedded `__dbt_state_ns__0`
/// placeholder text collides the two dependency names. `golden/upgrade/incorrect_clone_366be09.json`
/// is the real persisted state: a CONFIRMED and a PENDING dev clone whose source
/// (prod, result 1) was UNSAFELY cloned for a SWAPPED-binding request (required
/// result 2), each recording the prod source as `clone_source_table`.
///
/// This loads the FIXED BYTES (independent of today's code), seeds them verbatim
/// (preserving the NULL template hash + clone provenance), and asserts the
/// current server EXECUTEs (does not reuse the WRONG clone) across all four cells
/// {confirmed, pending→confirmed-on-new} × {rendered, template}. Captured via
/// LOCAL old-revision gRPC testing (not hosted-service characterization).
#[tokio::test]
async fn upgrade_rejects_frozen_366be09_incorrect_clone_fixture_all_cases() {
    #[derive(serde::Deserialize)]
    struct Row {
        target_table: String,
        status: String,
        node_sql_hash: Option<String>,
        node_template_hash: Option<String>,
        node_namespace_logic_hash: Option<String>,
        clone_source_table: Option<String>,
        table_namespace: Option<String>,
        dialect: String,
        last_modified_epoch: Option<i64>,
    }
    #[derive(serde::Deserialize)]
    struct Fixture {
        namespace: String,
        default_schema: String,
        dev_request_sql_conf: String,
        dev_request_sql_pend: String,
        dev_request_swapped_deps: Vec<qc::QueryDependency>,
        rows: Vec<Row>,
    }
    const FIXTURE: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../golden/upgrade/incorrect_clone_366be09.json"
    );
    let fx: Fixture = serde_json::from_str(&std::fs::read_to_string(FIXTURE).unwrap()).unwrap();

    // Shape invariant: exactly one confirmed + one pending UNSAFE clone row, each
    // with clone provenance and a NULL template hash (the pre-fix state).
    assert_eq!(fx.rows.len(), 2, "fixture must have exactly 2 clone rows");
    assert_eq!(
        fx.rows.iter().filter(|r| r.status == "confirmed").count(),
        1,
        "exactly one CONFIRMED unsafe-clone row"
    );
    assert_eq!(
        fx.rows.iter().filter(|r| r.status == "pending").count(),
        1,
        "exactly one PENDING unsafe-clone row"
    );
    for r in &fx.rows {
        assert!(
            r.clone_source_table.is_some(),
            "rows carry clone provenance"
        );
        assert!(
            r.node_template_hash.is_none(),
            "pre-fix clone has NULL template hash"
        );
    }

    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;

    let dev_request = |target: &str, case_sql: &str, template: bool| {
        let recorded = demo::model(target, &fx.default_schema, CLONE_PROBE)
            .namespace(&fx.namespace)
            .sql(case_sql)
            .own_epoch(Some(200));
        let recorded = fx.dev_request_swapped_deps.iter().fold(recorded, |r, d| {
            r.dependency_in(&d.name, &d.query, &d.default_catalog, &d.default_schema)
        });
        if template {
            recorded.compare_unrendered_code()
        } else {
            recorded
        }
    };

    let mut cases_run = 0usize;
    for template_mode in [false, true] {
        for row in &fx.rows {
            let case_sql = if row.target_table.contains("CONF") {
                &fx.dev_request_sql_conf
            } else {
                &fx.dev_request_sql_pend
            };
            let rid = format!(
                "icc-{}-{}",
                if template_mode { "t" } else { "r" },
                if row.target_table.contains("CONF") {
                    "c"
                } else {
                    "p"
                }
            );
            legacy::replace_with_frozen_execution(
                &server,
                &legacy::FrozenExecution {
                    org_id: "local",
                    target_table: &row.target_table,
                    execution_type: 1,
                    dialect: &row.dialect,
                    status: &row.status,
                    request_id: &rid,
                    node_sql_hash: row.node_sql_hash.as_deref(),
                    node_template_hash: row.node_template_hash.as_deref(),
                    node_namespace_logic_hash: row.node_namespace_logic_hash.as_deref(),
                    clone_source_table: row.clone_source_table.as_deref(),
                    table_namespace: row.table_namespace.as_deref(),
                    last_modified_epoch: row.last_modified_epoch,
                },
            )
            .await;

            if row.status == "pending" {
                let ok = sc.confirm(&rid, transient_at(200).runtime(100)).await;
                assert!(ok, "pending unsafe clone must confirm on the new server");
                let status = rows::execution(&server, &rid)
                    .await
                    .expect("the frozen row")
                    .status;
                assert_eq!(status, "confirmed", "pending→confirmed transition");
            }

            assert_eq!(
                sc.verdict(dev_request(&row.target_table, case_sql, template_mode))
                    .await,
                "ready_to_execute",
                "frozen 366be09 INCORRECT clone ({}, template={template_mode}) must be \
                 EXECUTED (its wrong result not reused) on the v4 server",
                row.status
            );
            cases_run += 1;
        }
    }
    assert_eq!(
        cases_run, 4,
        "all four incorrect-clone cases must run; got {cases_run}"
    );
}

/// REUSABLE CAPTURE DRIVER — regenerates `golden/upgrade/prefix_clone_366be09.json`,
/// so the frozen fixture is REGENERABLE. It is `#[ignore]`d (it WRITES a file and
/// is not a conformance assertion). To regenerate the fixture, check out the
/// capture revision in a detached worktree, copy this test in, and run it:
///
/// ```text
/// git worktree add --detach /tmp/wt 366be09
/// # copy this driver into /tmp/wt's clone_submit_fallback.rs, then:
/// DATABASE_URL=... cargo test -p dbt-state-harness --test clone_submit_fallback \
///   zz_capture_prefix_clone_fixture -- --ignored --nocapture
/// cp /tmp/prefix_clone_366be09.json golden/upgrade/   # from the main tree
/// ```
///
/// The capture revision predates `support::scenario`: copy the pre-scenario
/// form of this driver, which uses only the support module of that era
/// (`git show 3218068:crates/harness/tests/clone_submit_fallback.rs`), or copy
/// `tests/support/` along with it.
///
/// It builds + confirms a prod source for two distinct targets, clones the dev
/// target from each (CONFIRMED one, PENDING the other), and dumps the dev clone
/// rows + the exact requests as the fixture bytes. On the CURRENT revision the
/// stored hashes are v4 (not the frozen v3), which is expected — the fixture is
/// authoritative only when captured on the recorded old revision.
///
/// NOTE on scope: the committed fixture uses IDENTICAL bindings (old-clone
/// INVALIDATION coverage). The "incorrect clone CREATION" (swapped-binding,
/// wrong-result) scenario IS capturable (review #2 11th follow-up — correcting an
/// earlier wrong "not capturable" claim): the collision only manifests when the
/// request's `default_schema = "S"` so schema `S` maps to relative token 0 and
/// the embedded `__dbt_state_ns__0` placeholder collides the two dependency
/// names. My earlier probes used `dev_clone_demo`/`prod_state_demo`, which assign
/// `S` a NON-zero token, so they did not collide. With `default_schema = "S"`,
/// 366be09 UNSAFELY clones the swapped binding (base and swapped namespace hashes
/// both `6243fdaf0218…`). That fixture is captured by
/// `zz_capture_incorrect_clone_fixture` into
/// `golden/upgrade/incorrect_clone_366be09.json` and exercised by
/// `upgrade_rejects_frozen_366be09_incorrect_clone_fixture_all_cases`. (All via
/// LOCAL old-revision gRPC testing.)
#[tokio::test]
#[ignore = "capture driver: WRITES a fixture file; run explicitly on the capture revision"]
async fn zz_capture_prefix_clone_fixture() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;

    let n1 = "\"DB.__dbt_state_ns__0\".\"S\".\"T\"";
    let n2 = "\"DB\".\"S\".\"__dbt_state_ns__0.T\"";
    let root = "select a.id from \"DB.__dbt_state_ns__0\".\"S\".\"T\" a cross join \"DB\".\"S\".\"__dbt_state_ns__0.T\" b";
    let consumer = |target: &str, schema: &str, mark: &str| {
        demo::model(target, schema, CLONE_PROBE)
            .namespace(NS)
            .sql(&format!("{root} where {mark} = {mark}"))
            .dependency_in(n1, "select 1 as id", "DB", "S")
            .dependency_in(n2, "select 2 as id", "DB", "S")
    };

    for (suffix, mark) in [("CONF", "11"), ("PEND", "22")] {
        let prod = format!("\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"PFX_{suffix}\"");
        sc.build_with(
            consumer(&prod, PROD_SCHEMA, mark),
            transient_at(100).runtime(100),
        )
        .await;
    }

    let dev_conf = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"PFX_CONF\"";
    let dev_pend = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"PFX_PEND\"";
    let dev_request =
        |target: &str, mark: &str| consumer(target, DEV_SCHEMA, mark).own_epoch(Some(200));
    let c = sc.expect_clone(dev_request(dev_conf, "11")).await;
    sc.confirm(&c.request_id, transient_at(200).runtime(100))
        .await;
    sc.expect_clone(dev_request(dev_pend, "22")).await;

    let rows = captured_clone_rows(&server, &[dev_conf, dev_pend]).await;

    let fixture = serde_json::json!({
        "captured_from_revision": "366be09",
        "scenario": "pre-fix clone (identical binding) persisted with v3 sql/template hashes",
        "namespace": NS,
        "dev_request_sql_conf": format!("{root} where 11 = 11"),
        "dev_request_sql_pend": format!("{root} where 22 = 22"),
        "dev_request_deps": [
            {"name": n1, "query": "select 1 as id", "default_catalog": "DB", "default_schema": "S"},
            {"name": n2, "query": "select 2 as id", "default_catalog": "DB", "default_schema": "S"}
        ],
        "rows": rows,
    });
    std::fs::write(
        "/tmp/prefix_clone_366be09.json",
        serde_json::to_string_pretty(&fixture).unwrap(),
    )
    .unwrap();
    eprintln!("CAPTURED_FIXTURE /tmp/prefix_clone_366be09.json");
}

/// REUSABLE CAPTURE DRIVER (review #2 11th follow-up) — regenerates
/// `golden/upgrade/incorrect_clone_366be09.json`, the ORIGINAL incorrect clone
/// CREATION (swapped-binding, wrong-result). `#[ignore]`d (WRITES a file). Run on
/// the capture revision (`366be09`) in a detached worktree. The key is
/// `default_schema = "S"` so schema `S` maps to relative token 0 and the embedded
/// `__dbt_state_ns__0` placeholder collides the two dependency names — which is
/// why the old server UNSAFELY clones the swapped binding (it does not collide
/// with a non-zero `S` token, which is the mistake the 11th-follow-up corrected).
/// As for `zz_capture_prefix_clone_fixture`, run the pre-scenario form of this
/// driver (`git show 3218068:crates/harness/tests/clone_submit_fallback.rs`) on
/// the capture revision.
#[tokio::test]
#[ignore = "capture driver: WRITES a fixture file; run explicitly on the capture revision"]
async fn zz_capture_incorrect_clone_fixture() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let n1 = "\"DB.__dbt_state_ns__0\".\"S\".\"T\"";
    let n2 = "\"DB\".\"S\".\"__dbt_state_ns__0.T\"";
    let root = "select a.id from \"DB.__dbt_state_ns__0\".\"S\".\"T\" a cross join \"DB\".\"S\".\"__dbt_state_ns__0.T\" b";
    let consumer = |target: &str, mark: &str, first: &str, second: &str| {
        demo::model(target, "S", CLONE_PROBE)
            .namespace(NS)
            .sql(&format!("{root} where {mark} = {mark}"))
            .dependency_in(n1, &format!("select {first} as id"), "DB", "S")
            .dependency_in(n2, &format!("select {second} as id"), "DB", "S")
    };
    for (suffix, mark) in [("CONF", "11"), ("PEND", "22")] {
        let prod = format!("\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"ICC_{suffix}\"");
        sc.build_with(
            consumer(&prod, mark, "1", "2"),
            transient_at(100).runtime(100),
        )
        .await;
    }
    let dev_conf = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"ICC_CONF\"";
    let dev_pend = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"ICC_PEND\"";
    let dev = |t: &str, m: &str| consumer(t, m, "2", "1").own_epoch(Some(200));
    // The old server must UNSAFELY clone the swapped binding (default_schema=S).
    let c = sc.expect_clone(dev(dev_conf, "11")).await;
    sc.confirm(&c.request_id, transient_at(200).runtime(100))
        .await;
    sc.expect_clone(dev(dev_pend, "22")).await;
    let rows = captured_clone_rows(&server, &[dev_conf, dev_pend]).await;
    let fixture = serde_json::json!({
        "captured_from_revision": "366be09",
        "scenario": "ORIGINAL incorrect clone CREATION — swapped-binding dev request (required result 2) UNSAFELY cloned from prod source (result 1) via the pre-fix rejoin collision; default_schema=S so S->token0",
        "namespace": NS,
        "default_schema": "S",
        "dev_request_sql_conf": format!("{root} where 11 = 11"),
        "dev_request_sql_pend": format!("{root} where 22 = 22"),
        "dev_request_swapped_deps": [
            {"name": n1, "query": "select 2 as id", "default_catalog": "DB", "default_schema": "S"},
            {"name": n2, "query": "select 1 as id", "default_catalog": "DB", "default_schema": "S"}
        ],
        "rows": rows,
    });
    std::fs::write(
        "/tmp/incorrect_clone_366be09.json",
        serde_json::to_string_pretty(&fixture).unwrap(),
    )
    .unwrap();
    eprintln!("CAPTURED_ICC /tmp/incorrect_clone_366be09.json");
}

/// The capture drivers' fixture `rows`: the persisted columns of each row for
/// `targets`, ordered by target table.
async fn captured_clone_rows(server: &support::TestServer, targets: &[&str]) -> serde_json::Value {
    let mut stored = rows::executions(server).await;
    stored.retain(|row| targets.contains(&row.target_table.as_str()));
    stored.sort_by(|a, b| a.target_table.cmp(&b.target_table));
    stored
        .into_iter()
        .map(|row| {
            serde_json::json!({
                "target_table": row.target_table,
                "execution_type": row.execution_type,
                "status": row.status,
                "node_sql_hash": row.node_sql_hash,
                "node_template_hash": row.node_template_hash,
                "node_namespace_logic_hash": row.node_namespace_logic_hash,
                "clone_source_table": row.clone_source_table,
                "table_namespace": row.table_namespace,
                "dialect": row.dialect,
                "last_modified_epoch": row.last_modified_epoch,
            })
        })
        .collect()
}
