//! The Submit decision module, called directly against a real per-test Postgres
//! schema (no gRPC). Covers what the module owns: the node-state write, State
//! history selection, the result copy fallback and its precedence over the
//! verdict, id minting and the pending-row write, and the matched execution
//! used as the Explain freshness anchor. Wire shapes stay covered by the gRPC
//! suites (`clone_submit_fallback.rs`, `correctness.rs`, `conformance.rs`).

#[path = "support.rs"]
mod support;

use dbt_state_proto::query_cache as qc;
use dbt_state_server::decision::{DecisionReason, DecisionRecord, NoCandidate, Reuse};
use dbt_state_server::store::Store;
use dbt_state_server::submit_decision::{self, DecisionIds, ResultCopy, SubmitOutcome};
use support::rows;
use support::TestDatabase;

const ORG: &str = "org-submit-decision";
const NS: &str = "ns-submit-decision";
const PROD: &str = "\"DB\".\"PROD\".\"M\"";
const DEV: &str = "\"DB\".\"DEV\".\"M\"";
const BUILT: i64 = 1_000;

fn upstream(schema: &str) -> String {
    format!("\"DB\".\"{}\".\"UP\"", schema.to_uppercase())
}

/// A Snowflake model submit for `target` in environment `schema`. The SQL and
/// upstream follow the schema, so PROD and DEV share one schema-abstracted
/// logic identity (a Result copy equivalent) but not the same rendered SQL.
/// `own_epoch` is the target's current epoch (None: absent); the upstream sits
/// at `upstream_epoch`.
fn model(
    target: &str,
    schema: &str,
    own_epoch: Option<i64>,
    upstream_epoch: i64,
) -> qc::SubmitEnrichedSqlRequest {
    let mut tables = vec![qc::TableModifiedInfo {
        name: upstream(schema),
        last_modified_epoch: Some(upstream_epoch),
    }];
    if let Some(epoch) = own_epoch {
        tables.push(qc::TableModifiedInfo {
            name: target.into(),
            last_modified_epoch: Some(epoch),
        });
    }
    qc::SubmitEnrichedSqlRequest {
        target_table: Some(target.into()),
        dialect: "snowflake".into(),
        default_catalog: "DB".into(),
        default_schema: Some(schema.into()),
        execution_type: 1,
        sql: format!("select id from DB.{schema}.up"),
        tolerate_nondeterminism: true,
        tables,
        table_namespace: Some(NS.into()),
        dbt_node_state: Some(qc::DbtNodeState {
            node_unique_id: "model.p.m".into(),
            node_body_hash: Some("body".into()),
            project_id: Some("project".into()),
            target_name: schema.into(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn seed(values_hash: &str, own_epoch: Option<i64>) -> qc::SubmitValuesRequest {
    qc::SubmitValuesRequest {
        target_table: "\"DB\".\"PROD\".\"SEED\"".into(),
        dialect: "snowflake".into(),
        default_catalog: "DB".into(),
        values_hash: values_hash.into(),
        table_namespace: Some(NS.into()),
        last_modified_epoch: own_epoch,
        dbt_node_state: Some(qc::DbtNodeState {
            node_unique_id: "seed.p.s".into(),
            project_id: Some("project".into()),
            target_name: "prod".into(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Confirm a build at `epoch` through the store's confirm path.
async fn confirm(store: &Store, ids: &DecisionIds, epoch: i64) {
    assert!(store
        .confirm(
            ORG,
            &ids.request_id,
            Some(epoch),
            Some("TABLE"),
            Some(42),
            None,
            false
        )
        .await
        .expect("confirm"));
}

/// Submit, expect an ordinary Execute, and confirm it at `epoch`.
async fn build(store: &Store, req: &qc::SubmitEnrichedSqlRequest, epoch: i64) -> i64 {
    let outcome = submit_decision::submit_sql(store, ORG, req)
        .await
        .expect("submit");
    let SubmitOutcome::Execute { ids, .. } = outcome else {
        panic!("expected Execute, got {outcome:?}");
    };
    confirm(store, &ids, epoch).await;
    rows::execution_in(store, ORG, &ids.request_id)
        .await
        .expect("confirmed row")
        .id
}

#[tokio::test]
async fn first_build_executes_and_writes_its_pending_row() {
    let db = TestDatabase::new("submit_decision").await;
    let state = db.state(false).await;
    let store = &state.store;

    let outcome = submit_decision::submit_sql(store, ORG, &model(PROD, "prod", None, 500))
        .await
        .expect("submit");
    let SubmitOutcome::Execute {
        ids,
        matched_execution,
        record:
            DecisionRecord {
                description,
                skip_rejection_reason: Some(skip_rejection_reason),
                clone_rejection_reason: Some(clone_rejection_reason),
                is_stale,
                ..
            },
    } = outcome
    else {
        panic!("expected Execute, got {outcome:?}");
    };
    assert!(matched_execution.is_none(), "a first build matches nothing");
    assert_eq!(
        description,
        "model was executed because its table did not exist"
    );
    assert_eq!((skip_rejection_reason, clone_rejection_reason), (6, 6));
    assert!(!is_stale);
    assert_ne!(ids.request_id, ids.execution_decision_id);

    let row = rows::execution_in(store, ORG, &ids.request_id)
        .await
        .expect("pending row written");
    assert_eq!(row.status, "pending");
    assert_eq!(row.target_table, PROD);
    assert_eq!(
        row.execution_decision_id.as_deref(),
        Some(ids.execution_decision_id.as_str())
    );
    assert_eq!(row.clone_source_table, None);
    assert!(row.node_sql_hash.is_some() && row.node_namespace_logic_hash.is_some());
}

#[tokio::test]
async fn reuse_skips_against_the_matched_execution_without_a_pending_row() {
    let db = TestDatabase::new("submit_decision").await;
    let state = db.state(false).await;
    let store = &state.store;
    let built = build(store, &model(PROD, "prod", None, 500), BUILT).await;
    let written = rows::executions_in(store, ORG).await.len();

    let outcome = submit_decision::submit_sql(store, ORG, &model(PROD, "prod", Some(BUILT), 500))
        .await
        .expect("submit");
    let SubmitOutcome::Skip {
        execution_decision_id,
        matched_execution,
        record:
            DecisionRecord {
                description,
                is_stale,
                ..
            },
    } = outcome
    else {
        panic!("expected Skip, got {outcome:?}");
    };
    assert_eq!(
        matched_execution.id, built,
        "anchored on the confirmed build"
    );
    assert_eq!(matched_execution.execution_runtime_ms, Some(42));
    assert_eq!(
        description,
        "model was a no-op because both its query and its upstream data are up to date"
    );
    assert!(!is_stale);
    assert!(!execution_decision_id.is_empty());
    assert_eq!(
        rows::executions_in(store, ORG).await.len(),
        written,
        "a skip writes no row"
    );
}

#[tokio::test]
async fn freshness_driven_execute_keeps_the_matched_execution() {
    let db = TestDatabase::new("submit_decision").await;
    let state = db.state(false).await;
    let store = &state.store;
    let built = build(store, &model(PROD, "prod", None, 500), BUILT).await;

    // Same identity, but the upstream changed after the build.
    let outcome = submit_decision::submit_sql(store, ORG, &model(PROD, "prod", Some(BUILT), 2_000))
        .await
        .expect("submit");
    let SubmitOutcome::Execute {
        ids,
        matched_execution,
        record: DecisionRecord { description, .. },
        ..
    } = outcome
    else {
        panic!("expected Execute, got {outcome:?}");
    };
    assert_eq!(matched_execution.map(|r| r.id), Some(built));
    assert_eq!(
        description,
        "model was executed because either its query didn't match or its upstream data is out of date"
    );
    let row = rows::execution_in(store, ORG, &ids.request_id)
        .await
        .expect("pending row written");
    assert_eq!(
        (row.status.as_str(), row.target_table.as_str()),
        ("pending", PROD)
    );
    assert_eq!(row.clone_source_table, None);
}

#[tokio::test]
async fn equivalent_under_another_name_is_copied_with_a_copy_pending_row() {
    let db = TestDatabase::new("submit_decision").await;
    let state = db.state(false).await;
    let store = &state.store;
    build(store, &model(PROD, "prod", None, 500), BUILT).await;

    let outcome = submit_decision::submit_sql(store, ORG, &model(DEV, "dev", None, 500))
        .await
        .expect("submit");
    assert!(
        outcome.matched_execution().is_none(),
        "a copy matches nothing"
    );
    let SubmitOutcome::Copy {
        ids,
        copy:
            ResultCopy {
                source_table,
                clone_sqls,
                source_last_modified_epoch,
                source_execution_runtime_ms,
            },
        ..
    } = outcome
    else {
        panic!("expected Copy, got {outcome:?}");
    };
    assert_eq!(source_table, PROD);
    assert_eq!(clone_sqls.len(), 1);
    assert!(
        clone_sqls[0].contains(&format!("CLONE {PROD}")),
        "{clone_sqls:?}"
    );
    assert_eq!(source_last_modified_epoch, Some(BUILT));
    assert_eq!(source_execution_runtime_ms, Some(42));

    let row = rows::execution_in(store, ORG, &ids.request_id)
        .await
        .expect("pending row written");
    assert_eq!(row.status, "pending");
    assert_eq!(row.target_table, DEV, "keyed on the submit target");
    assert_eq!(row.clone_source_table.as_deref(), Some(PROD));
    assert_eq!(
        row.execution_decision_id.as_deref(),
        Some(ids.execution_decision_id.as_str())
    );
    assert!(
        row.node_namespace_logic_hash.is_some(),
        "inherits the equivalent's logic"
    );
}

/// A reuse candidate takes precedence over an equivalent under another name:
/// the copy is not considered, whether the candidate is reused or freshness
/// forces a rebuild.
#[tokio::test]
async fn reuse_candidate_takes_precedence_over_result_copy() {
    let db = TestDatabase::new("submit_decision").await;
    let state = db.state(false).await;
    let store = &state.store;
    build(store, &model(PROD, "prod", None, 500), BUILT).await;
    // DEV becomes a confirmed copy of PROD: its own reuse candidate.
    let SubmitOutcome::Copy { ids, .. } =
        submit_decision::submit_sql(store, ORG, &model(DEV, "dev", None, 500))
            .await
            .expect("submit")
    else {
        panic!("expected the first DEV submit to copy");
    };
    confirm(store, &ids, BUILT).await;
    let dev_copy = rows::execution_in(store, ORG, &ids.request_id)
        .await
        .unwrap()
        .id;

    let outcome = submit_decision::submit_sql(store, ORG, &model(DEV, "dev", Some(BUILT), 500))
        .await
        .expect("submit");
    let SubmitOutcome::Skip {
        matched_execution, ..
    } = outcome
    else {
        panic!("a fresh candidate skips instead of copying, got {outcome:?}");
    };
    assert_eq!(matched_execution.id, dev_copy);

    let outcome = submit_decision::submit_sql(store, ORG, &model(DEV, "dev", Some(BUILT), 2_000))
        .await
        .expect("submit");
    let SubmitOutcome::Execute {
        ids,
        matched_execution,
        ..
    } = outcome
    else {
        panic!("a stale candidate rebuilds instead of copying, got {outcome:?}");
    };
    assert_eq!(matched_execution.map(|r| r.id), Some(dev_copy));
    let row = rows::execution_in(store, ORG, &ids.request_id)
        .await
        .unwrap();
    assert_eq!(row.clone_source_table, None, "an ordinary build row");
}

#[tokio::test]
async fn copy_forbidding_target_gets_the_chain_limit_execute() {
    let db = TestDatabase::new("submit_decision").await;
    let state = db.state(false).await;
    let store = &state.store;
    build(store, &model(PROD, "prod", None, 500), BUILT).await;

    let mut req = model(DEV, "dev", None, 500);
    req.clone_chain_depth_limit = Some(0);
    let outcome = submit_decision::submit_sql(store, ORG, &req)
        .await
        .expect("submit");
    assert!(outcome.matched_execution().is_none());
    let SubmitOutcome::ChainLimitExecute { ids, .. } = outcome else {
        panic!("expected ChainLimitExecute, got {outcome:?}");
    };
    let row = rows::execution_in(store, ORG, &ids.request_id)
        .await
        .expect("pending row written");
    assert_eq!(row.status, "pending");
    assert_eq!(row.target_table, DEV);
    assert_eq!(row.clone_source_table, None, "an ordinary build row");
    assert_eq!(
        row.execution_decision_id.as_deref(),
        Some(ids.execution_decision_id.as_str())
    );
}

#[tokio::test]
async fn unavailable_copy_falls_back_to_the_ordinary_decision() {
    let db = TestDatabase::new("submit_decision").await;
    let state = db.state(false).await;
    let store = &state.store;
    build(store, &model(PROD, "prod", None, 500), BUILT).await;

    // No copy DDL exists for an expiring clone, so the equivalent is unusable.
    let mut req = model(DEV, "dev", None, 500);
    req.clone_table_properties = Some(qc::TableProperties {
        hours_to_expiration: Some(1),
        ..Default::default()
    });
    let outcome = submit_decision::submit_sql(store, ORG, &req)
        .await
        .expect("submit");
    let SubmitOutcome::Execute {
        ids,
        matched_execution,
        record: DecisionRecord { description, .. },
        ..
    } = outcome
    else {
        panic!("expected Execute, got {outcome:?}");
    };
    assert!(matched_execution.is_none());
    assert_eq!(
        description,
        "model was executed because its table did not exist"
    );
    let row = rows::execution_in(store, ORG, &ids.request_id)
        .await
        .unwrap();
    assert_eq!(
        (row.target_table.as_str(), row.clone_source_table),
        (DEV, None)
    );
}

#[tokio::test]
async fn both_entry_points_record_node_state_for_selectors() {
    let db = TestDatabase::new("submit_decision").await;
    let state = db.state(false).await;
    let store = &state.store;

    submit_decision::submit_sql(store, ORG, &model(PROD, "prod", None, 500))
        .await
        .expect("submit");
    submit_decision::submit_values(store, ORG, &seed("v1", None))
        .await
        .expect("submit");

    let uids = ["model.p.m".to_string(), "seed.p.s".to_string()];
    let recorded = store
        .fetch_node_states(ORG, "project", "prod", &uids)
        .await
        .expect("read node states");
    assert_eq!(
        recorded["model.p.m"].node_body_hash.as_deref(),
        Some("body")
    );
    assert!(recorded.contains_key("seed.p.s"));
    let fqns = store
        .resolve_deferred_fqns(ORG, "project", "prod", &uids)
        .await
        .expect("resolve");
    assert_eq!(fqns["model.p.m"], PROD);
    assert_eq!(fqns["seed.p.s"], "\"DB\".\"PROD\".\"SEED\"");
    assert_eq!(state.selector_write_failures(), 0);
}

#[tokio::test]
async fn values_submits_share_execute_skip_and_the_anchor() {
    let db = TestDatabase::new("submit_decision").await;
    let state = db.state(false).await;
    let store = &state.store;

    let outcome = submit_decision::submit_values(store, ORG, &seed("v1", None))
        .await
        .expect("submit");
    let SubmitOutcome::Execute {
        ids,
        matched_execution,
        record: DecisionRecord { description, .. },
        ..
    } = outcome
    else {
        panic!("expected Execute, got {outcome:?}");
    };
    assert!(matched_execution.is_none());
    assert_eq!(description, "seed was loaded because it did not exist");
    let row = rows::execution_in(store, ORG, &ids.request_id)
        .await
        .expect("pending row written");
    assert_eq!(row.status, "pending");
    assert_eq!(
        row.execution_decision_id.as_deref(),
        Some(ids.execution_decision_id.as_str())
    );
    assert!(row.node_sql_hash.is_some() && row.node_namespace_logic_hash.is_none());
    confirm(store, &ids, BUILT).await;
    let built = row.id;

    let outcome = submit_decision::submit_values(store, ORG, &seed("v1", Some(BUILT)))
        .await
        .expect("submit");
    let SubmitOutcome::Skip {
        matched_execution,
        record: DecisionRecord { description, .. },
        ..
    } = outcome
    else {
        panic!("expected Skip, got {outcome:?}");
    };
    assert_eq!(matched_execution.id, built);
    assert_eq!(
        description,
        "seed was a no-op because its data has not changed"
    );

    let outcome = submit_decision::submit_values(store, ORG, &seed("v2", Some(BUILT)))
        .await
        .expect("submit");
    let SubmitOutcome::Execute {
        matched_execution,
        record: DecisionRecord { description, .. },
        ..
    } = outcome
    else {
        panic!("expected Execute, got {outcome:?}");
    };
    assert!(matched_execution.is_none(), "changed values match nothing");
    assert_eq!(description, "seed was loaded because its data changed");
}

/// Every outcome carries a decision record naming its one decision reason,
/// anchored on the same matched execution the outcome carries.
#[tokio::test]
async fn every_outcome_carries_its_decision_reason() {
    let db = TestDatabase::new("submit_decision").await;
    let state = db.state(false).await;
    let store = &state.store;
    let sql = |req: qc::SubmitEnrichedSqlRequest| async move {
        let outcome = submit_decision::submit_sql(store, ORG, &req)
            .await
            .expect("submit");
        assert_eq!(
            outcome.record().matched_execution_id,
            outcome.matched_execution().map(|r| r.id),
            "{outcome:?}"
        );
        outcome
    };

    let first = sql(model(PROD, "prod", None, 500)).await;
    assert_eq!(first.record().reason, DecisionReason::FirstBuild);
    let SubmitOutcome::Execute { ids, .. } = first else {
        panic!("expected Execute, got {first:?}");
    };
    confirm(store, &ids, BUILT).await;

    let fresh = sql(model(PROD, "prod", Some(BUILT), 500)).await;
    assert_eq!(fresh.record().reason, DecisionReason::Reused(Reuse::Fresh));
    let outdated = sql(model(PROD, "prod", Some(BUILT), 2_000)).await;
    assert_eq!(outdated.record().reason, DecisionReason::UpstreamOutdated);
    let edited = sql(model(PROD, "prod", Some(BUILT + 1), 500)).await;
    assert_eq!(
        edited.record().reason,
        DecisionReason::NoReuseCandidate(NoCandidate::OwnTargetChangedOrMissing)
    );

    let mut forbids_copies = model(DEV, "dev", None, 500);
    forbids_copies.clone_chain_depth_limit = Some(0);
    let limit = sql(forbids_copies).await;
    assert_eq!(limit.record().reason, DecisionReason::CopyChainLimit);
    let copy = sql(model(DEV, "dev", None, 500)).await;
    assert_eq!(copy.record().reason, DecisionReason::ResultCopy);

    let seed_first = submit_decision::submit_values(store, ORG, &seed("v1", None))
        .await
        .expect("submit");
    assert_eq!(seed_first.record().reason, DecisionReason::FirstBuild);
    let SubmitOutcome::Execute { ids, .. } = seed_first else {
        panic!("expected Execute, got {seed_first:?}");
    };
    confirm(store, &ids, BUILT).await;
    let changed = submit_decision::submit_values(store, ORG, &seed("v2", Some(BUILT)))
        .await
        .expect("submit");
    assert_eq!(
        changed.record().reason,
        DecisionReason::NoReuseCandidate(NoCandidate::IdentityMismatch)
    );
    assert_eq!(changed.record().matched_execution_id, None);
}

/// A DuckDB table model in environment `schema`, reported under the wire
/// spelling `dialect` and requesting volatile evaluation. DuckDB has no checked
/// volatile-function detector, so only its schema-abstracted copy identity lets
/// such a submit be copied.
fn duckdb_model(dialect: &str, namespace: &str, schema: &str) -> qc::SubmitEnrichedSqlRequest {
    qc::SubmitEnrichedSqlRequest {
        target_table: Some(format!("\"db\".\"{schema}\".\"result\"")),
        dialect: dialect.into(),
        default_catalog: "db".into(),
        default_schema: Some(schema.into()),
        table_namespace: Some(namespace.into()),
        sql: "select id, amount + 1 as amount from db.raw.input".into(),
        execution_type: 1,
        tolerate_nondeterminism: false,
        semantic_extras: [("duckdb_copy_contract".into(), "rows-schema-v1".into())].into(),
        tables: vec![qc::TableModifiedInfo {
            name: "db.raw.input".into(),
            last_modified_epoch: Some(10),
        }],
        ..Default::default()
    }
}

/// Dialect spelling does not change result copy: a client reporting `DuckDB`
/// gets the same copy, past the volatility gate, as one reporting `duckdb`.
#[tokio::test]
async fn duckdb_result_copy_ignores_dialect_case() {
    let db = TestDatabase::new("submit_decision").await;
    let state = db.state(false).await;
    let store = &state.store;
    for dialect in ["duckdb", "DuckDB", "DUCKDB"] {
        let namespace = format!("ns-{dialect}");
        let prod = duckdb_model(dialect, &namespace, "prod");
        let outcome = submit_decision::submit_sql(store, ORG, &prod)
            .await
            .expect("submit");
        let SubmitOutcome::Execute { ids, .. } = outcome else {
            panic!("{dialect}: expected the first build, got {outcome:?}");
        };
        assert!(store
            .confirm(
                ORG,
                &ids.request_id,
                Some(20),
                Some("DUCKDB CTAS TABLE"),
                Some(42),
                None,
                false
            )
            .await
            .expect("confirm"));

        let outcome =
            submit_decision::submit_sql(store, ORG, &duckdb_model(dialect, &namespace, "dev"))
                .await
                .expect("submit");
        let SubmitOutcome::Copy {
            copy:
                ResultCopy {
                    source_table,
                    clone_sqls,
                    ..
                },
            ..
        } = outcome
        else {
            panic!("{dialect}: expected Copy, got {outcome:?}");
        };
        assert_eq!(source_table, "\"db\".\"prod\".\"result\"", "{dialect}");
        assert_eq!(
            clone_sqls,
            ["CREATE OR REPLACE TABLE \"db\".\"dev\".\"result\" AS SELECT * FROM \"db\".\"prod\".\"result\""],
            "{dialect}"
        );
    }
}
