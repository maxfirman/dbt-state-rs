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
use support::scenario::{
    confirm_in, duckdb_ctas_at, duckdb_model, environment_model, environment_seed,
    environment_upstream, outcome,
};
use support::TestDatabase;

const ORG: &str = "org-submit-decision";
const PROD: &str = "\"DB\".\"PROD\".\"M\"";
const DEV: &str = "\"DB\".\"DEV\".\"M\"";
const BUILT: i64 = 1_000;

async fn submit_sql(store: &Store, req: impl Into<qc::SubmitEnrichedSqlRequest>) -> SubmitOutcome {
    submit_decision::submit_sql(store, ORG, &req.into(), now_ms())
        .await
        .expect("submit")
}

async fn submit_values(store: &Store, req: impl Into<qc::SubmitValuesRequest>) -> SubmitOutcome {
    submit_decision::submit_values(store, ORG, &req.into(), now_ms())
        .await
        .expect("submit")
}

fn now_ms() -> i64 {
    dbt_state_server::Clock::System.now_ms(&Default::default())
}

/// Confirm a build at `epoch` through the store's confirm path.
async fn confirm(store: &Store, ids: &DecisionIds, epoch: i64) {
    let confirmed = outcome().epoch(epoch).table().runtime(42);
    assert!(confirm_in(store, ORG, confirmed.confirm(&ids.request_id)).await);
}

/// Submit, expect an ordinary Execute, and confirm it at `epoch`.
async fn build(store: &Store, req: impl Into<qc::SubmitEnrichedSqlRequest>, epoch: i64) -> i64 {
    let outcome = submit_sql(store, req).await;
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

    let outcome = submit_sql(store, environment_model("prod")).await;
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
    let built = build(store, environment_model("prod"), BUILT).await;
    let written = rows::executions_in(store, ORG).await.len();

    let outcome = submit_sql(store, environment_model("prod").own_epoch(Some(BUILT))).await;
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
    let built = build(store, environment_model("prod"), BUILT).await;

    // Same identity, but the upstream changed after the build.
    let outcome = submit_sql(
        store,
        environment_model("prod")
            .own_epoch(Some(BUILT))
            .upstream(&environment_upstream("prod"), 2_000),
    )
    .await;
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
    build(store, environment_model("prod"), BUILT).await;

    let outcome = submit_sql(store, environment_model("dev")).await;
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
    build(store, environment_model("prod"), BUILT).await;
    // DEV becomes a confirmed copy of PROD: its own reuse candidate.
    let SubmitOutcome::Copy { ids, .. } = submit_sql(store, environment_model("dev")).await else {
        panic!("expected the first DEV submit to copy");
    };
    confirm(store, &ids, BUILT).await;
    let dev_copy = rows::execution_in(store, ORG, &ids.request_id)
        .await
        .unwrap()
        .id;

    let outcome = submit_sql(store, environment_model("dev").own_epoch(Some(BUILT))).await;
    let SubmitOutcome::Skip {
        matched_execution, ..
    } = outcome
    else {
        panic!("a fresh candidate skips instead of copying, got {outcome:?}");
    };
    assert_eq!(matched_execution.id, dev_copy);

    let outcome = submit_sql(
        store,
        environment_model("dev")
            .own_epoch(Some(BUILT))
            .upstream(&environment_upstream("dev"), 2_000),
    )
    .await;
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
    build(store, environment_model("prod"), BUILT).await;

    let outcome = submit_sql(store, environment_model("dev").clone_chain_depth_limit(0)).await;
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
    build(store, environment_model("prod"), BUILT).await;

    // No copy DDL exists for an expiring clone, so the equivalent is unusable.
    let req = environment_model("dev").with(|r| {
        r.clone_table_properties = Some(qc::TableProperties {
            hours_to_expiration: Some(1),
            ..Default::default()
        })
    });
    let outcome = submit_sql(store, req).await;
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

    submit_sql(store, environment_model("prod")).await;
    submit_values(store, environment_seed("prod").values("v1")).await;

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

    let outcome = submit_values(store, environment_seed("prod").values("v1")).await;
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

    let outcome = submit_values(
        store,
        environment_seed("prod").values("v1").own_epoch(Some(BUILT)),
    )
    .await;
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

    let outcome = submit_values(
        store,
        environment_seed("prod").values("v2").own_epoch(Some(BUILT)),
    )
    .await;
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
        let outcome = submit_sql(store, req).await;
        assert_eq!(
            outcome.record().matched_execution_id,
            outcome.matched_execution().map(|r| r.id),
            "{outcome:?}"
        );
        outcome
    };

    let first = sql(environment_model("prod").build()).await;
    assert_eq!(first.record().reason, DecisionReason::FirstBuild);
    let SubmitOutcome::Execute { ids, .. } = first else {
        panic!("expected Execute, got {first:?}");
    };
    confirm(store, &ids, BUILT).await;

    let fresh = sql(environment_model("prod").own_epoch(Some(BUILT)).build()).await;
    assert_eq!(fresh.record().reason, DecisionReason::Reused(Reuse::Fresh));
    let outdated = sql(environment_model("prod")
        .own_epoch(Some(BUILT))
        .upstream(&environment_upstream("prod"), 2_000)
        .build())
    .await;
    assert_eq!(outdated.record().reason, DecisionReason::UpstreamOutdated);
    let edited = sql(environment_model("prod").own_epoch(Some(BUILT + 1)).build()).await;
    assert_eq!(
        edited.record().reason,
        DecisionReason::NoReuseCandidate(NoCandidate::OwnTargetModified)
    );
    assert_eq!(
        edited.record().description,
        "model was executed because the table was modified after the cached execution"
    );
    let dropped = sql(environment_model("prod").own_epoch(None).build()).await;
    assert_eq!(
        dropped.record().reason,
        DecisionReason::NoReuseCandidate(NoCandidate::OwnTargetMissing)
    );
    assert_eq!(
        dropped.record().description,
        "model was executed because its table did not exist"
    );
    assert_eq!(
        (
            dropped.record().skip_rejection_reason,
            dropped.record().clone_rejection_reason
        ),
        (Some(4), Some(7)),
        "TARGET_TABLE_DOES_NOT_EXIST / TARGET_TABLE_MATCH: the dropped build \
         would otherwise be reusable"
    );
    // VERIFIED LIVE (2026-11-10, lag_dropped_targets `ddc`): when the dropped
    // target's build would be stale too, hosted reports 6/6.
    let dropped_stale = sql(environment_model("prod")
        .own_epoch(None)
        .upstream(&environment_upstream("prod"), 2_000)
        .build())
    .await;
    assert_eq!(
        dropped_stale.record().reason,
        DecisionReason::NoReuseCandidate(NoCandidate::OwnTargetMissing)
    );
    assert_eq!(
        (
            dropped_stale.record().skip_rejection_reason,
            dropped_stale.record().clone_rejection_reason
        ),
        (Some(6), Some(6))
    );

    let forbids_copies = environment_model("dev").clone_chain_depth_limit(0);
    let limit = sql(forbids_copies.build()).await;
    assert_eq!(limit.record().reason, DecisionReason::CopyChainLimit);
    let copy = sql(environment_model("dev").build()).await;
    assert_eq!(copy.record().reason, DecisionReason::ResultCopy);

    let seed_first = submit_values(store, environment_seed("prod").values("v1")).await;
    assert_eq!(seed_first.record().reason, DecisionReason::FirstBuild);
    let SubmitOutcome::Execute { ids, .. } = seed_first else {
        panic!("expected Execute, got {seed_first:?}");
    };
    confirm(store, &ids, BUILT).await;
    let changed = submit_values(
        store,
        environment_seed("prod").values("v2").own_epoch(Some(BUILT)),
    )
    .await;
    assert_eq!(
        changed.record().reason,
        DecisionReason::NoReuseCandidate(NoCandidate::IdentityMismatch)
    );
    assert_eq!(changed.record().matched_execution_id, None);
}

/// Reuse reads only the target's latest confirmed row in scope: an older
/// build that still matches does not resurface once a different build of the
/// target was confirmed after it, even when that later build's row was
/// written first.
#[tokio::test]
async fn reuse_reads_only_the_latest_confirmed_build() {
    let db = TestDatabase::new("submit_decision").await;
    let state = db.state(false).await;
    let store = &state.store;
    let original = environment_model("prod");
    let changed = environment_model("prod").sql("select id, 2 as v from DB.prod.up");

    // `changed` is submitted first and `original` second, so `original`'s
    // row has the larger id; `original` is confirmed first, `changed` last.
    let SubmitOutcome::Execute {
        ids: changed_ids, ..
    } = submit_sql(store, changed.clone()).await
    else {
        panic!("expected the first build");
    };
    let SubmitOutcome::Execute {
        ids: original_ids, ..
    } = submit_sql(store, original.clone()).await
    else {
        panic!("expected the first build");
    };
    confirm(store, &original_ids, BUILT).await;
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    confirm(store, &changed_ids, BUILT).await;
    let current = rows::execution_in(store, ORG, &changed_ids.request_id)
        .await
        .expect("confirmed row")
        .id;

    // Both builds report the target at BUILT; only the later one is current.
    let outcome = submit_sql(store, original.own_epoch(Some(BUILT))).await;
    let SubmitOutcome::Execute {
        matched_execution,
        record,
        ..
    } = outcome
    else {
        panic!("an older matching build must not be reused, got {outcome:?}");
    };
    assert!(matched_execution.is_none());
    assert_eq!(
        record.reason,
        DecisionReason::NoReuseCandidate(NoCandidate::IdentityMismatch)
    );

    let outcome = submit_sql(store, changed.own_epoch(Some(BUILT))).await;
    let SubmitOutcome::Skip {
        matched_execution, ..
    } = outcome
    else {
        panic!("the current build is reused, got {outcome:?}");
    };
    assert_eq!(matched_execution.id, current);
}

/// A pending row is not State history: a later submit that was never
/// confirmed does not hide the latest confirmed build.
#[tokio::test]
async fn pending_row_does_not_hide_the_latest_confirmed_build() {
    let db = TestDatabase::new("submit_decision").await;
    let state = db.state(false).await;
    let store = &state.store;
    let original = environment_model("prod");
    let built = build(store, original.clone(), BUILT).await;

    let changed = environment_model("prod")
        .sql("select id, 2 as v from DB.prod.up")
        .own_epoch(Some(BUILT));
    let outcome = submit_sql(store, changed).await;
    let SubmitOutcome::Execute { ids, .. } = outcome else {
        panic!("changed SQL rebuilds, got {outcome:?}");
    };
    assert!(rows::execution_in(store, ORG, &ids.request_id)
        .await
        .expect("pending row written")
        .is_pending());

    let outcome = submit_sql(store, original.own_epoch(Some(BUILT))).await;
    let SubmitOutcome::Skip {
        matched_execution, ..
    } = outcome
    else {
        panic!("the confirmed build is still current, got {outcome:?}");
    };
    assert_eq!(matched_execution.id, built);
}

/// The Result copy fallback reads each target's latest confirmed row too:
/// once the equivalent's target was rebuilt with other logic, its older
/// matching build is no copy source.
#[tokio::test]
async fn result_copy_reads_only_the_latest_confirmed_build_of_each_target() {
    let db = TestDatabase::new("submit_decision").await;
    let state = db.state(false).await;
    let store = &state.store;
    build(store, environment_model("prod"), BUILT).await;
    let rebuilt = environment_model("prod")
        .sql("select id, 2 as v from DB.prod.up")
        .own_epoch(Some(BUILT));
    build(store, rebuilt, BUILT + 1).await;

    let outcome = submit_sql(store, environment_model("dev")).await;
    let SubmitOutcome::Execute {
        matched_execution,
        record,
        ..
    } = outcome
    else {
        panic!("PROD's older build must not be copied, got {outcome:?}");
    };
    assert!(matched_execution.is_none());
    assert_eq!(record.reason, DecisionReason::FirstBuild);
}

/// A same-upstream equivalent rejected on freshness does not end the copy
/// search (Codex review, PR #69; VERIFIED LIVE 2026-11-10, lag_copy_candidates
/// `sne`): the newest build of an equivalent can have consumed older
/// upstream data than an older build of another, and hosted then copies the
/// current one.
#[tokio::test]
async fn result_copy_skips_a_stale_newest_equivalent() {
    let db = TestDatabase::new("submit_decision").await;
    let state = db.state(false).await;
    let store = &state.store;
    let up = environment_upstream("prod");
    let named =
        |table: &str| environment_model("prod").target(&format!("\"DB\".\"PROD\".\"{table}\""));

    // STALE read the upstream at 500 but its build finished last (epoch 9_000);
    // FRESH read it at 2_000 and finished earlier (epoch 3_000).
    build(store, named("STALE"), 9_000).await;
    build(store, named("FRESH").upstream(&up, 2_000), 3_000).await;

    let outcome = submit_sql(store, environment_model("prod").upstream(&up, 2_000)).await;
    let SubmitOutcome::Copy { copy, record, .. } = outcome else {
        panic!("the current equivalent is copied, got {outcome:?}");
    };
    assert_eq!(copy.source_table, "\"DB\".\"PROD\".\"FRESH\"");
    assert!(!record.is_stale);
}

/// After a same-upstream equivalent is rejected on freshness, the search
/// accepts only another same-upstream equivalent: falling back to an unjudged
/// copy from another environment is uncharacterized, so the target rebuilds.
#[tokio::test]
async fn result_copy_does_not_fall_back_to_another_environment_after_a_stale_equivalent() {
    let db = TestDatabase::new("submit_decision").await;
    let state = db.state(false).await;
    let store = &state.store;
    let up = environment_upstream("prod");
    build(store, environment_model("dev"), 5_000).await;
    // STALE, in prod, is copied from dev and confirmed later (newest build).
    let stale = environment_model("prod").target("\"DB\".\"PROD\".\"STALE\"");
    let SubmitOutcome::Copy { ids, .. } = submit_sql(store, stale).await else {
        panic!("STALE is copied from dev");
    };
    confirm(store, &ids, 9_000).await;

    // Control: with prod's upstream unchanged, STALE is current and copied.
    let outcome = submit_sql(store, environment_model("prod")).await;
    let SubmitOutcome::Copy { copy, .. } = outcome else {
        panic!("the current equivalent is copied, got {outcome:?}");
    };
    assert_eq!(copy.source_table, "\"DB\".\"PROD\".\"STALE\"");

    // Prod's upstream moved on: STALE is rejected, and dev is not a fallback.
    let outcome = submit_sql(store, environment_model("prod").upstream(&up, 2_000)).await;
    let SubmitOutcome::Execute { record, .. } = outcome else {
        panic!("no characterized copy remains, got {outcome:?}");
    };
    assert_eq!(record.reason, DecisionReason::FirstBuild);
}
/// gets the same copy, past the volatility gate, as one reporting `duckdb`.
#[tokio::test]
async fn duckdb_result_copy_ignores_dialect_case() {
    let db = TestDatabase::new("submit_decision").await;
    let state = db.state(false).await;
    let store = &state.store;
    for dialect in ["duckdb", "DuckDB", "DUCKDB"] {
        let namespace = format!("ns-{dialect}");
        // The copy-eligible DuckDB model in `schema`, reported under the wire
        // spelling `dialect` and requesting volatile evaluation. DuckDB has no
        // checked volatile-function detector, so only its schema-abstracted
        // copy identity lets such a submit be copied.
        let model = |schema: &str| {
            duckdb_model(schema)
                .dialect(dialect)
                .namespace(&namespace)
                .tolerate_nondeterminism(false)
        };
        let outcome = submit_sql(store, model("prod")).await;
        let SubmitOutcome::Execute { ids, .. } = outcome else {
            panic!("{dialect}: expected the first build, got {outcome:?}");
        };
        let confirmed = duckdb_ctas_at(20).runtime(42);
        assert!(confirm_in(store, ORG, confirmed.confirm(&ids.request_id)).await);

        let outcome = submit_sql(store, model("dev")).await;
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

// ---- seed copy on submit (#91) ---------------------------------------------

const PROD_SEED: &str = "\"DB\".\"PROD\".\"SEED\"";
const DEV_SEED: &str = "\"DB\".\"DEV\".\"SEED\"";
const SEED_COPY_DESCRIPTION: &str =
    "an equivalent seed exists under another name so we cloned that one";

/// Submit a seed, expect an ordinary load, and confirm it at `epoch`.
async fn load(store: &Store, req: impl Into<qc::SubmitValuesRequest>, epoch: i64) -> i64 {
    let outcome = submit_values(store, req).await;
    let SubmitOutcome::Execute { ids, .. } = outcome else {
        panic!("expected a load, got {outcome:?}");
    };
    confirm(store, &ids, epoch).await;
    rows::execution_in(store, ORG, &ids.request_id)
        .await
        .expect("confirmed row")
        .id
}

/// Expect the ordinary seed load: no copy.
fn expect_load(outcome: SubmitOutcome, why: &str) -> DecisionRecord {
    let SubmitOutcome::Execute { record, .. } = outcome else {
        panic!("{why}: the seed is loaded, not copied; got {outcome:?}");
    };
    record
}

/// A Snowflake seed loaded with the same values in another schema of its
/// namespace is copied into its absent table (VERIFIED LIVE 2026-11-10,
/// explain_copy_probes.jsonl entry 12), and the confirmed copy is then reused.
#[tokio::test]
async fn a_seed_loaded_in_another_schema_is_copied_into_its_absent_table() {
    let db = TestDatabase::new("submit_decision").await;
    let state = db.state(false).await;
    let store = &state.store;
    load(store, environment_seed("prod").values("v1"), BUILT).await;

    let outcome = submit_values(store, environment_seed("dev").values("v1")).await;
    let SubmitOutcome::Copy { ids, copy, record } = outcome else {
        panic!("expected Copy, got {outcome:?}");
    };
    assert_eq!(copy.source_table, PROD_SEED);
    assert_eq!(
        copy.clone_sqls,
        [format!(
            "\n            CREATE OR REPLACE TABLE {DEV_SEED}\n            CLONE {PROD_SEED}\n            COPY GRANTS"
        )]
    );
    assert_eq!(copy.source_last_modified_epoch, Some(BUILT));
    assert_eq!(copy.source_execution_runtime_ms, Some(42));
    assert_eq!(record.reason, DecisionReason::ResultCopy);
    assert_eq!(record.description, SEED_COPY_DESCRIPTION);
    assert_eq!(
        (record.skip_rejection_reason, record.clone_rejection_reason),
        (Some(1), None)
    );
    assert_eq!(record.matched_execution_id, None);

    let row = rows::execution_in(store, ORG, &ids.request_id)
        .await
        .expect("pending row written");
    assert_eq!(row.target_table, DEV_SEED, "keyed on the submit target");
    assert_eq!(row.clone_source_table.as_deref(), Some(PROD_SEED));
    assert_eq!(row.execution_type, 9);
    assert!(row.node_sql_hash.is_some(), "the seed's own identity");
    confirm(store, &ids, BUILT + 1).await;

    let outcome = submit_values(
        store,
        environment_seed("dev")
            .values("v1")
            .own_epoch(Some(BUILT + 1)),
    )
    .await;
    assert!(
        matches!(outcome, SubmitOutcome::Skip { .. }),
        "the confirmed copy is reused, got {outcome:?}"
    );
}

/// A seed whose table holds a load with other values is copied over it
/// (VERIFIED LIVE 2026-11-11, explain_seed_copy_existing.jsonl entry 14). The
/// replaced load stays the decision's Explain anchor ("but it's old").
#[tokio::test]
async fn a_seed_is_copied_over_its_table_holding_other_values() {
    let db = TestDatabase::new("submit_decision").await;
    let state = db.state(false).await;
    let store = &state.store;
    load(store, environment_seed("prod").values("v1"), BUILT).await;
    let old = load(store, environment_seed("dev").values("v0"), BUILT + 1).await;

    let outcome = submit_values(
        store,
        environment_seed("dev")
            .values("v1")
            .own_epoch(Some(BUILT + 1)),
    )
    .await;
    let SubmitOutcome::Copy { copy, record, .. } = outcome else {
        panic!("expected Copy, got {outcome:?}");
    };
    assert_eq!(copy.source_table, PROD_SEED);
    assert_eq!(record.description, SEED_COPY_DESCRIPTION);
    assert_eq!(record.matched_execution_id, Some(old));
}

/// A seed table modified after a load with the submit's values is reloaded,
/// not copied (AGENTS.md reuse invariant 3; hosted SKIPs it, #129).
#[tokio::test]
async fn a_seed_whose_table_was_modified_is_reloaded_not_copied() {
    let db = TestDatabase::new("submit_decision").await;
    let state = db.state(false).await;
    let store = &state.store;
    load(store, environment_seed("prod").values("v1"), BUILT).await;
    // The dev seed is copied from prod, then its table is modified.
    let SubmitOutcome::Copy { ids, .. } =
        submit_values(store, environment_seed("dev").values("v1")).await
    else {
        panic!("the dev seed is copied from prod");
    };
    confirm(store, &ids, BUILT + 1).await;

    let outcome = submit_values(
        store,
        environment_seed("dev")
            .values("v1")
            .own_epoch(Some(BUILT + 5)),
    )
    .await;
    let record = expect_load(outcome, "modified table");
    assert_eq!(
        record.reason,
        DecisionReason::NoReuseCandidate(NoCandidate::OwnTargetChangedOrMissing)
    );
}

#[tokio::test]
async fn a_seed_is_not_copied_from_other_values() {
    let db = TestDatabase::new("submit_decision").await;
    let state = db.state(false).await;
    let store = &state.store;
    load(store, environment_seed("prod").values("v1"), BUILT).await;

    let outcome = submit_values(store, environment_seed("dev").values("v2")).await;
    let record = expect_load(outcome, "another values_hash");
    assert_eq!(record.reason, DecisionReason::FirstBuild);
}

/// The same values under other seed config (column types) are another seed.
#[tokio::test]
async fn a_seed_is_not_copied_from_other_seed_config() {
    let db = TestDatabase::new("submit_decision").await;
    let state = db.state(false).await;
    let store = &state.store;
    let typed = |schema: &str, types: &str| {
        environment_seed(schema)
            .values("v1")
            .extra("column_types", types)
    };
    load(store, typed("prod", "{\"id\": \"varchar\"}"), BUILT).await;

    let outcome = submit_values(store, typed("dev", "{\"id\": \"integer\"}")).await;
    expect_load(outcome, "other column types");
    let outcome = submit_values(store, environment_seed("dev").values("v1")).await;
    expect_load(outcome, "no column types");
}

#[tokio::test]
async fn a_seed_is_not_copied_from_another_namespace() {
    let db = TestDatabase::new("submit_decision").await;
    let state = db.state(false).await;
    let store = &state.store;
    load(
        store,
        environment_seed("prod").values("v1").namespace("ns-other"),
        BUILT,
    )
    .await;

    let outcome = submit_values(store, environment_seed("dev").values("v1")).await;
    expect_load(outcome, "another namespace");
}

/// Only the latest load in the source's own State history is a copy source:
/// a prod load since reloaded with other values is superseded.
#[tokio::test]
async fn a_seed_is_not_copied_from_a_superseded_load() {
    let db = TestDatabase::new("submit_decision").await;
    let state = db.state(false).await;
    let store = &state.store;
    load(store, environment_seed("prod").values("v1"), BUILT).await;
    load(
        store,
        environment_seed("prod").values("v2").own_epoch(Some(BUILT)),
        BUILT + 1,
    )
    .await;

    let outcome = submit_values(store, environment_seed("dev").values("v1")).await;
    expect_load(outcome, "superseded prod load");
}

/// `allow_clones: false` (client 2.0.11 sends it with
/// `clone_chain_depth_limit = 0`) and a copy-forbidding target do not copy.
/// Hosted's wording for such a seed is uncaptured, so it gets the ordinary
/// load answer; its decision reason says why it was not copied, so Explain
/// renders no tree claiming there is no other table to copy.
#[tokio::test]
async fn a_seed_is_not_copied_when_clones_are_disallowed() {
    let db = TestDatabase::new("submit_decision").await;
    let state = db.state(false).await;
    let store = &state.store;
    load(store, environment_seed("prod").values("v1"), BUILT).await;

    for (allow_clones, limit, reason) in [
        (Some(false), Some(0), DecisionReason::CopyDisabled),
        (Some(false), None, DecisionReason::CopyDisabled),
        (None, Some(0), DecisionReason::CopyChainLimit),
    ] {
        let seed = environment_seed("dev").values("v1").with(|r| {
            r.allow_clones = allow_clones;
            r.clone_chain_depth_limit = limit;
        });
        let why = format!("allow_clones {allow_clones:?}, limit {limit:?}");
        let record = expect_load(submit_values(store, seed).await, &why);
        assert_eq!(
            record.description, "seed was loaded because it did not exist",
            "{why}"
        );
        assert_eq!(
            (record.skip_rejection_reason, record.clone_rejection_reason),
            (Some(6), Some(6)),
            "{why}"
        );
        assert_eq!(record.reason, reason, "{why}");
    }
}

/// DuckDB reloads seeds: hosted does not engage DuckDB, and its checked copy
/// subset has no seeds (docs/duckdb.md).
#[tokio::test]
async fn a_duckdb_seed_is_reloaded_not_copied() {
    let db = TestDatabase::new("submit_decision").await;
    let state = db.state(false).await;
    let store = &state.store;
    let duck = |schema: &str| environment_seed(schema).values("v1").dialect("duckdb");
    load(store, duck("prod"), BUILT).await;

    let outcome = submit_values(store, duck("dev")).await;
    expect_load(outcome, "duckdb");
}
