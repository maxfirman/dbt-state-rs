//! #271: a client-requested copy (RegisterClone) is reused only over the
//! inputs the build it holds read.
//!
//! The dev-clone pre-step copies prod's downstream into an absent dev
//! relation before the downstream's own submit. The copy holds the rows the
//! prod build computed from prod's upstream. When dev built its own upstream
//! (another relation, with other rows), the post-copy submit must rebuild
//! over dev's upstream; when both read the same physical upstream, it SKIPs.
//! VERIFIED LIVE 2026-11-28 (hosted, dbt 2.0.11,
//! `golden/fixtures/explain_register_clone_input_origin.jsonl`; report
//! `docs/reports/snowflake-register-clone-input-origin-2026-11-28.md`).

#[path = "support.rs"]
mod support;

use dbt_state_proto::query_cache as qc;
use support::demo::{self, relation, transient_at, DEV_SCHEMA, NS, PROD_SCHEMA};
use support::legacy;
use support::scenario::{ExecutionType, Scenario, Submit};
use support::start_server;

const BASE: i64 = 1_795_864_600_000;
const INPUT_EPOCH: i64 = BASE;
const PROD_UP_EPOCH: i64 = BASE + 5_000;
const PROD_DN_EPOCH: i64 = BASE + 10_000;
const DEV_UP_EPOCH: i64 = BASE + 20_000;
const COPY_EPOCH: i64 = BASE + 30_000;
const REBUILD_EPOCH: i64 = BASE + 40_000;
const NOW: i64 = BASE + 60_000;

/// The upstream table `name` of `schema`, over its own input `IN_<name>` of
/// that schema.
fn upstream(schema: &str, name: &str) -> Submit {
    demo::model(
        &relation(schema, name),
        schema,
        &format!("model.jaffle_shop.{}", name.to_lowercase()),
    )
    .sql(&format!(
        "select id, v from CDO_DEMO_DB.{schema}.in_{}",
        name.to_lowercase()
    ))
    .namespace(NS)
    .tolerance_seconds(0)
    .upstream(&relation(schema, &format!("IN_{name}")), INPUT_EPOCH)
}

/// The downstream `name` of `schema`, of `kind`, reading `from` at `epoch`,
/// its own relation reported at `own`.
fn downstream(
    kind: ExecutionType,
    schema: &str,
    name: &str,
    from: &str,
    epoch: i64,
    own: Option<i64>,
) -> Submit {
    let [_, from_schema, from_name] = split(from);
    let model = demo::model(
        &relation(schema, name),
        schema,
        &format!("model.jaffle_shop.{}", name.to_lowercase()),
    )
    .execution_type(kind)
    .sql(&format!(
        "select id, v from CDO_DEMO_DB.{}.{}",
        from_schema.to_lowercase(),
        from_name.to_lowercase()
    ))
    .namespace(NS)
    .own_epoch(own)
    .upstream(from, epoch);
    // A snapshot cannot set its Lag tolerance: the client sends 45 minutes.
    if kind == ExecutionType::Snapshot {
        model
    } else {
        model.tolerance_seconds(0)
    }
}

fn split(name: &str) -> [String; 3] {
    let parts: Vec<String> = name
        .split('.')
        .map(|p| p.trim_matches('"').into())
        .collect();
    parts.try_into().expect("a three-part name")
}

/// Prod builds its upstream and the downstream `name` over it; the
/// pre-step copies that build into the absent dev relation, confirmed at
/// [`COPY_EPOCH`].
async fn copy_prod_downstream(sc: &Scenario, kind: ExecutionType, name: &str, up: &str) {
    sc.build_with(
        downstream(kind, PROD_SCHEMA, name, up, PROD_UP_EPOCH, None),
        transient_at(PROD_DN_EPOCH).runtime(1_500),
    )
    .await;
    let copy = sc
        .register_clone(
            demo::copy(&relation(PROD_SCHEMA, name), &relation(DEV_SCHEMA, name))
                .execution_type(kind)
                // The client reports the source's warehouse metadata epoch,
                // slightly before the build's confirmation (379 ms in the
                // capture).
                .source_epoch(PROD_DN_EPOCH - 379)
                .label(
                    "dbt_node_unique_id",
                    &format!("model.jaffle_shop.{}", name.to_lowercase()),
                ),
        )
        .await
        .expect("RegisterClone")
        .ready_to_clone
        .expect("the pre-step copy is granted");
    assert!(sc.confirm(&copy.request_id, transient_at(COPY_EPOCH)).await);
}

fn render(lines: &[qc::ExplainLine], depth: usize, out: &mut Vec<String>) {
    for line in lines {
        out.push(format!("{}{}", "  ".repeat(depth), line.text));
        render(&line.children, depth + 1, out);
    }
}

async fn explained(sc: &Scenario, id: &str) -> Vec<String> {
    let explained = sc.explain(&[id]).await;
    assert_eq!(explained.messages.len(), 1);
    let mut out = Vec::new();
    render(&explained.messages[0].explain_lines, 0, &mut out);
    out
}

/// Case I: dev's upstream was built independently, from dev's own input,
/// before the copy. The first post-copy submit rebuilds over it, with
/// hosted's answer and tree: the dev upstream, which the copied build did
/// not read, is listed with its `last updated` line only. The unchanged
/// rerun reuses that rebuild.
#[tokio::test]
async fn a_copy_over_an_independently_built_dev_upstream_rebuilds_once_then_skips() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await.at(NOW);
    // Each downstream reads its own upstream, so neither is an equivalent of
    // the other's rebuild.
    for (kind, name, up, noun) in [
        (ExecutionType::Merge, "DM", "UM", "model"),
        (ExecutionType::Snapshot, "DS", "US", "snapshot"),
    ] {
        let prod_up = relation(PROD_SCHEMA, up);
        let dev_up = relation(DEV_SCHEMA, up);
        sc.build(upstream(PROD_SCHEMA, up), PROD_UP_EPOCH).await;
        sc.build(upstream(DEV_SCHEMA, up), DEV_UP_EPOCH).await;
        copy_prod_downstream(&sc, kind, name, &prod_up).await;

        let post_copy = downstream(
            kind,
            DEV_SCHEMA,
            name,
            &dev_up,
            DEV_UP_EPOCH,
            Some(COPY_EPOCH),
        );
        let x = sc.expect_execute(post_copy.clone()).await;
        let ed = x.explained_decision.expect("explained_decision");
        assert_eq!(
            (
                ed.skip_rejection_reason,
                ed.clone_rejection_reason,
                ed.is_stale
            ),
            (Some(6), Some(6), false),
            "{name}: hosted's 6 / 6"
        );
        assert_eq!(
            ed.decision_description,
            format!(
                "{noun} was executed because either its query didn't match or its upstream data is out of date"
            )
        );
        let tree = explained(&sc, x.execution_decision_id.as_deref().unwrap()).await;
        assert_eq!(
            tree,
            [
                format!("table analysis ({})", relation(DEV_SCHEMA, name)),
                format!("  the {noun} table exists already"),
                "query analysis".into(),
                format!("  the {noun} query has not changed"),
                "  upstream model queries have not changed".into(),
                "data freshness analysis".into(),
                "  upstream dependencies".into(),
                format!("    {dev_up}"),
                "      last updated: 40 seconds ago, 2026-11-28 11:17:00 UTC".into(),
            ],
            "{name}: hosted's tree (no verdict, no summary)"
        );
        assert!(
            sc.confirm(&x.request_id, transient_at(REBUILD_EPOCH)).await,
            "{name}: the rebuild over the copy is confirmed"
        );

        let rerun = downstream(
            kind,
            DEV_SCHEMA,
            name,
            &dev_up,
            DEV_UP_EPOCH,
            Some(REBUILD_EPOCH),
        );
        sc.expect_skip(rerun).await;
    }
}

/// Case S: both downstreams read prod's upstream. The copy holds a build of
/// exactly what the submit reads, so the post-copy submit reuses it.
#[tokio::test]
async fn a_copy_over_the_shared_physical_upstream_is_reused() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await.at(NOW);
    let prod_up = relation(PROD_SCHEMA, "UP");
    sc.build(upstream(PROD_SCHEMA, "UP"), PROD_UP_EPOCH).await;
    for kind in [ExecutionType::Merge, ExecutionType::Snapshot] {
        let name = if kind == ExecutionType::Merge {
            "SM"
        } else {
            "SS"
        };
        copy_prod_downstream(&sc, kind, name, &prod_up).await;
        let post_copy = downstream(
            kind,
            DEV_SCHEMA,
            name,
            &prod_up,
            PROD_UP_EPOCH,
            Some(COPY_EPOCH),
        );
        let skip = sc.expect_skip(post_copy.clone()).await;
        assert_eq!(
            skip.execution_runtime_ms,
            Some(1_500),
            "the copied build's runtime"
        );
        let tree = explained(&sc, skip.execution_decision_id.as_deref().unwrap()).await;
        assert!(
            tree.contains(&format!(
                "      no updates since {} last executed",
                relation(DEV_SCHEMA, name)
            )) && tree
                .last()
                .unwrap()
                .ends_with("upstream data is up to date"),
            "{name}: the FRESH listing of the SKIP: {tree:#?}"
        );
        sc.expect_skip(post_copy).await;
    }
}

/// The copy holds the rows its source build computed from prod's upstream
/// at [`PROD_UP_EPOCH`]. A prod upstream rebuilt after that build, but
/// before the copy, is drift for the copy too: the copy's age proves
/// nothing about the inputs of the rows it holds.
#[tokio::test]
async fn a_copy_is_judged_against_the_inputs_of_the_build_it_holds() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await.at(NOW);
    let prod_up = relation(PROD_SCHEMA, "UP");
    sc.build(upstream(PROD_SCHEMA, "UP"), PROD_UP_EPOCH).await;
    copy_prod_downstream(&sc, ExecutionType::Merge, "SM", &prod_up).await;
    let newer = PROD_DN_EPOCH + 5_000;
    assert!(newer < COPY_EPOCH);
    let post_copy = downstream(
        ExecutionType::Merge,
        DEV_SCHEMA,
        "SM",
        &prod_up,
        newer,
        Some(COPY_EPOCH),
    );
    let x = sc.expect_execute(post_copy).await;
    assert_eq!(
        x.explained_decision.unwrap().decision_description,
        "model was executed because either its query didn't match or its upstream data is out of date"
    );
}

/// A copy registered without the build it was copied from (before migration
/// 0029 recorded it) has no evidence of what its rows were computed from:
/// its copy time alone never proves its inputs, so a submit with upstreams
/// rebuilds.
#[tokio::test]
async fn a_copy_without_source_ancestry_is_not_reused_over_upstreams() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await.at(NOW);
    let prod_up = relation(PROD_SCHEMA, "UP");
    sc.build(upstream(PROD_SCHEMA, "UP"), PROD_UP_EPOCH).await;
    copy_prod_downstream(&sc, ExecutionType::Merge, "SM", &prod_up).await;
    let dev = relation(DEV_SCHEMA, "SM");
    assert_eq!(legacy::forget_copy_source_execution(&server, &dev).await, 1);
    let post_copy = downstream(
        ExecutionType::Merge,
        DEV_SCHEMA,
        "SM",
        &prod_up,
        PROD_UP_EPOCH,
        Some(COPY_EPOCH),
    );
    // Prod's build, which read the submit's input, is reported as the
    // equivalent under another name (1 / 6); a MERGE is not copied on submit.
    let x = sc.expect_execute(post_copy).await;
    let ed = x.explained_decision.expect("explained_decision");
    assert_eq!(
        (ed.skip_rejection_reason, ed.clone_rejection_reason),
        (Some(1), Some(6))
    );
}
