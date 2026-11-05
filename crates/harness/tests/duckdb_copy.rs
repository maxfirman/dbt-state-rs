//! Conservative table result copying through the production service.
mod support;
use dbt_state_proto::query_cache as qc;
use qc::{execution_client::ExecutionClient, sql_client::SqlClient};
use support::{channel, response_variant, start_server};
fn request(schema: &str) -> qc::SubmitEnrichedSqlRequest {
    qc::SubmitEnrichedSqlRequest {
        target_table: Some(format!("\"db\".\"{schema}\".\"result\"")),
        dialect: "duckdb".into(),
        default_catalog: "db".into(),
        default_schema: Some(schema.into()),
        table_namespace: Some("copy-file-generation".into()),
        sql: "select id, amount + 1 as amount from db.raw.input".into(),
        execution_type: 1,
        tolerate_nondeterminism: true,
        semantic_extras: [("duckdb_copy_contract".into(), "rows-schema-v1".into())].into(),
        tables: vec![qc::TableModifiedInfo {
            name: "db.raw.input".into(),
            last_modified_epoch: Some(10),
        }],
        ..Default::default()
    }
}
#[tokio::test]
async fn eligible_table_copy_confirms_then_skips_preserving_physical_inputs() {
    let (addr, _) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);
    let prod = sql
        .submit_enriched_sql(request("prod"))
        .await
        .unwrap()
        .into_inner();
    let Some(qc::submit_sql_response::Response::ReadyToExecute(build)) = prod.response else {
        panic!("BUILD required")
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: build.request_id,
        last_modified_epoch: Some(20),
        table_type: Some("DUCKDB CTAS TABLE".into()),
        ..Default::default()
    })
    .await
    .unwrap();
    let dev = request("dev");
    for case in 0..10 {
        let mut unsupported = dev.clone();
        // An unrelated physical base input must never become equivalent.
        match case {
            0 => unsupported.sql = unsupported.sql.replace("raw.input", "other.input"),
            1 => unsupported.default_schema = Some("unapproved".into()),
            2 => {
                unsupported.semantic_extras.remove("duckdb_copy_contract");
            }
            3 => unsupported.sql = "select id from input".into(),
            4 => unsupported.allow_clones = Some(false),
            5 => {
                unsupported.clone_table_properties = Some(qc::TableProperties {
                    hours_to_expiration: Some(1),
                    partition_expiration_days: None,
                })
            }
            6 => unsupported.tables.clear(),
            7 => unsupported.tables[0].last_modified_epoch = None,
            8 => unsupported.tables.push(unsupported.tables[0].clone()),
            _ => unsupported.tables.push(qc::TableModifiedInfo {
                name: "db._dbt_state.unresolved".into(),
                last_modified_epoch: None,
            }),
        }
        let result = sql
            .submit_enriched_sql(unsupported)
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response_variant(&result), "ready_to_execute");
    }
    let result = sql
        .submit_enriched_sql(dev.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response_variant(&result), "ready_to_clone");
    let Some(qc::submit_sql_response::Response::ReadyToClone(clone)) = result.response else {
        panic!("copy required")
    };
    assert_eq!(clone.clone_required_last_modified_epoch, Some(20));
    assert_eq!(clone.clone_source, "\"db\".\"prod\".\"result\"");
    assert_eq!(clone.clone_target, dev.target_table.clone().unwrap());
    assert_eq!(clone.clone_sqls, ["CREATE OR REPLACE TABLE \"db\".\"dev\".\"result\" AS SELECT * FROM \"db\".\"prod\".\"result\""]);
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: clone.request_id,
        last_modified_epoch: Some(30),
        table_type: Some("DUCKDB CTAS TABLE".into()),
        ..Default::default()
    })
    .await
    .unwrap();
    let mut later = dev;
    later.tables.push(qc::TableModifiedInfo {
        name: later.target_table.clone().unwrap(),
        last_modified_epoch: Some(30),
    });
    assert_eq!(
        response_variant(&sql.submit_enriched_sql(later).await.unwrap().into_inner()),
        "skip_execution"
    );
}

#[tokio::test]
async fn failed_sql_copy_records_actual_build_and_becomes_an_ordinary_source() {
    let (addr, _) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);
    let prod = sql
        .submit_enriched_sql(request("prod"))
        .await
        .unwrap()
        .into_inner();
    let Some(qc::submit_sql_response::Response::ReadyToExecute(build)) = prod.response else {
        panic!("BUILD required")
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: build.request_id,
        last_modified_epoch: Some(20),
        table_type: Some("DUCKDB CTAS TABLE".into()),
        ..Default::default()
    })
    .await
    .unwrap();
    let result = sql
        .submit_enriched_sql(request("dev"))
        .await
        .unwrap()
        .into_inner();
    let Some(qc::submit_sql_response::Response::ReadyToClone(copy)) = result.response else {
        panic!("copy required")
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: copy.request_id,
        failed_to_clone: true,
        last_modified_epoch: Some(30),
        table_type: Some("DUCKDB CTAS TABLE".into()),
        ..Default::default()
    })
    .await
    .unwrap();
    let mut next = request("prod");
    next.target_table = Some("\"db\".\"prod\".\"next\"".into());
    next.clone_chain_depth_limit = Some(1);
    let result = sql.submit_enriched_sql(next).await.unwrap().into_inner();
    let Some(qc::submit_sql_response::Response::ReadyToClone(copy)) = result.response else {
        panic!("ordinary fallback should be a valid source")
    };
    // With a one-hop client limit, selecting this fallback proves it became an
    // ordinary source rather than retaining false copy provenance.
    assert_eq!(copy.clone_source, "\"db\".\"dev\".\"result\"");
    assert_eq!(copy.clone_required_last_modified_epoch, Some(30));
}

fn graph_request(schema: &str) -> qc::SubmitEnrichedSqlRequest {
    let mut req = request(schema);
    req.sql = format!("select id, amount + 1 as amount from db.{schema}.upper_view");
    req.query_dependencies = vec![
        qc::QueryDependency {
            name: format!("db.{schema}.upper_view"),
            query: format!(
                "CREATE VIEW {schema}.upper_view AS SELECT id, amount FROM {schema}.lower_view"
            ),
            default_catalog: "db".into(),
            default_schema: schema.into(),
        },
        qc::QueryDependency {
            name: format!("db.{schema}.lower_view"),
            query: "CREATE VIEW lower_view AS SELECT id, amount FROM raw.input".into(),
            default_catalog: "db".into(),
            default_schema: schema.into(),
        },
    ];
    req
}

#[tokio::test]
async fn view_graph_copy_binds_definitions_context_and_exact_leaves() {
    let (addr, _) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);
    let response = sql
        .submit_enriched_sql(graph_request("prod"))
        .await
        .unwrap()
        .into_inner();
    let Some(qc::submit_sql_response::Response::ReadyToExecute(build)) = response.response else {
        panic!("build")
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: build.request_id,
        last_modified_epoch: Some(20),
        table_type: Some("DUCKDB CTAS TABLE".into()),
        ..Default::default()
    })
    .await
    .unwrap();
    let dev = graph_request("dev");
    let response = sql
        .submit_enriched_sql(dev.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response_variant(&response), "ready_to_clone");
    // An unqualified reference resolves through the explicit query context.
    let mut unqualified = dev.clone();
    unqualified.sql = "select id, amount + 1 as amount from upper_view".into();
    assert_eq!(
        response_variant(
            &sql.submit_enriched_sql(unqualified)
                .await
                .unwrap()
                .into_inner()
        ),
        "ready_to_clone"
    );
    // Dependency list order changes no reference-to-definition bindings.
    let mut reordered = dev.clone();
    reordered.query_dependencies.reverse();
    assert_eq!(
        response_variant(
            &sql.submit_enriched_sql(reordered)
                .await
                .unwrap()
                .into_inner()
        ),
        "ready_to_clone"
    );
    for case in 0..8 {
        let mut req = dev.clone();
        match case {
            0 => {
                req.query_dependencies[1].query =
                    "select id, amount + 3 as amount from raw.input".into()
            }
            1 => req.query_dependencies[1].query = "select id, amount from other.input".into(),
            2 => req.query_dependencies[1].default_schema = "other".into(),
            3 => req
                .query_dependencies
                .push(req.query_dependencies[0].clone()),
            4 => req.query_dependencies.swap(0, 1), // restore order below, but swap bound definitions
            5 => req.query_dependencies[1].query = "select id, amount from lower_view".into(),
            6 => {
                req.sql = req.sql.replace("dev.upper_view", "environment.upper_view");
                req.query_dependencies[0].name = "db.environment.upper_view".into();
            }
            _ => {
                let mut duplicate = req.query_dependencies[0].clone();
                duplicate.name = "db.dev.UPPER_VIEW".into();
                req.query_dependencies.push(duplicate);
            }
        }
        if case == 4 {
            let query = req.query_dependencies[0].query.clone();
            req.query_dependencies[0].query = req.query_dependencies[1].query.clone();
            req.query_dependencies[1].query = query;
        }
        let response = sql.submit_enriched_sql(req).await.unwrap().into_inner();
        assert_eq!(
            response_variant(&response),
            "ready_to_execute",
            "negative {case}"
        );
    }
}

#[tokio::test]
async fn graph_copy_chain_supports_two_hops_and_rejects_a_third() {
    let (addr, _) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);
    let mut previous = String::new();
    for (step, schema) in ["prod", "dev", "prod", "dev"].iter().enumerate() {
        if step == 3 {
            // A later ordinary rebuild of an ancestor must not shorten the
            // already confirmed third relation's two-hop provenance.
            let mut rebuild = graph_request("dev");
            rebuild.target_table = Some("\"db\".\"dev\".\"result1\"".into());
            rebuild.allow_clones = Some(false);
            let response = sql.submit_enriched_sql(rebuild).await.unwrap().into_inner();
            let Some(qc::submit_sql_response::Response::ReadyToExecute(build)) = response.response
            else {
                panic!("ordinary ancestor rebuild");
            };
            exec.confirm_execution(qc::ConfirmExecutionRequest {
                request_id: build.request_id,
                last_modified_epoch: Some(35),
                table_type: Some("DUCKDB CTAS TABLE".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        }
        let mut req = graph_request(schema);
        req.target_table = Some(format!("\"db\".\"{schema}\".\"result{step}\""));
        let response = sql
            .submit_enriched_sql(req.clone())
            .await
            .unwrap()
            .into_inner();
        let id = match response.response {
            Some(qc::submit_sql_response::Response::ReadyToExecute(build))
                if step == 0 || step == 3 =>
            {
                build.request_id
            }
            Some(qc::submit_sql_response::Response::ReadyToClone(copy))
                if step == 1 || step == 2 =>
            {
                assert_eq!(copy.clone_source, previous);
                assert_eq!(copy.clone_target, req.target_table.clone().unwrap());
                copy.request_id
            }
            _ => panic!("unexpected decision at step {step}"),
        };
        previous = req.target_table.unwrap();
        exec.confirm_execution(qc::ConfirmExecutionRequest {
            request_id: id,
            last_modified_epoch: Some(20 + step as i64 * 10),
            table_type: Some("DUCKDB CTAS TABLE".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    }
}
