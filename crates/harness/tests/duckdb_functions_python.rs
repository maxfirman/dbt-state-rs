//! Real dbt seam: changed function bindings cannot certify stale results.
mod support;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment"]
async fn real_dbt_function_bindings_prevent_stale_reuse() {
    let server = support::start_server().await;
    let project = server.python_project("duckdb-functions");
    project.run("functions.py", &[]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment"]
async fn real_dbt_hook_changes_and_pre_hook_bindings_rebuild() {
    let server = support::start_server().await;
    let project = server.python_project("duckdb-hooks");
    project.run("hooks.py", &[]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment"]
async fn real_dbt_hidden_rowid_references_rebuild_after_reordering() {
    let server = support::start_server().await;
    let project = server.python_project("duckdb-rowid");
    project.run("rowid.py", &[]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment"]
async fn real_dbt_named_type_redefinitions_rebuild() {
    let server = support::start_server().await;
    let project = server.python_project("duckdb-types");
    project.run("semantics.py", &[]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment"]
async fn real_dbt_permissive_scalar_subqueries_rebuild_after_reordering() {
    let server = support::start_server().await;
    let project = server.python_project("duckdb-scalar");
    project.run("semantics.py", &["scalar"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment"]
async fn real_dbt_worker_collation_changes_rebuild() {
    let server = support::start_server().await;
    let project = server.python_project("duckdb-collation");
    project.run("semantics.py", &["collation"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment"]
async fn real_dbt_timezone_sensitive_casts_rebuild() {
    let server = support::start_server().await;
    let project = server.python_project("duckdb-timezone");
    project.run("semantics.py", &["timezone"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment"]
async fn real_dbt_nested_floating_casts_rebuild_after_reordering() {
    let server = support::start_server().await;
    let project = server.python_project("duckdb-nested-float");
    project.run("semantics.py", &["nested-float"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment"]
async fn real_dbt_integer_division_rebuilds_after_reordering() {
    let server = support::start_server().await;
    let project = server.python_project("duckdb-integer-division");
    project.run("semantics.py", &["integer-division"]).await;
}
