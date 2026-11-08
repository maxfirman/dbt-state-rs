//! Recovery through real dbt and the production router.
mod support;
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment"]
async fn real_dbt_reset_and_generation_recovery() {
    let server = support::start_server().await;
    let project = server.python_project("duckdb-recovery");
    project.run("recovery.py", &[]).await;
}
