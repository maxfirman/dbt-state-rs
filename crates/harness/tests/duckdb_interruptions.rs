//! Recovery through real dbt and the production router.
mod support;
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment"]
async fn real_dbt_interrupted_work_requires_recovery_before_reuse() {
    let server = support::start_server().await;
    let project = server.python_project("duckdb-interruptions");
    project.run("interruptions.py", &[]).await;
}
