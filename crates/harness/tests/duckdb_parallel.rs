//! Recovery through real dbt and the production router.
mod support;
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment"]
async fn real_dbt_parallel_workers_preserve_committed_evidence() {
    let server = support::start_server().await;
    let project = server.python_project("duckdb-parallel");
    project.run("parallel.py", &[]).await;
}
