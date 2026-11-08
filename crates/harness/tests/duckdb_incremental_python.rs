mod support;
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment"]
async fn real_dbt_incremental_build_copy_and_adoption() {
    let server = support::start_server().await;
    let project = server.python_project("duckdb-incremental");
    project.run("incremental.py", &[]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment"]
async fn real_dbt_incremental_self_reads_require_reuse_proof() {
    let server = support::start_server().await;
    let project = server.python_project("duckdb-self-read");
    project.run("incremental_self_reads.py", &[]).await;
}
