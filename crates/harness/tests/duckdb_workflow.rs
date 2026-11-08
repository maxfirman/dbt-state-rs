mod support;
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment"]
async fn real_dbt_fresh_mixed_project_copy_reset_restore_and_file_isolation() {
    let server = support::start_server().await;
    let project = server.python_project("duckdb-workflow");
    project.run("workflow.py", &[]).await;
}
