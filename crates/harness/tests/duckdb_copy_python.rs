mod support;
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment"]
async fn real_dbt_table_copy_and_adoption() {
    let server = support::start_server().await;
    let project = server.python_project("duckdb-copy");
    project.run("table_copy.py", &[]).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment"]
async fn real_dbt_view_graph_copy_chains_and_allowed_staleness() {
    let server = support::start_server().await;
    let project = server.python_project("duckdb-graph-copy");
    project.run("graph_copy.py", &[]).await;
}
