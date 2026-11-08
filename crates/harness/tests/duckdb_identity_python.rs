//! Real dbt seam: SQL trivia changes reuse rows and their column structure.
mod support;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment"]
async fn real_dbt_sql_formatting_reuses_independently_verified_results() {
    let server = support::start_server().await;
    let project = server.python_project("duckdb-identity");
    project.run("identity.py", &[]).await;
}
