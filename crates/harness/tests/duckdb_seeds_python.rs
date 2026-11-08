//! Real dbt seam: Seed values, configuration, failures and physical target invalidation.
mod support;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment"]
async fn real_dbt_seed_reuse_and_change_propagation() {
    let server = support::start_server().await;
    let project = server.python_project("duckdb-seeds");
    project.run("seeds.py", &[]).await;
}
