//! The real Python/dbt/warehouse compatibility gate. Install integrations/duckdb first.
mod support;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment; run scripts/duckdb-smoke.sh"]
async fn real_dbt_durable_table_reuse_and_external_changes() {
    let server = support::start_server().await;
    let project = server.python_project("duckdb-smoke");
    let temp = project.path();
    for (name, mode) in [
        ("first", "normal"),
        ("second", "normal"),
        ("first", "skip"),
        ("first", "rewrite"),
        ("first", "input"),
        ("first", "skip"),
        ("first", "target"),
        ("first", "target-schema"),
        ("first", "drop"),
        ("first", "schema"),
        ("first", "sql"),
        ("first", "identical"),
        ("first", "skip"),
        ("first", "transport"),
        ("first", "rebuild"),
        ("first", "skip"),
        ("scalar", "normal"),
        ("scalar", "skip"),
        ("scalar", "target"),
        ("scalar", "input"),
        ("scalar", "skip"),
        ("hooks", "normal"),
        ("hooks", "skip"),
        ("first", "unknown"),
        ("first", "unsupported"),
    ] {
        project.run_in("smoke.py", name, &[mode]).await;
    }
    let observations = |name: &str| -> Vec<serde_json::Value> {
        std::fs::read_to_string(temp.join(name).join("observations.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    };
    let first = observations("first");
    let second = observations("second");
    assert_ne!(
        first[0]["namespace"], second[0]["namespace"],
        "independent files isolate history"
    );
    for event in &first[..first.len() - 1] {
        assert_eq!(
            event["namespace"], first[0]["namespace"],
            "normal restarts preserve the generation"
        );
    }
    assert_ne!(
        first.last().unwrap()["namespace"],
        first[0]["namespace"],
        "incompatible metadata must not match valid history"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment; run scripts/duckdb-smoke.sh"]
async fn real_dbt_lag_tolerance_reuse_expiry_and_external_change() {
    let server = support::start_server().await;
    server
        .python_project("duckdb-lag-tolerance")
        .run("lag_tolerance.py", &[])
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment; run scripts/duckdb-smoke.sh"]
async fn real_dbt_views_dependency_evidence() {
    let server = support::start_server().await;
    let project = server.python_project("duckdb-views");
    project.run("views.py", &[]).await;
}
