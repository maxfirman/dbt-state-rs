//! Real-dbt projects, process configuration and failure artifacts.

use std::path::{Path, PathBuf};

use super::TestServer;

pub struct PythonProject<'a> {
    server: &'a TestServer,
    path: PathBuf,
    failed: std::cell::Cell<bool>,
}

impl TestServer {
    pub fn python_project(&self, prefix: &str) -> PythonProject<'_> {
        let path = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).expect("create Python project");
        PythonProject {
            server: self,
            path,
            failed: std::cell::Cell::new(false),
        }
    }
}

impl PythonProject<'_> {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub async fn run(&self, script: &str, args: &[&str]) {
        self.run_in(script, "project", args).await;
    }

    pub async fn run_in(&self, script: &str, directory: &str, args: &[&str]) {
        let python = std::env::var("DBT_STATE_DUCKDB_PYTHON").expect("pinned Python executable");
        let result = tokio::process::Command::new(python)
            .arg(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../../integrations/duckdb/tests")
                    .join(script),
            )
            .arg(self.path.join(directory))
            .args(args)
            .env("DBT_STATE_LOCAL", "1")
            .env("RUN_CACHE_API_URL", self.server.addr.to_string())
            .env("RUN_CACHE_API_SECURE", "false")
            .env("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
            .env("RUN_CACHE_HOME", self.path.join("auth"))
            .env("CI", "true")
            .env(
                "PYTHONPATH",
                concat!(env!("CARGO_MANIFEST_DIR"), "/../../integrations/duckdb/src"),
            )
            .kill_on_drop(true)
            .output()
            .await
            .expect("execute real dbt");
        if !result.status.success() {
            self.failed.set(true);
            std::fs::write(self.path.join("stdout.log"), &result.stdout).expect("save stdout");
            std::fs::write(self.path.join("stderr.log"), &result.stderr).expect("save stderr");
            panic!(
                "retained project: {}\n{}\n{}",
                self.path.display(),
                String::from_utf8_lossy(&result.stdout),
                String::from_utf8_lossy(&result.stderr)
            );
        }
    }
}

impl Drop for PythonProject<'_> {
    fn drop(&mut self) {
        if self.failed.get() || std::thread::panicking() {
            eprintln!("retained project: {}", self.path.display());
        } else {
            std::fs::remove_dir_all(&self.path).expect("remove successful Python project");
        }
    }
}
