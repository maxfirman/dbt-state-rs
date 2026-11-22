//! The pinned dbt-state Python client's driver
//! (`crates/harness/python-client/driver.py`), run with `uv` from its locked
//! environment against a [`TestServer`].
//!
//! The driver is configured only through the client's supported
//! environment settings (see the driver's docstring). Its environment is
//! otherwise cleared, so a developer's own dbt or dbt State settings (for
//! example `DBT_CLOUD_TOKEN`, which would select token exchange) never
//! leak in.
//!
//! Without `uv` on `PATH`, [`PythonClient::available`] skips with a message,
//! unless `DBT_STATE_REQUIRE_PYTHON_CLIENT` is set (as CI does), when it
//! fails instead.

use std::fmt;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};

use serde_json::Value;

use super::TestServer;

/// Set (non-empty) to fail, rather than skip, without `uv`.
pub const REQUIRE: &str = "DBT_STATE_REQUIRE_PYTHON_CLIENT";

/// Environment the driver's `uv` and its network need, passed through.
const PASSED_THROUGH: &[&str] = &[
    "PATH",
    "HOME",
    "TMPDIR",
    "XDG_CACHE_HOME",
    "XDG_DATA_HOME",
    "XDG_CONFIG_HOME",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "http_proxy",
    "https_proxy",
];

fn project() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("python-client")
}

/// The driver, configured for one deployment.
pub struct PythonClient {
    env: Vec<(String, String)>,
    /// `RUN_CACHE_HOME`: where the client caches tokens on disk.
    home: tempfile::TempDir,
}

/// One driver run.
pub struct DriverRun {
    pub status: ExitStatus,
    /// The driver's JSON result (`Null` when it printed none).
    pub result: Value,
    pub stdout: String,
    pub stderr: String,
}

impl fmt::Display for DriverRun {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "driver {}\n--- stdout\n{}\n--- stderr\n{}",
            self.status, self.stdout, self.stderr
        )
    }
}

impl PythonClient {
    /// The driver, pointed at `server`'s TLS gRPC listener and Token broker,
    /// trusting the test CA. `None` (after a message) when `uv` is missing
    /// and [`REQUIRE`] is unset.
    pub fn available(server: &TestServer) -> Option<Self> {
        let uv = Command::new("uv").arg("--version").output();
        if !uv.as_ref().is_ok_and(|output| output.status.success()) {
            if std::env::var_os(REQUIRE).is_some_and(|value| !value.is_empty()) {
                panic!("{REQUIRE} is set, but `uv --version` failed: {uv:?}");
            }
            eprintln!(
                "SKIPPED: the pinned Python client driver needs `uv` on PATH \
                 (set {REQUIRE}=1 to fail instead of skipping)"
            );
            return None;
        }
        assert!(server.tls, "the dbt client sends credentials only over TLS");
        let broker = server.broker_url();
        let ca = super::tls::ca_file()
            .to_str()
            .expect("UTF-8 path")
            .to_owned();
        let home = tempfile::tempdir().expect("client home");
        let env = [
            ("RUN_CACHE_API_URL", server.addr.to_string()),
            ("RUN_CACHE_API_SECURE", "true".into()),
            ("RUN_CACHE_AUTH_URL", broker.clone()),
            ("RUN_CACHE_TOKEN_URL", format!("{broker}/token")),
            ("RUN_CACHE_CA_BUNDLE", ca.clone()),
            ("REQUESTS_CA_BUNDLE", ca),
            (
                "RUN_CACHE_HOME",
                home.path().to_str().expect("UTF-8 path").into(),
            ),
            ("DBT_SEND_ANONYMOUS_USAGE_STATS", "false".into()),
            // Never through a proxy: the deployment is on loopback.
            ("NO_PROXY", "127.0.0.1,localhost".into()),
            ("no_proxy", "127.0.0.1,localhost".into()),
        ];
        Some(Self {
            env: env
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value))
                .collect(),
            home,
        })
    }

    /// A CI machine with its Upstream IdP app registration: headless, so
    /// the client keeps its token in memory only.
    pub fn machine(mut self, client_id: &str, secret: &str) -> Self {
        for (key, value) in [
            ("CI", "true"),
            ("DBT_ENGINE_STATE_OAUTH_CLIENT_ID", client_id),
            ("DBT_ENV_SECRET_STATE_OAUTH_CLIENT_SECRET", secret),
        ] {
            self.env.push((key.into(), value.into()));
        }
        self
    }

    /// Where the client caches tokens on disk.
    pub fn home(&self) -> &Path {
        self.home.path()
    }

    /// Run the driver's `scenario` from the locked environment.
    pub async fn run(&self, scenario: &str) -> DriverRun {
        let project = project();
        let mut command = tokio::process::Command::new("uv");
        command
            .arg("run")
            .arg("--locked")
            .arg("--project")
            .arg(&project)
            .arg("python")
            .arg(project.join("driver.py"))
            .arg(scenario)
            .env_clear()
            .kill_on_drop(true);
        for key in PASSED_THROUGH {
            if let Some(value) = std::env::var_os(key) {
                command.env(key, value);
            }
        }
        for (key, value) in std::env::vars_os() {
            if key.to_str().is_some_and(|key| key.starts_with("UV_")) {
                command.env(key, value);
            }
        }
        command.envs(self.env.iter().map(|(key, value)| (key, value)));
        let output = command.output().await.expect("run uv");
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let result = stdout
            .lines()
            .rev()
            .find_map(|line| serde_json::from_str(line).ok())
            .unwrap_or(Value::Null);
        DriverRun {
            status: output.status,
            result,
            stdout,
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        }
    }
}
