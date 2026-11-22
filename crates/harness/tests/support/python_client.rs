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
//! A person ([`PythonClient::person`]) signs in through the client's own
//! browser flow: the client opens its sign-in URL with Python's
//! `webbrowser`, which the driver points (through `BROWSER`) at the
//! harness browser (`python-client/browser.py`). That follows the
//! redirects through the Token broker and the fake Upstream IdP to the
//! client's loopback handler, and records each visit
//! ([`PythonClient::browser_visits`]).
//!
//! The client's loopback handler binds the fixed port 29525, so no two
//! drivers may sign a person in at once, from this test binary or any
//! other. [`PythonClient::person`] takes an exclusive lock on a file in
//! the temp directory ([`LOOPBACK_LOCK`]) and holds it until the client is
//! dropped; a person test waits for the one before it.
//!
//! Without `uv` on `PATH`, [`PythonClient::available`] skips with a message,
//! unless `DBT_STATE_REQUIRE_PYTHON_CLIENT` is set (as CI does), when it
//! fails instead.

use std::fmt;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::task::JoinHandle;

use super::TestServer;

/// Set (non-empty) to fail, rather than skip, without `uv`.
pub const REQUIRE: &str = "DBT_STATE_REQUIRE_PYTHON_CLIENT";

/// The file (in the temp directory) whose lock serializes every driver
/// that may bind the client's loopback handler port.
pub const LOOPBACK_LOCK: &str = "dbt-state-python-client-loopback-29525.lock";

/// How long the driver may take to reach its next pause or its end.
const DRIVER_TIMEOUT: Duration = Duration::from_secs(120);

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
    /// Where the harness browser records its visits.
    browser: tempfile::TempDir,
    /// The loopback port lock, held while a person may sign in.
    _loopback: Option<File>,
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
        let uv = std::process::Command::new("uv").arg("--version").output();
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
        let browser = tempfile::tempdir().expect("browser profile");
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
            browser,
            _loopback: None,
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

    /// A person at a terminal with a browser, who signs in through the
    /// browser flow and whose client caches its token in its home. Waits
    /// for the loopback port lock (see the module docs) and holds it until
    /// the client is dropped.
    pub async fn person(mut self) -> Self {
        let path = std::env::temp_dir().join(LOOPBACK_LOCK);
        let lock = tokio::task::spawn_blocking(move || {
            let file = File::options()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&path)
                .unwrap_or_else(|error| panic!("open {}: {error}", path.display()));
            file.lock()
                .unwrap_or_else(|error| panic!("lock {}: {error}", path.display()));
            file
        })
        .await
        .expect("loopback port lock");
        self._loopback = Some(lock);
        let log = self.browser_log().to_str().expect("UTF-8 path").to_owned();
        self.env.push(("DRIVER_BROWSER_LOG".into(), log));
        self
    }

    fn browser_log(&self) -> PathBuf {
        self.browser.path().join("visits.jsonl")
    }

    /// Where the client caches tokens on disk.
    pub fn home(&self) -> &Path {
        self.home.path()
    }

    /// Every visit of the harness browser so far, in order: the URL the
    /// client opened (`opened`), each redirect it followed (`redirects`,
    /// without queries), where it landed (`landed`, without query) and the
    /// final status (`status`).
    pub fn browser_visits(&self) -> Vec<Value> {
        let log = self.browser_log();
        match std::fs::read_to_string(&log) {
            Ok(text) => text
                .lines()
                .map(|line| serde_json::from_str(line).expect("browser log line"))
                .collect(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => panic!("read {}: {error}", log.display()),
        }
    }

    /// Run the driver with `args` (a scenario and its steps) to its end.
    pub async fn run(&self, args: &[&str]) -> DriverRun {
        self.start(args).finish().await
    }

    /// Start the driver with `args`, to drive it through its pauses.
    pub fn start(&self, args: &[&str]) -> Driver {
        let project = project();
        let mut command = Command::new("uv");
        command
            .arg("run")
            .arg("--locked")
            .arg("--project")
            .arg(&project)
            .arg("python")
            .arg(project.join("driver.py"))
            .args(args)
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
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
        let mut child = command.spawn().expect("run uv");
        let stdout = BufReader::new(child.stdout.take().expect("driver stdout")).lines();
        let stdin = child.stdin.take();
        let mut stderr = child.stderr.take().expect("driver stderr");
        let stderr = tokio::spawn(async move {
            let mut text = String::new();
            let _ = stderr.read_to_string(&mut text).await;
            text
        });
        Driver {
            child,
            stdout,
            stdin,
            output: String::new(),
            stderr,
        }
    }
}

/// A running driver. Its `wait` steps pause it until [`Driver::resume`].
pub struct Driver {
    child: Child,
    stdout: Lines<BufReader<ChildStdout>>,
    stdin: Option<ChildStdin>,
    /// Its stdout so far.
    output: String,
    stderr: JoinHandle<String>,
}

impl Driver {
    /// The driver's next stdout line, or `None` at its end.
    async fn line(&mut self) -> Option<String> {
        let line = tokio::time::timeout(DRIVER_TIMEOUT, self.stdout.next_line())
            .await
            .unwrap_or_else(|_| panic!("the driver printed nothing for {DRIVER_TIMEOUT:?}"))
            .expect("read driver stdout")?;
        self.output.push_str(&line);
        self.output.push('\n');
        Some(line)
    }

    /// Wait for the driver to pause at its next `wait` step: its report
    /// (`{"waiting": true, "rpcs": <made so far>}`). Panics, with its
    /// output, if it ends instead.
    pub async fn paused(&mut self) -> Value {
        while let Some(line) = self.line().await {
            if let Ok(report) = serde_json::from_str::<Value>(&line) {
                if report["waiting"] == true {
                    return report;
                }
            }
        }
        let run = self.end().await;
        panic!("the driver ended before pausing: {run}");
    }

    /// Resume the paused driver with its wall clock moved `elapsed` forward
    /// (as the host's would be after that long).
    pub async fn resume(&mut self, elapsed: Duration) {
        let line = format!("{}\n", json!({ "advance_seconds": elapsed.as_secs() }));
        let stdin = self.stdin.as_mut().expect("driver stdin");
        stdin
            .write_all(line.as_bytes())
            .await
            .expect("resume driver");
        stdin.flush().await.expect("resume driver");
    }

    /// Run the driver to its end.
    pub async fn finish(mut self) -> DriverRun {
        while self.line().await.is_some() {}
        self.end().await
    }

    async fn end(&mut self) -> DriverRun {
        drop(self.stdin.take());
        let status = tokio::time::timeout(DRIVER_TIMEOUT, self.child.wait())
            .await
            .expect("the driver did not exit")
            .expect("driver status");
        let stderr = (&mut self.stderr).await.unwrap_or_default();
        let stdout = std::mem::take(&mut self.output);
        let result = stdout
            .lines()
            .rev()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .find(|value| value["waiting"] != true)
            .unwrap_or(Value::Null);
        DriverRun {
            status,
            result,
            stdout,
            stderr,
        }
    }
}
