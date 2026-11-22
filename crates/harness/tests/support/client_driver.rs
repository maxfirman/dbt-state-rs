//! What the pinned dbt client drivers share: a running driver process and
//! its pause protocol ([`Driver`]), its result ([`DriverRun`]), the
//! harness browser's visit log ([`browser_visits`]) and the lock on the
//! dbt clients' loopback handler port ([`hold_loopback_port`]).
//!
//! Both drivers (the Python one, [`super::python_client`], and the Fusion
//! one, [`super::fusion_client`]) print JSON lines on stdout and end with
//! one JSON result. A `wait` step prints `{"waiting": true, "rpcs": N}`
//! and reads `{"advance_seconds": N}` from stdin before it moves the
//! driver's wall clock on.

use std::fmt;
use std::fs::File;
use std::path::Path;
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::task::JoinHandle;

/// The file (in the temp directory) whose lock serializes every driver
/// that may bind the dbt clients' loopback handler port (29525), Python or
/// Fusion, from any test binary or worktree.
pub const LOOPBACK_LOCK: &str = "dbt-state-python-client-loopback-29525.lock";

/// How long a driver may take to reach its next pause or its end.
const DRIVER_TIMEOUT: Duration = Duration::from_secs(120);

/// Wait for the exclusive lock on [`LOOPBACK_LOCK`]; the port is the
/// holder's until the file is dropped.
pub async fn hold_loopback_port() -> File {
    let path = std::env::temp_dir().join(LOOPBACK_LOCK);
    tokio::task::spawn_blocking(move || {
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
    .expect("loopback port lock")
}

/// Every visit in the harness browser's log (one JSON object per line), in
/// order; none before the first.
pub fn browser_visits(log: &Path) -> Vec<Value> {
    match std::fs::read_to_string(log) {
        Ok(text) => text
            .lines()
            .map(|line| serde_json::from_str(line).expect("browser log line"))
            .collect(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => panic!("read {}: {error}", log.display()),
    }
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
    /// Spawn `command` (its environment already set) with piped stdio.
    pub fn spawn(mut command: Command) -> Self {
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().expect("spawn the driver");
        let stdout = BufReader::new(child.stdout.take().expect("driver stdout")).lines();
        let stdin = child.stdin.take();
        let mut stderr = child.stderr.take().expect("driver stderr");
        let stderr = tokio::spawn(async move {
            let mut text = String::new();
            let _ = stderr.read_to_string(&mut text).await;
            text
        });
        Self {
            child,
            stdout,
            stdin,
            output: String::new(),
            stderr,
        }
    }

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
