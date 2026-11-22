//! The pinned Fusion driver (`crates/harness/fusion-driver`, built outside
//! the workspace against dbt-fusion@9977b6c's `dbt-run-cache`), run
//! against a [`TestServer`].
//!
//! The driver is opt-in: build it, then name the binary in
//! `DBT_STATE_FUSION_DRIVER` ([`DRIVER`]). Without it,
//! [`FusionClient::available`] skips with a message. dbt-fusion is licensed
//! under the Elastic License 2.0, so the driver only compiles against it
//! and is never vendored, published or shipped (see the driver's README).
//!
//! The driver is configured only through Fusion's supported settings (see
//! the driver's docs): API, auth and token URLs, a machine's client id and
//! secret, `HOME` (where Fusion keeps a person's token,
//! `~/.dbt/state_auth.json`), `SSL_CERT_FILE` (the test CA) and `PATH`.
//! Its environment is otherwise cleared, so a developer's own dbt platform
//! credentials (`DBT_CLOUD_*`, `~/.dbt/dbt_cloud.yml`), which would select
//! token exchange, never leak in.
//!
//! A person ([`FusionClient::person`]) signs in through Fusion's own
//! browser flow: Fusion opens its sign-in URL with the desktop's URL
//! opener, `xdg-open` on `PATH`, which the harness provides in a temp
//! directory: it runs the driver's harness browser (`fusion-driver browser
//! <url>`), which follows the redirects through the Token broker and the
//! fake Upstream IdP to Fusion's loopback handler and records each visit
//! ([`FusionClient::browser_visits`]). The loopback handler binds port
//! 29525, which the Python driver's person tests also bind, so
//! [`FusionClient::person`] holds the same lock
//! ([`super::client_driver::LOOPBACK_LOCK`]).

use std::fs::File;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use serde_json::Value;
use tokio::process::Command;

pub use super::client_driver::{Driver, DriverRun};
use super::TestServer;

/// The built driver binary's path. Unset (or empty): every Fusion test
/// skips.
pub const DRIVER: &str = "DBT_STATE_FUSION_DRIVER";

/// Environment the driver passes through from the harness.
const PASSED_THROUGH: &[&str] = &["TMPDIR", "RUST_BACKTRACE"];

/// The driver, configured for one deployment.
pub struct FusionClient {
    driver: PathBuf,
    env: Vec<(String, String)>,
    /// `HOME`: where Fusion keeps a person's token on disk.
    home: tempfile::TempDir,
    /// The harness's `xdg-open`, and the browser's visit log.
    desktop: tempfile::TempDir,
    /// The loopback port lock, held while a person may sign in.
    _loopback: Option<File>,
}

impl FusionClient {
    /// The driver named in [`DRIVER`], pointed at `server`'s TLS gRPC
    /// listener and Token broker, trusting the test CA. `None` (after a
    /// message) when [`DRIVER`] is unset.
    pub fn available(server: &TestServer) -> Option<Self> {
        let driver = match std::env::var_os(DRIVER) {
            Some(path) if !path.is_empty() => PathBuf::from(path),
            _ => {
                eprintln!(
                    "SKIPPED: the pinned Fusion driver is opt-in: build \
                     crates/harness/fusion-driver and set {DRIVER} to the binary"
                );
                return None;
            }
        };
        assert!(
            driver.is_file(),
            "{DRIVER} names no file: {}",
            driver.display()
        );
        assert!(server.tls, "Fusion authenticates only a secure channel");
        let broker = server.broker_url();
        let ca = super::tls::ca_file()
            .to_str()
            .expect("UTF-8 path")
            .to_owned();
        let home = tempfile::tempdir().expect("client home");
        let desktop = tempfile::tempdir().expect("desktop");
        let env = [
            ("RUN_CACHE_API_URL", server.addr.to_string()),
            ("RUN_CACHE_API_SECURE", "true".into()),
            ("RUN_CACHE_AUTH_URL", broker.clone()),
            ("RUN_CACHE_TOKEN_URL", format!("{broker}/token")),
            ("SSL_CERT_FILE", ca),
            ("HOME", home.path().to_str().expect("UTF-8 path").into()),
            // Never through a proxy: the deployment is on loopback.
            ("NO_PROXY", "127.0.0.1,localhost".into()),
            ("no_proxy", "127.0.0.1,localhost".into()),
        ];
        Some(Self {
            driver,
            env: env
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value))
                .collect(),
            home,
            desktop,
            _loopback: None,
        })
    }

    /// A CI machine with its Upstream IdP app registration: Fusion logs in
    /// with `client_credentials` and keeps the token in memory only.
    pub fn machine(mut self, client_id: &str, secret: &str) -> Self {
        for (key, value) in [
            ("DBT_ENGINE_STATE_OAUTH_CLIENT_ID", client_id),
            ("DBT_ENV_SECRET_STATE_OAUTH_CLIENT_SECRET", secret),
        ] {
            self.env.push((key.into(), value.into()));
        }
        self
    }

    /// A person at a desktop with a browser, who signs in through Fusion's
    /// browser flow and whose Fusion keeps the token in their home. Waits
    /// for the loopback port lock (see the module docs) and holds it until
    /// the client is dropped.
    pub async fn person(mut self) -> Self {
        self._loopback = Some(super::client_driver::hold_loopback_port().await);
        let opener = self.desktop.path().join("xdg-open");
        let driver = self.driver.to_str().expect("UTF-8 path");
        let script = format!(
            "#!/bin/sh\nexec '{}' browser \"$@\"\n",
            driver.replace('\'', r"'\''")
        );
        std::fs::write(&opener, script).expect("write xdg-open");
        std::fs::set_permissions(&opener, std::fs::Permissions::from_mode(0o755))
            .expect("make xdg-open executable");
        let log = self.browser_log().to_str().expect("UTF-8 path").to_owned();
        self.env.push(("DRIVER_BROWSER_LOG".into(), log));
        self
    }

    fn browser_log(&self) -> PathBuf {
        self.desktop.path().join("visits.jsonl")
    }

    /// The token Fusion keeps on disk for a person, in their home.
    pub fn token_file(&self) -> PathBuf {
        self.home.path().join(".dbt").join("state_auth.json")
    }

    /// Every visit of the harness browser so far, in order: the URL Fusion
    /// opened (`opened`), each redirect it followed (`redirects`, without
    /// queries), where it landed (`landed`, without query) and the final
    /// status (`status`).
    pub fn browser_visits(&self) -> Vec<Value> {
        super::client_driver::browser_visits(&self.browser_log())
    }

    /// Run the driver with `args` (a scenario and its steps) to its end.
    pub async fn run(&self, args: &[&str]) -> DriverRun {
        self.start(args).finish().await
    }

    /// Start the driver with `args`, to drive it through its pauses.
    pub fn start(&self, args: &[&str]) -> Driver {
        let mut command = Command::new(&self.driver);
        command.args(args).env_clear();
        for key in PASSED_THROUGH {
            if let Some(value) = std::env::var_os(key) {
                command.env(key, value);
            }
        }
        // The desktop's URL opener comes first; the rest of `PATH` gives
        // `xdg-open` its shell.
        let path = std::env::var_os("PATH").unwrap_or_default();
        let path = std::env::join_paths(
            std::iter::once(self.desktop.path().to_path_buf()).chain(std::env::split_paths(&path)),
        )
        .expect("PATH");
        command.env("PATH", path);
        command.envs(self.env.iter().map(|(key, value)| (key, value)));
        Driver::spawn(command)
    }
}
