//! Starting the standalone `dbt-state-broker` binary: it needs an auth
//! config with `[signing]` and `[upstream]`, and then serves OIDC discovery
//! on `DBT_STATE_BROKER_LISTEN`.

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::Duration;

const BINARY: &str = env!("CARGO_BIN_EXE_dbt-state-broker");

fn testdata(file: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../auth/testdata")
        .join(file)
}

/// An auth config in a temp dir: one Organization, `signing` and
/// `upstream` when asked for.
fn config(signing: bool, upstream: bool) -> (tempfile::TempDir, PathBuf) {
    let mut text = String::from(
        "[broker]\nissuer = \"https://broker.dbt-state.test\"\n[api]\naudience = \"dbt-state-api\"\n",
    );
    if signing {
        text.push_str(&format!(
            "[signing]\nactive_key_id = \"k\"\n[[signing.keys]]\nid = \"k\"\nprivate_key_file = {:?}\n",
            testdata("signing-key-1.pem").to_str().unwrap()
        ));
    }
    text.push_str("[[organizations]]\nid = \"analytics\"\n");
    if upstream {
        text.push_str(
            "[upstream]\nissuer = \"http://127.0.0.1:1\"\napi_audience = \"api://dbt-state\"\n",
        );
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("auth.toml");
    std::fs::write(&path, text).unwrap();
    (dir, path)
}

fn broker(vars: &[(&str, &str)]) -> Command {
    let mut command = Command::new(BINARY);
    command.env_clear().env("RUST_LOG", "info");
    for (key, value) in vars {
        command.env(key, value);
    }
    command
}

fn assert_refused(case: &str, mut command: Command, needle: &str) {
    let output = command.output().expect("run broker");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!output.status.success(), "{case}: started\n{text}");
    assert!(text.contains(needle), "{case}: no {needle:?} in\n{text}");
}

#[test]
fn configs_that_cannot_run_a_broker_refuse_to_start() {
    assert_refused("no config", broker(&[]), "DBT_STATE_AUTH_CONFIG");
    let (_a, unsigned) = config(false, true);
    assert_refused(
        "no signing keys",
        broker(&[("DBT_STATE_AUTH_CONFIG", unsigned.to_str().unwrap())]),
        "[signing]",
    );
    let (_b, no_upstream) = config(true, false);
    assert_refused(
        "no Upstream IdP",
        broker(&[("DBT_STATE_AUTH_CONFIG", no_upstream.to_str().unwrap())]),
        "[upstream]",
    );
    let (_c, valid) = config(true, true);
    assert_refused(
        "a listen address that is no address",
        broker(&[
            ("DBT_STATE_AUTH_CONFIG", valid.to_str().unwrap()),
            ("DBT_STATE_BROKER_LISTEN", "localhost"),
        ]),
        "DBT_STATE_BROKER_LISTEN",
    );
}

struct Running(Child);

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn a_valid_config_serves_discovery() {
    let (_dir, path) = config(true, true);
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let addr = format!("127.0.0.1:{port}");
    let _broker = Running(
        broker(&[
            ("DBT_STATE_AUTH_CONFIG", path.to_str().unwrap()),
            ("DBT_STATE_BROKER_LISTEN", &addr),
        ])
        .spawn()
        .expect("spawn broker"),
    );
    let url = format!("http://{addr}/.well-known/openid-configuration");
    let mut discovery = None;
    for _ in 0..200 {
        if let Ok(response) = reqwest::get(&url).await {
            discovery = Some(response.json::<serde_json::Value>().await.unwrap());
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let discovery = discovery.expect("the broker serves discovery");
    assert_eq!(discovery["issuer"], "https://broker.dbt-state.test");
    assert_eq!(discovery["jwks_uri"], "https://broker.dbt-state.test/jwks");
}
