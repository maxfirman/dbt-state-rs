//! Starting the console binary: which settings it refuses before it
//! serves, and that `oidc` mode starts sending visitors to sign in.
//!
//! - `DBT_STATE_AUTH=oidc` needs an auth config that registers the UI
//!   (`[clients.ui]`, with the console's `/auth/callback` as its
//!   `redirect_uri`), the UI's private key and a 32-byte cookie key, and
//!   refuses `DBT_STATE_UI_ORG_ID`, which the signed-in person's
//!   Organizations replace.
//! - `none` mode refuses the `oidc`-only variables, so a console meant to
//!   require sign-in never starts without it.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const BINARY: &str = env!("CARGO_BIN_EXE_dbt-state-ui");
/// A base64 32-byte cookie key (`openssl rand -base64 32`).
const COOKIE_KEY: &str = "q7F0c2m3Vb9yX1kLr8tA5nE6pW4sZ0dGh2jK3lM9oPc=";

fn testdata(file: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../auth/testdata")
        .join(file)
}

fn path(path: &Path) -> String {
    path.to_str().expect("UTF-8 path").to_owned()
}

/// A deployment's files: an auth config registering the UI at
/// `redirect_uri` (none when `None`), and the UI's cookie key.
struct Files {
    _dir: tempfile::TempDir,
    config: String,
    cookie_key: String,
}

fn files(redirect_uri: Option<&str>, cookie_key: &str) -> Files {
    let dir = tempfile::tempdir().unwrap();
    let ui = redirect_uri
        .map(|uri| {
            format!(
                "[clients.ui]\nclient_id = \"dbt-state-ui\"\nredirect_uri = {uri:?}\n\
                 public_key_file = {:?}\n",
                path(&testdata("ui-client-public.pem"))
            )
        })
        .unwrap_or_default();
    let config = dir.path().join("auth.toml");
    std::fs::write(
        &config,
        format!(
            "[broker]\nissuer = \"https://broker.dbt-state.test\"\n\
             [api]\naudience = \"dbt-state-api\"\n\
             [[organizations]]\nid = \"analytics\"\n{ui}"
        ),
    )
    .unwrap();
    let key = dir.path().join("ui-cookie.key");
    std::fs::write(&key, cookie_key).unwrap();
    Files {
        config: path(&config),
        cookie_key: path(&key),
        _dir: dir,
    }
}

fn registered() -> Files {
    files(
        Some("https://console.dbt-state.test/auth/callback"),
        COOKIE_KEY,
    )
}

/// The console's environment: only the given variables, plus a database
/// URL nothing listens on (refusals happen before connecting).
fn console(vars: &[(&str, &str)]) -> Command {
    let mut command = Command::new(BINARY);
    command
        .env_clear()
        .env("DATABASE_URL", "postgres://nobody:nothing@127.0.0.1:1/none")
        .env("RUST_LOG", "info");
    for (key, value) in vars {
        command.env(key, value);
    }
    command
}

/// `oidc` mode with `files`, the UI's key and `extra`.
fn oidc(files: &Files, extra: &[(&str, &str)]) -> Command {
    let key = path(&testdata("ui-client-key.pem"));
    let mut vars = vec![
        ("DBT_STATE_AUTH", "oidc"),
        ("DBT_STATE_AUTH_CONFIG", files.config.as_str()),
        ("DBT_STATE_UI_PRIVATE_KEY_FILE", key.as_str()),
        ("DBT_STATE_UI_COOKIE_KEY_FILE", files.cookie_key.as_str()),
    ];
    for (name, value) in extra {
        vars.retain(|(set, _)| set != name);
        vars.push((name, value));
    }
    console(&vars)
}

/// Run `command` to completion; it must fail, and its output must mention
/// every one of `needles`.
fn assert_refused(case: &str, mut command: Command, needles: &[&str]) {
    let output = command.output().expect("run the console");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!output.status.success(), "{case}: started\n{text}");
    for needle in needles {
        assert!(text.contains(needle), "{case}: no {needle:?} in\n{text}");
    }
    assert!(
        !text.contains(COOKIE_KEY),
        "{case}: the cookie key is in the output\n{text}"
    );
}

#[test]
fn unsafe_console_settings_refuse_to_start() {
    let deployment = registered();
    for value in ["analytics", "local"] {
        assert_refused(
            "an Organization id in oidc mode",
            oidc(&deployment, &[("DBT_STATE_UI_ORG_ID", value)]),
            &["DBT_STATE_UI_ORG_ID", "DBT_STATE_AUTH=oidc"],
        );
    }
    assert_refused(
        "oidc without a config",
        console(&[("DBT_STATE_AUTH", "oidc")]),
        &["DBT_STATE_AUTH_CONFIG"],
    );
    let unregistered = files(None, COOKIE_KEY);
    assert_refused(
        "no UI registration",
        oidc(&unregistered, &[]),
        &["[clients.ui]"],
    );
    let elsewhere = files(
        Some("https://console.dbt-state.test/oidc/return"),
        COOKIE_KEY,
    );
    assert_refused(
        "a callback the console does not serve",
        oidc(&elsewhere, &[]),
        &["redirect_uri", "/auth/callback"],
    );
    assert_refused(
        "no private key",
        oidc(&deployment, &[("DBT_STATE_UI_PRIVATE_KEY_FILE", "")]),
        &["DBT_STATE_UI_PRIVATE_KEY_FILE"],
    );
    let other_key = path(&testdata("untrusted-key.pem"));
    assert_refused(
        "a private key the broker does not know",
        oidc(
            &deployment,
            &[("DBT_STATE_UI_PRIVATE_KEY_FILE", &other_key)],
        ),
        &["DBT_STATE_UI_PRIVATE_KEY_FILE", "public_key_file"],
    );
    assert_refused(
        "no cookie key",
        oidc(&deployment, &[("DBT_STATE_UI_COOKIE_KEY_FILE", "")]),
        &["DBT_STATE_UI_COOKIE_KEY_FILE"],
    );
    for (case, key) in [("a short cookie key", "c2hvcnQ="), ("not base64", "%%%")] {
        let bad = files(Some("https://console.dbt-state.test/auth/callback"), key);
        assert_refused(case, oidc(&bad, &[]), &["DBT_STATE_UI_COOKIE_KEY_FILE"]);
    }
    assert_refused(
        "a plain-http broker URL",
        oidc(
            &deployment,
            &[("DBT_STATE_UI_BROKER_URL", "http://broker.internal:8080")],
        ),
        &["DBT_STATE_UI_BROKER_URL", "https"],
    );
    for name in [
        "DBT_STATE_UI_PRIVATE_KEY_FILE",
        "DBT_STATE_UI_COOKIE_KEY_FILE",
        "DBT_STATE_UI_BROKER_URL",
    ] {
        assert_refused(
            "an oidc setting in none mode",
            console(&[(name, "set")]),
            &[name, "DBT_STATE_AUTH=oidc"],
        );
    }
    assert_refused(
        "unknown mode",
        console(&[("DBT_STATE_AUTH", "basic")]),
        &["DBT_STATE_AUTH", "basic"],
    );
}

/// A free loopback port.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// With valid `oidc` settings the console serves, and sends a visitor to
/// sign in at the broker.
#[test]
fn a_console_in_oidc_mode_starts_and_sends_visitors_to_sign_in() {
    let deployment = registered();
    let port = free_port().to_string();
    let database = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://dbtstate:dbtstate@localhost:55441/dbtstate".into());
    let mut child = oidc(
        &deployment,
        &[("PORT", port.as_str()), ("DATABASE_URL", database.as_str())],
    )
    .stdout(Stdio::null())
    .stderr(Stdio::null())
    .spawn()
    .expect("start the console");

    let deadline = Instant::now() + Duration::from_secs(30);
    let response = loop {
        let request = std::net::TcpStream::connect(("127.0.0.1", port.parse::<u16>().unwrap()))
            .and_then(|mut stream| {
                use std::io::{Read, Write};
                stream
                    .write_all(b"GET / HTTP/1.1\r\nHost: console\r\nConnection: close\r\n\r\n")?;
                let mut response = String::new();
                stream.read_to_string(&mut response)?;
                Ok(response)
            });
        match request {
            Ok(response) => break response,
            Err(_) if Instant::now() < deadline => {
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "the console exited before serving"
                );
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(error) => panic!("the console did not serve: {error}"),
        }
    };
    let _ = child.kill();
    let _ = child.wait();
    assert!(response.starts_with("HTTP/1.1 303"), "{response}");
    assert!(
        response
            .to_ascii_lowercase()
            .contains("location: https://broker.dbt-state.test/?response_type=code"),
        "{response}"
    );
}
