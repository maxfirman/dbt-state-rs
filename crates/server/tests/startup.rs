//! Starting the server binary: which auth settings it refuses before it
//! serves, and which it starts with.
//!
//! - `DBT_STATE_AUTH=none` (the default) on a non-loopback address refuses to
//!   start unless `DBT_STATE_ALLOW_INSECURE=true`.
//! - `DBT_STATE_AUTH=oidc` needs a valid `DBT_STATE_AUTH_CONFIG`, and refuses
//!   any `DBT_STATE_REQUIRE_ORG_HEADER`.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use sqlx::postgres::PgPoolOptions;

const BINARY: &str = env!("CARGO_BIN_EXE_dbt-state-server");

fn dsn() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://dbtstate:dbtstate@localhost:55441/dbtstate".into())
}

fn testdata(file: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../auth/testdata")
        .join(file)
}

/// An auth config with one Organization `organization`, in a temp dir.
fn auth_config(organization: &str) -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("auth.toml");
    std::fs::write(
        &path,
        format!(
            r#"
[broker]
issuer = "https://broker.dbt-state.test"
[api]
audience = "dbt-state-api"
[signing]
active_key_id = "k"
[[signing.keys]]
id = "k"
private_key_file = {key:?}
[[organizations]]
id = {organization:?}
"#,
            key = testdata("signing-key-1.pem").to_str().unwrap(),
        ),
    )
    .unwrap();
    (dir, path)
}

/// The server's environment: only the given variables, plus a database URL
/// nothing listens on (refusals happen before connecting).
fn server(vars: &[(&str, &str)]) -> Command {
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

/// Run `command` to completion; it must fail, and its output must mention
/// every one of `needles`.
fn assert_refused(case: &str, mut command: Command, needles: &[&str]) {
    let output = command.output().expect("run server");
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
        !text.contains("connecting to postgres"),
        "{case}: refused only after connecting\n{text}"
    );
}

#[test]
fn unsafe_auth_settings_refuse_to_start() {
    let (_dir, config) = auth_config("analytics");
    let config = config.to_str().unwrap();
    let (_bad_dir, bad_config) = auth_config("has:colon");
    let bad_config = bad_config.to_str().unwrap();

    for listen in ["0.0.0.0:50051", "[::]:50051", "192.0.2.1:50051"] {
        assert_refused(
            listen,
            server(&[("DBT_STATE_LISTEN", listen)]),
            &["DBT_STATE_ALLOW_INSECURE"],
        );
        assert_refused(
            listen,
            server(&[
                ("DBT_STATE_LISTEN", listen),
                ("DBT_STATE_AUTH", "none"),
                ("DBT_STATE_ALLOW_INSECURE", "false"),
            ]),
            &["DBT_STATE_ALLOW_INSECURE"],
        );
    }
    for value in ["true", "false", ""] {
        assert_refused(
            "org header flag in oidc mode",
            server(&[
                ("DBT_STATE_AUTH", "oidc"),
                ("DBT_STATE_AUTH_CONFIG", config),
                ("DBT_STATE_REQUIRE_ORG_HEADER", value),
            ]),
            &["DBT_STATE_REQUIRE_ORG_HEADER"],
        );
    }
    assert_refused(
        "oidc without a config",
        server(&[("DBT_STATE_AUTH", "oidc")]),
        &["DBT_STATE_AUTH_CONFIG"],
    );
    assert_refused(
        "oidc with an invalid config",
        server(&[
            ("DBT_STATE_AUTH", "oidc"),
            ("DBT_STATE_AUTH_CONFIG", bad_config),
        ]),
        &["Organization id", "has:colon"],
    );
    assert_refused(
        "unknown mode",
        server(&[("DBT_STATE_AUTH", "basic")]),
        &["DBT_STATE_AUTH", "basic"],
    );
}

/// A fresh schema the server may migrate, removed afterwards.
struct Schema {
    name: String,
    runtime: tokio::runtime::Runtime,
}

impl Schema {
    fn new() -> Self {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let name = format!("startup_{}", uuid::Uuid::new_v4().simple());
        runtime.block_on(async {
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .connect(&dsn())
                .await
                .expect("connect");
            sqlx::query(&format!("CREATE SCHEMA \"{name}\""))
                .execute(&pool)
                .await
                .unwrap();
        });
        Self { name, runtime }
    }

    /// `DATABASE_URL` scoped to this schema.
    fn dsn(&self) -> String {
        let base = dsn();
        let separator = if base.contains('?') { '&' } else { '?' };
        format!("{base}{separator}options[search_path]={}", self.name)
    }
}

impl Drop for Schema {
    fn drop(&mut self) {
        let name = self.name.clone();
        self.runtime.block_on(async {
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .connect(&dsn())
                .await
                .expect("connect");
            sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{name}\" CASCADE"))
                .execute(&pool)
                .await
                .unwrap();
        });
    }
}

/// Start the server with `vars` and wait until it reports it is listening,
/// then stop it.
fn assert_starts(case: &str, schema: &Schema, vars: &[(&str, &str)]) {
    let mut command = server(vars);
    command
        .env("DATABASE_URL", schema.dsn())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn server");
    let (lines, received) = mpsc::channel();
    for stream in [
        Box::new(child.stdout.take().unwrap()) as Box<dyn std::io::Read + Send>,
        Box::new(child.stderr.take().unwrap()),
    ] {
        let lines = lines.clone();
        std::thread::spawn(move || {
            for line in BufReader::new(stream).lines().map_while(Result::ok) {
                if lines.send(line).is_err() {
                    break;
                }
            }
        });
    }
    drop(lines);
    let mut seen = Vec::new();
    let started = loop {
        match received.recv_timeout(Duration::from_secs(60)) {
            Ok(line) if line.contains("dbt-state-server listening") => break true,
            Ok(line) => seen.push(line),
            Err(_) => break false,
        }
    };
    let _ = child.kill();
    let _ = child.wait();
    assert!(started, "{case}: did not start\n{}", seen.join("\n"));
}

#[test]
fn allowed_auth_settings_start() {
    let schema = Schema::new();
    let (_dir, config) = auth_config("analytics");
    let config = config.to_str().unwrap();
    assert_starts(
        "none on loopback",
        &schema,
        &[("DBT_STATE_LISTEN", "127.0.0.1:0")],
    );
    assert_starts(
        "none on loopback, org header required",
        &schema,
        &[
            ("DBT_STATE_LISTEN", "127.0.0.1:0"),
            ("DBT_STATE_REQUIRE_ORG_HEADER", "true"),
        ],
    );
    assert_starts(
        "none on any address with the insecure override",
        &schema,
        &[
            ("DBT_STATE_LISTEN", "0.0.0.0:0"),
            ("DBT_STATE_ALLOW_INSECURE", "true"),
        ],
    );
    assert_starts(
        "oidc on any address",
        &schema,
        &[
            ("DBT_STATE_LISTEN", "0.0.0.0:0"),
            ("DBT_STATE_AUTH", "oidc"),
            ("DBT_STATE_AUTH_CONFIG", config),
        ],
    );
}
