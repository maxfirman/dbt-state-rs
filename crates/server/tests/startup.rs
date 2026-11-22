//! Starting the server binary: which auth settings it refuses before it
//! serves, and which it starts with.
//!
//! - `DBT_STATE_AUTH=none` (the default) on a non-loopback address refuses to
//!   start unless `DBT_STATE_ALLOW_INSECURE=true`.
//! - `DBT_STATE_AUTH=oidc` needs a valid `DBT_STATE_AUTH_CONFIG`, and refuses
//!   any `DBT_STATE_REQUIRE_ORG_HEADER`.
//! - The gRPC and Token broker listeners serve native TLS from mounted
//!   certificate and key files, or plain HTTP behind a TLS-terminating proxy.

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
    write_config(&format!(
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
    ))
}

/// `auth_config("analytics")` plus an Upstream IdP and a machine rule: a
/// config that can run the Token broker. Nothing listens at the IdP; the
/// broker discovers it on first use.
fn broker_config() -> (tempfile::TempDir, PathBuf) {
    let (_dir, path) = auth_config("analytics");
    let text = std::fs::read_to_string(&path).unwrap();
    write_config(&format!(
        r#"{text}
[upstream]
issuer = "http://127.0.0.1:1"
api_audience = "api://dbt-state"
[[rules]]
kind = "machine"
claim = "roles"
value = "dbt-state.ci"
organization = "analytics"
role = "developer"
"#
    ))
}

/// `text` as an auth config file in a temp dir.
fn write_config(text: &str) -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("auth.toml");
    std::fs::write(&path, text).unwrap();
    (dir, path)
}

/// `path`'s config without its `[signing]` table.
fn without_signing(path: &Path) -> (tempfile::TempDir, PathBuf) {
    let text = std::fs::read_to_string(path).unwrap();
    let start = text.find("[signing]").unwrap();
    let end = text.find("[[organizations]]").unwrap();
    let mut text = text.clone();
    text.replace_range(start..end, "");
    write_config(&text)
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

    let (_broker_dir, broker) = broker_config();
    let broker = broker.to_str().unwrap();
    let (_unsigned_dir, unsigned) = without_signing(Path::new(broker));
    let unsigned = unsigned.to_str().unwrap();
    assert_refused(
        "a Token broker in none mode",
        server(&[("DBT_STATE_BROKER_LISTEN", "127.0.0.1:0")]),
        &["DBT_STATE_BROKER_LISTEN", "DBT_STATE_AUTH=oidc"],
    );
    assert_refused(
        "a Token broker without an Upstream IdP",
        server(&[
            ("DBT_STATE_AUTH", "oidc"),
            ("DBT_STATE_AUTH_CONFIG", config),
            ("DBT_STATE_BROKER_LISTEN", "127.0.0.1:0"),
        ]),
        &["DBT_STATE_BROKER_LISTEN", "[upstream]"],
    );
    assert_refused(
        "a Token broker without signing keys",
        server(&[
            ("DBT_STATE_AUTH", "oidc"),
            ("DBT_STATE_AUTH_CONFIG", unsigned),
            ("DBT_STATE_BROKER_LISTEN", "127.0.0.1:0"),
        ]),
        &["DBT_STATE_BROKER_LISTEN", "[signing]"],
    );
    assert_refused(
        "a Token broker address that is no address",
        server(&[
            ("DBT_STATE_AUTH", "oidc"),
            ("DBT_STATE_AUTH_CONFIG", broker),
            ("DBT_STATE_BROKER_LISTEN", "localhost"),
        ]),
        &["DBT_STATE_BROKER_LISTEN", "localhost"],
    );
}

/// TLS files that are half set, unreadable, not PEM or not a matching
/// certificate and key refuse startup, as does a Token broker certificate
/// without a Token broker listener.
#[test]
fn unusable_tls_settings_refuse_to_start() {
    let (_dir, broker) = broker_config();
    let broker = broker.to_str().unwrap();
    let (cert, key) = tls_files();
    let rsa_key = testdata("signing-key-1.pem");
    let rsa_key = rsa_key.to_str().unwrap();
    for (cert_var, key_var, listen) in [
        ("DBT_STATE_TLS_CERT_FILE", "DBT_STATE_TLS_KEY_FILE", None),
        (
            "DBT_STATE_BROKER_TLS_CERT_FILE",
            "DBT_STATE_BROKER_TLS_KEY_FILE",
            Some(("DBT_STATE_BROKER_LISTEN", "127.0.0.1:0")),
        ),
    ] {
        let oidc = |vars: &[(&'static str, &str)]| {
            let mut all = vec![
                ("DBT_STATE_AUTH", "oidc"),
                ("DBT_STATE_AUTH_CONFIG", broker),
            ];
            all.extend(listen);
            all.extend_from_slice(vars);
            server(&all)
        };
        assert_refused(
            "a certificate without its key",
            oidc(&[(cert_var, &cert)]),
            &[cert_var, key_var],
        );
        assert_refused(
            "a key without its certificate",
            oidc(&[(key_var, &key)]),
            &[cert_var, key_var],
        );
        assert_refused(
            "an unreadable certificate",
            oidc(&[(cert_var, "/nonexistent/tls.pem"), (key_var, &key)]),
            &[cert_var, "/nonexistent/tls.pem"],
        );
        assert_refused(
            "a certificate file without a certificate",
            oidc(&[(cert_var, &key), (key_var, &key)]),
            &[cert_var, "no PEM certificate"],
        );
        assert_refused(
            "a key file without a key",
            oidc(&[(cert_var, &cert), (key_var, &cert)]),
            &[key_var, "no PEM private key"],
        );
        assert_refused(
            "a key of another certificate",
            oidc(&[(cert_var, &cert), (key_var, rsa_key)]),
            &[cert_var, "does not belong to the certificate"],
        );
    }
    assert_refused(
        "a Token broker certificate without a Token broker",
        server(&[
            ("DBT_STATE_AUTH", "oidc"),
            ("DBT_STATE_AUTH_CONFIG", broker),
            ("DBT_STATE_BROKER_TLS_CERT_FILE", &cert),
            ("DBT_STATE_BROKER_TLS_KEY_FILE", &key),
        ]),
        &["DBT_STATE_BROKER_TLS_CERT_FILE", "DBT_STATE_BROKER_LISTEN"],
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
    assert_starts_logging(case, schema, vars, &[]);
}

/// [`assert_starts`], and the server logged each of `needles` first.
fn assert_starts_logging(case: &str, schema: &Schema, vars: &[(&str, &str)], needles: &[&str]) {
    let (child, seen) = start(case, schema, vars);
    drop(child);
    for needle in needles {
        assert!(
            seen.iter().any(|line| line.contains(needle)),
            "{case}: no {needle:?} in\n{}",
            seen.join("\n")
        );
    }
}

/// Start the server with `vars` and wait until it reports it is listening.
/// Returns the running server and every line it logged until then.
fn start(case: &str, schema: &Schema, vars: &[(&str, &str)]) -> (Running, Vec<String>) {
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
    let child = Running(child);
    if !started {
        panic!("{case}: did not start\n{}", seen.join("\n"));
    }
    (child, seen)
}

/// A started server, stopped when dropped.
struct Running(std::process::Child);

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
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

    let (_broker_dir, broker) = broker_config();
    assert_starts_logging(
        "oidc with the Token broker on its own listener",
        &schema,
        &[
            ("DBT_STATE_LISTEN", "127.0.0.1:0"),
            ("DBT_STATE_AUTH", "oidc"),
            ("DBT_STATE_AUTH_CONFIG", broker.to_str().unwrap()),
            ("DBT_STATE_BROKER_LISTEN", "127.0.0.1:0"),
        ],
        &["Token broker listening"],
    );
    // Without signing keys, the Token broker's keys are fetched when a
    // request needs them, so an unreachable broker does not stop startup.
    let (_unsigned_dir, unsigned) = without_signing(Path::new(&config));
    assert_starts_logging(
        "oidc verifying with a standalone Token broker's keys",
        &schema,
        &[
            ("DBT_STATE_LISTEN", "127.0.0.1:0"),
            ("DBT_STATE_AUTH", "oidc"),
            ("DBT_STATE_AUTH_CONFIG", unsigned.to_str().unwrap()),
        ],
        &["broker jwks"],
    );
}

/// A loopback address nothing listens on yet.
fn free_address() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}

fn tls_files() -> (String, String) {
    (
        testdata("tls-server.pem").to_str().unwrap().into(),
        testdata("tls-server-key.pem").to_str().unwrap().into(),
    )
}

/// With mounted certificate and key files, the gRPC listener serves gRPC
/// over TLS and the Token broker's listener serves HTTPS, both trusted
/// through the certificate's CA.
#[test]
fn both_listeners_serve_tls_from_mounted_files() {
    let schema = Schema::new();
    let (_dir, broker) = broker_config();
    let (cert, key) = tls_files();
    let (grpc, http) = (free_address(), free_address());
    let (_server, seen) = start(
        "native TLS",
        &schema,
        &[
            ("DBT_STATE_LISTEN", &grpc),
            ("DBT_STATE_AUTH", "oidc"),
            ("DBT_STATE_AUTH_CONFIG", broker.to_str().unwrap()),
            ("DBT_STATE_TLS_CERT_FILE", &cert),
            ("DBT_STATE_TLS_KEY_FILE", &key),
            ("DBT_STATE_BROKER_LISTEN", &http),
            ("DBT_STATE_BROKER_TLS_CERT_FILE", &cert),
            ("DBT_STATE_BROKER_TLS_KEY_FILE", &key),
        ],
    );
    let ca = std::fs::read(testdata("tls-ca.pem")).unwrap();
    schema.runtime.block_on(async {
        let endpoint = tonic::transport::Channel::from_shared(format!("https://{grpc}"))
            .unwrap()
            .tls_config(
                tonic::transport::ClientTlsConfig::new()
                    .ca_certificate(tonic::transport::Certificate::from_pem(&ca)),
            )
            .unwrap();
        // The server logs that it listens just before it binds.
        let mut attempts = 0;
        let channel = loop {
            match endpoint.connect().await {
                Ok(channel) => break channel,
                Err(error) if attempts == 50 => {
                    panic!("TLS gRPC: {error:?}\n{}", seen.join("\n"))
                }
                Err(_) => {
                    attempts += 1;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        };
        let health = tonic_health::pb::health_client::HealthClient::new(channel)
            .check(tonic_health::pb::HealthCheckRequest::default())
            .await
            .expect("health over TLS")
            .into_inner();
        assert_eq!(
            health.status(),
            tonic_health::pb::health_check_response::ServingStatus::Serving
        );

        let https = reqwest::Client::builder()
            .add_root_certificate(reqwest::Certificate::from_pem(&ca).unwrap())
            .build()
            .unwrap();
        let discovery: serde_json::Value = https
            .get(format!(
                "https://localhost:{}/.well-known/openid-configuration",
                http.rsplit(':').next().unwrap()
            ))
            .send()
            .await
            .expect("broker over TLS")
            .json()
            .await
            .unwrap();
        assert_eq!(discovery["issuer"], "https://broker.dbt-state.test");
        let plain = reqwest::get(format!("http://{http}/.well-known/openid-configuration")).await;
        assert!(
            plain.map_or(true, |response| !response.status().is_success()),
            "the TLS broker listener also answered plain HTTP"
        );
    });
}
