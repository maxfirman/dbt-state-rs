//! The admin command an operator runs against a deployment's Postgres:
//!
//! ```text
//! dbt-state-server revoke-sessions --issuer <upstream issuer> --subject <upstream subject>
//! ```
//!
//! It revokes every Token broker session of the Principal the Upstream IdP
//! issuer and subject name, so none of them renews and the gRPC server
//! refuses their person tokens within 30 s
//! (`crates/harness/tests/session_revocation.rs`). It needs only
//! `DATABASE_URL`, prints how many sessions it revoked, and logs the action
//! on stderr.

use std::process::{Command, Output};

use sqlx::postgres::PgPoolOptions;

const BINARY: &str = env!("CARGO_BIN_EXE_dbt-state-server");
const ISSUER: &str = "https://idp.example.test/realms/corp";

fn dsn() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://dbtstate:dbtstate@localhost:55441/dbtstate".into())
}

/// A fresh migrated schema, removed afterwards.
struct Schema {
    name: String,
    runtime: tokio::runtime::Runtime,
}

impl Schema {
    fn new() -> Self {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let name = format!("admin_{}", uuid::Uuid::new_v4().simple());
        let schema = Self { name, runtime };
        schema.runtime.block_on(async {
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .connect(&dsn())
                .await
                .expect("connect");
            sqlx::query(&format!("CREATE SCHEMA \"{}\"", schema.name))
                .execute(&pool)
                .await
                .unwrap();
            let pool = schema.pool().await;
            sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        });
        schema
    }

    /// `DATABASE_URL` scoped to this schema.
    fn dsn(&self) -> String {
        let base = dsn();
        let separator = if base.contains('?') { '&' } else { '?' };
        format!("{base}{separator}options[search_path]={}", self.name)
    }

    async fn pool(&self) -> sqlx::PgPool {
        PgPoolOptions::new()
            .max_connections(1)
            .connect(&self.dsn())
            .await
            .expect("connect")
    }

    /// A person Principal, as the Token broker records one at sign-in, with
    /// `active` live sessions and one already revoked.
    fn person(&self, subject: &str, active: usize) {
        self.runtime.block_on(async {
            let pool = self.pool().await;
            let principal = format!("principal-{subject}");
            sqlx::query(
                "INSERT INTO principals (issuer, subject, id, kind) VALUES ($1, $2, $3, 'person')",
            )
            .bind(ISSUER)
            .bind(subject)
            .bind(&principal)
            .execute(&pool)
            .await
            .unwrap();
            for revoked in std::iter::repeat_n(false, active).chain([true]) {
                sqlx::query(
                    "INSERT INTO broker_person_sessions (id, principal_id, upstream_issuer, \
                     upstream_subject, client_id, scope, upstream_refresh_token, \
                     upstream_renewed_at, created_at, expires_at, revoked_at) \
                     VALUES ($1, $2, $3, $4, 'dbt', '', '\\x00', now(), now(), \
                     now() + interval '12 hours', CASE WHEN $5 THEN now() END)",
                )
                .bind(uuid::Uuid::new_v4())
                .bind(&principal)
                .bind(ISSUER)
                .bind(subject)
                .bind(revoked)
                .execute(&pool)
                .await
                .unwrap();
            }
        });
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

/// `dbt-state-server args...` with only `vars` set.
fn run(vars: &[(&str, &str)], args: &[&str]) -> Output {
    let mut command = Command::new(BINARY);
    command.env_clear().env("RUST_LOG", "info").args(args);
    for (key, value) in vars {
        command.env(key, value);
    }
    command.output().expect("run dbt-state-server")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// Every live session of the Principal is revoked and counted; another
/// Principal's sessions are not touched.
#[test]
fn revoke_sessions_revokes_every_session_of_a_principal() {
    let schema = Schema::new();
    schema.person("alice-sub", 2);
    schema.person("bob-sub", 1);
    let database = schema.dsn();
    let env = [("DATABASE_URL", database.as_str())];

    let output = run(
        &env,
        &[
            "revoke-sessions",
            "--issuer",
            ISSUER,
            "--subject",
            "alice-sub",
        ],
    );
    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(
        stdout(&output),
        "revoked 2 sessions of Principal principal-alice-sub\n"
    );
    let log = stderr(&output);
    for needle in [
        "person sessions revoked",
        &format!("issuer=\"{ISSUER}\""),
        "subject=\"alice-sub\"",
        "principal=\"principal-alice-sub\"",
        "sessions=2",
    ] {
        assert!(log.contains(needle), "no {needle:?} in\n{log}");
    }

    // Already revoked: nothing more to revoke.
    let again = run(
        &env,
        &[
            "revoke-sessions",
            &format!("--issuer={ISSUER}"),
            "--subject=alice-sub",
        ],
    );
    assert!(again.status.success(), "{}", stderr(&again));
    assert_eq!(
        stdout(&again),
        "revoked 0 sessions of Principal principal-alice-sub\n"
    );

    let bob = run(
        &env,
        &[
            "revoke-sessions",
            "--issuer",
            ISSUER,
            "--subject",
            "bob-sub",
        ],
    );
    assert_eq!(
        stdout(&bob),
        "revoked 1 session of Principal principal-bob-sub\n"
    );
}

/// An issuer and subject naming no known Principal (a typo, or another
/// issuer spelling) fail rather than report success.
#[test]
fn revoke_sessions_of_an_unknown_principal_fails() {
    let schema = Schema::new();
    schema.person("alice-sub", 1);
    let database = schema.dsn();
    for (issuer, subject) in [
        (ISSUER, "alice"),
        ("https://other.example.test", "alice-sub"),
    ] {
        let output = run(
            &[("DATABASE_URL", database.as_str())],
            &["revoke-sessions", "--issuer", issuer, "--subject", subject],
        );
        assert!(!output.status.success(), "{subject}: {}", stdout(&output));
        assert!(
            stderr(&output).contains("no Principal"),
            "{}",
            stderr(&output)
        );
    }
    let alice = run(
        &[("DATABASE_URL", database.as_str())],
        &[
            "revoke-sessions",
            "--issuer",
            ISSUER,
            "--subject",
            "alice-sub",
        ],
    );
    assert_eq!(
        stdout(&alice),
        "revoked 1 session of Principal principal-alice-sub\n"
    );
}

/// A refused command line: its name, environment, arguments, and what the
/// refusal must mention.
type Case<'a> = (&'a str, &'a [(&'a str, &'a str)], &'a [&'a str], &'a str);

/// The command needs an issuer, a subject and `DATABASE_URL`; anything
/// else on the command line is refused before connecting, and never
/// starts the server.
#[test]
fn malformed_admin_commands_are_refused() {
    let nowhere = [("DATABASE_URL", "postgres://nobody:nothing@127.0.0.1:1/none")];
    let cases: &[Case] = &[
        (
            "no subject",
            &nowhere,
            &["revoke-sessions", "--issuer", ISSUER],
            "--subject",
        ),
        (
            "no issuer",
            &nowhere,
            &["revoke-sessions", "--subject", "alice-sub"],
            "--issuer",
        ),
        (
            "an empty subject",
            &nowhere,
            &["revoke-sessions", "--issuer", ISSUER, "--subject", ""],
            "--subject",
        ),
        (
            "a flag without its value",
            &nowhere,
            &["revoke-sessions", "--subject", "alice-sub", "--issuer"],
            "--issuer",
        ),
        (
            "an unknown flag",
            &nowhere,
            &[
                "revoke-sessions",
                "--issuer",
                ISSUER,
                "--subject",
                "alice-sub",
                "--all",
            ],
            "--all",
        ),
        (
            "no DATABASE_URL",
            &[],
            &[
                "revoke-sessions",
                "--issuer",
                ISSUER,
                "--subject",
                "alice-sub",
            ],
            "DATABASE_URL",
        ),
        (
            "a misspelled command",
            &nowhere,
            &[
                "revoke-session",
                "--issuer",
                ISSUER,
                "--subject",
                "alice-sub",
            ],
            "revoke-session",
        ),
    ];
    for (case, vars, args, needle) in cases {
        let output = run(vars, args);
        let text = format!("{}{}", stdout(&output), stderr(&output));
        assert!(!output.status.success(), "{case}: succeeded\n{text}");
        assert!(text.contains(needle), "{case}: no {needle:?} in\n{text}");
        assert!(text.contains("usage:"), "{case}: no usage in\n{text}");
        for never in ["connecting to postgres", "listening"] {
            assert!(!text.contains(never), "{case}: {never}\n{text}");
        }
    }
}
