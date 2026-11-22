//! The server binary's command line: no arguments serves gRPC (configured
//! by environment, `config.rs`); `revoke-sessions` is the admin command an
//! operator runs against the deployment's Postgres in an incident.
//!
//! ```text
//! dbt-state-server revoke-sessions --issuer <upstream issuer> --subject <upstream subject>
//! ```
//!
//! It revokes every Token broker session of the Principal the Upstream IdP
//! issuer and subject name (`dbt_state_broker::revoke_sessions`): none of
//! them renews, and every gRPC server refuses their person tokens within
//! `dbt_state_broker::SESSION_STATUS_MAX_AGE`. It needs only
//! `DATABASE_URL`, prints the count on stdout and logs on stderr.

use std::ffi::OsString;
use std::time::{Duration, SystemTime};

use anyhow::{bail, Context};

pub(crate) const USAGE: &str = "\
usage: dbt-state-server
       dbt-state-server revoke-sessions --issuer <upstream issuer> --subject <upstream subject>

With no command, serve gRPC, configured by environment.

revoke-sessions revokes every Token broker session of the Principal the
Upstream IdP issuer and subject identify (the issuer as in [upstream] issuer,
the subject as its subject_claim): no session renews, and gRPC refuses its
person tokens within 30 s. A later sign-in starts a new session; disable the
person at the Upstream IdP to keep them out. Needs only DATABASE_URL.";

/// What the command line asks for.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Command {
    Serve,
    Help,
    RevokeSessions { issuer: String, subject: String },
}

impl Command {
    /// Parse the arguments after the program name. Anything unrecognised
    /// is an error, so a misspelled command never starts the server.
    pub(crate) fn parse(args: impl IntoIterator<Item = OsString>) -> anyhow::Result<Self> {
        let mut args = args
            .into_iter()
            .map(|arg| {
                arg.into_string()
                    .map_err(|arg| usage(format!("{arg:?} is not UTF-8")))
            })
            .collect::<anyhow::Result<Vec<_>>>()?
            .into_iter();
        let Some(command) = args.next() else {
            return Ok(Self::Serve);
        };
        match command.as_str() {
            "-h" | "--help" | "help" => Ok(Self::Help),
            "revoke-sessions" => {
                let (mut issuer, mut subject) = (None, None);
                while let Some(arg) = args.next() {
                    let (flag, inline) = match arg.split_once('=') {
                        Some((flag, value)) => (flag.to_owned(), Some(value.to_owned())),
                        None => (arg.clone(), None),
                    };
                    let slot = match flag.as_str() {
                        "--issuer" => &mut issuer,
                        "--subject" => &mut subject,
                        "-h" | "--help" => return Ok(Self::Help),
                        _ => return Err(usage(format!("unknown argument {arg:?}"))),
                    };
                    let value = inline
                        .or_else(|| args.next())
                        .ok_or_else(|| usage(format!("{flag} needs a value")))?;
                    if value.is_empty() {
                        return Err(usage(format!("{flag} must not be empty")));
                    }
                    *slot = Some(value);
                }
                Ok(Self::RevokeSessions {
                    issuer: issuer.ok_or_else(|| usage("--issuer is required".into()))?,
                    subject: subject.ok_or_else(|| usage("--subject is required".into()))?,
                })
            }
            other => Err(usage(format!("unknown command {other:?}"))),
        }
    }
}

fn usage(problem: String) -> anyhow::Error {
    anyhow::anyhow!("{problem}\n\n{USAGE}")
}

/// Revoke every session of the Principal `issuer` and `subject` identify,
/// in `DATABASE_URL`'s schema, and print how many were revoked. An issuer
/// and subject naming no known Principal is an error.
pub(crate) async fn revoke_sessions(issuer: &str, subject: &str) -> anyhow::Result<()> {
    let database_url = std::env::var("DATABASE_URL")
        .ok()
        .filter(|url| !url.is_empty())
        .ok_or_else(|| usage("revoke-sessions needs DATABASE_URL, the server's Postgres".into()))?;
    tracing::info!("connecting to postgres");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&database_url)
        .await
        .context("cannot connect to DATABASE_URL")?;
    let revocation = dbt_state_broker::revoke_sessions(&pool, issuer, subject, SystemTime::now())
        .await
        .context("cannot revoke the sessions")?;
    let principal = match (&revocation.principal, revocation.sessions) {
        (Some(principal), _) => format!("Principal {principal}"),
        (None, 0) => bail!(
            "no Principal with Upstream IdP issuer {issuer:?} and subject {subject:?} is \
             known, so nothing was revoked; check both against the Token broker's \
             \"token issued\" logs"
        ),
        (None, _) => format!("the Principal with issuer {issuer:?} and subject {subject:?}"),
    };
    let sessions = revocation.sessions;
    let noun = if sessions == 1 { "session" } else { "sessions" };
    println!("revoked {sessions} {noun} of {principal}");
    Ok(())
}
