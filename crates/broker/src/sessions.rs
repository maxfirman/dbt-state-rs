//! Person sessions as the gRPC server and an operator see them: whether a
//! person token's session is still active ([`SessionCheck`]), and revoking
//! every session of a Principal ([`revoke_sessions`], the admin command).
//!
//! A session is active until it is revoked or reaches its 12-hour cap
//! (`broker_person_sessions.revoked_at`, `expires_at`). The gRPC server and
//! the Token broker share the table, whether the broker runs in the
//! server's process or as standalone replicas on the same Postgres.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use dbt_state_auth::SessionRefusal;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::Clock;

/// How long a replica trusts a session status it read: a revoked session's
/// tokens are refused at most this long after the revocation.
pub const SESSION_STATUS_MAX_AGE: Duration = Duration::from_secs(30);

/// Above this many cached statuses, the stale ones are dropped.
const PRUNE_ABOVE: usize = 4096;

/// A session as last read.
#[derive(Debug, Clone, Copy)]
enum Status {
    Unknown,
    Revoked,
    Open { expires_at: SystemTime },
}

impl Status {
    fn check(self, now: SystemTime) -> Result<(), SessionRefusal> {
        match self {
            Self::Unknown => Err(SessionRefusal::Unknown),
            Self::Revoked => Err(SessionRefusal::Revoked),
            Self::Open { expires_at } if expires_at <= now => Err(SessionRefusal::Expired),
            Self::Open { .. } => Ok(()),
        }
    }
}

struct Cached {
    /// When the read that found `status` started.
    read_at: SystemTime,
    status: Status,
}

impl Cached {
    fn is_fresh(&self, now: SystemTime) -> bool {
        // A clock that went back makes the entry stale: read again.
        now.duration_since(self.read_at)
            .is_ok_and(|age| age < SESSION_STATUS_MAX_AGE)
    }
}

/// Why a session check did not pass.
#[derive(Debug, thiserror::Error)]
pub enum SessionCheckError {
    #[error("the session is not active ({})", .0.reason())]
    Inactive(SessionRefusal),
    #[error("cannot read the session status: {0}")]
    Unavailable(#[source] sqlx::Error),
}

/// Checks that person sessions are active, caching each status this
/// replica reads for at most [`SESSION_STATUS_MAX_AGE`] on its clock.
pub struct SessionCheck {
    pool: PgPool,
    clock: Clock,
    cache: Mutex<HashMap<Uuid, Cached>>,
}

impl SessionCheck {
    /// Check sessions recorded in `pool`'s schema, with `clock` deciding
    /// cache age and the 12-hour cap.
    pub fn new(pool: PgPool, clock: Clock) -> Self {
        Self {
            pool,
            clock,
            cache: Mutex::default(),
        }
    }

    /// Whether the session a person token names (`sid`) is active now. A
    /// status this replica read less than [`SESSION_STATUS_MAX_AGE`] ago
    /// is reused; otherwise it is read from Postgres, and a failed read is
    /// [`SessionCheckError::Unavailable`], never a pass.
    pub async fn check(&self, session: &str) -> Result<(), SessionCheckError> {
        let Ok(id) = Uuid::parse_str(session) else {
            return Err(SessionCheckError::Inactive(SessionRefusal::Unknown));
        };
        let now = self.clock.now();
        let status = match self.cached(id, now) {
            Some(status) => status,
            None => {
                let status = self
                    .read(id)
                    .await
                    .map_err(SessionCheckError::Unavailable)?;
                self.remember(id, now, status);
                status
            }
        };
        status.check(now).map_err(SessionCheckError::Inactive)
    }

    fn cached(&self, id: Uuid, now: SystemTime) -> Option<Status> {
        let cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        cache
            .get(&id)
            .filter(|cached| cached.is_fresh(now))
            .map(|cached| cached.status)
    }

    /// Cache `status`, read from `read_at`, unless a later read is cached.
    fn remember(&self, id: Uuid, read_at: SystemTime, status: Status) {
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        if cache.len() >= PRUNE_ABOVE {
            cache.retain(|_, cached| cached.is_fresh(read_at));
        }
        let cached = cache.entry(id).or_insert(Cached { read_at, status });
        if cached.read_at < read_at {
            *cached = Cached { read_at, status };
        }
    }

    async fn read(&self, id: Uuid) -> Result<Status, sqlx::Error> {
        let row = sqlx::query(
            "SELECT expires_at, revoked_at IS NOT NULL AS revoked \
             FROM broker_person_sessions WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(Status::Unknown);
        };
        if row.try_get::<bool, _>("revoked")? {
            return Ok(Status::Revoked);
        }
        let expires_at: DateTime<Utc> = row.try_get("expires_at")?;
        Ok(Status::Open {
            expires_at: expires_at.into(),
        })
    }
}

/// What [`revoke_sessions`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Revocation {
    /// The Principal's id (token `sub`), when the Token broker has a record
    /// of it.
    pub principal: Option<String>,
    /// How many sessions were revoked (not counting ones already revoked).
    pub sessions: u64,
}

/// Revoke, at `at`, every session of the Principal the Upstream IdP
/// `issuer` names `subject`. None of them renews again, and the gRPC server
/// refuses their person tokens within [`SESSION_STATUS_MAX_AGE`]. A later
/// sign-in at the Upstream IdP starts a new session. Logged at `info`.
pub async fn revoke_sessions(
    pool: &PgPool,
    issuer: &str,
    subject: &str,
    at: SystemTime,
) -> Result<Revocation, sqlx::Error> {
    let principal: Option<String> =
        sqlx::query_scalar("SELECT id FROM principals WHERE issuer = $1 AND subject = $2")
            .bind(issuer)
            .bind(subject)
            .fetch_optional(pool)
            .await?;
    let revoked = sqlx::query(
        "UPDATE broker_person_sessions SET revoked_at = $3 \
         WHERE upstream_issuer = $1 AND upstream_subject = $2 AND revoked_at IS NULL",
    )
    .bind(issuer)
    .bind(subject)
    .bind(DateTime::<Utc>::from(at))
    .execute(pool)
    .await?;
    let revocation = Revocation {
        principal,
        sessions: revoked.rows_affected(),
    };
    tracing::info!(
        issuer,
        subject,
        principal = revocation.principal.as_deref(),
        sessions = revocation.sessions,
        "person sessions revoked"
    );
    Ok(revocation)
}
