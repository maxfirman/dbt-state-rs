//! The Token broker's Postgres state: pending logins, person sessions,
//! authorization codes and refresh-token family members
//! (`crates/server/migrations/0031_broker_person_login.sql`), and the
//! Principals it issues tokens to (`0032_principals.sql`).
//!
//! Every bearer value is looked up by its SHA-256 hash; none is stored.

use chrono::{DateTime, Utc};
use dbt_state_auth::{PrincipalId, PrincipalKind};
use sqlx::{PgPool, Row};
use uuid::Uuid;

/// How long a used or expired authorization code is kept, so a replay is
/// still recognized as one.
const USED_CODE_RETENTION: chrono::Duration = chrono::Duration::minutes(10);

/// A browser sign-in in progress.
#[derive(Debug, Clone)]
pub(crate) struct PendingLogin {
    pub state_hash: Vec<u8>,
    pub client_id: String,
    pub redirect_uri: String,
    pub client_state: Option<String>,
    pub code_challenge: String,
    pub upstream_nonce: String,
    pub upstream_code_verifier: String,
    pub client_ip: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

/// A Principal as the Upstream IdP authenticated it: its `principals`
/// record, keyed by issuer and subject. The gRPC server records `id` (the
/// token's `sub`) on each decision and execution the Principal makes.
pub(crate) struct Principal<'a> {
    pub id: &'a PrincipalId,
    pub issuer: &'a str,
    pub subject: &'a str,
    pub kind: PrincipalKind,
    /// For display only; a later sign-in without one keeps the known email.
    pub email: Option<&'a str>,
}

/// Create or refresh `principal`'s record.
async fn record_principal<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    principal: &Principal<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO principals (issuer, subject, id, kind, email) \
         VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (issuer, subject) DO UPDATE \
             SET kind = EXCLUDED.kind, \
                 email = COALESCE(EXCLUDED.email, principals.email), \
                 last_seen_at = now()",
    )
    .bind(principal.issuer)
    .bind(principal.subject)
    .bind(principal.id.as_str())
    .bind(principal.kind.as_str())
    .bind(principal.email)
    .execute(executor)
    .await?;
    Ok(())
}

/// A person session being created at the end of a sign-in.
pub(crate) struct NewSession {
    pub id: Uuid,
    pub principal_id: String,
    pub upstream_issuer: String,
    pub upstream_subject: String,
    pub client_id: String,
    pub scope: String,
    pub email: Option<String>,
    pub name: Option<String>,
    pub sealed_upstream_refresh_token: Vec<u8>,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

/// The authorization code a sign-in ends with.
pub(crate) struct NewCode {
    pub code_hash: Vec<u8>,
    pub client_id: String,
    pub redirect_uri: String,
    pub code_challenge: String,
    pub expires_at: DateTime<Utc>,
}

/// A person session as a code exchange reads it.
#[derive(Debug, Clone)]
pub(crate) struct Session {
    pub id: Uuid,
    pub principal_id: String,
    pub upstream_subject: String,
    pub scope: String,
    pub email: Option<String>,
    pub name: Option<String>,
    pub expires_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}

/// What presenting an authorization code found.
pub(crate) enum Redeemed {
    /// No such code (or one long expired).
    Unknown,
    /// The code was presented before; its session is now revoked.
    Replayed { session: Uuid },
    /// First presentation: the code is now used, whatever the exchange's
    /// outcome.
    First(Box<CodeGrant>),
}

/// An authorization code and what it is bound to.
pub(crate) struct CodeGrant {
    pub session: Session,
    pub client_id: String,
    pub redirect_uri: String,
    pub code_challenge: String,
    pub expires_at: DateTime<Utc>,
}

/// See the module docs.
#[derive(Clone)]
pub(crate) struct Store {
    pool: PgPool,
}

impl Store {
    pub(crate) fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Record `principal` (a machine, before its token is issued).
    pub(crate) async fn record_principal(
        &self,
        principal: &Principal<'_>,
    ) -> Result<(), sqlx::Error> {
        record_principal(&self.pool, principal).await
    }

    /// Record `login`, unless its client IP already has `cap` pending
    /// logins that have not expired. Expired pending logins are removed
    /// first. Returns whether it was recorded.
    pub(crate) async fn begin_login(
        &self,
        login: &PendingLogin,
        cap: u32,
    ) -> Result<bool, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        // Serialize sign-ins from one IP across replicas, so the cap holds.
        sqlx::query(
            "SELECT pg_advisory_xact_lock(hashtextextended('broker-pending-login:' || $1, 0))",
        )
        .bind(&login.client_ip)
        .execute(&mut *tx)
        .await?;
        sqlx::query("DELETE FROM broker_pending_logins WHERE expires_at <= $1")
            .bind(login.created_at)
            .execute(&mut *tx)
            .await?;
        let pending: i64 =
            sqlx::query_scalar("SELECT count(*) FROM broker_pending_logins WHERE client_ip = $1")
                .bind(&login.client_ip)
                .fetch_one(&mut *tx)
                .await?;
        if pending >= i64::from(cap) {
            return Ok(false);
        }
        sqlx::query(
            "INSERT INTO broker_pending_logins (state_hash, client_id, redirect_uri, \
             client_state, code_challenge, upstream_nonce, upstream_code_verifier, client_ip, \
             created_at, expires_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(&login.state_hash)
        .bind(&login.client_id)
        .bind(&login.redirect_uri)
        .bind(&login.client_state)
        .bind(&login.code_challenge)
        .bind(&login.upstream_nonce)
        .bind(&login.upstream_code_verifier)
        .bind(&login.client_ip)
        .bind(login.created_at)
        .bind(login.expires_at)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(true)
    }

    /// Remove and return the pending login whose `state` hashes to
    /// `state_hash`: each is used once, whether or not it has expired.
    pub(crate) async fn take_login(
        &self,
        state_hash: &[u8],
    ) -> Result<Option<PendingLogin>, sqlx::Error> {
        let row = sqlx::query(
            "DELETE FROM broker_pending_logins WHERE state_hash = $1 RETURNING state_hash, \
             client_id, redirect_uri, client_state, code_challenge, upstream_nonce, \
             upstream_code_verifier, client_ip, created_at, expires_at",
        )
        .bind(state_hash)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            Ok(PendingLogin {
                state_hash: row.try_get("state_hash")?,
                client_id: row.try_get("client_id")?,
                redirect_uri: row.try_get("redirect_uri")?,
                client_state: row.try_get("client_state")?,
                code_challenge: row.try_get("code_challenge")?,
                upstream_nonce: row.try_get("upstream_nonce")?,
                upstream_code_verifier: row.try_get("upstream_code_verifier")?,
                client_ip: row.try_get("client_ip")?,
                created_at: row.try_get("created_at")?,
                expires_at: row.try_get("expires_at")?,
            })
        })
        .transpose()
    }

    /// Create `session` and its authorization `code`, and record the
    /// session's person `principal`. Codes past [`USED_CODE_RETENTION`] are
    /// removed.
    pub(crate) async fn start_session(
        &self,
        principal: &Principal<'_>,
        session: &NewSession,
        code: &NewCode,
    ) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        record_principal(&mut *tx, principal).await?;
        sqlx::query("DELETE FROM broker_authorization_codes WHERE expires_at < $1")
            .bind(session.created_at - USED_CODE_RETENTION)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "INSERT INTO broker_person_sessions (id, principal_id, upstream_issuer, \
             upstream_subject, client_id, scope, email, name, upstream_refresh_token, \
             upstream_renewed_at, created_at, expires_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10, $11)",
        )
        .bind(session.id)
        .bind(&session.principal_id)
        .bind(&session.upstream_issuer)
        .bind(&session.upstream_subject)
        .bind(&session.client_id)
        .bind(&session.scope)
        .bind(&session.email)
        .bind(&session.name)
        .bind(&session.sealed_upstream_refresh_token)
        .bind(session.created_at)
        .bind(session.expires_at)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO broker_authorization_codes (code_hash, session_id, client_id, \
             redirect_uri, code_challenge, created_at, expires_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(&code.code_hash)
        .bind(session.id)
        .bind(&code.client_id)
        .bind(&code.redirect_uri)
        .bind(&code.code_challenge)
        .bind(session.created_at)
        .bind(code.expires_at)
        .execute(&mut *tx)
        .await?;
        tx.commit().await
    }

    /// Present the authorization code hashing to `code_hash` at `now`. The
    /// first presentation marks it used. A later one is a replay (RFC 6749
    /// §4.1.2): the code's session is revoked, so nothing issued from it
    /// renews.
    pub(crate) async fn redeem_code(
        &self,
        code_hash: &[u8],
        now: DateTime<Utc>,
    ) -> Result<Redeemed, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let used = sqlx::query(
            "UPDATE broker_authorization_codes SET used_at = $2 \
             WHERE code_hash = $1 AND used_at IS NULL \
             RETURNING session_id, client_id, redirect_uri, code_challenge, expires_at",
        )
        .bind(code_hash)
        .bind(now)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(code) = used else {
            let session: Option<Uuid> = sqlx::query_scalar(
                "SELECT session_id FROM broker_authorization_codes WHERE code_hash = $1",
            )
            .bind(code_hash)
            .fetch_optional(&mut *tx)
            .await?;
            let Some(session) = session else {
                return Ok(Redeemed::Unknown);
            };
            sqlx::query(
                "UPDATE broker_person_sessions SET revoked_at = $2 \
                 WHERE id = $1 AND revoked_at IS NULL",
            )
            .bind(session)
            .bind(now)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            return Ok(Redeemed::Replayed { session });
        };
        let session_id: Uuid = code.try_get("session_id")?;
        let row = sqlx::query(
            "SELECT id, principal_id, upstream_subject, scope, email, name, expires_at, \
             revoked_at FROM broker_person_sessions WHERE id = $1",
        )
        .bind(session_id)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Redeemed::First(Box::new(CodeGrant {
            session: Session {
                id: row.try_get("id")?,
                principal_id: row.try_get("principal_id")?,
                upstream_subject: row.try_get("upstream_subject")?,
                scope: row.try_get("scope")?,
                email: row.try_get("email")?,
                name: row.try_get("name")?,
                expires_at: row.try_get("expires_at")?,
                revoked_at: row.try_get("revoked_at")?,
            },
            client_id: code.try_get("client_id")?,
            redirect_uri: code.try_get("redirect_uri")?,
            code_challenge: code.try_get("code_challenge")?,
            expires_at: code.try_get("expires_at")?,
        })))
    }

    /// Record a refresh token issued in `session`: the first member of its
    /// family.
    pub(crate) async fn add_refresh_token(
        &self,
        session: Uuid,
        token_hash: &[u8],
        now: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO broker_refresh_tokens (token_hash, session_id, issued_at) \
             VALUES ($1, $2, $3)",
        )
        .bind(token_hash)
        .bind(session)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
