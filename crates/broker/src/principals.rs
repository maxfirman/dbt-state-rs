//! The `principals` records: each Principal the broker issues a token to,
//! keyed by its Upstream IdP issuer and subject, with its kind and a display
//! email when known.
//!
//! The gRPC server records a token's opaque Principal id (`sub`) on every
//! decision and execution; this record names who that is. The table is
//! defined by the server's migrations (`0030_principals.sql`), in the
//! Postgres schema the broker shares with the server.

use dbt_state_auth::{PrincipalId, PrincipalKind};
use sqlx::PgPool;

/// A Principal as the Upstream IdP authenticated it.
pub(crate) struct Principal<'a> {
    pub(crate) id: &'a PrincipalId,
    pub(crate) issuer: &'a str,
    pub(crate) subject: &'a str,
    pub(crate) kind: PrincipalKind,
    /// For display only; a later login without one keeps the known email.
    pub(crate) email: Option<&'a str>,
}

/// Create or refresh `principal`'s record.
pub(crate) async fn record(pool: &PgPool, principal: &Principal<'_>) -> sqlx::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO principals (issuer, subject, id, kind, email)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (issuer, subject) DO UPDATE
            SET kind         = EXCLUDED.kind,
                email        = COALESCE(EXCLUDED.email, principals.email),
                last_seen_at = now()
        "#,
    )
    .bind(principal.issuer)
    .bind(principal.subject)
    .bind(principal.id.as_str())
    .bind(principal.kind.as_str())
    .bind(principal.email)
    .execute(pool)
    .await?;
    Ok(())
}
