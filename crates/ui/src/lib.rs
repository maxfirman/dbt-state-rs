//! Read-only web console for the dbt State server, built with Topcoat.
//!
//! Server-rendered, multipage, accessible. Reads the same Postgres the dbt
//! State server writes to.

mod app;
mod db;
mod lineage;
mod view_helpers;

use topcoat::context::app_context;

/// Shared application state: the read pool.
pub struct Ctx {
    pub pool: sqlx::PgPool,
    pub org_id: String,
}

/// Convenience accessor used by pages.
pub fn pool(cx: &topcoat::context::Cx) -> &sqlx::PgPool {
    &app_context::<Ctx>(cx).pool
}

/// The console is configured for one organization; URL IDs cannot change it.
pub fn org_id(cx: &topcoat::context::Cx) -> &str {
    &app_context::<Ctx>(cx).org_id
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let pool = db::connect().await?;
    let org_id = std::env::var("DBT_STATE_UI_ORG_ID").unwrap_or_else(|_| "local".into());
    let router = app::router().app_context(Ctx { pool, org_id }).build();
    topcoat::start(router).await?;
    Ok(())
}
