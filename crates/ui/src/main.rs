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
}

/// Convenience accessor used by pages.
pub fn pool(cx: &topcoat::context::Cx) -> &sqlx::PgPool {
    &app_context::<Ctx>(cx).pool
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let pool = db::connect().await?;
    let router = app::router().app_context(Ctx { pool }).build();
    topcoat::start(router).await?;
    Ok(())
}
