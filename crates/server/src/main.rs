use dbt_state_server::config::Config;
use dbt_state_server::{build_router, AppState};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = Config::from_env();
    tracing::info!("connecting to postgres");
    let state = AppState::connect(&config.database_url)
        .await?
        .with_require_org_header(config.require_org_header);
    if config.require_org_header {
        tracing::info!("x-organization-id header is REQUIRED (multi-tenant mode)");
    }

    let addr = config.listen.parse()?;
    tracing::info!(%addr, "dbt-state-server listening");

    build_router(state).serve(addr).await?;

    Ok(())
}
