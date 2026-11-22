use dbt_state_server::config::{AuthMode, Config};
use dbt_state_server::{build_router, AppState};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    // Every startup refusal happens here, before any connection is made.
    let config = Config::from_env()?;

    tracing::info!("connecting to postgres");
    let state = AppState::connect(&config.database_url).await?;
    let state = match &config.auth {
        AuthMode::Oidc { path, config: auth } => {
            tracing::info!(
                config = %path.display(),
                organizations = auth.organizations().iter().count(),
                "auth mode oidc: every RPC except health needs a token"
            );
            state.with_oidc(auth.request_authenticator())
        }
        AuthMode::None { require_org_header } => {
            if !config.listen.ip().is_loopback() {
                tracing::warn!(
                    addr = %config.listen,
                    "auth mode none on a non-loopback address (DBT_STATE_ALLOW_INSECURE=true)"
                );
            }
            if *require_org_header {
                tracing::info!("x-organization-id header is REQUIRED (multi-tenant mode)");
            }
            state.with_require_org_header(*require_org_header)
        }
    };

    let addr = config.listen;
    tracing::info!(%addr, "dbt-state-server listening");

    build_router(state).serve(addr).await?;

    Ok(())
}
