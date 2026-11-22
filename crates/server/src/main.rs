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
                keys = if auth.verification_keys().is_some() {
                    "signing"
                } else {
                    "broker jwks"
                },
                "auth mode oidc: every RPC except health needs a token"
            );
            if let Some(addr) = config.broker_listen {
                // The broker shares this config, and so the key set the
                // gRPC server checks tokens against, and this database, where
                // it records the Principals it issues tokens to.
                let broker =
                    dbt_state_broker::Broker::with_database(auth, state.store.pool().clone())?;
                let listener = tokio::net::TcpListener::bind(addr).await?;
                tracing::info!(%addr, issuer = auth.broker_issuer(), "Token broker listening");
                tokio::spawn(async move {
                    if let Err(error) = dbt_state_broker::serve(listener, broker).await {
                        tracing::error!(%error, "Token broker stopped");
                        std::process::exit(1);
                    }
                });
            }
            state.with_oidc(auth)?
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
