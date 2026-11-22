mod admin;

use admin::Command;
use dbt_state_server::config::{AuthMode, Config};
use dbt_state_server::{build_router, build_tls_router, AppState};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let filter =
        || tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    match Command::parse(std::env::args_os().skip(1))? {
        Command::Serve => {
            tracing_subscriber::fmt().with_env_filter(filter()).init();
            serve().await
        }
        Command::Help => {
            println!("{}", admin::USAGE);
            Ok(())
        }
        Command::RevokeSessions { issuer, subject } => {
            // stdout carries only the result.
            use std::io::IsTerminal;
            tracing_subscriber::fmt()
                .with_env_filter(filter())
                .with_writer(std::io::stderr)
                .with_ansi(std::io::stderr().is_terminal())
                .init();
            admin::revoke_sessions(&issuer, &subject).await
        }
    }
}

async fn serve() -> anyhow::Result<()> {
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
                // gRPC server checks tokens against.
                let broker = dbt_state_broker::Broker::new(auth, state.store.pool().clone())?;
                let listener = tokio::net::TcpListener::bind(addr).await?;
                let tls = config.broker_tls.clone();
                tracing::info!(
                    %addr,
                    issuer = auth.broker_issuer(),
                    tls = tls.is_some(),
                    "Token broker listening"
                );
                tokio::spawn(async move {
                    let served = match tls {
                        Some(identity) => {
                            dbt_state_broker::serve_tls(listener, broker, &identity).await
                        }
                        None => dbt_state_broker::serve(listener, broker).await,
                    };
                    if let Err(error) = served {
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
    let router = match &config.tls {
        Some(identity) => build_tls_router(state, identity)?,
        None => build_router(state),
    };
    tracing::info!(%addr, tls = config.tls.is_some(), "dbt-state-server listening");

    router.serve(addr).await?;

    Ok(())
}
