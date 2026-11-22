//! The standalone Token broker.
//!
//! | Variable | Meaning |
//! |---|---|
//! | `DBT_STATE_BROKER_LISTEN` | HTTP listen address (default `127.0.0.1:8080`) |
//! | `DBT_STATE_AUTH_CONFIG` | path of the TOML auth config; needs `[signing]` and `[upstream]` |
//!
//! A gRPC server pointed at this broker (its auth config without
//! `[signing]`, `broker.issuer` this broker's public URL) fetches and caches
//! the keys this broker publishes.

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Context;
use dbt_state_auth::AuthConfig;
use dbt_state_broker::Broker;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let var = |key: &str| std::env::var(key).ok().filter(|value| !value.is_empty());
    let listen = var("DBT_STATE_BROKER_LISTEN").unwrap_or_else(|| "127.0.0.1:8080".into());
    let listen: SocketAddr = listen
        .parse()
        .with_context(|| format!("DBT_STATE_BROKER_LISTEN={listen:?} is not a socket address"))?;
    let path: PathBuf = var("DBT_STATE_AUTH_CONFIG")
        .context("the Token broker needs DBT_STATE_AUTH_CONFIG, the auth config path")?
        .into();
    let config = AuthConfig::load(&path)?;
    let broker = Broker::new(&config)?;

    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("cannot listen on {listen}"))?;
    tracing::info!(
        addr = %listen,
        issuer = config.broker_issuer(),
        config = %path.display(),
        "dbt-state-broker listening"
    );
    dbt_state_broker::serve(listener, broker).await?;
    Ok(())
}
