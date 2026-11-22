//! The console binary: [`dbt_state_ui::Settings::from_env`], then serve on
//! `HOST`:`PORT`.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let settings = dbt_state_ui::Settings::from_env()?;
    let pool = dbt_state_ui::connect().await?;
    let router = dbt_state_ui::router(pool, settings, dbt_state_ui::Clock::system())?;
    topcoat::start(router).await?;
    Ok(())
}
