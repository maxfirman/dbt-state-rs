//! Open-source dbt State ("query cache") gRPC server with a Postgres backend.

pub mod capture;
pub mod clone;
pub mod config;
pub mod decision;
pub mod services;
pub mod sql_norm;
pub mod store;

pub use dbt_state_proto::grpc_health;
pub use dbt_state_proto::query_cache;

use std::sync::Arc;

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

use crate::store::Store;

/// Shared application state handed to every gRPC service.
#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Store>,
}

impl AppState {
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool: PgPool = PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await?;
        sqlx::migrate!("./migrations").run(&pool).await?;
        Ok(Self {
            store: Arc::new(Store::new(pool)),
        })
    }

    /// Build from an existing pool (used by tests with a transactional/temp DB).
    pub fn from_pool(pool: PgPool) -> Self {
        Self {
            store: Arc::new(Store::new(pool)),
        }
    }
}

/// Build the tonic router with all services registered against `state`.
/// Shared by the binary and by in-process differential tests.
pub fn build_router(state: AppState) -> tonic::transport::server::Router {
    use crate::query_cache::client_validation_server::ClientValidationServer;
    use crate::query_cache::clone_server::CloneServer;
    use crate::query_cache::execution_server::ExecutionServer;
    use crate::query_cache::explain_server::ExplainServer;
    use crate::query_cache::selector_service_server::SelectorServiceServer;
    use crate::query_cache::sql_server::SqlServer;
    use crate::services::{
        ClientValidationService, CloneServiceImpl, ExecutionService, ExplainService,
        SelectorServiceImpl, SqlService,
    };

    tonic::transport::Server::builder()
        .add_service(health_service())
        .add_service(SqlServer::new(SqlService(state.clone())))
        .add_service(ExecutionServer::new(ExecutionService(state.clone())))
        .add_service(CloneServer::new(CloneServiceImpl(state.clone())))
        .add_service(ClientValidationServer::new(ClientValidationService))
        .add_service(ExplainServer::new(ExplainService))
        .add_service(SelectorServiceServer::new(SelectorServiceImpl))
}

/// A gRPC health service that reports SERVING. The dbt client may probe
/// grpc.health.v1.Health before using the service.
fn health_service(
) -> tonic_health::pb::health_server::HealthServer<impl tonic_health::pb::health_server::Health> {
    let (reporter, service) = tonic_health::server::health_reporter();
    // Report SERVING synchronously via a detached task (reporter is async).
    tokio::spawn(async move {
        reporter
            .set_service_status("", tonic_health::ServingStatus::Serving)
            .await;
    });
    service
}
