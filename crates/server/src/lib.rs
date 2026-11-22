//! Open-source dbt State ("query cache") gRPC server with a Postgres backend.

pub mod analyzed_workload;
pub mod capture;
pub mod config;
mod copy_lifecycle;
mod copy_registration;
mod data_test_history;
mod databricks_submit_copy;
pub mod decision;
mod execution;
pub mod explain;
pub mod grpc_auth;
mod request_scope;
mod result_copy;
mod reuse_candidate;
pub mod selector;
pub mod services;
pub mod sql_identity;
pub mod sql_norm;
pub mod store;
pub mod submit_decision;
pub mod volatile_evaluation;
pub mod warehouse;

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
    pub(crate) captures: Arc<capture::CaptureTasks>,
    /// When true, reject requests lacking a non-empty `x-organization-id` header
    /// instead of bucketing them into the default tenant. See `config::Config`.
    pub require_org_header: bool,
    /// The clock decisions measure build age against (ADR 0007).
    pub clock: Clock,
    /// `oidc` mode: authenticates every call but health. `None` is `none`
    /// mode. See `grpc_auth`.
    pub(crate) oidc: Option<Arc<grpc_auth::Oidc>>,
}

/// The server clock. Lag tolerance compares a reuse candidate's build age on
/// this clock, and ConfirmExecution stamps its receipt with it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Clock {
    /// The host's wall clock.
    #[default]
    System,
    /// Replaying captured traffic in tests: a request's
    /// [`REPLAY_NOW_HEADER`] (Unix ms), when present, is the clock. Never
    /// enable it in a deployment: a client could choose its own build age.
    Replay,
}

/// The metadata header [`Clock::Replay`] reads.
pub const REPLAY_NOW_HEADER: &str = "x-dbt-state-replay-now-ms";

impl Clock {
    /// Now, in Unix milliseconds, for a request with `metadata`.
    pub fn now_ms(self, metadata: &tonic::metadata::MetadataMap) -> i64 {
        self.now_micros(metadata).div_euclid(1000)
    }

    /// Now, in Unix microseconds (UTC), for a request with `metadata`. A
    /// replayed instant has millisecond precision.
    pub fn now_micros(self, metadata: &tonic::metadata::MetadataMap) -> i64 {
        let replayed = match self {
            Self::System => None,
            Self::Replay => metadata
                .get(REPLAY_NOW_HEADER)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<i64>().ok()),
        };
        replayed.map_or_else(
            || {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX))
            },
            |ms| ms.saturating_mul(1000),
        )
    }
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
            captures: Arc::default(),
            require_org_header: false,
            clock: Clock::System,
            oidc: None,
        })
    }

    /// Opt into rejecting requests without an organization header (multi-tenant
    /// hardening). Chainable on a freshly-built state.
    pub fn with_require_org_header(mut self, require: bool) -> Self {
        self.require_org_header = require;
        self
    }

    /// Build from an existing pool (used by tests with a transactional/temp DB).
    pub fn from_pool(pool: PgPool) -> Self {
        Self {
            store: Arc::new(Store::new(pool)),
            captures: Arc::default(),
            require_org_header: false,
            clock: Clock::System,
            oidc: None,
        }
    }

    /// `oidc` mode: every call except `grpc.health.v1` needs a token
    /// `config` accepts for the Organization it names, and handlers take the
    /// Organization from the authenticated context.
    ///
    /// With `[signing]`, tokens are checked against those keys: the same key
    /// set an in-process Token broker publishes. Without it, against the
    /// Token broker's published keys, fetched and cached
    /// (`dbt_state_broker::RemoteKeys`).
    ///
    /// A person token also needs an active Token broker session in this
    /// state's Postgres (`grpc_auth`).
    pub fn with_oidc(self, config: &dbt_state_auth::AuthConfig) -> anyhow::Result<Self> {
        self.with_oidc_clock(config, dbt_state_broker::Clock::system())
    }

    /// [`Self::with_oidc`], with `clock` deciding how old a cached person
    /// session status is and when a session reaches its cap (tests move
    /// it; nothing a client sends can).
    pub fn with_oidc_clock(
        mut self,
        config: &dbt_state_auth::AuthConfig,
        clock: dbt_state_broker::Clock,
    ) -> anyhow::Result<Self> {
        let pool = self.store.pool().clone();
        self.oidc = Some(Arc::new(grpc_auth::Oidc::new(config, pool, clock)?));
        Ok(self)
    }

    /// Use `clock` for build ages and confirmation receipt times.
    pub fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    /// Current count of failed best-effort selector-state writes (for metrics
    /// and the observability test). See `Store::selector_write_failures`.
    pub fn selector_write_failures(&self) -> u64 {
        self.store.selector_write_failures()
    }
}

/// The router [`build_router`] returns: every service behind the
/// authentication layer.
pub type GrpcRouter = tonic::transport::server::Router<
    tower::layer::util::Stack<grpc_auth::GrpcAuthLayer, tower::layer::util::Identity>,
>;

/// Build the tonic router with all services registered against `state`,
/// behind the authentication layer (`grpc_auth`). Shared by the binary and by
/// in-process differential tests.
pub fn build_router(state: AppState) -> GrpcRouter {
    use crate::query_cache::client_telemetry_server::ClientTelemetryServer;
    use crate::query_cache::client_validation_server::ClientValidationServer;
    use crate::query_cache::clone_server::CloneServer;
    use crate::query_cache::execution_server::ExecutionServer;
    use crate::query_cache::explain_server::ExplainServer;
    use crate::query_cache::selector_service_server::SelectorServiceServer;
    use crate::query_cache::sql_server::SqlServer;
    use crate::services::{
        ClientTelemetryService, ClientValidationService, CloneServiceImpl, ExecutionService,
        ExplainService, SelectorServiceImpl, SqlService,
    };

    tonic::transport::Server::builder()
        .layer(grpc_auth::GrpcAuthLayer::new(state.oidc.clone()))
        .add_service(health_service())
        .add_service(SqlServer::new(SqlService(state.clone())))
        .add_service(ExecutionServer::new(ExecutionService(state.clone())))
        .add_service(CloneServer::new(CloneServiceImpl(state.clone())))
        .add_service(ClientValidationServer::new(ClientValidationService))
        .add_service(ClientTelemetryServer::new(ClientTelemetryService))
        .add_service(ExplainServer::new(ExplainService(state.clone())))
        .add_service(SelectorServiceServer::new(SelectorServiceImpl(
            state.clone(),
        )))
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
