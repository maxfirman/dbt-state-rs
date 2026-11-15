//! Recording gRPC proxy for the dbt State ("query cache") protocol.
//!
//! Listens on a local **insecure** port. The Fusion dbt client is pointed here
//! via `RUN_CACHE_API_URL=127.0.0.1:<port>` and `RUN_CACHE_API_SECURE=false`,
//! which disables the client's own OAuth. The proxy forwards every call to the
//! real `api.state.dbt.com:443` over TLS, attaching a freshly minted Bearer
//! token + org id (platform token-exchange, see `auth`). Each decoded
//! request/response pair is appended to a JSON golden file.
//!
//! Env:
//!   PROXY_LISTEN      default 127.0.0.1:50099
//!   PROXY_UPSTREAM    default https://api.state.dbt.com:443
//!   PROXY_LOCAL_UPSTREAM explicit literal loopback HTTP endpoint, without cloud auth
//!   GOLDEN_DIR        default ./golden
//!   RUST_LOG          tracing filter

use std::sync::Arc;

use tonic::transport::{Channel, ClientTlsConfig, Server};
use tonic::{Request, Response, Status};

use dbt_state_harness::auth::{DbtCloudCredential, TokenMinter};
use dbt_state_harness::golden::{GoldenEntry, GoldenLog, RecordedMetadata, RpcStatus};

use dbt_state_proto::query_cache as qc;

use qc::client_telemetry_client::ClientTelemetryClient;
use qc::client_telemetry_server::{ClientTelemetry, ClientTelemetryServer};
use qc::client_validation_client::ClientValidationClient;
use qc::client_validation_server::{ClientValidation, ClientValidationServer};
use qc::clone_client::CloneClient;
use qc::clone_server::{Clone as CloneSvc, CloneServer};
use qc::execution_client::ExecutionClient;
use qc::execution_server::{Execution, ExecutionServer};
use qc::explain_client::ExplainClient;
use qc::explain_server::{Explain, ExplainServer};
use qc::selector_service_client::SelectorServiceClient;
use qc::selector_service_server::{SelectorService, SelectorServiceServer};
use qc::sql_client::SqlClient;
use qc::sql_server::{Sql, SqlServer};

const AUTHORIZATION_HEADER: &str = "authorization";
const ORG_ID_HEADER: &str = "x-organization-id";
/// Inbound headers the proxy replaces (auth) or that belong to the local
/// transport hop rather than the client's request.
const NOT_FORWARDED: &[&str] = &[
    AUTHORIZATION_HEADER,
    ORG_ID_HEADER,
    "te",
    "content-type",
    "user-agent",
    "host",
];

/// Shared proxy state: upstream channel, token minter, golden log.
struct Proxy {
    channel: Channel,
    minter: Option<Arc<TokenMinter>>,
    golden: Arc<GoldenLog>,
}

/// Local newtype wrapper so we can implement the (foreign) generated service
/// traits without violating the orphan rule, and so tonic gets a `Clone`
/// service. All state lives behind the shared `Arc<Proxy>`.
#[derive(Clone)]
struct ProxyService(Arc<Proxy>);

impl std::ops::Deref for ProxyService {
    type Target = Proxy;
    fn deref(&self) -> &Proxy {
        &self.0
    }
}

impl Proxy {
    /// Capture inbound metadata headers (for the golden record) as plain pairs.
    fn capture_metadata<T>(req: &Request<T>) -> Vec<(String, String)> {
        req.metadata()
            .iter()
            .filter_map(|kv| match kv {
                tonic::metadata::KeyAndValueRef::Ascii(k, v) => {
                    Some((k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
                }
                _ => None,
            })
            .collect()
    }

    /// Build an upstream request from a decoded message. Forwards the client's
    /// own metadata (`x-submitted-at-epoch`, `x-dbt-invocation-id`, ...) so the
    /// hosted service sees what a direct client would send, then attaches this
    /// proxy's auth metadata in place of the client's.
    async fn upstream_request<T>(
        &self,
        inbound: &[(String, String)],
        msg: T,
    ) -> Result<Request<T>, Status> {
        let mut req = Request::new(msg);
        let md = req.metadata_mut();
        for (key, value) in inbound {
            let local_org = self.minter.is_none() && key == ORG_ID_HEADER;
            if (NOT_FORWARDED.contains(&key.as_str()) && !local_org) || key.starts_with("grpc-") {
                continue;
            }
            if let (Ok(k), Ok(v)) = (
                key.parse::<tonic::metadata::MetadataKey<tonic::metadata::Ascii>>(),
                value.parse(),
            ) {
                md.append(k, v);
            }
        }
        let Some(minter) = &self.minter else {
            return Ok(req);
        };
        let token = minter
            .token()
            .await
            .map_err(|_| Status::unauthenticated("token exchange failed"))?;
        md.insert(
            AUTHORIZATION_HEADER,
            format!("Bearer {}", token.id_token)
                .parse()
                .map_err(|_| Status::internal("bad bearer metadata"))?,
        );
        md.insert(
            ORG_ID_HEADER,
            token
                .org_id
                .parse()
                .map_err(|_| Status::internal("bad org metadata"))?,
        );
        Ok(req)
    }

    fn log<Req, Resp>(
        &self,
        service: &str,
        method: &str,
        meta: Vec<(String, String)>,
        req: &Req,
        outcome: Result<Response<Resp>, Status>,
    ) -> Result<Response<Resp>, Status>
    where
        Req: serde::Serialize,
        Resp: serde::Serialize,
    {
        let entry = GoldenEntry {
            service: service.to_string(),
            method: method.to_string(),
            recorded_at: chrono::Utc::now().to_rfc3339(),
            metadata: meta
                .into_iter()
                .filter(|(key, _)| dbt_state_harness::golden::record_header(key))
                .collect(),
            request: req,
            response: outcome.as_ref().ok().map(|response| response.get_ref()),
            status: RpcStatus::capture(&outcome),
            response_metadata: outcome
                .as_ref()
                .ok()
                .map(|response| RecordedMetadata::capture(response.metadata()))
                .unwrap_or_default(),
        };
        self.golden
            .append(&entry)
            .map_err(|_| Status::internal("capture failed to record RPC outcome"))?;
        outcome
    }
}

// ---- SQL service ----
#[tonic::async_trait]
impl Sql for ProxyService {
    async fn submit_enriched_sql(
        &self,
        request: Request<qc::SubmitEnrichedSqlRequest>,
    ) -> Result<Response<qc::SubmitSqlResponse>, Status> {
        self.golden
            .begin()
            .map_err(|_| Status::internal("capture failed before forwarding"))?;
        let meta = Proxy::capture_metadata(&request);
        let msg = request.into_inner();
        let resp = async {
            let up = self.upstream_request(&meta, msg.clone()).await?;
            SqlClient::new(self.channel.clone())
                .submit_enriched_sql(up)
                .await
        }
        .await;
        self.log("SQL", "SubmitEnrichedSQL", meta, &msg, resp)
    }

    async fn submit_values(
        &self,
        request: Request<qc::SubmitValuesRequest>,
    ) -> Result<Response<qc::SubmitSqlResponse>, Status> {
        self.golden
            .begin()
            .map_err(|_| Status::internal("capture failed before forwarding"))?;
        let meta = Proxy::capture_metadata(&request);
        let msg = request.into_inner();
        let resp = async {
            let up = self.upstream_request(&meta, msg.clone()).await?;
            SqlClient::new(self.channel.clone()).submit_values(up).await
        }
        .await;
        self.log("SQL", "SubmitValues", meta, &msg, resp)
    }

    async fn submit_enriched_sql_speculative(
        &self,
        request: Request<qc::SubmitEnrichedSqlRequest>,
    ) -> Result<Response<qc::SubmitSqlSpeculativeResponse>, Status> {
        self.golden
            .begin()
            .map_err(|_| Status::internal("capture failed before forwarding"))?;
        let meta = Proxy::capture_metadata(&request);
        let msg = request.into_inner();
        let resp = async {
            let up = self.upstream_request(&meta, msg.clone()).await?;
            SqlClient::new(self.channel.clone())
                .submit_enriched_sql_speculative(up)
                .await
        }
        .await;
        self.log("SQL", "SubmitEnrichedSQLSpeculative", meta, &msg, resp)
    }
}

// ---- Execution service ----
#[tonic::async_trait]
impl Execution for ProxyService {
    async fn confirm_execution(
        &self,
        request: Request<qc::ConfirmExecutionRequest>,
    ) -> Result<Response<qc::ConfirmExecutionResponse>, Status> {
        self.golden
            .begin()
            .map_err(|_| Status::internal("capture failed before forwarding"))?;
        let meta = Proxy::capture_metadata(&request);
        let msg = request.into_inner();
        let resp = async {
            let up = self.upstream_request(&meta, msg.clone()).await?;
            ExecutionClient::new(self.channel.clone())
                .confirm_execution(up)
                .await
        }
        .await;
        self.log("Execution", "ConfirmExecution", meta, &msg, resp)
    }

    async fn record_executions(
        &self,
        request: Request<qc::RecordExecutionsRequest>,
    ) -> Result<Response<qc::RecordExecutionsResponse>, Status> {
        self.golden
            .begin()
            .map_err(|_| Status::internal("capture failed before forwarding"))?;
        let meta = Proxy::capture_metadata(&request);
        let msg = request.into_inner();
        let resp = async {
            let up = self.upstream_request(&meta, msg.clone()).await?;
            ExecutionClient::new(self.channel.clone())
                .record_executions(up)
                .await
        }
        .await;
        self.log("Execution", "RecordExecutions", meta, &msg, resp)
    }

    async fn resolve_deferred_relations(
        &self,
        request: Request<qc::ResolveDeferredRelationsRequest>,
    ) -> Result<Response<qc::ResolveDeferredRelationsResponse>, Status> {
        self.golden
            .begin()
            .map_err(|_| Status::internal("capture failed before forwarding"))?;
        let meta = Proxy::capture_metadata(&request);
        let msg = request.into_inner();
        let resp = async {
            let up = self.upstream_request(&meta, msg.clone()).await?;
            ExecutionClient::new(self.channel.clone())
                .resolve_deferred_relations(up)
                .await
        }
        .await;
        self.log("Execution", "ResolveDeferredRelations", meta, &msg, resp)
    }
}

// ---- Clone service ----
#[tonic::async_trait]
impl CloneSvc for ProxyService {
    async fn register_clone(
        &self,
        request: Request<qc::CloneRequest>,
    ) -> Result<Response<qc::CloneResponse>, Status> {
        self.golden
            .begin()
            .map_err(|_| Status::internal("capture failed before forwarding"))?;
        let meta = Proxy::capture_metadata(&request);
        let msg = request.into_inner();
        let resp = async {
            let up = self.upstream_request(&meta, msg.clone()).await?;
            CloneClient::new(self.channel.clone())
                .register_clone(up)
                .await
        }
        .await;
        self.log("Clone", "RegisterClone", meta, &msg, resp)
    }
}

// ---- ClientValidation service ----
#[tonic::async_trait]
impl ClientValidation for ProxyService {
    async fn validate_client_version(
        &self,
        request: Request<qc::ValidateClientVersionRequest>,
    ) -> Result<Response<qc::ValidateClientVersionResponse>, Status> {
        self.golden
            .begin()
            .map_err(|_| Status::internal("capture failed before forwarding"))?;
        let meta = Proxy::capture_metadata(&request);
        let msg = request.into_inner();
        let resp = async {
            let up = self.upstream_request(&meta, msg.clone()).await?;
            ClientValidationClient::new(self.channel.clone())
                .validate_client_version(up)
                .await
        }
        .await;
        self.log(
            "ClientValidation",
            "ValidateClientVersion",
            meta,
            &msg,
            resp,
        )
    }
}

// ---- Explain service ----
#[tonic::async_trait]
impl Explain for ProxyService {
    async fn get_explain_messages(
        &self,
        request: Request<qc::GetExplainMessagesRequest>,
    ) -> Result<Response<qc::GetExplainMessagesResponse>, Status> {
        self.golden
            .begin()
            .map_err(|_| Status::internal("capture failed before forwarding"))?;
        let meta = Proxy::capture_metadata(&request);
        let msg = request.into_inner();
        let resp = async {
            let up = self.upstream_request(&meta, msg.clone()).await?;
            ExplainClient::new(self.channel.clone())
                .get_explain_messages(up)
                .await
        }
        .await;
        self.log("Explain", "GetExplainMessages", meta, &msg, resp)
    }

    async fn get_upstream_dependency_changes(
        &self,
        request: Request<qc::GetUpstreamDependencyChangesRequest>,
    ) -> Result<Response<qc::GetUpstreamDependencyChangesResponse>, Status> {
        self.golden
            .begin()
            .map_err(|_| Status::internal("capture failed before forwarding"))?;
        let meta = Proxy::capture_metadata(&request);
        let msg = request.into_inner();
        let resp = async {
            let up = self.upstream_request(&meta, msg.clone()).await?;
            ExplainClient::new(self.channel.clone())
                .get_upstream_dependency_changes(up)
                .await
        }
        .await;
        self.log("Explain", "GetUpstreamDependencyChanges", meta, &msg, resp)
    }
}

// ---- SelectorService (state:* selectors) ----
#[tonic::async_trait]
impl SelectorService for ProxyService {
    async fn get_state_selection(
        &self,
        request: Request<qc::SelectorRequest>,
    ) -> Result<Response<qc::SelectorResponse>, Status> {
        self.golden
            .begin()
            .map_err(|_| Status::internal("capture failed before forwarding"))?;
        let meta = Proxy::capture_metadata(&request);
        let msg = request.into_inner();
        let resp = async {
            let up = self.upstream_request(&meta, msg.clone()).await?;
            SelectorServiceClient::new(self.channel.clone())
                .get_state_selection(up)
                .await
        }
        .await;
        self.log("SelectorService", "GetStateSelection", meta, &msg, resp)
    }
}

// ---- ClientTelemetry (metering; selector fires ClientSelectorEvent) ----
#[tonic::async_trait]
impl ClientTelemetry for ProxyService {
    // RegisterSessionStart is deprecated in the proto but the proxy must still
    // forward it verbatim if a client sends it.
    #[allow(deprecated)]
    async fn register_session_start(
        &self,
        request: Request<qc::SessionStartRequest>,
    ) -> Result<Response<qc::SessionStartResponse>, Status> {
        self.golden
            .begin()
            .map_err(|_| Status::internal("capture failed before forwarding"))?;
        let meta = Proxy::capture_metadata(&request);
        let msg = request.into_inner();
        let resp = async {
            let up = self.upstream_request(&meta, msg.clone()).await?;
            ClientTelemetryClient::new(self.channel.clone())
                .register_session_start(up)
                .await
        }
        .await;
        self.log("ClientTelemetry", "RegisterSessionStart", meta, &msg, resp)
    }

    async fn submit_telemetry_batch(
        &self,
        request: Request<qc::SubmitTelemetryBatchRequest>,
    ) -> Result<Response<qc::SubmitTelemetryBatchResponse>, Status> {
        self.golden
            .begin()
            .map_err(|_| Status::internal("capture failed before forwarding"))?;
        let meta = Proxy::capture_metadata(&request);
        let msg = request.into_inner();
        let resp = async {
            let up = self.upstream_request(&meta, msg.clone()).await?;
            ClientTelemetryClient::new(self.channel.clone())
                .submit_telemetry_batch(up)
                .await
        }
        .await;
        self.log("ClientTelemetry", "SubmitTelemetryBatch", meta, &msg, resp)
    }
}

fn local_endpoint(value: &str) -> anyhow::Result<String> {
    let invalid = || {
        anyhow::anyhow!(
            "PROXY_LOCAL_UPSTREAM must be a literal loopback HTTP endpoint with an explicit port"
        )
    };
    let addr: std::net::SocketAddr = value
        .strip_prefix("http://")
        .ok_or_else(invalid)?
        .parse()
        .map_err(|_| invalid())?;
    if !addr.ip().is_loopback() || addr.port() == 0 || value != format!("http://{addr}") {
        return Err(invalid());
    }
    Ok(value.to_owned())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let listen = std::env::var("PROXY_LISTEN").unwrap_or_else(|_| "127.0.0.1:50099".into());
    let upstream =
        std::env::var("PROXY_UPSTREAM").unwrap_or_else(|_| "https://api.state.dbt.com:443".into());
    let golden_dir = std::env::var("GOLDEN_DIR").unwrap_or_else(|_| "./golden".into());

    let (channel, minter) = if let Ok(local) = std::env::var("PROXY_LOCAL_UPSTREAM") {
        let local = local_endpoint(&local)?;
        let channel = Channel::from_shared(local)?.connect().await?;
        (channel, None)
    } else {
        let credential = DbtCloudCredential::from_default_config()?;
        tracing::info!(
            host = %credential.host,
            project = %credential.project_id,
            "loaded dbt Cloud credential"
        );

        // Pre-mint once to fail fast if auth is broken.
        let minter = Arc::new(TokenMinter::new(credential));
        let tok = minter
            .token()
            .await
            .map_err(|_| anyhow::anyhow!("token exchange failed"))?;
        tracing::info!(org_id = %tok.org_id, "token exchange OK");

        let channel = Channel::from_shared(upstream.clone())?
            .tls_config(ClientTlsConfig::new().with_native_roots())?
            .connect()
            .await?;
        tracing::info!(%upstream, "connected to upstream");
        (channel, Some(minter))
    };

    let golden = Arc::new(GoldenLog::create(&golden_dir)?);
    tracing::info!(path = %golden.path().display(), "recording golden traffic");

    let proxy = Arc::new(Proxy {
        channel,
        minter,
        golden: golden.clone(),
    });
    let svc = ProxyService(proxy);

    let addr = listen.parse()?;
    tracing::info!(%listen, "recording proxy listening (insecure h2c)");

    let serving = Server::builder()
        .add_service(SqlServer::new(svc.clone()))
        .add_service(ExecutionServer::new(svc.clone()))
        .add_service(CloneServer::new(svc.clone()))
        .add_service(ClientValidationServer::new(svc.clone()))
        .add_service(ExplainServer::new(svc.clone()))
        .add_service(SelectorServiceServer::new(svc.clone()))
        .add_service(ClientTelemetryServer::new(svc))
        .serve_with_shutdown(addr, shutdown_signal())
        .await;
    golden.finish()?;
    serving?;

    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
#[path = "../proxy_tests.rs"]
mod tests;
