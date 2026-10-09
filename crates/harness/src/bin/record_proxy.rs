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
//!   GOLDEN_DIR        default ./golden
//!   RUST_LOG          tracing filter

use std::sync::Arc;

use tonic::transport::{Channel, ClientTlsConfig, Server};
use tonic::{Request, Response, Status};

use dbt_state_harness::auth::{DbtCloudCredential, TokenMinter};
use dbt_state_harness::golden::{GoldenEntry, GoldenLog};

use dbt_state_proto::query_cache as qc;

use qc::client_validation_client::ClientValidationClient;
use qc::client_validation_server::{ClientValidation, ClientValidationServer};
use qc::clone_client::CloneClient;
use qc::clone_server::{Clone as CloneSvc, CloneServer};
use qc::execution_client::ExecutionClient;
use qc::execution_server::{Execution, ExecutionServer};
use qc::explain_client::ExplainClient;
use qc::explain_server::{Explain, ExplainServer};
use qc::sql_client::SqlClient;
use qc::sql_server::{Sql, SqlServer};

const AUTHORIZATION_HEADER: &str = "authorization";
const ORG_ID_HEADER: &str = "x-organization-id";

/// Shared proxy state: upstream channel, token minter, golden log.
struct Proxy {
    channel: Channel,
    minter: Arc<TokenMinter>,
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

    /// Build an upstream request from a decoded message, attaching auth metadata.
    async fn upstream_request<T>(&self, msg: T) -> Result<Request<T>, Status> {
        let token = self
            .minter
            .token()
            .await
            .map_err(|e| Status::unauthenticated(format!("token exchange failed: {e}")))?;
        let mut req = Request::new(msg);
        let md = req.metadata_mut();
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
        resp: &Resp,
    ) where
        Req: serde::Serialize,
        Resp: serde::Serialize,
    {
        let entry = GoldenEntry {
            service: service.to_string(),
            method: method.to_string(),
            recorded_at: chrono::Utc::now().to_rfc3339(),
            metadata: meta,
            request: req,
            response: resp,
        };
        self.golden.append(&entry);
    }
}

// ---- SQL service ----
#[tonic::async_trait]
impl Sql for ProxyService {
    async fn submit_enriched_sql(
        &self,
        request: Request<qc::SubmitEnrichedSqlRequest>,
    ) -> Result<Response<qc::SubmitSqlResponse>, Status> {
        let meta = Proxy::capture_metadata(&request);
        let msg = request.into_inner();
        let up = self.upstream_request(msg.clone()).await?;
        let resp = SqlClient::new(self.channel.clone())
            .submit_enriched_sql(up)
            .await?
            .into_inner();
        self.log("SQL", "SubmitEnrichedSQL", meta, &msg, &resp);
        Ok(Response::new(resp))
    }

    async fn submit_values(
        &self,
        request: Request<qc::SubmitValuesRequest>,
    ) -> Result<Response<qc::SubmitSqlResponse>, Status> {
        let meta = Proxy::capture_metadata(&request);
        let msg = request.into_inner();
        let up = self.upstream_request(msg.clone()).await?;
        let resp = SqlClient::new(self.channel.clone())
            .submit_values(up)
            .await?
            .into_inner();
        self.log("SQL", "SubmitValues", meta, &msg, &resp);
        Ok(Response::new(resp))
    }

    async fn submit_enriched_sql_speculative(
        &self,
        request: Request<qc::SubmitEnrichedSqlRequest>,
    ) -> Result<Response<qc::SubmitSqlSpeculativeResponse>, Status> {
        let meta = Proxy::capture_metadata(&request);
        let msg = request.into_inner();
        let up = self.upstream_request(msg.clone()).await?;
        let resp = SqlClient::new(self.channel.clone())
            .submit_enriched_sql_speculative(up)
            .await?
            .into_inner();
        self.log("SQL", "SubmitEnrichedSQLSpeculative", meta, &msg, &resp);
        Ok(Response::new(resp))
    }
}

// ---- Execution service ----
#[tonic::async_trait]
impl Execution for ProxyService {
    async fn confirm_execution(
        &self,
        request: Request<qc::ConfirmExecutionRequest>,
    ) -> Result<Response<qc::ConfirmExecutionResponse>, Status> {
        let meta = Proxy::capture_metadata(&request);
        let msg = request.into_inner();
        let up = self.upstream_request(msg.clone()).await?;
        let resp = ExecutionClient::new(self.channel.clone())
            .confirm_execution(up)
            .await?
            .into_inner();
        self.log("Execution", "ConfirmExecution", meta, &msg, &resp);
        Ok(Response::new(resp))
    }

    async fn record_executions(
        &self,
        request: Request<qc::RecordExecutionsRequest>,
    ) -> Result<Response<qc::RecordExecutionsResponse>, Status> {
        let meta = Proxy::capture_metadata(&request);
        let msg = request.into_inner();
        let up = self.upstream_request(msg.clone()).await?;
        let resp = ExecutionClient::new(self.channel.clone())
            .record_executions(up)
            .await?
            .into_inner();
        self.log("Execution", "RecordExecutions", meta, &msg, &resp);
        Ok(Response::new(resp))
    }

    async fn resolve_deferred_relations(
        &self,
        request: Request<qc::ResolveDeferredRelationsRequest>,
    ) -> Result<Response<qc::ResolveDeferredRelationsResponse>, Status> {
        let meta = Proxy::capture_metadata(&request);
        let msg = request.into_inner();
        let up = self.upstream_request(msg.clone()).await?;
        let resp = ExecutionClient::new(self.channel.clone())
            .resolve_deferred_relations(up)
            .await?
            .into_inner();
        self.log("Execution", "ResolveDeferredRelations", meta, &msg, &resp);
        Ok(Response::new(resp))
    }
}

// ---- Clone service ----
#[tonic::async_trait]
impl CloneSvc for ProxyService {
    async fn register_clone(
        &self,
        request: Request<qc::CloneRequest>,
    ) -> Result<Response<qc::CloneResponse>, Status> {
        let meta = Proxy::capture_metadata(&request);
        let msg = request.into_inner();
        let up = self.upstream_request(msg.clone()).await?;
        let resp = CloneClient::new(self.channel.clone())
            .register_clone(up)
            .await?
            .into_inner();
        self.log("Clone", "RegisterClone", meta, &msg, &resp);
        Ok(Response::new(resp))
    }
}

// ---- ClientValidation service ----
#[tonic::async_trait]
impl ClientValidation for ProxyService {
    async fn validate_client_version(
        &self,
        request: Request<qc::ValidateClientVersionRequest>,
    ) -> Result<Response<qc::ValidateClientVersionResponse>, Status> {
        let meta = Proxy::capture_metadata(&request);
        let msg = request.into_inner();
        let up = self.upstream_request(msg.clone()).await?;
        let resp = ClientValidationClient::new(self.channel.clone())
            .validate_client_version(up)
            .await?
            .into_inner();
        self.log(
            "ClientValidation",
            "ValidateClientVersion",
            meta,
            &msg,
            &resp,
        );
        Ok(Response::new(resp))
    }
}

// ---- Explain service ----
#[tonic::async_trait]
impl Explain for ProxyService {
    async fn get_explain_messages(
        &self,
        request: Request<qc::GetExplainMessagesRequest>,
    ) -> Result<Response<qc::GetExplainMessagesResponse>, Status> {
        let meta = Proxy::capture_metadata(&request);
        let msg = request.into_inner();
        let up = self.upstream_request(msg.clone()).await?;
        let resp = ExplainClient::new(self.channel.clone())
            .get_explain_messages(up)
            .await?
            .into_inner();
        self.log("Explain", "GetExplainMessages", meta, &msg, &resp);
        Ok(Response::new(resp))
    }

    async fn get_upstream_dependency_changes(
        &self,
        request: Request<qc::GetUpstreamDependencyChangesRequest>,
    ) -> Result<Response<qc::GetUpstreamDependencyChangesResponse>, Status> {
        let meta = Proxy::capture_metadata(&request);
        let msg = request.into_inner();
        let up = self.upstream_request(msg.clone()).await?;
        let resp = ExplainClient::new(self.channel.clone())
            .get_upstream_dependency_changes(up)
            .await?
            .into_inner();
        self.log("Explain", "GetUpstreamDependencyChanges", meta, &msg, &resp);
        Ok(Response::new(resp))
    }
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

    let credential = DbtCloudCredential::from_default_config()?;
    tracing::info!(
        host = %credential.host,
        project = %credential.project_id,
        "loaded dbt Cloud credential"
    );

    // Pre-mint once to fail fast if auth is broken.
    let minter = Arc::new(TokenMinter::new(credential));
    let tok = minter.token().await?;
    tracing::info!(org_id = %tok.org_id, "token exchange OK");

    let channel = Channel::from_shared(upstream.clone())?
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .connect()
        .await?;
    tracing::info!(%upstream, "connected to upstream");

    let golden = Arc::new(GoldenLog::create(&golden_dir)?);
    tracing::info!(path = %golden.path().display(), "recording golden traffic");

    let proxy = Arc::new(Proxy {
        channel,
        minter,
        golden,
    });
    let svc = ProxyService(proxy);

    let addr = listen.parse()?;
    tracing::info!(%listen, "recording proxy listening (insecure h2c)");

    Server::builder()
        .add_service(SqlServer::new(svc.clone()))
        .add_service(ExecutionServer::new(svc.clone()))
        .add_service(CloneServer::new(svc.clone()))
        .add_service(ClientValidationServer::new(svc.clone()))
        .add_service(ExplainServer::new(svc))
        .serve(addr)
        .await?;

    Ok(())
}
