//! Send ONE crafted dbt State RPC and print the response as JSON.
//!
//! For live characterization experiments: point it at the recording proxy
//! (default `http://127.0.0.1:50099`), which authenticates, forwards to the
//! hosted service and records the exchange in the golden log. The request is
//! read from stdin as the JSON form the golden log uses.
//!
//! Usage: `state-probe <submit|confirm|record|clone|explain|values|validate|select|resolve> < request.json`
//! (`clone` sends `Clone.RegisterClone`, the dev-clone pre-step's copy request.)
//!
//! Env:
//!   PROBE_URL          default http://127.0.0.1:50099
//!   PROBE_SESSION_ID   sent as x-session-id (default: random)
//!   PROBE_INVOCATION   sent as x-dbt-invocation-id (default: random)
//!   PROBE_SUBMITTED_SHIFT_MS  shift x-submitted-at-epoch by this many ms
//!
//! Every call also sends `x-request-id` (random) and `x-submitted-at-epoch`
//! (wall clock, Unix ms), like the dbt client.

use std::io::Read;

use dbt_state_proto::query_cache as qc;
use tonic::transport::Channel;
use tonic::Request;

fn with_client_metadata<T>(msg: T) -> anyhow::Result<Request<T>> {
    let mut req = Request::new(msg);
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as i64
        + std::env::var("PROBE_SUBMITTED_SHIFT_MS")
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);
    let random = || uuid::Uuid::new_v4().simple().to_string();
    let session = std::env::var("PROBE_SESSION_ID").unwrap_or_else(|_| random());
    let invocation = std::env::var("PROBE_INVOCATION").unwrap_or_else(|_| random());
    let md = req.metadata_mut();
    md.insert("x-request-id", random().parse()?);
    md.insert("x-session-id", session.parse()?);
    md.insert("x-submitted-at-epoch", now_ms.to_string().parse()?);
    md.insert("x-dbt-invocation-id", invocation.parse()?);
    md.insert("x-os-name", "linux".parse()?);
    Ok(req)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let op = std::env::args().nth(1).ok_or_else(|| {
        anyhow::anyhow!(
            "usage: state-probe <submit|confirm|record|clone|explain|values|validate|select|resolve>"
        )
    })?;
    let mut body = String::new();
    std::io::stdin().read_to_string(&mut body)?;
    let url = std::env::var("PROBE_URL").unwrap_or_else(|_| "http://127.0.0.1:50099".into());
    let channel = Channel::from_shared(url)?.connect().await?;

    let out = match op.as_str() {
        "validate" => {
            let msg: qc::ValidateClientVersionRequest = serde_json::from_str(&body)?;
            let resp = qc::client_validation_client::ClientValidationClient::new(channel)
                .validate_client_version(with_client_metadata(msg)?)
                .await?
                .into_inner();
            serde_json::to_value(resp)?
        }
        "select" => {
            let msg: qc::SelectorRequest = serde_json::from_str(&body)?;
            let resp = qc::selector_service_client::SelectorServiceClient::new(channel)
                .get_state_selection(with_client_metadata(msg)?)
                .await?
                .into_inner();
            serde_json::to_value(resp)?
        }
        "submit" => {
            let msg: qc::SubmitEnrichedSqlRequest = serde_json::from_str(&body)?;
            let resp = qc::sql_client::SqlClient::new(channel)
                .submit_enriched_sql(with_client_metadata(msg)?)
                .await?
                .into_inner();
            serde_json::to_value(resp)?
        }
        "values" => {
            let msg: qc::SubmitValuesRequest = serde_json::from_str(&body)?;
            let resp = qc::sql_client::SqlClient::new(channel)
                .submit_values(with_client_metadata(msg)?)
                .await?
                .into_inner();
            serde_json::to_value(resp)?
        }
        "resolve" => {
            let msg: qc::ResolveDeferredRelationsRequest = serde_json::from_str(&body)?;
            let resp = qc::execution_client::ExecutionClient::new(channel)
                .resolve_deferred_relations(with_client_metadata(msg)?)
                .await?
                .into_inner();
            serde_json::to_value(resp)?
        }
        "record" => {
            let msg: qc::RecordExecutionsRequest = serde_json::from_str(&body)?;
            let resp = qc::execution_client::ExecutionClient::new(channel)
                .record_executions(with_client_metadata(msg)?)
                .await?
                .into_inner();
            serde_json::to_value(resp)?
        }
        "confirm" => {
            let msg: qc::ConfirmExecutionRequest = serde_json::from_str(&body)?;
            let resp = qc::execution_client::ExecutionClient::new(channel)
                .confirm_execution(with_client_metadata(msg)?)
                .await?
                .into_inner();
            serde_json::to_value(resp)?
        }
        "clone" => {
            let msg: qc::CloneRequest = serde_json::from_str(&body)?;
            let resp = qc::clone_client::CloneClient::new(channel)
                .register_clone(with_client_metadata(msg)?)
                .await?
                .into_inner();
            serde_json::to_value(resp)?
        }
        "explain" => {
            let msg: qc::GetExplainMessagesRequest = serde_json::from_str(&body)?;
            let resp = qc::explain_client::ExplainClient::new(channel)
                .get_explain_messages(with_client_metadata(msg)?)
                .await?
                .into_inner();
            serde_json::to_value(resp)?
        }
        other => anyhow::bail!("unknown op {other}"),
    };
    println!("{}", serde_json::to_string(&out)?);
    Ok(())
}
