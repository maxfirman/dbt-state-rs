//! Differential test support: spin up our server against an isolated Postgres
//! schema, replay golden requests, and compare responses to the recorded ones.

use std::collections::HashMap;

use crate::golden::{RecordedMetadata, RpcStatus};
use dbt_state_proto::query_cache as qc;
use serde::Deserialize;
use tonic::{transport::Channel, Response, Status};

/// A recorded golden entry (as written by the record-proxy).
#[derive(Debug, Clone, Deserialize)]
pub struct GoldenEntry {
    pub service: String,
    pub method: String,
    /// When the recording proxy logged the exchange (RFC 3339), just after
    /// the hosted response. Replays decide at this time.
    #[serde(default)]
    pub recorded_at: Option<String>,
    #[serde(default)]
    pub metadata: Vec<(String, String)>,
    pub request: serde_json::Value,
    pub response: serde_json::Value,
    #[serde(default)]
    pub status: Option<RpcStatus>,
    #[serde(default)]
    pub response_metadata: Option<RecordedMetadata>,
}

impl GoldenEntry {
    /// [`GoldenEntry::recorded_at`] in Unix milliseconds.
    pub fn recorded_at_ms(&self) -> Option<i64> {
        let at = chrono::DateTime::parse_from_rfc3339(self.recorded_at.as_deref()?).ok()?;
        Some(at.timestamp_millis())
    }
}

/// A replayed call retains a status even when no protobuf response exists.
#[derive(Debug)]
pub struct RpcOutcome {
    pub response: serde_json::Value,
    pub status: RpcStatus,
    pub response_metadata: RecordedMetadata,
}

impl RpcOutcome {
    pub fn capture<T: serde::Serialize>(
        outcome: Result<Response<T>, Status>,
    ) -> anyhow::Result<Self> {
        let status = RpcStatus::capture(&outcome);
        let response_metadata = outcome
            .as_ref()
            .ok()
            .map(|response| RecordedMetadata::capture(response.metadata()))
            .unwrap_or_default();
        let response = match outcome {
            Ok(response) => serde_json::to_value(response.into_inner())?,
            Err(_) => serde_json::Value::Null,
        };
        Ok(Self {
            response,
            status,
            response_metadata,
        })
    }
}

impl GoldenEntry {
    /// Exact comparison for RPCs without correlated IDs. The existing causal
    /// replay retains its ID mapping for successful execution lifecycle calls.
    pub fn compare_outcome(&self, actual: &RpcOutcome) -> anyhow::Result<()> {
        if let Some(expected) = &self.status {
            anyhow::ensure!(
                expected.equivalent(&actual.status),
                "RPC status mismatch: expected {expected:?}, received {:?}",
                actual.status
            );
        } else {
            anyhow::ensure!(
                actual.status.code == 0,
                "legacy successful RPC returned {:?}",
                actual.status
            );
        }
        anyhow::ensure!(
            self.response == actual.response,
            "RPC response mismatch: expected {}, received {}",
            self.response,
            actual.response
        );
        if let Some(expected) = &self.response_metadata {
            anyhow::ensure!(
                expected.protocol() == actual.response_metadata.protocol(),
                "RPC response metadata mismatch"
            );
        }
        Ok(())
    }
}

/// Replay a single decoded request via generated clients. Covers every method
/// recorded by the proxy, including rejected requests and client telemetry.
/// Lifecycle callers remap correlated request/decision IDs before calling.
pub async fn replay_rpc(channel: Channel, entry: &GoldenEntry) -> anyhow::Result<RpcOutcome> {
    fn request<T>(body: T, entry: &GoldenEntry) -> anyhow::Result<tonic::Request<T>> {
        let mut request = tonic::Request::new(body);
        for (key, value) in &entry.metadata {
            if crate::golden::record_header(key) {
                let key: tonic::metadata::MetadataKey<tonic::metadata::Ascii> = key.parse()?;
                request.metadata_mut().append(key, value.parse()?);
            }
        }
        if let Some(at) = entry.recorded_at_ms() {
            request
                .metadata_mut()
                .insert("x-dbt-state-replay-now-ms", at.to_string().parse()?);
        }
        Ok(request)
    }
    macro_rules! call {
        ($client:ty, $method:ident, $request:ty) => {{
            let body: $request = serde_json::from_value(entry.request.clone())?;
            let outcome = <$client>::new(channel).$method(request(body, entry)?).await;
            RpcOutcome::capture(outcome)
        }};
    }
    #[allow(deprecated)]
    match (entry.service.as_str(), entry.method.as_str()) {
        ("SQL", "SubmitEnrichedSQL") => call!(
            qc::sql_client::SqlClient<Channel>,
            submit_enriched_sql,
            qc::SubmitEnrichedSqlRequest
        ),
        ("SQL", "SubmitValues") => call!(
            qc::sql_client::SqlClient<Channel>,
            submit_values,
            qc::SubmitValuesRequest
        ),
        ("SQL", "SubmitEnrichedSQLSpeculative") => call!(
            qc::sql_client::SqlClient<Channel>,
            submit_enriched_sql_speculative,
            qc::SubmitEnrichedSqlRequest
        ),
        ("Execution", "ConfirmExecution") => call!(
            qc::execution_client::ExecutionClient<Channel>,
            confirm_execution,
            qc::ConfirmExecutionRequest
        ),
        ("Execution", "RecordExecutions") => call!(
            qc::execution_client::ExecutionClient<Channel>,
            record_executions,
            qc::RecordExecutionsRequest
        ),
        ("Execution", "ResolveDeferredRelations") => call!(
            qc::execution_client::ExecutionClient<Channel>,
            resolve_deferred_relations,
            qc::ResolveDeferredRelationsRequest
        ),
        ("Clone", "RegisterClone") => call!(
            qc::clone_client::CloneClient<Channel>,
            register_clone,
            qc::CloneRequest
        ),
        ("ClientValidation", "ValidateClientVersion") => call!(
            qc::client_validation_client::ClientValidationClient<Channel>,
            validate_client_version,
            qc::ValidateClientVersionRequest
        ),
        ("Explain", "GetExplainMessages") => call!(
            qc::explain_client::ExplainClient<Channel>,
            get_explain_messages,
            qc::GetExplainMessagesRequest
        ),
        ("Explain", "GetUpstreamDependencyChanges") => call!(
            qc::explain_client::ExplainClient<Channel>,
            get_upstream_dependency_changes,
            qc::GetUpstreamDependencyChangesRequest
        ),
        ("SelectorService", "GetStateSelection") => call!(
            qc::selector_service_client::SelectorServiceClient<Channel>,
            get_state_selection,
            qc::SelectorRequest
        ),
        ("ClientTelemetry", "RegisterSessionStart") => call!(
            qc::client_telemetry_client::ClientTelemetryClient<Channel>,
            register_session_start,
            qc::SessionStartRequest
        ),
        ("ClientTelemetry", "SubmitTelemetryBatch") => call!(
            qc::client_telemetry_client::ClientTelemetryClient<Channel>,
            submit_telemetry_batch,
            qc::SubmitTelemetryBatchRequest
        ),
        _ => anyhow::bail!(
            "unsupported captured RPC {}.{}",
            entry.service,
            entry.method
        ),
    }
}

/// Load and parse a golden `.jsonl` file.
pub fn load_golden(path: &str) -> std::io::Result<Vec<GoldenEntry>> {
    let text = std::fs::read_to_string(path)?;
    parse_golden(&text)
}

fn parse_golden(text: &str) -> std::io::Result<Vec<GoldenEntry>> {
    let mut out = Vec::new();
    for (index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<GoldenEntry>(line) {
            Ok(e) => out.push(e),
            Err(e) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("golden line {}: {e}", index + 1),
                ))
            }
        }
    }
    Ok(out)
}

/// The decision variant name of a SubmitSQLResponse golden `response` value,
/// e.g. "skip_execution" | "ready_to_execute" | "ready_to_clone".
pub fn decision_variant(response: &serde_json::Value) -> Option<String> {
    response
        .get("response")?
        .as_object()?
        .keys()
        .next()
        .cloned()
}

/// Extract the integer `decision` from an ExplainedDecision inside a response.
pub fn explained_decision_code(response: &serde_json::Value) -> Option<i64> {
    let r = response.get("response")?.as_object()?;
    let (_variant, body) = r.iter().next()?;
    body.get("explained_decision")?.get("decision")?.as_i64()
}

/// Group golden entries by node unique id (from request labels) for scenario
/// reconstruction.
pub fn by_node(entries: &[GoldenEntry]) -> HashMap<String, Vec<&GoldenEntry>> {
    let mut m: HashMap<String, Vec<&GoldenEntry>> = HashMap::new();
    for e in entries {
        if let Some(uid) = e
            .request
            .get("labels")
            .and_then(|l| l.get("dbt_node_unique_id"))
            .and_then(|v| v.as_str())
        {
            m.entry(uid.to_string()).or_default().push(e);
        }
    }
    m
}

#[cfg(test)]
mod tests {
    #[test]
    fn malformed_trace_is_an_error_not_partial_success() {
        let valid = r#"{"service":"SQL","method":"SubmitEnrichedSQL","request":{},"response":{}}"#;
        let error = super::parse_golden(&format!("{valid}\nnot json\n{valid}")).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("line 2"));
    }
}
