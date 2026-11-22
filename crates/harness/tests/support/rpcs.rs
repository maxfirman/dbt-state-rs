//! Every RPC of the dbt State protocol, for checks that hold for all of
//! them (authentication and authorization in `oidc` mode).

use dbt_state_proto::query_cache as qc;
use tonic::Status;

use super::scenario::Scenario;

/// Each RPC of the dbt State protocol, called with `sc`'s metadata and a
/// default body.
pub async fn call_every_rpc(sc: &Scenario) -> Vec<(&'static str, Result<(), Status>)> {
    let channel = sc.channel();
    let mut sql = qc::sql_client::SqlClient::new(channel.clone());
    let mut execution = qc::execution_client::ExecutionClient::new(channel.clone());
    let mut clone = qc::clone_client::CloneClient::new(channel.clone());
    let mut validation = qc::client_validation_client::ClientValidationClient::new(channel.clone());
    let mut telemetry = qc::client_telemetry_client::ClientTelemetryClient::new(channel.clone());
    let mut explain = qc::explain_client::ExplainClient::new(channel.clone());
    let mut selector = qc::selector_service_client::SelectorServiceClient::new(channel);
    fn done<T>(result: Result<tonic::Response<T>, Status>) -> Result<(), Status> {
        result.map(|_| ())
    }
    #[allow(deprecated)]
    let session_start = telemetry
        .register_session_start(sc.request(Default::default()))
        .await;
    vec![
        (
            "SubmitEnrichedSQL",
            done(
                sql.submit_enriched_sql(sc.request(Default::default()))
                    .await,
            ),
        ),
        (
            "SubmitValues",
            done(sql.submit_values(sc.request(Default::default())).await),
        ),
        (
            "SubmitEnrichedSQLSpeculative",
            done(
                sql.submit_enriched_sql_speculative(sc.request(Default::default()))
                    .await,
            ),
        ),
        (
            "ConfirmExecution",
            done(
                execution
                    .confirm_execution(sc.request(Default::default()))
                    .await,
            ),
        ),
        (
            "RecordExecutions",
            done(
                execution
                    .record_executions(sc.request(Default::default()))
                    .await,
            ),
        ),
        (
            "ResolveDeferredRelations",
            done(
                execution
                    .resolve_deferred_relations(sc.request(Default::default()))
                    .await,
            ),
        ),
        (
            "RegisterClone",
            done(clone.register_clone(sc.request(Default::default())).await),
        ),
        (
            "ValidateClientVersion",
            done(
                validation
                    .validate_client_version(sc.request(Default::default()))
                    .await,
            ),
        ),
        ("RegisterSessionStart", done(session_start)),
        (
            "SubmitTelemetryBatch",
            done(
                telemetry
                    .submit_telemetry_batch(sc.request(Default::default()))
                    .await,
            ),
        ),
        (
            "GetExplainMessages",
            done(
                explain
                    .get_explain_messages(sc.request(Default::default()))
                    .await,
            ),
        ),
        (
            "GetUpstreamDependencyChanges",
            done(
                explain
                    .get_upstream_dependency_changes(sc.request(Default::default()))
                    .await,
            ),
        ),
        (
            "GetStateSelection",
            done(
                selector
                    .get_state_selection(sc.request(Default::default()))
                    .await,
            ),
        ),
    ]
}
