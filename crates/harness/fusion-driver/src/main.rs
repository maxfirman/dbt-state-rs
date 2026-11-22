//! Drive pinned dbt Fusion's own dbt State client code.
//!
//! The harness (`crates/harness/tests/fusion_client.rs`) runs this against
//! a dbt State deployment and its Token broker. Nothing here reimplements
//! or patches Fusion: the driver loads Fusion's config from the environment
//! with Fusion's own loader (`RunCacheServiceConfig::from_env`), connects
//! Fusion's own gRPC client (`GrpcRunCacheServiceClient::connect`, as a
//! `dbt build` does), and calls its RPC methods. Each RPC looks up a
//! credential through Fusion's token source (`OAuthTokenSource::token`):
//! its in-memory token while that has more than 300 s left, else the token
//! on disk (read at most once per process, refreshed if stale), else a new
//! token (`client_credentials` for a machine, the browser flow for a
//! person).
//!
//! Fusion reads only its supported settings from the environment:
//!
//! - `RUN_CACHE_API_URL`, `RUN_CACHE_API_SECURE`: the gRPC listener, over
//!   TLS (Fusion authenticates only a secure channel);
//! - `RUN_CACHE_AUTH_URL`, `RUN_CACHE_TOKEN_URL`: the Token broker;
//! - `DBT_ENGINE_STATE_OAUTH_CLIENT_ID`,
//!   `DBT_ENV_SECRET_STATE_OAUTH_CLIENT_SECRET`: a machine's Upstream IdP
//!   app registration;
//! - `HOME`: where Fusion keeps a person's token (`~/.dbt/state_auth.json`)
//!   and looks for dbt platform credentials (none here, so a person signs
//!   in through the browser);
//! - `SSL_CERT_FILE`: the CA the host trusts, which Fusion's gRPC channel
//!   (native roots) reads, and its token requests through the build
//!   feature `Cargo.toml` explains;
//! - `PATH`: where Fusion's URL opener finds `xdg-open` (see
//!   [`browser`]).
//!
//! Scenarios:
//!
//! - `fusion-driver machine`: a CI machine validates its client version
//!   and submits one model, the RPCs a `dbt build` starts with.
//! - `fusion-driver person STEP...`: a person's long-running dbt process
//!   runs each step: `rpc` submits one model (one credential lookup);
//!   `wait` pauses for the harness (see below).
//! - `fusion-driver browser URL`: the harness browser ([`browser`]).
//!
//! `wait` prints `{"waiting": true, "rpcs": <made so far>}` and reads one
//! JSON line from stdin, `{"advance_seconds": N}`: the harness has moved
//! the deployment's clock N seconds on, and the driver moves the process's
//! wall clock as far ([`clock`]), as if that long had passed.
//!
//! Prints one JSON object on stdout at the end: the outcome of each step,
//! or the error Fusion returned. Exits 1 when Fusion returned an error.

mod browser;
mod clock;

use std::error::Error;
use std::process::ExitCode;

use dbt_run_cache::proto::query_cache::{
    submit_sql_response, ModelExecutionType, SubmitEnrichedSqlRequest,
};
use dbt_run_cache::service_client::{
    ClientVersionStatus, GrpcRunCacheServiceClient, RunCacheServiceClient, RunCacheServiceError,
};
use dbt_run_cache::service_config::RunCacheServiceConfig;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, BufReader};

const TARGET: &str = r#""DB"."S"."T""#;

/// A run that could not start: bad arguments or harness protocol.
struct Usage(String);

async fn connect() -> Result<GrpcRunCacheServiceClient, RunCacheServiceError> {
    let config = RunCacheServiceConfig::from_env()?;
    GrpcRunCacheServiceClient::connect(config).await
}

/// One model submit: the decision's response type.
async fn submit(client: &GrpcRunCacheServiceClient) -> Result<&'static str, RunCacheServiceError> {
    let response = client
        .submit_enriched_sql(SubmitEnrichedSqlRequest {
            target_table: Some(TARGET.to_owned()),
            dialect: "snowflake".to_owned(),
            default_catalog: "DB".to_owned(),
            execution_type: ModelExecutionType::Full as i32,
            sql: "select 1 as id".to_owned(),
            tolerate_nondeterminism: true,
            ..Default::default()
        })
        .await?;
    Ok(match response.response {
        Some(submit_sql_response::Response::ReadyToExecute(_)) => "ReadyToExecuteResponse",
        Some(submit_sql_response::Response::SkipExecution(_)) => "SkipExecutionResponse",
        Some(submit_sql_response::Response::ReadyToClone(_)) => "ReadyToCloneResponse",
        None => "None",
    })
}

/// A CI machine's run: the RPCs a `dbt build` starts with.
async fn machine(steps: &[String]) -> Result<Result<Value, RunCacheServiceError>, Usage> {
    if !steps.is_empty() {
        return Err(Usage("machine takes no steps".into()));
    }
    Ok(async {
        let client = connect().await?;
        let supported = client.validate_client_version().await? == ClientVersionStatus::Supported;
        let decision = submit(&client).await?;
        Ok(json!({
            "outcome": "ok",
            "client_version_supported": supported,
            "decision": decision,
        }))
    }
    .await)
}

/// A person's long-running dbt process: run `steps`.
async fn person(steps: &[String]) -> Result<Result<Value, RunCacheServiceError>, Usage> {
    if let Some(step) = steps
        .iter()
        .find(|step| !matches!(step.as_str(), "rpc" | "wait"))
    {
        return Err(Usage(format!("unknown person step: {step}")));
    }
    let client = match connect().await {
        Ok(client) => client,
        Err(error) => return Ok(Err(error)),
    };
    let mut control = BufReader::new(tokio::io::stdin()).lines();
    let mut decisions = Vec::new();
    for step in steps {
        if step == "rpc" {
            match submit(&client).await {
                Ok(decision) => decisions.push(decision),
                Err(error) => return Ok(Err(error)),
            }
            continue;
        }
        println!("{}", json!({ "waiting": true, "rpcs": decisions.len() }));
        let line = control
            .next_line()
            .await
            .map_err(|error| Usage(format!("read the harness's resume: {error}")))?
            .ok_or_else(|| Usage("the harness ended the wait without resuming".into()))?;
        let resume: Value = serde_json::from_str(&line)
            .map_err(|error| Usage(format!("the harness's resume {line:?}: {error}")))?;
        let seconds = resume["advance_seconds"].as_u64().ok_or_else(|| {
            Usage(format!(
                "the harness's resume {line:?} has no advance_seconds"
            ))
        })?;
        clock::advance(seconds);
    }
    Ok(Ok(json!({ "outcome": "ok", "decisions": decisions })))
}

/// The error Fusion returned, with its kind, the gRPC status or HTTP status
/// it carries, and its causes.
fn failure(error: &RunCacheServiceError) -> Value {
    let kind = match error {
        RunCacheServiceError::Disabled => "Disabled",
        RunCacheServiceError::Config(_) => "Config",
        RunCacheServiceError::Transport(_) => "Transport",
        RunCacheServiceError::AuthRequest(_) => "AuthRequest",
        RunCacheServiceError::Auth(_) => "Auth",
        RunCacheServiceError::OrgDisabled { .. } => "OrgDisabled",
        RunCacheServiceError::Metadata(_) => "Metadata",
        RunCacheServiceError::Rpc(_) => "Rpc",
        RunCacheServiceError::Aborted => "Aborted",
        RunCacheServiceError::Timeout(_) => "Timeout",
    };
    let mut causes = Vec::new();
    let mut source = error.source();
    while let Some(cause) = source {
        causes.push(cause.to_string());
        source = cause.source();
    }
    let mut result = json!({
        "outcome": "error",
        "error": kind,
        "message": error.to_string(),
        "causes": causes,
    });
    match error {
        RunCacheServiceError::Rpc(status) => {
            result["code"] = json!(format!("{:?}", status.code()));
            result["message"] = json!(status.message());
        }
        RunCacheServiceError::AuthRequest(request) => {
            result["status"] = json!(request.status().map(|status| status.as_u16()));
        }
        _ => {}
    }
    result
}

#[tokio::main]
async fn main() -> ExitCode {
    // As the dbt binary does when it starts.
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("install the rustls crypto provider");
    }
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (scenario, steps) = match args.split_first() {
        Some((scenario, steps)) => (scenario.as_str(), steps),
        None => ("", &[][..]),
    };
    let outcome = match scenario {
        "machine" => machine(steps).await,
        "person" => person(steps).await,
        "browser" => return browser::main(steps).await,
        _ => {
            eprintln!("usage: fusion-driver {{machine|person [STEP...]|browser URL}}");
            return ExitCode::from(2);
        }
    };
    let result = match outcome {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => failure(&error),
        Err(Usage(message)) => {
            eprintln!("fusion-driver: {message}");
            return ExitCode::from(2);
        }
    };
    println!("{result}");
    if result["outcome"] == "ok" {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
