//! Exercises the production recording handlers through the public gRPC seam.
use super::*;
use tonic::metadata::MetadataValue;

#[derive(Clone)]
struct Upstream;

#[tonic::async_trait]
impl ClientValidation for Upstream {
    async fn validate_client_version(
        &self,
        request: Request<qc::ValidateClientVersionRequest>,
    ) -> Result<Response<qc::ValidateClientVersionResponse>, Status> {
        let version = request.into_inner().dbt_run_cache_version;
        if let Some(path) = version.strip_prefix("break-capture:") {
            // Inject a disk failure after forwarding but before outcome capture.
            std::fs::remove_file(path).unwrap();
            std::fs::create_dir(path).unwrap();
        }
        if version == "reject" {
            let mut status = Status::with_details(
                tonic::Code::InvalidArgument,
                "unsupported version",
                vec![0, 255, 17].into(),
            );
            status
                .metadata_mut()
                .insert("set-cookie", "upstream-secret".parse().unwrap());
            status
                .metadata_mut()
                .insert("x-rejection-reason", "version".parse().unwrap());
            status
                .metadata_mut()
                .insert_bin("x-evidence-bin", MetadataValue::from_bytes(&[0, 254]));
            return Err(status);
        }
        let mut response = Response::new(qc::ValidateClientVersionResponse { is_supported: true });
        response
            .metadata_mut()
            .insert("x-evidence", "supported".parse().unwrap());
        Ok(response)
    }
}

async fn listen() -> (
    std::net::SocketAddr,
    tokio_stream::wrappers::TcpListenerStream,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    (
        listener.local_addr().unwrap(),
        tokio_stream::wrappers::TcpListenerStream::new(listener),
    )
}

#[tokio::test]
async fn successful_and_rejected_rpc_survive_forwarding_and_capture() {
    let (up_addr, up_listener) = listen().await;
    let upstream = tokio::spawn(
        Server::builder()
            .add_service(ClientValidationServer::new(Upstream))
            .serve_with_incoming(up_listener),
    );
    let channel = Channel::from_shared(format!("http://{up_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let dir = std::env::temp_dir().join(format!("capture-{}", uuid::Uuid::new_v4()));
    let golden = Arc::new(GoldenLog::create(&dir).unwrap());
    let proxy = ProxyService(Arc::new(Proxy {
        channel,
        minter: None,
        golden: golden.clone(),
    }));
    let (addr, listener) = listen().await;
    let server = tokio::spawn(
        Server::builder()
            .add_service(ClientValidationServer::new(proxy))
            .serve_with_incoming(listener),
    );
    let mut client = ClientValidationClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let mut request = Request::new(qc::ValidateClientVersionRequest {
        dbt_run_cache_version: "2.0.11".into(),
    });
    request
        .metadata_mut()
        .insert("authorization", "Bearer client-secret".parse().unwrap());
    request
        .metadata_mut()
        .insert("cookie", "session=cookie-secret".parse().unwrap());
    request
        .metadata_mut()
        .insert("x-api-key", "key-secret".parse().unwrap());
    request
        .metadata_mut()
        .insert("x-request-id", "trace-evidence".parse().unwrap());
    let response = client.validate_client_version(request).await.unwrap();
    assert!(response.get_ref().is_supported);
    let status = client
        .validate_client_version(qc::ValidateClientVersionRequest {
            dbt_run_cache_version: "reject".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert_eq!(status.message(), "unsupported version");
    assert_eq!(status.details(), &[0, 255, 17]);
    assert_eq!(
        status.metadata().get("x-rejection-reason").unwrap(),
        "version"
    );
    let rows: Vec<serde_json::Value> = std::fs::read_to_string(golden.path())
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(rows.len(), 2, "the rejected call must not disappear");
    assert_eq!(
        rows[0]["response"],
        serde_json::json!({"is_supported": true})
    );
    assert_eq!(rows[1]["response"], serde_json::Value::Null);
    assert_eq!(rows[1]["status"]["code"], 3);
    assert_eq!(rows[1]["status"]["message"], "unsupported version");
    assert_eq!(
        rows[1]["status"]["details"],
        serde_json::json!([0, 255, 17])
    );
    assert_eq!(
        rows[1]["status"]["metadata"]["binary"],
        serde_json::json!([["x-evidence-bin", [0, 254]]])
    );
    for row in &rows {
        let entry: dbt_state_harness::diff::GoldenEntry =
            serde_json::from_value(row.clone()).unwrap();
        let channel = Channel::from_shared(format!("http://{up_addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let replay = dbt_state_harness::diff::replay_rpc(channel, &entry)
            .await
            .unwrap();
        entry.compare_outcome(&replay).unwrap();
    }
    assert_eq!(response.metadata().get("x-evidence").unwrap(), "supported");
    let capture = std::fs::read_to_string(golden.path()).unwrap();
    for secret in [
        "client-secret",
        "cookie-secret",
        "key-secret",
        "upstream-secret",
    ] {
        assert!(
            !capture.contains(secret),
            "credential leaked into capture: {secret}"
        );
    }
    assert!(capture.contains("trace-evidence"));
    golden.finish().unwrap();
    let state: serde_json::Value =
        serde_json::from_slice(&std::fs::read(golden.state_path()).unwrap()).unwrap();
    assert_eq!(
        state,
        serde_json::json!({"started": 2, "recorded": 2, "complete": true, "failed": false})
    );
    server.abort();
    upstream.abort();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn capture_write_failure_fails_rpc_and_session() {
    let (up_addr, up_listener) = listen().await;
    let upstream = tokio::spawn(
        Server::builder()
            .add_service(ClientValidationServer::new(Upstream))
            .serve_with_incoming(up_listener),
    );
    let channel = Channel::from_shared(format!("http://{up_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let dir = std::env::temp_dir().join(format!("capture-{}", uuid::Uuid::new_v4()));
    let golden = Arc::new(GoldenLog::create(&dir).unwrap());
    let proxy = ProxyService(Arc::new(Proxy {
        channel,
        minter: None,
        golden: golden.clone(),
    }));
    let (addr, listener) = listen().await;
    let server = tokio::spawn(
        Server::builder()
            .add_service(ClientValidationServer::new(proxy))
            .serve_with_incoming(listener),
    );
    let mut client = ClientValidationClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    // A filesystem failure at the capture boundary, without permissions or
    // process-global limits that behave differently under root or in CI.
    let status = client
        .validate_client_version(qc::ValidateClientVersionRequest {
            dbt_run_cache_version: format!("break-capture:{}", golden.state_path().display()),
        })
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::Internal);
    assert_eq!(status.message(), "capture failed to record RPC outcome");
    assert!(
        golden.finish().is_err(),
        "write loss must not certify a completed campaign"
    );
    server.abort();
    upstream.abort();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tonic::async_trait]
impl CloneSvc for Upstream {
    async fn register_clone(
        &self,
        request: Request<qc::CloneRequest>,
    ) -> Result<Response<qc::CloneResponse>, Status> {
        let ready = qc::ReadyToCloneResponse {
            execution_decision_id: (request.into_inner().target_table == "present")
                .then(String::new),
            clone_execution_results: Some(qc::Struct::default()),
            ..Default::default()
        };
        #[allow(deprecated)]
        Ok(Response::new(qc::CloneResponse {
            ready_to_clone: Some(ready.clone()),
            response: Some(qc::clone_response::Response::ReadyToCloneV1(ready)),
            unable_to_clone: None,
        }))
    }
}

#[tonic::async_trait]
impl ClientTelemetry for Upstream {
    #[allow(deprecated)]
    async fn register_session_start(
        &self,
        _: Request<qc::SessionStartRequest>,
    ) -> Result<Response<qc::SessionStartResponse>, Status> {
        Err(Status::unimplemented("not used by pinned client"))
    }

    async fn submit_telemetry_batch(
        &self,
        request: Request<qc::SubmitTelemetryBatchRequest>,
    ) -> Result<Response<qc::SubmitTelemetryBatchResponse>, Status> {
        let request = request.into_inner();
        let Some(qc::client_telemetry_event::Request::SessionEnd(end)) =
            request.events[0].request.as_ref()
        else {
            panic!("expected session-end event");
        };
        assert_eq!(
            end.session_duration,
            Some(prost_types::Duration {
                seconds: 12,
                nanos: 345_678_901
            })
        );
        Ok(Response::new(qc::SubmitTelemetryBatchResponse {
            success: true,
        }))
    }
}

#[tokio::test]
async fn session_end_duration_survives_public_proxy_capture_and_replay() {
    let (up_addr, up_listener) = listen().await;
    let upstream = tokio::spawn(
        Server::builder()
            .add_service(ClientTelemetryServer::new(Upstream))
            .serve_with_incoming(up_listener),
    );
    let channel = Channel::from_shared(format!("http://{up_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let dir = std::env::temp_dir().join(format!("capture-{}", uuid::Uuid::new_v4()));
    let golden = Arc::new(GoldenLog::create(&dir).unwrap());
    let proxy = ProxyService(Arc::new(Proxy {
        channel: channel.clone(),
        minter: None,
        golden: golden.clone(),
    }));
    let (addr, listener) = listen().await;
    let server = tokio::spawn(
        Server::builder()
            .add_service(ClientTelemetryServer::new(proxy))
            .serve_with_incoming(listener),
    );
    let mut client = ClientTelemetryClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    client
        .submit_telemetry_batch(qc::SubmitTelemetryBatchRequest {
            events: vec![qc::ClientTelemetryEvent {
                event_order: Some(3),
                request: Some(qc::client_telemetry_event::Request::SessionEnd(
                    qc::SessionEndRequest {
                        session_duration: Some(prost_types::Duration {
                            seconds: 12,
                            nanos: 345_678_901,
                        }),
                        result: 0,
                        result_description: "completed".into(),
                        metrics: None,
                    },
                )),
            }],
        })
        .await
        .unwrap();
    golden.finish().unwrap();
    let entries = dbt_state_harness::diff::load_golden(golden.path().to_str().unwrap()).unwrap();
    let entry = &entries[0];
    assert_eq!(
        entry.request["events"][0]["request"]["session_end"]["session_duration"],
        serde_json::json!({"seconds": 12, "nanos": 345_678_901})
    );
    let replay = dbt_state_harness::diff::replay_rpc(channel, entry)
        .await
        .unwrap();
    entry.compare_outcome(&replay).unwrap();
    server.abort();
    upstream.abort();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn clone_mirrors_and_optional_field_presence_survive_capture_and_replay() {
    let (up_addr, up_listener) = listen().await;
    let upstream = tokio::spawn(
        Server::builder()
            .add_service(CloneServer::new(Upstream))
            .serve_with_incoming(up_listener),
    );
    let channel = Channel::from_shared(format!("http://{up_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let dir = std::env::temp_dir().join(format!("capture-{}", uuid::Uuid::new_v4()));
    let golden = Arc::new(GoldenLog::create(&dir).unwrap());
    let proxy = ProxyService(Arc::new(Proxy {
        channel: channel.clone(),
        minter: None,
        golden: golden.clone(),
    }));
    let (addr, listener) = listen().await;
    let server = tokio::spawn(
        Server::builder()
            .add_service(CloneServer::new(proxy))
            .serve_with_incoming(listener),
    );
    let mut client = CloneClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    for target in ["present", "absent"] {
        let response = client
            .register_clone(qc::CloneRequest {
                target_table: target.into(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response.ready_to_clone.unwrap().execution_decision_id,
            if target == "present" {
                Some(String::new())
            } else {
                None
            }
        );
    }
    golden.finish().unwrap();
    let entries = dbt_state_harness::diff::load_golden(golden.path().to_str().unwrap()).unwrap();
    for (index, entry) in entries.iter().enumerate() {
        assert_eq!(
            entry.response["ready_to_clone"],
            entry.response["response"]["ready_to_clone_v1"]
        );
        assert_eq!(
            entry.response["ready_to_clone"]["clone_execution_results"],
            serde_json::json!({"fields": {}})
        );
        let expected = if index == 0 {
            serde_json::json!("")
        } else {
            serde_json::Value::Null
        };
        assert_eq!(
            entry.response["ready_to_clone"]["execution_decision_id"],
            expected
        );
        let replay = dbt_state_harness::diff::replay_rpc(channel.clone(), entry)
            .await
            .unwrap();
        entry.compare_outcome(&replay).unwrap();
        let mut missing = entry.clone();
        missing.response["ready_to_clone"]
            .as_object_mut()
            .unwrap()
            .remove("execution_decision_id");
        assert!(
            missing.compare_outcome(&replay).is_err(),
            "omitting an optional field cannot hide a presence mismatch"
        );
    }
    server.abort();
    upstream.abort();
    std::fs::remove_dir_all(dir).unwrap();
}
