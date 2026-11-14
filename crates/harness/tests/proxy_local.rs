//! Explicit local recording mode is a public CLI boundary and needs no cloud.
mod support;

#[test]
fn local_recording_refuses_nonliteral_or_nonloopback_destinations() {
    for destination in [
        "http://example.com:50051",
        "http://localhost:50051",
        "http://192.0.2.1:50051",
        "https://127.0.0.1:50051",
        "http://user:pass@127.0.0.1:50051",
        "http://127.0.0.1:50051/path",
    ] {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_record-proxy"))
            .env("PROXY_LOCAL_UPSTREAM", destination)
            .env("PROXY_UPSTREAM", "invalid-default-must-not-be-used")
            .output()
            .unwrap();
        assert!(!output.status.success(), "accepted {destination}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("literal loopback HTTP endpoint"),
            "{destination}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn local_recording_forwards_and_certifies_public_rpcs() {
    let server = support::start_server().await;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let dir = std::env::temp_dir().join(format!("proxy-local-{}", uuid::Uuid::new_v4()));
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_record-proxy"))
        .env("PROXY_LOCAL_UPSTREAM", format!("http://{}", server.addr))
        .env("PROXY_UPSTREAM", "invalid-default-must-not-be-used")
        .env("PROXY_LISTEN", addr.to_string())
        .env("GOLDEN_DIR", &dir)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let endpoint = format!("http://{addr}");
    let mut client = None;
    for _ in 0..100 {
        if let Ok(connection) =
            dbt_state_proto::query_cache::client_validation_client::ClientValidationClient::connect(
                endpoint.clone(),
            )
            .await
        {
            client = Some(connection);
            break;
        }
        assert!(child.try_wait().unwrap().is_none(), "proxy exited early");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let response = client
        .expect("local proxy startup")
        .validate_client_version(dbt_state_proto::query_cache::ValidateClientVersionRequest {
            dbt_run_cache_version: "2.0.11".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(response.is_supported);
    let status = std::process::Command::new("kill")
        .args(["-TERM", &child.id().unwrap().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    assert!(child.wait().await.unwrap().success());
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../experiments/capture_integrity.py");
    let capture = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|ext| ext == "jsonl"))
        .expect("capture JSONL");
    let output = std::process::Command::new("python3")
        .env("PYTHONPATH", script.parent().unwrap())
        .args(["-c", "import sys; from capture_integrity import verify_capture; assert len(verify_capture(sys.argv[1])) == 1"])
        .arg(&capture).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let rows = dbt_state_harness::diff::load_golden(capture.to_str().unwrap()).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].response, serde_json::json!({"is_supported": true}));
    std::fs::remove_dir_all(dir).unwrap();
}
