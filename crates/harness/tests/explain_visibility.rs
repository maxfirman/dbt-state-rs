mod support;

use dbt_state_proto::query_cache as qc;

#[tokio::test]
async fn unrelated_explain_ids_do_not_wait_for_another_capture() {
    let server = support::start_server().await;
    let hold = support::fault::hold_decision_capture(&server).await;
    let channel = support::channel(server.addr).await;
    let reply = qc::sql_client::SqlClient::new(channel.clone())
        .submit_enriched_sql(support::scenario::model("ISOLATED_VISIBILITY").build())
        .await
        .unwrap()
        .into_inner();
    let qc::submit_sql_response::Response::ReadyToExecute(ready) = reply.response.unwrap() else {
        panic!("first submission must build");
    };
    let id = ready.execution_decision_id.unwrap();
    let mut client = qc::explain_client::ExplainClient::new(channel);
    for (org, ids) in [("other", vec![id]), ("local", vec!["unknown".into()])] {
        let mut request = tonic::Request::new(qc::GetExplainMessagesRequest {
            execution_decision_ids: ids,
        });
        request
            .metadata_mut()
            .insert("x-organization-id", org.parse().unwrap());
        request.set_timeout(std::time::Duration::from_secs(2));
        let response = client
            .get_explain_messages(request)
            .await
            .unwrap()
            .into_inner();
        assert!(response.messages.is_empty());
    }
    hold.rollback().await.unwrap();
}

#[tokio::test]
async fn first_explain_for_returned_decision_survives_capture_lag() {
    let server = support::start_server().await;
    let hold = support::fault::hold_decision_capture(&server).await;
    let release = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        hold.rollback().await.unwrap();
    });
    let channel = support::channel(server.addr).await;
    let reply = qc::sql_client::SqlClient::new(channel.clone())
        .submit_enriched_sql(support::scenario::model("VISIBILITY").build())
        .await
        .unwrap()
        .into_inner();
    let qc::submit_sql_response::Response::ReadyToExecute(ready) = reply.response.unwrap() else {
        panic!("first submission must build");
    };
    let id = ready.execution_decision_id.unwrap();
    let result = qc::explain_client::ExplainClient::new(channel)
        .get_explain_messages(qc::GetExplainMessagesRequest {
            execution_decision_ids: vec![id.clone()],
        })
        .await
        .unwrap()
        .into_inner();
    release.await.unwrap();
    assert_eq!(
        result.messages.len(),
        1,
        "the first Explain must include the returned decision"
    );
    assert_eq!(result.messages[0].execution_decision_id, id);
    assert!(!result.messages[0].explain_lines.is_empty());
}
