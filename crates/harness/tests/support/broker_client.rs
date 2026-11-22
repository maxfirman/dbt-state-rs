//! Token broker requests exactly as the pinned dbt clients send them.
//!
//! - `client_credentials`: HTTP Basic `client_id:secret` and the form
//!   `grant_type=client_credentials&scope=runcache:scope:orgs`
//!   (`dbt-fusion@9977b6c` `crates/dbt-run-cache/src/auth/oauth.rs`
//!   `fetch_client_credentials`; `dbt-state@b0f3030`
//!   `clients/dbt_state/src/dbt_state/auth/sso.py`
//!   `_login_with_client_credentials`, authlib's default
//!   `client_secret_basic` with the session scope `runcache:scope:orgs`).
//! - token exchange: the form Fusion's `fetch_platform_token_exchange` posts
//!   when a dbt platform credential is found.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde_json::{Map, Value};

/// The scope both clients request.
pub const ORGS_SCOPE: &str = "runcache:scope:orgs";

/// POST `client_credentials` to the broker's token endpoint at `broker`.
pub async fn client_credentials(broker: &str, client_id: &str, secret: &str) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{broker}/token"))
        .basic_auth(client_id, Some(secret))
        .form(&[("grant_type", "client_credentials"), ("scope", ORGS_SCOPE)])
        .send()
        .await
        .expect("token request")
}

/// POST Fusion's dbt platform token exchange.
pub async fn token_exchange(
    broker: &str,
    client_id: &str,
    platform_token: &str,
) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{broker}/token"))
        .form(&[
            (
                "grant_type",
                "urn:ietf:params:oauth:grant-type:token-exchange",
            ),
            ("subject_token_type", "dbt"),
            ("subject_token", platform_token),
            ("dbt_hostname", "ab123.us1.dbt.com"),
            ("client_id", client_id),
        ])
        .send()
        .await
        .expect("token request")
}

/// The JSON object a response carries.
pub async fn json(response: reqwest::Response) -> Map<String, Value> {
    match response.json::<Value>().await.expect("JSON body") {
        Value::Object(body) => body,
        other => panic!("not a JSON object: {other}"),
    }
}

/// The claims of a JWT, read without verifying it (as both clients read
/// their `id_token`).
pub fn claims(token: &str) -> Map<String, Value> {
    let payload = token.split('.').nth(1).expect("a JWT");
    serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).expect("base64url payload"))
        .expect("JSON claims")
}
