//! Keycloak as the Upstream IdP, for the opt-in suite in `tests/keycloak.rs`.
//!
//! Keycloak runs in Docker (`scripts/keycloak.sh start`) with the realm in
//! `crates/harness/keycloak/dbt-state-realm.json`: the Token broker's
//! confidential client, a CI client and a client for another API, the
//! `data-eng` and `marketing` groups, the person `alice`, and the
//! harness's own user-administration client. The suite runs only when
//! `DBT_STATE_KEYCLOAK=1`; `DBT_STATE_KEYCLOAK_URL` names Keycloak's base
//! URL (default [`DEFAULT_URL`]).
//!
//! People sign in through Keycloak's own HTML login form, which the test
//! [`Browser`] fills in and submits. [`Keycloak::create_person`] adds a
//! person of the test's own through the admin REST API, so tests that
//! disable a person or change their groups never touch another test's.

use serde_json::{json, Value};

use super::browser::{Browser, Landing};
use super::oidc::UpstreamRegistration;

/// Set to `1` to run the suite.
pub const ENABLE_VAR: &str = "DBT_STATE_KEYCLOAK";
/// Keycloak's base URL, if not [`DEFAULT_URL`].
pub const URL_VAR: &str = "DBT_STATE_KEYCLOAK_URL";
/// Where `scripts/keycloak.sh` publishes Keycloak.
pub const DEFAULT_URL: &str = "http://127.0.0.1:18080";
/// The test realm.
pub const REALM: &str = "dbt-state";

/// The `aud` the CI client's access tokens carry (its audience mapper).
pub const API_AUDIENCE: &str = "dbt-state-api";
/// The Token broker's confidential client, for browser sign-in.
pub const BROKER_CLIENT_ID: &str = "dbt-state-broker";
const BROKER_CLIENT_SECRET: &str = "keycloak-broker-secret-Q4m9";
/// The Token broker's client that authenticates with `private_key_jwt`
/// (the public key of `crates/auth/testdata/upstream-rp-key.pem` is in its
/// `jwks.string`).
pub const BROKER_JWT_CLIENT_ID: &str = "dbt-state-broker-jwt";
/// A CI pipeline's client (service account), whose access tokens carry
/// [`API_AUDIENCE`].
pub const CI_CLIENT_ID: &str = "dbt-state-ci";
pub const CI_CLIENT_SECRET: &str = "keycloak-ci-secret-Jw27";
/// A client for another API: its access tokens carry `aud` `other-api`.
pub const OTHER_CLIENT_ID: &str = "other-service";
pub const OTHER_CLIENT_SECRET: &str = "keycloak-other-secret-Lp53";
/// The harness's user administration (service account with
/// `realm-management` user roles).
const ADMIN_CLIENT_ID: &str = "harness-admin";
const ADMIN_CLIENT_SECRET: &str = "keycloak-admin-secret-Vb61";

/// A person in the realm.
#[derive(Debug, Clone)]
pub struct KeycloakPerson {
    /// Keycloak's user id: the `sub` of their tokens.
    pub id: String,
    pub username: String,
    pub password: String,
    pub email: String,
}

impl KeycloakPerson {
    /// Their username and password, for the login form.
    pub fn credentials(&self) -> (&str, &str) {
        (&self.username, &self.password)
    }
}

/// `alice`, in `data-eng`, from the realm file: username and password.
pub fn alice() -> (&'static str, &'static str) {
    ("alice", "alice-password")
}

/// The running Keycloak. See the module docs.
pub struct Keycloak {
    base: String,
    http: reqwest::Client,
}

impl Keycloak {
    /// Keycloak, when the suite is enabled; `None` (with a message) when it
    /// is not. An enabled suite whose Keycloak does not answer panics.
    pub async fn from_env() -> Option<Self> {
        if std::env::var(ENABLE_VAR).as_deref() != Ok("1") {
            eprintln!(
                "skipping: the Keycloak suite runs only with {ENABLE_VAR}=1 \
                 (start Keycloak with scripts/keycloak.sh start)"
            );
            return None;
        }
        let base = std::env::var(URL_VAR).unwrap_or_else(|_| DEFAULT_URL.into());
        let keycloak = Self {
            base: base.trim_end_matches('/').into(),
            http: reqwest::Client::new(),
        };
        let discovery = format!("{}/.well-known/openid-configuration", keycloak.issuer());
        let status = keycloak
            .http
            .get(&discovery)
            .send()
            .await
            .unwrap_or_else(|error| panic!("{ENABLE_VAR}=1 but Keycloak is unreachable: {error}"))
            .status();
        assert!(status.is_success(), "{discovery}: HTTP {status}");
        Some(keycloak)
    }

    /// The realm's issuer.
    pub fn issuer(&self) -> String {
        format!("{}/realms/{REALM}", self.base)
    }

    /// The realm and the broker's client there, as `[upstream]` names
    /// them. No `client_credentials_scope`: Keycloak refuses a scope the
    /// client was not given (`invalid_scope`), and the CI client's
    /// audience mapper applies without one.
    pub fn registration(&self) -> UpstreamRegistration {
        UpstreamRegistration {
            issuer: self.issuer(),
            api_audience: API_AUDIENCE.into(),
            client_credentials_scope: None,
            client_id: BROKER_CLIENT_ID.into(),
            client_secret: BROKER_CLIENT_SECRET.into(),
        }
    }

    /// [`Self::registration`] with the broker's `private_key_jwt` client:
    /// use with `BrokerCredential::PrivateKeyJwt`.
    pub fn private_key_jwt_registration(&self) -> UpstreamRegistration {
        UpstreamRegistration {
            client_id: BROKER_JWT_CLIENT_ID.into(),
            client_secret: String::new(),
            ..self.registration()
        }
    }

    /// Sign in at Keycloak's login form: open `url` in `browser`, submit
    /// `username` and `password` on the page it lands on, and follow the
    /// redirects to `loopback`.
    pub async fn sign_in(
        &self,
        browser: &Browser,
        url: &str,
        (username, password): (&str, &str),
        loopback: &str,
    ) -> Landing {
        let page = match browser.visit(url, loopback).await {
            Landing::Page { status: 200, body } => body,
            other => panic!("no Keycloak login page: {other:?}"),
        };
        let action = login_form_action(&page);
        browser
            .submit(
                &action,
                &[
                    ("username", username),
                    ("password", password),
                    ("credentialId", ""),
                ],
                loopback,
            )
            .await
    }

    /// The user id of `username`.
    pub async fn user_id(&self, username: &str) -> String {
        let users = self
            .admin(reqwest::Method::GET, "users")
            .await
            .query(&[("username", username), ("exact", "true")])
            .send()
            .await
            .expect("Keycloak admin API")
            .error_for_status()
            .expect("list users")
            .json::<Vec<Value>>()
            .await
            .expect("users JSON");
        match &users[..] {
            [user] => user["id"].as_str().expect("user id").into(),
            other => panic!("{username}: {other:?}"),
        }
    }

    /// A new person of this test's own, with a password, in `groups`.
    pub async fn create_person(&self, groups: &[&str]) -> KeycloakPerson {
        let username = format!("person-{}", uuid::Uuid::new_v4().simple());
        let password = format!("pw-{}", uuid::Uuid::new_v4().simple());
        let email = format!("{username}@example.test");
        let groups: Vec<String> = groups.iter().map(|group| format!("/{group}")).collect();
        self.admin(reqwest::Method::POST, "users")
            .await
            .json(&json!({
                "username": username,
                "enabled": true,
                "email": email,
                "emailVerified": true,
                "firstName": "Test",
                "lastName": "Person",
                "groups": groups,
                "credentials": [{"type": "password", "value": password, "temporary": false}],
            }))
            .send()
            .await
            .expect("Keycloak admin API")
            .error_for_status()
            .expect("create user");
        KeycloakPerson {
            id: self.user_id(&username).await,
            username,
            password,
            email,
        }
    }

    /// Disable `person` in Keycloak, as an administrator offboarding them.
    pub async fn disable(&self, person: &KeycloakPerson) {
        self.admin(reqwest::Method::PUT, &format!("users/{}", person.id))
            .await
            .json(&json!({"enabled": false}))
            .send()
            .await
            .expect("Keycloak admin API")
            .error_for_status()
            .expect("disable user");
    }

    /// Remove `person` from the group `group`.
    pub async fn leave_group(&self, person: &KeycloakPerson, group: &str) {
        let groups = self
            .admin(reqwest::Method::GET, "groups")
            .await
            .query(&[("search", group), ("exact", "true")])
            .send()
            .await
            .expect("Keycloak admin API")
            .error_for_status()
            .expect("list groups")
            .json::<Vec<Value>>()
            .await
            .expect("groups JSON");
        let id = groups
            .iter()
            .find(|found| found["name"] == group)
            .and_then(|found| found["id"].as_str())
            .unwrap_or_else(|| panic!("no group {group}: {groups:?}"));
        self.admin(
            reqwest::Method::DELETE,
            &format!("users/{}/groups/{id}", person.id),
        )
        .await
        .send()
        .await
        .expect("Keycloak admin API")
        .error_for_status()
        .expect("leave group");
    }

    /// A request to the realm's admin REST API at `path`, authorized as
    /// the harness's administration client.
    async fn admin(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let token: Value = self
            .http
            .post(format!("{}/protocol/openid-connect/token", self.issuer()))
            .basic_auth(ADMIN_CLIENT_ID, Some(ADMIN_CLIENT_SECRET))
            .form(&[("grant_type", "client_credentials")])
            .send()
            .await
            .expect("Keycloak token endpoint")
            .error_for_status()
            .expect("admin token")
            .json()
            .await
            .expect("token JSON");
        let token = token["access_token"].as_str().expect("admin access token");
        self.http
            .request(method, format!("{}/admin/realms/{REALM}/{path}", self.base))
            .bearer_auth(token)
    }
}

/// The `action` URL of Keycloak's login form (`<form id="kc-form-login"
/// ... action="...">`), its `&amp;` entities decoded.
fn login_form_action(page: &str) -> String {
    let form = page
        .find("id=\"kc-form-login\"")
        .map(|at| &page[at..])
        .unwrap_or_else(|| panic!("no Keycloak login form in\n{page}"));
    let action = form
        .split_once("action=\"")
        .and_then(|(_, rest)| rest.split_once('"'))
        .map(|(action, _)| action)
        .expect("a login form action");
    action.replace("&amp;", "&")
}
