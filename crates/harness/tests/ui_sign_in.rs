//! The UI console in `oidc` mode: a person signs in through the Token
//! broker (the console is its registered confidential client,
//! `[clients.ui]`) and sees only the Organizations they belong to.
//!
//! The console keeps its session in an encrypted `__Host-` cookie that is
//! `HttpOnly`, `Secure` and `SameSite=Lax`, so page scripts never see a
//! token. Before it shows a page, it checks the person's broker session
//! (revocation reaches it within 30 s), renews at the broker before the
//! person token expires, and refuses Organizations its current config does
//! not have active. In `none` mode it shows `DBT_STATE_UI_ORG_ID`'s data
//! with no sign-in, as before.
//!
//! The console runs as a deployment runs it, configured by the operator's
//! environment variables (`support/console.rs`); a browser follows its
//! redirects through the broker and the fake Upstream IdP and back.

#[path = "support.rs"]
mod support;

#[path = "support/console.rs"]
#[allow(dead_code)]
mod console;

use std::collections::BTreeMap;
use std::time::Duration;

use console::{start_ui, start_ui_with, ConsoleBrowser, Page, CONSOLE};
use dbt_state_auth::Role;
use serde_json::json;
use support::fake_idp::{FakeIdp, UpstreamPerson};
use support::oidc::{TestAuth, TestAuthBuilder, ISSUER};
use support::rows;
use support::scenario::{into_execute, model, Scenario};
use support::ui_client::{UI_CLIENT_ID, UI_REDIRECT_URI};
use support::{start_server, start_server_with_broker, TestServer};

const TARGET: &str = "\"DB\".\"S\".\"T\"";
/// The console's session cookie, as the browser stores it.
const SESSION: &str = "__Host-dbt-state-ui-session";
/// Each Organization's dbt project.
const ANALYTICS_PROJECT: &str = "warehouse-analytics";
const RESEARCH_PROJECT: &str = "lab-research";

/// Two active Organizations and a disabled one.
const ORGANIZATIONS: &[(&str, bool)] =
    &[("analytics", false), ("research", false), ("finance", true)];

/// The deployment over `organizations`: people in `data-eng` are
/// developers in `analytics`, people in `lab` admins in `research`, and
/// the UI is registered.
fn deployment_of(idp: &FakeIdp, organizations: &[(&str, bool)]) -> TestAuthBuilder {
    TestAuth::builder(organizations)
        .upstream(idp)
        .ui_client()
        .rule("person", "groups", "data-eng", "analytics", "developer")
        .rule("person", "groups", "lab", "research", "admin")
        .rule("person", "groups", "data-eng", "finance", "developer")
}

fn deployment(idp: &FakeIdp) -> TestAuthBuilder {
    deployment_of(idp, ORGANIZATIONS)
}

/// A developer in `analytics` only.
fn alice() -> UpstreamPerson {
    UpstreamPerson::new("alice-sub").claim("groups", json!(["data-eng"]))
}

/// A developer in `analytics` and an admin in `research`.
fn carol() -> UpstreamPerson {
    UpstreamPerson::new("carol-sub").claim("groups", json!(["data-eng", "lab"]))
}

/// A CI run in each active Organization, each of its own dbt project,
/// recorded through the gRPC server.
async fn record_runs(server: &TestServer, auth: &TestAuth) {
    for (organization, project) in [
        ("analytics", ANALYTICS_PROJECT),
        ("research", RESEARCH_PROJECT),
    ] {
        let token = auth
            .signer()
            .machine("ci")
            .grant(organization, Role::Developer)
            .token();
        let run = Scenario::connect(server)
            .await
            .bearer(&token)
            .org(organization);
        into_execute(
            run.submit(model(TARGET).node_state(|state| state.project_name = project.into()))
                .await,
        );
    }
    rows::decisions(server, 2).await;
}

/// A run of `project` in `organization` with no token (`none` mode).
async fn record_unauthenticated_run(server: &TestServer, organization: &str, project: &str) {
    let run = Scenario::connect(server).await.org(organization);
    into_execute(
        run.submit(model(TARGET).node_state(|state| state.project_name = project.into()))
            .await,
    );
}

/// Follow a sign-in from `path` up to the broker's redirect to the
/// console's callback, without opening it: the callback URL.
async fn callback_url(browser: &ConsoleBrowser, path: &str) -> String {
    let mut url = path.to_owned();
    for _ in 0..8 {
        let exchange = browser.request(&url, None).await;
        let location = exchange.location.expect("a redirect");
        if location.starts_with(UI_REDIRECT_URI) {
            return location;
        }
        url = location;
    }
    panic!("no redirect to the console's callback");
}

/// The `Set-Cookie` headers for `name` among `headers`.
fn set_cookie<'a>(headers: &'a [String], name: &str) -> Vec<&'a String> {
    let prefix = format!("{name}=");
    headers
        .iter()
        .filter(|header| header.starts_with(&prefix))
        .collect()
}

/// Which Organization's project `page` shows.
fn shown_projects(page: &Page) -> Vec<&'static str> {
    [ANALYTICS_PROJECT, RESEARCH_PROJECT]
        .into_iter()
        .filter(|project| page.body.contains(project))
        .collect()
}

/// Sign in through the broker by opening `path`; the page it lands on.
async fn sign_in(browser: &ConsoleBrowser, path: &str) -> Page {
    let page = browser.open(path).await;
    assert_eq!(page.status, 200, "{page:?}");
    assert_eq!(page.url, format!("{CONSOLE}{path}"), "{:?}", page.visited);
    page
}

/// Switch to `organization` with the switcher on `page`.
async fn switch(browser: &ConsoleBrowser, page: &Page, organization: &str) -> Page {
    let csrf = page.field("csrf").expect("the switcher's CSRF token");
    browser
        .submit(
            "/organization",
            &[("organization", organization), ("csrf", &csrf)],
        )
        .await
}

/// The refresh grants the Upstream IdP received (each a renewal at the
/// broker).
fn upstream_refreshes(idp: &FakeIdp) -> usize {
    idp.token_requests()
        .into_iter()
        .filter(|request| request.grant_type() == Some("refresh_token"))
        .count()
}

/// The query of `url`.
fn query(url: &str) -> BTreeMap<String, String> {
    let query = url.split_once('?').map(|(_, query)| query).unwrap_or("");
    form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect()
}

/// The attributes of `Set-Cookie: header`, lowercased, without its
/// name and value.
fn attributes(header: &str) -> Vec<String> {
    header
        .split(';')
        .skip(1)
        .map(|attribute| attribute.trim().to_ascii_lowercase())
        .collect()
}

/// A visitor without a session is sent to the broker's authorize endpoint
/// as the UI's registered client, with a fresh `state`, a `nonce` and a
/// PKCE S256 challenge; the pending sign-in waits in an `HttpOnly`,
/// `Secure`, `SameSite=Lax` host-only cookie.
#[tokio::test]
async fn a_visitor_without_a_session_is_sent_to_sign_in_at_the_broker() {
    let idp = FakeIdp::start().await;
    let auth = deployment(&idp).build();
    let server = start_server_with_broker(&auth).await;
    let ui = start_ui(&server, &auth);
    let browser = ConsoleBrowser::new(&ui, &server);

    let first = browser.request("/projects", None).await;
    assert_eq!(first.status, 303, "{first:?}");
    let location = first.location.expect("a redirect");
    assert!(location.starts_with(&format!("{ISSUER}/?")), "{location}");
    let authorize = query(&location);
    assert_eq!(authorize["response_type"], "code");
    assert_eq!(authorize["client_id"], UI_CLIENT_ID);
    assert_eq!(authorize["redirect_uri"], UI_REDIRECT_URI);
    assert!(
        authorize["scope"].split(' ').any(|scope| scope == "openid"),
        "{authorize:?}"
    );
    assert_eq!(authorize["code_challenge_method"], "S256");
    for parameter in ["state", "nonce", "code_challenge"] {
        assert!(
            authorize
                .get(parameter)
                .is_some_and(|value| value.len() >= 22),
            "{parameter}: {authorize:?}"
        );
    }

    let set_cookies = browser.set_cookies();
    assert_eq!(set_cookies.len(), 1, "{set_cookies:?}");
    let pending = &set_cookies[0];
    assert!(pending.starts_with("__Host-"), "{pending}");
    let attributes = attributes(pending);
    for expected in ["httponly", "secure", "samesite=lax", "path=/"] {
        assert!(
            attributes.iter().any(|a| a == expected),
            "{expected}: {pending}"
        );
    }
    assert!(
        !attributes.iter().any(|a| a.starts_with("domain")),
        "{pending}"
    );
    for parameter in ["state", "nonce"] {
        assert!(!pending.contains(&authorize[parameter]), "{pending}");
    }

    // A second visit starts another sign-in with fresh values.
    let second = browser.request("/", None).await;
    let again = query(&second.location.expect("a redirect"));
    for parameter in ["state", "nonce", "code_challenge"] {
        assert_ne!(again[parameter], authorize[parameter], "{parameter}");
    }
}

/// A person signs in through the broker and lands on the page they
/// opened. The session is one encrypted `__Host-` cookie, `HttpOnly`,
/// `Secure` and `SameSite=Lax`, that holds no readable token; the pending
/// sign-in cookie is cleared. Every page shows only their Organization's
/// data, with no switcher for a single Organization.
#[tokio::test]
async fn a_person_signs_in_and_sees_only_their_organization() {
    let idp = FakeIdp::start().await;
    idp.sign_in(alice());
    let auth = deployment(&idp).build();
    let server = start_server_with_broker(&auth).await;
    record_runs(&server, &auth).await;
    let ui = start_ui(&server, &auth);
    let browser = ConsoleBrowser::new(&ui, &server);

    let page = sign_in(&browser, "/projects").await;
    assert!(
        page.visited
            .iter()
            .any(|url| url.starts_with(&format!("{UI_REDIRECT_URI}?"))),
        "{:?}",
        page.visited
    );
    assert_eq!(shown_projects(&page), [ANALYTICS_PROJECT], "{}", page.body);
    assert!(page.body.contains("analytics"), "{}", page.body);
    assert!(!page.body.contains("research"), "{}", page.body);
    assert!(page.field("csrf").is_none(), "no switcher: {}", page.body);

    let headers = browser.set_cookies();
    let sessions = set_cookie(&headers, SESSION);
    assert_eq!(sessions.len(), 1, "{headers:?}");
    let session = sessions[0];
    let attributes = attributes(session);
    for expected in ["httponly", "secure", "samesite=lax", "path=/"] {
        assert!(
            attributes.iter().any(|a| a == expected),
            "{expected}: {session}"
        );
    }
    assert!(
        !attributes.iter().any(|a| a.starts_with("domain")),
        "{session}"
    );
    let value = &browser.cookies()[SESSION];
    for readable in ["eyJ", "alice", "analytics", "refresh"] {
        assert!(!value.contains(readable), "{readable} in {value}");
    }
    assert_eq!(
        browser.cookies().keys().collect::<Vec<_>>(),
        [SESSION],
        "only the session remains"
    );

    for path in ["/", "/invocations"] {
        let page = browser.open(path).await;
        assert_eq!(page.status, 200, "{path}");
        assert_eq!(page.visited.len(), 1, "{path}: no new sign-in");
        assert_eq!(shown_projects(&page), [ANALYTICS_PROJECT], "{path}");
    }
    let exchange = browser.request("/projects", None).await;
    assert_eq!(exchange.cache_control.as_deref(), Some("no-store"));
}

/// A person in two Organizations, with a different Role in each, gets a
/// switcher listing exactly those two. Switching scopes every page to the
/// other Organization, and a page id of the first one shows nothing there.
#[tokio::test]
async fn a_person_in_two_organizations_switches_between_them() {
    let idp = FakeIdp::start().await;
    idp.sign_in(carol());
    let auth = deployment(&idp).build();
    let server = start_server_with_broker(&auth).await;
    record_runs(&server, &auth).await;
    let ui = start_ui(&server, &auth);
    let browser = ConsoleBrowser::new(&ui, &server);

    let page = sign_in(&browser, "/invocations").await;
    assert_eq!(shown_projects(&page), [ANALYTICS_PROJECT], "{}", page.body);
    let options: Vec<&str> = page
        .body
        .match_indices("<option value=\"")
        .map(|(at, needle)| {
            let rest = &page.body[at + needle.len()..];
            &rest[..rest.find('"').unwrap()]
        })
        .collect();
    assert_eq!(options, ["analytics", "research"], "{}", page.body);
    assert!(page.body.contains("analytics (developer)"), "{}", page.body);
    assert!(page.body.contains("research (admin)"), "{}", page.body);
    let invocation = {
        let at = page
            .body
            .find("href=\"/invocations/")
            .expect("an invocation link");
        let rest = &page.body[at + "href=\"".len()..];
        rest[..rest.find('"').unwrap()].to_owned()
    };

    let switched = switch(&browser, &page, "research").await;
    assert_eq!(switched.status, 200, "{switched:?}");
    assert_eq!(
        switched.url,
        format!("{CONSOLE}/"),
        "{:?}",
        switched.visited
    );
    assert_eq!(shown_projects(&switched), [RESEARCH_PROJECT]);
    for path in ["/projects", "/invocations"] {
        let page = browser.open(path).await;
        assert_eq!(page.status, 200, "{path}");
        assert_eq!(shown_projects(&page), [RESEARCH_PROJECT], "{path}");
    }
    let foreign = browser.open(&invocation).await;
    assert_eq!(shown_projects(&foreign), Vec::<&str>::new(), "{invocation}");

    let back = switch(&browser, &switched, "analytics").await;
    assert_eq!(shown_projects(&back), [ANALYTICS_PROJECT]);
    let detail = browser.open(&invocation).await;
    assert_eq!(shown_projects(&detail), [ANALYTICS_PROJECT], "{invocation}");
}

/// The switcher refuses an Organization outside the session (disabled in
/// config, so only an `app` entry in the token, or unknown), a form
/// without the session's CSRF token, and a cross-site form post; the
/// selection stays as it was.
#[tokio::test]
async fn a_switch_outside_the_session_is_refused() {
    let idp = FakeIdp::start().await;
    idp.sign_in(carol());
    let auth = deployment(&idp).build();
    let server = start_server_with_broker(&auth).await;
    record_runs(&server, &auth).await;
    let ui = start_ui(&server, &auth);
    let browser = ConsoleBrowser::new(&ui, &server);
    let page = sign_in(&browser, "/").await;
    let csrf = page.field("csrf").expect("a CSRF token");

    for (organization, token) in [
        ("finance", csrf.as_str()),
        ("marketing", csrf.as_str()),
        ("research", "not-the-token"),
        ("research", ""),
    ] {
        let refused = browser
            .request(
                "/organization",
                Some(&[("organization", organization), ("csrf", token)]),
            )
            .await;
        assert_eq!(refused.status, 403, "{organization} with {token:?}");
    }
    let cross_site = browser
        .request_from(
            "/organization",
            Some(&[("organization", "research"), ("csrf", &csrf)]),
            "cross-site",
        )
        .await;
    assert_eq!(cross_site.status, 403);
    let page = browser.open("/projects").await;
    assert_eq!(shown_projects(&page), [ANALYTICS_PROJECT]);
    assert!(!page.body.contains("finance"), "{}", page.body);
}

/// The console renews at the broker (the refresh grant, with a fresh
/// client assertion) once less than five minutes of the person token are
/// left, so a person keeps using it past the first token's lifetime. The
/// broker re-checks the person at the Upstream IdP each time.
#[tokio::test]
async fn the_console_renews_at_the_broker_within_the_person_token_lifetime() {
    let idp = FakeIdp::start().await;
    idp.sign_in(alice());
    let auth = deployment(&idp).build();
    let server = start_server_with_broker(&auth).await;
    idp.follow(&server.broker_clock);
    record_runs(&server, &auth).await;
    let ui = start_ui(&server, &auth);
    let browser = ConsoleBrowser::new(&ui, &server);
    sign_in(&browser, "/projects").await;
    let minutes = |n: u64| Duration::from_secs(n * 60);

    server.broker_clock.advance(minutes(50));
    let page = browser.open("/projects").await;
    assert_eq!((page.status, page.visited.len()), (200, 1));
    assert_eq!(upstream_refreshes(&idp), 0, "ten minutes left: no renewal");

    for renewal in 1..=2 {
        let before = browser.set_cookies().len();
        server
            .broker_clock
            .advance(minutes(if renewal == 1 { 6 } else { 56 }));
        let page = browser.open("/projects").await;
        assert_eq!(page.status, 200, "renewal {renewal}: {page:?}");
        assert_eq!(page.visited.len(), 1, "renewal {renewal}: no sign-in");
        assert_eq!(shown_projects(&page), [ANALYTICS_PROJECT]);
        assert_eq!(upstream_refreshes(&idp), renewal);
        let renewed = browser.set_cookies()[before..].to_vec();
        assert_eq!(set_cookie(&renewed, SESSION).len(), 1, "{renewed:?}");
    }
}

/// When renewal fails (here the person was disabled at the Upstream IdP),
/// the console ends its session: the cookie is cleared and the browser is
/// sent to sign in again.
#[tokio::test]
async fn a_failed_renewal_ends_the_console_session() {
    let idp = FakeIdp::start().await;
    idp.sign_in(alice());
    let auth = deployment(&idp).build();
    let server = start_server_with_broker(&auth).await;
    idp.follow(&server.broker_clock);
    let ui = start_ui(&server, &auth);
    let browser = ConsoleBrowser::new(&ui, &server);
    sign_in(&browser, "/projects").await;

    idp.disable("alice-sub");
    server.broker_clock.advance(Duration::from_secs(56 * 60));
    let ended = browser.request("/projects", None).await;
    assert_eq!(ended.status, 303, "{ended:?}");
    let location = ended.location.expect("a redirect");
    assert!(location.starts_with(&format!("{ISSUER}/?")), "{location}");
    assert_eq!(upstream_refreshes(&idp), 1);
    assert!(
        !browser.cookies().contains_key(SESSION),
        "{:?}",
        browser.set_cookies()
    );
    let removal = browser.set_cookies();
    let removal = set_cookie(&removal, SESSION);
    assert!(
        removal
            .last()
            .is_some_and(|header| attributes(header).contains(&"max-age=0".into())),
        "{removal:?}"
    );
}

/// Revoking the person's broker sessions (the admin command) ends their
/// console session within the 30 s session-status bound.
#[tokio::test]
async fn a_revoked_session_ends_the_console_session_within_30_seconds() {
    let idp = FakeIdp::start().await;
    idp.sign_in(alice());
    let auth = deployment(&idp).build();
    let server = start_server_with_broker(&auth).await;
    let ui = start_ui(&server, &auth);
    let browser = ConsoleBrowser::new(&ui, &server);
    sign_in(&browser, "/projects").await;

    let now = server.broker_clock.broker_clock().now();
    let revoked = dbt_state_broker::revoke_sessions(
        server.state.store.pool(),
        &idp.issuer(),
        "alice-sub",
        now,
    )
    .await
    .expect("revoke");
    assert_eq!(revoked.sessions, 1);
    server
        .broker_clock
        .advance(dbt_state_broker::SESSION_STATUS_MAX_AGE);
    let ended = browser.request("/projects", None).await;
    assert_eq!(ended.status, 303, "{ended:?}");
    assert!(ended
        .location
        .is_some_and(|location| location.starts_with(&format!("{ISSUER}/?"))),);
    assert!(!browser.cookies().contains_key(SESSION));
}

/// An Organization disabled in config is refused once the console restarts
/// with that config, even though the person's token still has it active:
/// it is not shown or selectable, and the console falls back to another.
/// A person left with no Organization the console shows gets 403.
#[tokio::test]
async fn an_organization_disabled_in_config_is_refused_after_the_console_restarts() {
    let idp = FakeIdp::start().await;
    idp.sign_in(carol());
    let auth = deployment(&idp).build();
    let server = start_server_with_broker(&auth).await;
    record_runs(&server, &auth).await;
    let ui = start_ui(&server, &auth);
    let browser = ConsoleBrowser::new(&ui, &server);
    let page = sign_in(&browser, "/").await;
    let csrf = page.field("csrf").expect("a switcher");
    let page = switch(&browser, &page, "research").await;
    assert_eq!(shown_projects(&page), [RESEARCH_PROJECT]);

    let restarted = deployment_of(
        &idp,
        &[("analytics", false), ("research", true), ("finance", true)],
    )
    .build();
    let ui = start_ui(&server, &restarted);
    let browser = browser.against(&ui);
    let page = browser.open("/projects").await;
    assert_eq!((page.status, page.visited.len()), (200, 1), "{page:?}");
    assert_eq!(shown_projects(&page), [ANALYTICS_PROJECT]);
    assert!(!page.body.contains("research"), "{}", page.body);
    let refused = browser
        .request(
            "/organization",
            Some(&[("organization", "research"), ("csrf", &csrf)]),
        )
        .await;
    assert_eq!(refused.status, 403);

    let none_left = deployment_of(
        &idp,
        &[("analytics", true), ("research", true), ("finance", true)],
    )
    .build();
    let ui = start_ui(&server, &none_left);
    let browser = browser.against(&ui);
    let refused = browser.request("/projects", None).await;
    assert_eq!(refused.status, 403, "{refused:?}");
    assert!(!refused.body.contains(ANALYTICS_PROJECT));
}

/// A person no person rule maps is denied at the broker, and the console's
/// callback says so with fixed text and starts no session.
#[tokio::test]
async fn a_person_without_access_is_told_so_and_gets_no_session() {
    let idp = FakeIdp::start().await;
    idp.sign_in(UpstreamPerson::new("mallory-sub").claim("groups", json!(["sales"])));
    let auth = deployment(&idp).build();
    let server = start_server_with_broker(&auth).await;
    let ui = start_ui(&server, &auth);
    let browser = ConsoleBrowser::new(&ui, &server);

    let page = browser.open("/").await;
    assert_eq!(page.status, 403, "{page:?}");
    assert!(page.url.starts_with(UI_REDIRECT_URI), "{}", page.url);
    assert!(
        page.body.contains("You have no access to dbt State"),
        "{}",
        page.body
    );
    assert!(browser.cookies().is_empty(), "{:?}", browser.set_cookies());
}

/// A callback completes only in the browser that started its sign-in: in
/// another browser (no pending sign-in) it is refused before the code is
/// used, so nobody can sign a victim's browser in as themselves. The
/// browser that started it then completes it.
#[tokio::test]
async fn a_callback_completes_only_in_the_browser_that_started_the_sign_in() {
    let idp = FakeIdp::start().await;
    idp.sign_in(alice());
    let auth = deployment(&idp).build();
    let server = start_server_with_broker(&auth).await;
    record_runs(&server, &auth).await;
    let ui = start_ui(&server, &auth);
    let started = ConsoleBrowser::new(&ui, &server);
    let callback = callback_url(&started, "/projects").await;

    // A browser with no sign-in under way, and one with its own.
    let fresh = ConsoleBrowser::new(&ui, &server);
    let signing_in = ConsoleBrowser::new(&ui, &server);
    callback_url(&signing_in, "/").await;
    for other in [fresh, signing_in] {
        let refused = other.open(&callback).await;
        assert_eq!(refused.status, 400, "{refused:?}");
        assert!(refused.body.contains("expired"), "{}", refused.body);
        assert!(!other.cookies().contains_key(SESSION));
    }

    let page = started.open(&callback).await;
    assert_eq!(page.status, 200, "{page:?}");
    assert_eq!(page.url, format!("{CONSOLE}/projects"));
    assert_eq!(shown_projects(&page), [ANALYTICS_PROJECT]);
}

/// A console whose auth config has no `[signing]` (it never holds the
/// broker's private keys) checks the broker's tokens with the keys the
/// broker publishes.
#[tokio::test]
async fn a_console_without_signing_keys_uses_the_broker_s_published_keys() {
    let idp = FakeIdp::start().await;
    idp.sign_in(alice());
    let auth = deployment(&idp).build();
    let server = start_server_with_broker(&auth).await;
    record_runs(&server, &auth).await;
    let console_auth = TestAuth::builder(ORGANIZATIONS)
        .ui_client()
        .without_signing()
        .build();
    assert!(console_auth.config().verification_keys().is_none());
    let ui = start_ui(&server, &console_auth);
    let browser = ConsoleBrowser::new(&ui, &server);

    let page = sign_in(&browser, "/projects").await;
    assert_eq!(shown_projects(&page), [ANALYTICS_PROJECT]);
}

/// In `none` mode the console is as before: no sign-in, no cookies, every
/// page scoped to `DBT_STATE_UI_ORG_ID` (default `local`), and no sign-in
/// or switcher routes.
#[tokio::test]
async fn in_none_mode_the_console_shows_one_organization_without_sign_in() {
    let server = start_server().await;
    record_unauthenticated_run(&server, "analytics", ANALYTICS_PROJECT).await;
    record_unauthenticated_run(&server, "local", RESEARCH_PROJECT).await;
    rows::decisions(&server, 2).await;

    let configured = start_ui_with(&server, |_| {
        vec![("DBT_STATE_UI_ORG_ID".into(), "analytics".into())]
    });
    let browser = ConsoleBrowser::new(&configured, &server);
    for path in ["/", "/projects", "/invocations"] {
        let page = browser.open(path).await;
        assert_eq!((page.status, page.visited.len()), (200, 1), "{path}");
        assert_eq!(shown_projects(&page), [ANALYTICS_PROJECT], "{path}");
    }
    for path in ["/auth/login", "/auth/callback?code=c&state=s"] {
        assert_eq!(browser.request(path, None).await.status, 404, "{path}");
    }
    let switch = browser
        .request(
            "/organization",
            Some(&[("organization", "local"), ("csrf", "")]),
        )
        .await;
    assert_eq!(switch.status, 404);
    assert!(
        browser.set_cookies().is_empty(),
        "{:?}",
        browser.set_cookies()
    );

    let default = start_ui_with(&server, |_| Vec::new());
    let page = ConsoleBrowser::new(&default, &server)
        .open("/projects")
        .await;
    assert_eq!(shown_projects(&page), [RESEARCH_PROJECT]);
}

/// `count` active Organizations `org-000`, `org-001`, …, each granting
/// people in `data-eng` the developer Role, and the UI registered.
fn many_organizations(idp: &FakeIdp, count: usize) -> TestAuth {
    let ids: Vec<String> = (0..count).map(|n| format!("org-{n:03}")).collect();
    let declared: Vec<(&str, bool)> = ids.iter().map(|id| (id.as_str(), false)).collect();
    ids.iter()
        .fold(
            TestAuth::builder(&declared).upstream(idp).ui_client(),
            |builder, id| builder.rule("person", "groups", "data-eng", id, "developer"),
        )
        .build()
}

/// Every cookie the console sets fits a browser's 4096-byte limit on a
/// cookie's name and value: a person in 60 Organizations signs in and can
/// pick any of them.
#[tokio::test]
async fn a_person_in_many_organizations_gets_a_session_cookie_a_browser_keeps() {
    let idp = FakeIdp::start().await;
    idp.sign_in(alice());
    let auth = many_organizations(&idp, 60);
    let server = start_server_with_broker(&auth).await;
    let ui = start_ui(&server, &auth);
    let browser = ConsoleBrowser::new(&ui, &server);

    let page = sign_in(&browser, "/").await;
    assert_eq!(page.body.matches("<option value=\"").count(), 60);
    for header in browser.set_cookies() {
        let pair = header.split(';').next().unwrap();
        assert!(pair.len() <= 4096, "{} bytes: {header}", pair.len());
    }
    let page = switch(&browser, &page, "org-059").await;
    assert_eq!(page.status, 200, "{page:?}");
    assert!(
        page.body.contains("value=\"org-059\" selected"),
        "{}",
        page.body
    );
}

/// A person in more Organizations than one session cookie can hold is told
/// so at the callback and gets no session, rather than a cookie the
/// browser drops and an endless sign-in loop.
#[tokio::test]
async fn a_person_in_too_many_organizations_for_one_cookie_is_told_so() {
    let idp = FakeIdp::start().await;
    idp.sign_in(alice());
    let auth = many_organizations(&idp, 400);
    let server = start_server_with_broker(&auth).await;
    let ui = start_ui(&server, &auth);
    let browser = ConsoleBrowser::new(&ui, &server);

    let page = browser.open("/").await;
    assert_eq!(page.status, 403, "{page:?}");
    assert!(page.url.starts_with(UI_REDIRECT_URI), "{}", page.url);
    assert!(
        page.body.contains("too many Organizations"),
        "{}",
        page.body
    );
    assert!(!browser.cookies().contains_key(SESSION));
}
