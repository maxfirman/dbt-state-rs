//! Loading the TOML auth config: what an operator may declare, and what
//! config load refuses before the server starts.

use std::path::{Path, PathBuf};
use std::time::Duration;

use dbt_state_auth::{AuthConfig, OrganizationId, PrincipalKind, Role, UpstreamClaims};
use proptest::prelude::*;

fn testdata(file: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
        .join(file)
}

/// A TOML string literal of `value`.
fn toml_string(value: &str) -> String {
    toml::Value::String(value.to_owned()).to_string()
}

/// A valid config whose `[[organizations]]` and `[tokens]` tables are
/// `organizations` and `tokens`.
fn config_text(organizations: &str, tokens: &str) -> String {
    format!(
        r#"
[broker]
issuer = "https://broker.example.test"

[api]
audience = "dbt-state-api"

{tokens}

[signing]
active_key_id = "test-key-1"

[[signing.keys]]
id = "test-key-1"
private_key_file = {key1}

[[signing.keys]]
id = "test-key-2"
private_key_file = {key2}

{organizations}
"#,
        key1 = toml_string(testdata("signing-key-1.pem").to_str().unwrap()),
        key2 = toml_string(testdata("signing-key-2.pem").to_str().unwrap()),
    )
}

fn load(text: &str) -> Result<AuthConfig, String> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("auth.toml");
    std::fs::write(&path, text).unwrap();
    AuthConfig::load(&path).map_err(|e| e.to_string())
}

fn org(id: &str) -> OrganizationId {
    OrganizationId::new(id).unwrap()
}

#[test]
fn a_complete_config_loads_with_default_lifetimes() {
    let config = load(&config_text(
        r#"
[[organizations]]
id = "analytics"

[[organizations]]
id = "finance"
disabled = true
"#,
        "",
    ))
    .expect("valid config");
    assert!(config.organizations().is_active(&org("analytics")));
    assert!(config.organizations().is_disabled(&org("finance")));
    assert!(!config.organizations().is_active(&org("finance")));
    assert!(!config.organizations().is_active(&org("unknown")));
    let lifetimes = config.token_lifetimes();
    assert_eq!(lifetimes.person, Duration::from_secs(60 * 60));
    assert_eq!(lifetimes.machine, Duration::from_secs(15 * 60));
}

/// Key files may be PKCS#8 or PKCS#1 PEM, and relative paths resolve from
/// the config file's directory.
#[test]
fn key_files_resolve_relative_to_the_config_and_accept_pkcs1() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("keys")).unwrap();
    std::fs::copy(
        testdata("signing-key-pkcs1.pem"),
        dir.path().join("keys/k.pem"),
    )
    .unwrap();
    let path = dir.path().join("auth.toml");
    std::fs::write(
        &path,
        r#"
[broker]
issuer = "https://broker.example.test"
[api]
audience = "dbt-state-api"
[signing]
active_key_id = "k"
[[signing.keys]]
id = "k"
private_key_file = "keys/k.pem"
[[organizations]]
id = "analytics"
"#,
    )
    .unwrap();
    AuthConfig::load(&path).expect("relative PKCS#1 key loads");
}

/// `examples/auth/auth.toml` loads once its key files are mounted.
#[test]
fn the_example_config_loads() {
    let example = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/auth/auth.toml");
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("keys")).unwrap();
    for (file, key) in [
        ("signing-2026-11.pem", "signing-key-1.pem"),
        ("signing-2026-05.pem", "signing-key-2.pem"),
    ] {
        std::fs::copy(testdata(key), dir.path().join("keys").join(file)).unwrap();
    }
    let path = dir.path().join("auth.toml");
    std::fs::copy(example, &path).unwrap();
    let config = AuthConfig::load(&path).expect("the example config loads");
    assert!(config.organizations().is_active(&org("analytics")));
    assert!(config.organizations().is_disabled(&org("finance")));
}

/// Each refusal names what is wrong.
#[test]
fn invalid_configs_are_refused() {
    let one_org = "[[organizations]]\nid = \"analytics\"\n";
    let cases: Vec<(String, &str)> = vec![
        (
            config_text(
                "[[organizations]]\nid = \"a\"\n[[organizations]]\nid = \"a\"\n",
                "",
            ),
            "more than once",
        ),
        (
            config_text(one_org, "").replace(
                "active_key_id = \"test-key-1\"",
                "active_key_id = \"missing\"",
            ),
            "active_key_id",
        ),
        (
            config_text(one_org, "").replace("id = \"test-key-2\"", "id = \"test-key-1\""),
            "more than once",
        ),
        (
            config_text(one_org, "").replace("signing-key-2.pem", "no-such-key.pem"),
            "no-such-key.pem",
        ),
        (
            config_text(one_org, "").replace("signing-key-2.pem", "README.md"),
            "README.md",
        ),
        (
            config_text(one_org, "")
                .replace("issuer = \"https://broker.example.test\"", "issuer = \"\""),
            "issuer",
        ),
        (
            config_text(one_org, "").replace(
                "issuer = \"https://broker.example.test\"",
                "issuer = \"broker\"",
            ),
            "issuer",
        ),
        (
            config_text(one_org, "").replace("audience = \"dbt-state-api\"", "audience = \"\""),
            "audience",
        ),
        (
            config_text("[[organizations]]\nid = \"a\"\ndisable = true\n", ""),
            "disable",
        ),
        (config_text(one_org, "").replace("[api]", "[apis]"), "apis"),
    ];
    for (text, needle) in cases {
        let error = load(&text).expect_err(needle);
        assert!(
            error.contains(needle),
            "{error:?} should mention {needle:?}"
        );
    }
}

/// A key file's contents never appear in a refusal.
#[test]
fn refusals_never_echo_key_material() {
    let dir = tempfile::tempdir().unwrap();
    let key = dir.path().join("bad.pem");
    let pem = std::fs::read_to_string(testdata("signing-key-1.pem")).unwrap();
    // A truncated PEM: the right header, a broken body.
    let broken = &pem[..pem.len() / 2];
    std::fs::write(&key, broken).unwrap();
    let text = config_text("[[organizations]]\nid = \"a\"\n", "").replace(
        &toml_string(testdata("signing-key-2.pem").to_str().unwrap()),
        &toml_string(key.to_str().unwrap()),
    );
    let error = load(&text).expect_err("a broken key file");
    let body_line = broken.lines().nth(1).unwrap();
    assert!(!error.contains(body_line), "{error}");
}

proptest! {
    /// Config load refuses an Organization id that is empty, contains `:` or
    /// whitespace, or is `*`, and accepts any other visible-ASCII id.
    #[test]
    fn organization_ids_are_validated_at_load(id in prop_oneof![
        "[!-~]{0,6}",
        "[a-z]{1,3}[ \t:][a-z]{0,3}",
        "[ -~]{0,4}",
        "\\PC{1,3}",
        Just("*".to_owned()),
        Just(String::new()),
    ]) {
        let text = config_text(&format!("[[organizations]]\nid = {}\n", toml_string(&id)), "");
        let valid = !id.is_empty()
            && id != "*"
            && id.chars().all(|c| c.is_ascii_graphic() && c != ':');
        let result = load(&text);
        prop_assert_eq!(result.is_ok(), valid, "{:?}: {:?}", id, result.err());
        if !valid {
            prop_assert!(result.unwrap_err().contains("Organization id"));
        }
    }

    /// Person token lifetimes are 15 to 240 minutes, machine ones 15 to 60.
    #[test]
    fn token_lifetimes_are_bounded(person in 0u32..300, machine in 0u32..100) {
        let tokens = format!(
            "[tokens]\nperson_lifetime_minutes = {person}\nmachine_lifetime_minutes = {machine}\n"
        );
        let text = config_text("[[organizations]]\nid = \"a\"\n", &tokens);
        let valid = (15..=240).contains(&person) && (15..=60).contains(&machine);
        let result = load(&text);
        prop_assert_eq!(result.is_ok(), valid, "{:?}", result.as_ref().err());
        match result {
            Ok(config) => {
                prop_assert_eq!(config.token_lifetimes().person, Duration::from_secs(u64::from(person) * 60));
                prop_assert_eq!(config.token_lifetimes().machine, Duration::from_secs(u64::from(machine) * 60));
            }
            Err(error) => prop_assert!(error.contains("lifetime"), "{}", error),
        }
    }
}

/// The Upstream IdP and the machine mapping rules the Token broker applies.
const UPSTREAM: &str = r#"
[upstream]
issuer = "https://login.example.test/tenant/v2.0"
api_audience = "api://dbt-state"
client_credentials_scope = "api://dbt-state/.default"

[[rules]]
kind = "machine"
claim = "roles"
value = "dbt-state.ci"
organization = "analytics"
role = "developer"

[[rules]]
kind = "person"
claim = "groups"
value = "data-platform"
organization = "analytics"
role = "admin"
"#;

#[test]
fn the_upstream_idp_and_mapping_rules_load() {
    let config = load(&config_text(
        &format!("[[organizations]]\nid = \"analytics\"\n{UPSTREAM}"),
        "",
    ))
    .expect("valid config");
    let upstream = config.upstream().expect("an Upstream IdP");
    assert_eq!(upstream.issuer(), "https://login.example.test/tenant/v2.0");
    assert_eq!(upstream.api_audience(), "api://dbt-state");
    assert_eq!(
        upstream.client_credentials_scope(),
        Some("api://dbt-state/.default")
    );

    let mut claims = serde_json::Map::new();
    claims.insert("roles".into(), serde_json::json!(["dbt-state.ci"]));
    claims.insert("groups".into(), serde_json::json!(["data-platform"]));
    let claims = UpstreamClaims::new(claims);
    let organizations = config.organizations();
    let machine = config
        .mapping_rules()
        .scope(PrincipalKind::Machine, &claims, organizations);
    assert_eq!(
        machine.active_role(&org("analytics")),
        Some(Role::Developer)
    );
    let person = config
        .mapping_rules()
        .scope(PrincipalKind::Person, &claims, organizations);
    assert_eq!(person.active_role(&org("analytics")), Some(Role::Admin));
}

/// A gRPC server that verifies tokens with the Token broker's published keys
/// needs no `[signing]` table, and issues nothing.
#[test]
fn signing_keys_are_optional() {
    let text = config_text("[[organizations]]\nid = \"analytics\"\n", "");
    let start = text.find("[signing]").unwrap();
    let end = text.find("[[organizations]]").unwrap();
    let mut text = text.clone();
    text.replace_range(start..end, "");
    let config = load(&text).expect("a config without signing keys");
    assert!(config.verification_keys().is_none());
    assert!(config.token_issuer().is_none());
    assert!(config.upstream().is_none());
}

/// Without `[signing]`, the gRPC server fetches the Token broker's keys, so
/// where it fetches them must use TLS (plain http only on loopback).
#[test]
fn fetched_keys_need_a_secure_location() {
    let text = config_text("[[organizations]]\nid = \"analytics\"\n", "");
    let start = text.find("[signing]").unwrap();
    let end = text.find("[[organizations]]").unwrap();
    let mut unsigned = text.clone();
    unsigned.replace_range(start..end, "");
    let issuer = "issuer = \"https://broker.example.test\"";
    let cases = [
        (unsigned.replace(issuer, "issuer = \"http://broker.example.test\""), "broker.issuer"),
        (
            unsigned.replace(
                issuer,
                "issuer = \"https://broker.example.test\"\njwks_url = \"http://broker.internal/jwks\"",
            ),
            "broker.jwks_url",
        ),
    ];
    for (text, needle) in cases {
        let error = load(&text).expect_err(needle);
        assert!(
            error.contains(needle),
            "{error:?} should mention {needle:?}"
        );
    }
    for allowed in [
        unsigned.replace(issuer, "issuer = \"http://127.0.0.1:8080\""),
        unsigned.replace(
            issuer,
            "issuer = \"http://broker.example.test\"\njwks_url = \"https://broker.internal/jwks\"",
        ),
        // With [signing] nothing is fetched, so the issuer is only a name.
        text.replace(issuer, "issuer = \"http://broker.example.test\""),
    ] {
        load(&allowed).unwrap_or_else(|error| panic!("{error}\n{allowed}"));
    }
}

/// Each refusal of an `[upstream]` or `[[rules]]` mistake names it.
#[test]
fn invalid_upstream_and_rules_are_refused() {
    let with = |upstream: &str| {
        config_text(
            &format!(
                "[[organizations]]\nid = \"analytics\"\n[[organizations]]\nid = \"finance\"\ndisabled = true\n{upstream}"
            ),
            "",
        )
    };
    let cases: Vec<(String, &str)> = vec![
        (with(&UPSTREAM.replace("kind = \"machine\"\n", "")), "kind"),
        (
            with(&UPSTREAM.replace("kind = \"machine\"", "kind = \"service\"")),
            "service",
        ),
        (
            with(&UPSTREAM.replace("role = \"developer\"", "role = \"owner\"")),
            "owner",
        ),
        (
            with(&UPSTREAM.replace(
                "organization = \"analytics\"\nrole = \"developer\"",
                "organization = \"nowhere\"\nrole = \"developer\"",
            )),
            "nowhere",
        ),
        (
            with(&UPSTREAM.replace(
                "organization = \"analytics\"\nrole = \"developer\"",
                "organization = \"*\"\nrole = \"developer\"",
            )),
            "Organization id",
        ),
        (
            with(&UPSTREAM.replace("claim = \"roles\"", "claim = \"\"")),
            "claim",
        ),
        (
            with(&UPSTREAM.replace("value = \"dbt-state.ci\"", "value = \"\"")),
            "value",
        ),
        (
            with(&UPSTREAM.replace("value = \"dbt-state.ci\"", "valu = \"x\"")),
            "valu",
        ),
        (
            with(&UPSTREAM.replace(
                "https://login.example.test/tenant/v2.0",
                "http://login.example.test",
            )),
            "upstream.issuer",
        ),
        (
            with(&UPSTREAM.replace("api_audience = \"api://dbt-state\"", "api_audience = \"\"")),
            "api_audience",
        ),
        (
            with(&UPSTREAM.replace("api_audience = \"api://dbt-state\"\n", "")),
            "api_audience",
        ),
    ];
    for (text, needle) in cases {
        let error = load(&text).expect_err(needle);
        assert!(
            error.contains(needle),
            "{error:?} should mention {needle:?}"
        );
    }
    // A disabled Organization may be mapped (its Principals are told it is
    // disabled), and a loopback Upstream IdP may use plain http.
    load(&with(
        &UPSTREAM
            .replace(
                "organization = \"analytics\"\nrole = \"developer\"",
                "organization = \"finance\"\nrole = \"developer\"",
            )
            .replace(
                "https://login.example.test/tenant/v2.0",
                "http://127.0.0.1:8081",
            ),
    ))
    .expect("a disabled Organization and a loopback http issuer");
}
