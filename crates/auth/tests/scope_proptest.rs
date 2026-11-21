//! The `scope` claim the Token broker issues, checked against ports of both
//! pinned dbt clients' scope rules (`client_scope/`).
//!
//! An issued scope must mean the same thing to each client as it does to the
//! gRPC server: the same active Organizations with their Roles, the same
//! disabled Organizations, never the `*` wildcard, and none of the inputs
//! either client refuses.

mod client_scope;

use std::collections::{BTreeMap, BTreeSet};

use client_scope::{fusion, python, OrgChoice};
use dbt_state_auth::{Organization, OrganizationId, Organizations, Role, Scope};
use proptest::prelude::*;

// ---------------------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------------------

/// Organization ids an operator may choose: visible ASCII without `:`,
/// never `*`, including the punctuation the clients split or match on.
fn organization_id() -> impl Strategy<Value = OrganizationId> {
    prop_oneof!["[a-z][a-z0-9_-]{0,11}", "act_[0-9]{1,6}", "[!-9;-~]{1,8}",]
        .prop_filter("not the wildcard", |id| id != "*")
        .prop_map(|id| OrganizationId::new(&id).expect("a valid Organization id"))
}

fn role() -> impl Strategy<Value = Role> {
    prop_oneof![Just(Role::Developer), Just(Role::Admin)]
}

/// Grants of a Role in an Organization.
type Grants = Vec<(OrganizationId, Role)>;
/// Declared Organizations as `(id, disabled)`.
type Declared = Vec<(OrganizationId, bool)>;

/// Declared Organizations, some disabled, and grants that name declared,
/// repeated and undeclared Organizations.
fn grants_and_organizations() -> impl Strategy<Value = (Grants, Declared)> {
    let declared = prop::collection::btree_map(organization_id(), any::<bool>(), 0..6);
    (
        declared,
        prop::collection::vec((organization_id(), role()), 0..4),
    )
        .prop_flat_map(|(declared, undeclared)| {
            let declared: Declared = declared.into_iter().collect();
            let ids: Vec<OrganizationId> = declared.iter().map(|(id, _)| id.clone()).collect();
            let picked = if ids.is_empty() {
                Just(Vec::new()).boxed()
            } else {
                prop::collection::vec((prop::sample::select(ids), role()), 0..8).boxed()
            };
            (picked, Just(undeclared), Just(declared)).prop_map(
                |(mut grants, undeclared, declared)| {
                    let known: BTreeSet<_> = declared.iter().map(|(id, _)| id.clone()).collect();
                    grants.extend(undeclared.into_iter().filter(|(id, _)| !known.contains(id)));
                    (grants, declared)
                },
            )
        })
}

fn organizations(declared: &[(OrganizationId, bool)]) -> Organizations {
    Organizations::new(declared.iter().map(|(id, disabled)| Organization {
        id: id.clone(),
        disabled: *disabled,
    }))
    .expect("distinct Organization ids")
}

/// Pieces of a `scope` claim: well-formed entries, and the malformed or
/// foreign ones the clients treat specially.
fn scope_piece() -> impl Strategy<Value = String> {
    let org = prop_oneof![
        "[a-z][a-z0-9_-]{0,5}",
        "[!-9;-~]{1,4}",
        Just("*".to_owned()),
        Just(String::new()),
    ];
    let kind = prop_oneof![Just("org"), Just("app"), Just("orgs"), Just("apps")];
    let level = prop_oneof![
        Just("developer".to_owned()),
        Just("admin".to_owned()),
        Just("owner".to_owned()),
        Just(String::new()),
    ];
    let well_formed = (
        prop_oneof![Just("org"), Just("app")],
        "[a-z][a-z0-9_-]{0,3}",
        prop_oneof![Just("developer"), Just("admin")],
    )
        .prop_map(|(kind, org, level)| format!("runcache:scope:{kind}:{org}:{level}"));
    prop_oneof![
        12 => well_formed,
        4 => (kind, org.clone(), level.clone())
            .prop_map(|(kind, org, level)| format!("runcache:scope:{kind}:{org}:{level}")),
        1 => (org.clone(), level.clone(), "[a-z:]{0,4}")
            .prop_map(|(org, level, tail)| format!("runcache:scope:org:{org}:{level}:{tail}")),
        1 => org.clone().prop_map(|org| format!("runcache:scope:org:{org}")),
        1 => (org.clone(), level.clone())
            .prop_map(|(org, level)| format!("conway:scope:org:{org}:{level}")),
        1 => Just("runcache:scope:orgs".to_owned()),
        1 => Just("runcache:scope:org:".to_owned()),
        1 => Just("openid".to_owned()),
        1 => "[a-z:]{0,12}",
    ]
}

/// A `scope` claim joined with single spaces, runs of spaces, tabs or
/// newlines.
fn scope_claim() -> impl Strategy<Value = String> {
    let separator = prop_oneof![
        24 => Just(" "),
        1 => Just("  "),
        1 => Just("\t"),
        1 => Just(" \n"),
    ];
    prop::collection::vec((scope_piece(), separator), 0..6).prop_map(|pieces| {
        let mut claim = String::new();
        for (i, (piece, separator)) in pieces.iter().enumerate() {
            if i > 0 {
                claim.push_str(separator);
            }
            claim.push_str(piece);
        }
        claim
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn ids<'a>(ids: impl IntoIterator<Item = &'a OrganizationId>) -> BTreeSet<String> {
    ids.into_iter().map(|id| id.as_str().to_owned()).collect()
}

fn set(ids: Vec<String>) -> BTreeSet<String> {
    ids.into_iter().collect()
}

/// What a client should conclude for an issued scope, from the grants alone.
fn expected_choice(
    configured: Option<&str>,
    active: &BTreeMap<OrganizationId, Role>,
    disabled: &BTreeSet<OrganizationId>,
) -> OrgChoice {
    match configured {
        Some(org) if active.keys().any(|id| id.as_str() == org) => OrgChoice::Use(org.to_owned()),
        Some(org) if disabled.iter().any(|id| id.as_str() == org) => {
            OrgChoice::Disabled(org.to_owned())
        }
        Some(_) => OrgChoice::NoAccess,
        None => match (active.len(), disabled.len()) {
            (1, _) => OrgChoice::Use(active.keys().next().unwrap().as_str().to_owned()),
            (0, 1) => OrgChoice::Disabled(disabled.iter().next().unwrap().as_str().to_owned()),
            (0, _) => OrgChoice::NoOrganization,
            _ => OrgChoice::Ambiguous,
        },
    }
}

// ---------------------------------------------------------------------------
// Properties
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// An issued scope grants each declared active Organization its highest
    /// granted Role as an `org` entry, lists each granted disabled
    /// Organization only as an `app` entry, omits undeclared Organizations
    /// and never contains the wildcard. Both pinned clients parse it
    /// without error and reach the same conclusions, and the server reads
    /// back exactly what was issued.
    #[test]
    fn issued_scope_means_the_same_to_both_clients(
        (grants, declared) in grants_and_organizations(),
        probe in organization_id(),
    ) {
        let organizations = organizations(&declared);
        let disabled_ids: BTreeSet<OrganizationId> = declared
            .iter()
            .filter(|(_, disabled)| *disabled)
            .map(|(id, _)| id.clone())
            .collect();
        let declared_ids: BTreeSet<OrganizationId> =
            declared.iter().map(|(id, _)| id.clone()).collect();

        let mut active: BTreeMap<OrganizationId, Role> = BTreeMap::new();
        let mut disabled: BTreeSet<OrganizationId> = BTreeSet::new();
        for (id, role) in &grants {
            if !declared_ids.contains(id) {
                continue;
            }
            if disabled_ids.contains(id) {
                disabled.insert(id.clone());
            } else {
                let best = active.entry(id.clone()).or_insert(*role);
                *best = (*best).max(*role);
            }
        }

        let scope = Scope::issue(grants.iter().cloned(), &organizations);
        let claim = scope.to_string();

        // The server reads back what it issued.
        let parsed = Scope::parse(&claim).expect("an issued scope parses");
        prop_assert_eq!(&parsed, &scope);
        prop_assert_eq!(parsed.active().collect::<BTreeMap<_, _>>(),
            active.iter().map(|(id, role)| (id, *role)).collect::<BTreeMap<_, _>>());
        prop_assert_eq!(ids(parsed.disabled()), ids(&disabled));

        // Python client.
        let py = python::Scope::from_string(&claim).expect("Python parses an issued scope");
        prop_assert_eq!(set(py.org_ids()), ids(active.keys()));
        prop_assert_eq!(set(py.disabled_org_ids()), ids(&disabled));
        prop_assert!(!py.org_ids().iter().any(|id| id == "*"));
        let levels: BTreeMap<String, String> = py.org_levels().into_iter().collect();
        let expected_levels: BTreeMap<String, String> = active
            .iter()
            .map(|(id, role)| (id.as_str().to_owned(), role.as_str().to_owned()))
            .collect();
        prop_assert_eq!(levels, expected_levels);

        // Fusion client.
        let fu = fusion::Scope::from_string(&claim).expect("Fusion parses an issued scope");
        prop_assert_eq!(set(fu.org_ids()), ids(active.keys()));
        prop_assert_eq!(set(fu.disabled_org_ids()), ids(&disabled));

        // Disabled Organizations appear only as `app` entries; the wildcard
        // never appears.
        let entries: Vec<&str> = if claim.is_empty() { Vec::new() } else { claim.split(' ').collect() };
        for entry in entries {
            let parts: Vec<&str> = entry.split(':').collect();
            prop_assert_eq!(parts.len(), 5, "entry {:?}", entry);
            prop_assert!(parts[3] != "*");
            let id = OrganizationId::new(parts[3]).unwrap();
            if parts[2] == "org" {
                prop_assert!(!disabled_ids.contains(&id), "disabled {:?} as an org entry", id);
            }
        }

        // Both clients choose the same Organization the grants imply.
        let candidates: Vec<Option<String>> = std::iter::once(None)
            .chain(declared_ids.iter().map(|id| Some(id.as_str().to_owned())))
            .chain(std::iter::once(Some(probe.as_str().to_owned())))
            .collect();
        for configured in &candidates {
            let configured = configured.as_deref();
            let expected = expected_choice(configured, &active, &disabled);
            prop_assert_eq!(python::determine_org_id(&py, configured), expected.clone());
            prop_assert_eq!(fusion::determine_org_id(&fu, configured), expected);
        }
    }

    /// Any `scope` claim the server accepts parses without error under both
    /// clients, to the same active and disabled Organizations. Everything
    /// either client refuses, and everything the clients would read
    /// differently, the server refuses.
    #[test]
    fn server_accepts_only_claims_both_clients_read_identically(claim in scope_claim()) {
        let py = python::Scope::from_string(&claim);
        let fu = fusion::Scope::from_string(&claim);
        if let Ok(scope) = Scope::parse(&claim) {
            let py = py.expect("Python accepts what the server accepts");
            let fu = fu.expect("Fusion accepts what the server accepts");
            let active = ids(scope.active().map(|(id, _)| id));
            let disabled = ids(scope.disabled());
            prop_assert_eq!(set(py.org_ids()), active.clone());
            prop_assert_eq!(set(fu.org_ids()), active);
            prop_assert_eq!(set(py.disabled_org_ids()), disabled.clone());
            prop_assert_eq!(set(fu.disabled_org_ids()), disabled);
            let levels: BTreeMap<String, String> = py.org_levels().into_iter().collect();
            let expected: BTreeMap<String, String> = scope
                .active()
                .map(|(id, role)| (id.as_str().to_owned(), role.as_str().to_owned()))
                .collect();
            prop_assert_eq!(levels, expected);
        }
    }

    /// An Organization id is accepted exactly when it is non-empty visible
    /// ASCII without `:` and not `*`.
    #[test]
    fn organization_ids_are_slugs(id in prop_oneof![
        "[!-~]{0,6}",
        "[ -~]{0,6}",
        "\\PC{0,4}",
        Just("*".to_owned()),
        Just(String::new()),
    ]) {
        let valid = !id.is_empty()
            && id != "*"
            && id.chars().all(|c| c.is_ascii_graphic() && c != ':');
        prop_assert_eq!(OrganizationId::new(&id).is_ok(), valid, "{:?}", id);
    }
}

// ---------------------------------------------------------------------------
// The ports themselves, on literal cases from the client sources
// ---------------------------------------------------------------------------

/// Cases from the pinned Fusion tests (`scope.rs` `mod tests`), which the
/// Python rules agree with.
#[test]
fn ports_agree_with_the_pinned_fusion_tests() {
    for (claim, configured, expected) in [
        (
            "runcache:scope:org:test-org:admin",
            None,
            OrgChoice::Use("test-org".into()),
        ),
        (
            "runcache:scope:org:other-org:admin runcache:scope:org:*:admin",
            Some("test-org"),
            OrgChoice::Use("test-org".into()),
        ),
        (
            "runcache:scope:org:other-org:admin",
            Some("test-org"),
            OrgChoice::NoAccess,
        ),
        (
            "runcache:scope:org:first-org:admin runcache:scope:org:second-org:admin",
            None,
            OrgChoice::Ambiguous,
        ),
        (
            "runcache:scope:app:disabled-org:developer",
            Some("disabled-org"),
            OrgChoice::Disabled("disabled-org".into()),
        ),
        (
            "runcache:scope:app:disabled-org:developer",
            None,
            OrgChoice::Disabled("disabled-org".into()),
        ),
        ("", None, OrgChoice::NoOrganization),
    ] {
        let py = python::Scope::from_string(claim).unwrap();
        let fu = fusion::Scope::from_string(claim).unwrap();
        assert_eq!(
            python::determine_org_id(&py, configured),
            expected,
            "{claim}"
        );
        assert_eq!(
            fusion::determine_org_id(&fu, configured),
            expected,
            "{claim}"
        );
    }

    let fu = fusion::Scope::from_string(
        "runcache:scope:app:active-org:developer runcache:scope:org:active-org:developer \
         runcache:scope:app:disabled-org:developer",
    )
    .unwrap();
    assert_eq!(fu.disabled_org_ids(), ["disabled-org"]);
    let wildcard = fusion::Scope::from_string(
        "runcache:scope:org:*:admin runcache:scope:app:other-org:developer",
    )
    .unwrap();
    assert!(!wildcard.is_org_id_disabled("other-org"));
}

/// Where the clients refuse, and where they read the same claim
/// differently. The server refuses every one of these claims.
#[test]
fn ports_keep_each_clients_error_cases() {
    // Both refuse an entry with an empty Organization and no Role.
    assert!(python::Scope::from_string("runcache:scope:org:").is_err());
    assert!(fusion::Scope::from_string("runcache:scope:org:").is_err());
    // Python matches `runcache:scope:org` without a colon, so the requested
    // scope name in a claim is a Python error; Fusion ignores it.
    assert!(python::Scope::from_string("runcache:scope:orgs").is_err());
    assert!(fusion::Scope::from_string("runcache:scope:orgs")
        .unwrap()
        .org_ids()
        .is_empty());
    // An empty Organization field: Fusion refuses, Python skips the entry.
    assert!(fusion::Scope::from_string("runcache:scope:org::developer").is_err());
    assert!(python::Scope::from_string("runcache:scope:org::developer")
        .unwrap()
        .org_ids()
        .is_empty());
    // Python still honours the legacy prefix; Fusion ignores it.
    assert_eq!(
        python::Scope::from_string("conway:scope:org:legacy:developer")
            .unwrap()
            .org_ids(),
        ["legacy"]
    );
    assert!(
        fusion::Scope::from_string("conway:scope:org:legacy:developer")
            .unwrap()
            .org_ids()
            .is_empty()
    );
    // Python splits on single spaces only; a tab-joined claim is one entry.
    assert_eq!(
        python::Scope::from_string("runcache:scope:org:a:admin\truncache:scope:org:b:admin")
            .unwrap()
            .org_ids(),
        ["a"]
    );
    assert_eq!(
        fusion::Scope::from_string("runcache:scope:org:a:admin\truncache:scope:org:b:admin")
            .unwrap()
            .org_ids(),
        ["a", "b"]
    );

    for claim in [
        "runcache:scope:org:",
        "runcache:scope:orgs",
        "runcache:scope:org::developer",
        "conway:scope:org:legacy:developer",
        "runcache:scope:org:a:admin\truncache:scope:org:b:admin",
        "runcache:scope:org:*:admin",
        "runcache:scope:org:a:owner",
        "runcache:scope:org:a:admin runcache:scope:org:a:developer",
        "runcache:scope:org:a:admin:extra",
        "runcache:scope:org:a:admin  runcache:scope:app:b:admin",
        "openid runcache:scope:org:a:admin",
    ] {
        assert!(Scope::parse(claim).is_err(), "the server refuses {claim:?}");
    }
}
