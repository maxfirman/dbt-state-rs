//! Mapping rules: how the Token broker turns a Principal's Upstream IdP
//! claims into Organizations and Roles.
//!
//! Each rule names a Principal kind, a claim, a value, an Organization and a
//! Role. The kind comes from the flow that authenticated the Principal, so
//! only rules of that kind apply. A rule matches when the claim is the value,
//! or is an array of strings holding it. The highest matched Role wins in
//! each Organization, and a Principal no rule matches gets nothing.
//!
//! Each case is built so the expected grants are known by construction:
//! "matched" rules use values the claims carry, "unmatched" rules use values
//! they never carry, and "other kind" rules match but belong to the other
//! Principal kind.

use std::collections::BTreeMap;

use dbt_state_auth::{
    MappingRule, MappingRules, Organization, OrganizationId, Organizations, PrincipalKind, Role,
    UpstreamClaims,
};
use proptest::prelude::*;
use serde_json::{json, Map, Value};

const CLAIMS: &[&str] = &["groups", "roles", "azp", "appid"];

fn role() -> impl Strategy<Value = Role> {
    prop_oneof![Just(Role::Developer), Just(Role::Admin)]
}

fn kind() -> impl Strategy<Value = PrincipalKind> {
    prop_oneof![Just(PrincipalKind::Person), Just(PrincipalKind::Machine)]
}

fn other(kind: PrincipalKind) -> PrincipalKind {
    match kind {
        PrincipalKind::Person => PrincipalKind::Machine,
        PrincipalKind::Machine => PrincipalKind::Person,
    }
}

/// A rule's target: an Organization index, a Role, a claim name and a value.
type Target = (usize, Role, &'static str, String);

fn target(value: &'static str) -> impl Strategy<Value = Target> {
    (
        0usize..4,
        role(),
        prop::sample::select(CLAIMS),
        value.prop_map(|v| v.to_owned()),
    )
}

#[derive(Debug, Clone)]
struct Case {
    kind: PrincipalKind,
    organizations: Vec<OrganizationId>,
    /// Rules of `kind` whose value the claims carry.
    matched: Vec<Target>,
    /// Rules of `kind` whose value the claims never carry.
    unmatched: Vec<Target>,
    /// Rules of the other kind whose value the claims carry.
    other_kind: Vec<Target>,
    /// Whether a claim with one value is sent as a string or an array.
    as_array: bool,
    /// Values the claims carry that no rule names.
    noise: Vec<(&'static str, String)>,
}

fn case() -> impl Strategy<Value = Case> {
    (
        kind(),
        prop::collection::vec(target("m[a-z]{0,3}"), 0..6),
        prop::collection::vec(target("u[a-z]{0,3}"), 0..6),
        prop::collection::vec(target("[mo][a-z]{0,3}"), 0..6),
        any::<bool>(),
        prop::collection::vec(
            (
                prop::sample::select(CLAIMS),
                "n[a-z]{0,3}".prop_map(String::from),
            ),
            0..4,
        ),
    )
        .prop_map(
            |(kind, matched, unmatched, other_kind, as_array, noise)| Case {
                kind,
                organizations: ["analytics", "research", "finance", "act_42"]
                    .iter()
                    .map(|id| OrganizationId::new(id).unwrap())
                    .collect(),
                matched,
                unmatched,
                other_kind,
                as_array,
                noise,
            },
        )
}

impl Case {
    fn declared(&self) -> Organizations {
        Organizations::new(self.organizations.iter().map(|id| Organization {
            id: id.clone(),
            disabled: false,
        }))
        .unwrap()
    }

    fn rule(&self, kind: PrincipalKind, (org, role, claim, value): &Target) -> MappingRule {
        MappingRule {
            kind,
            claim: (*claim).to_owned(),
            value: value.clone(),
            organization: self.organizations[*org].clone(),
            role: *role,
        }
    }

    /// Every rule, interleaved so order carries no meaning.
    fn rules(&self) -> MappingRules {
        let mut rules = Vec::new();
        let longest = self
            .matched
            .len()
            .max(self.unmatched.len())
            .max(self.other_kind.len());
        for i in 0..longest {
            if let Some(t) = self.unmatched.get(i) {
                rules.push(self.rule(self.kind, t));
            }
            if let Some(t) = self.other_kind.get(i) {
                rules.push(self.rule(other(self.kind), t));
            }
            if let Some(t) = self.matched.get(i) {
                rules.push(self.rule(self.kind, t));
            }
        }
        MappingRules::new(rules)
    }

    /// Claims carrying every matched and other-kind value, plus noise.
    fn claims(&self) -> UpstreamClaims {
        let mut values: BTreeMap<&str, Vec<String>> = BTreeMap::new();
        for (_, _, claim, value) in self.matched.iter().chain(&self.other_kind) {
            values.entry(claim).or_default().push(value.clone());
        }
        for (claim, value) in &self.noise {
            values.entry(claim).or_default().push(value.clone());
        }
        let mut map = Map::new();
        map.insert("sub".into(), json!("principal"));
        for (claim, mut values) in values {
            values.dedup();
            let value = if values.len() == 1 && !self.as_array {
                Value::String(values.remove(0))
            } else {
                json!(values)
            };
            map.insert(claim.to_owned(), value);
        }
        UpstreamClaims::new(map)
    }

    /// The highest matched Role per Organization.
    fn expected(&self) -> BTreeMap<OrganizationId, Role> {
        let mut expected = BTreeMap::new();
        for (org, role, _, _) in &self.matched {
            let best = expected
                .entry(self.organizations[*org].clone())
                .or_insert(*role);
            *best = (*best).max(*role);
        }
        expected
    }
}

fn granted(case: &Case, kind: PrincipalKind) -> BTreeMap<OrganizationId, Role> {
    case.rules()
        .scope(kind, &case.claims(), &case.declared())
        .active()
        .map(|(id, role)| (id.clone(), role))
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// A Principal no rule of its kind matches gets no Organization at all.
    #[test]
    fn deny_by_default(mut case in case()) {
        case.matched.clear();
        let scope = case.rules().scope(case.kind, &case.claims(), &case.declared());
        prop_assert!(scope.is_empty(), "{scope}");
    }

    /// Each Organization gets the highest Role among the matched rules of the
    /// Principal's kind, whatever the rule order.
    #[test]
    fn the_highest_role_wins(case in case()) {
        prop_assert_eq!(granted(&case, case.kind), case.expected());
    }

    /// Rules of the other Principal kind never grant anything, even when
    /// their claim and value match.
    #[test]
    fn rules_apply_only_to_their_principal_kind(mut case in case()) {
        // Only matching rules of the other kind: nothing is granted.
        case.matched.clear();
        case.unmatched.clear();
        prop_assert!(granted(&case, case.kind).is_empty());

        // The same rules, for a Principal of their own kind, do grant.
        let mut own = case.clone();
        own.kind = other(case.kind);
        own.matched = std::mem::take(&mut own.other_kind);
        prop_assert_eq!(granted(&own, own.kind), own.expected());
    }
}

/// A claim matches as a string equal to the value, or as an array of
/// strings holding it; nothing else matches.
#[test]
fn claims_match_as_strings_or_arrays_of_strings() {
    let analytics = OrganizationId::new("analytics").unwrap();
    let organizations = Organizations::new([Organization {
        id: analytics.clone(),
        disabled: false,
    }])
    .unwrap();
    let rules = MappingRules::new(vec![MappingRule {
        kind: PrincipalKind::Machine,
        claim: "roles".into(),
        value: "dbt.ci".into(),
        organization: analytics.clone(),
        role: Role::Developer,
    }]);
    let cases = [
        (json!("dbt.ci"), true),
        (json!(["other", "dbt.ci"]), true),
        (json!("DBT.CI"), false),
        (json!("dbt.ci other"), false),
        (json!(["dbt.ci.extra"]), false),
        (json!([["dbt.ci"]]), false),
        (json!({"dbt.ci": true}), false),
        (json!(true), false),
        (json!(null), false),
        (json!([]), false),
    ];
    for (value, matches) in cases {
        let mut claims = Map::new();
        claims.insert("roles".into(), value.clone());
        let scope = rules.scope(
            PrincipalKind::Machine,
            &UpstreamClaims::new(claims),
            &organizations,
        );
        assert_eq!(scope.active_role(&analytics).is_some(), matches, "{value}");
    }
    // A claim the rule does not name never matches.
    let mut claims = Map::new();
    claims.insert("groups".into(), json!("dbt.ci"));
    assert!(rules
        .scope(
            PrincipalKind::Machine,
            &UpstreamClaims::new(claims),
            &organizations
        )
        .is_empty());
}

/// A rule granting a disabled Organization lists it only as disabled, so the
/// dbt client reports that access has been disabled.
#[test]
fn a_disabled_organization_is_granted_only_as_disabled() {
    let finance = OrganizationId::new("finance").unwrap();
    let organizations = Organizations::new([Organization {
        id: finance.clone(),
        disabled: true,
    }])
    .unwrap();
    let rules = MappingRules::new(vec![MappingRule {
        kind: PrincipalKind::Machine,
        claim: "azp".into(),
        value: "ci".into(),
        organization: finance.clone(),
        role: Role::Admin,
    }]);
    let mut claims = Map::new();
    claims.insert("azp".into(), json!("ci"));
    let scope = rules.scope(
        PrincipalKind::Machine,
        &UpstreamClaims::new(claims),
        &organizations,
    );
    assert_eq!(scope.active_role(&finance), None);
    assert_eq!(scope.disabled().collect::<Vec<_>>(), vec![&finance]);
}
