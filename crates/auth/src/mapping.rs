//! Mapping rules: which Organizations and Roles a Principal's Upstream IdP
//! claims grant.
//!
//! A rule names a Principal kind, a claim, a value, an Organization and a
//! Role. The kind comes from the flow that authenticated the Principal
//! (`client_credentials` gives a machine), never from claims, and only rules
//! of that kind are evaluated. A rule matches when its claim is a string
//! equal to its value, or an array of strings holding it (`groups` and
//! `roles` are arrays in most IdPs). The highest matched Role wins in each
//! Organization, and no match means denial: an empty [`Scope`].

use std::fmt;

use serde_json::{Map, Value};

use crate::{OrganizationId, Organizations, PrincipalKind, Role, Scope};

/// The claims of a verified Upstream IdP token (or, later, userinfo).
#[derive(Clone, PartialEq)]
pub struct UpstreamClaims(Map<String, Value>);

impl fmt::Debug for UpstreamClaims {
    /// Claim names only: values can be personal data.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.0.keys()).finish()
    }
}

impl UpstreamClaims {
    pub fn new(claims: Map<String, Value>) -> Self {
        Self(claims)
    }

    /// The string value of `claim`, if it is a string.
    pub fn string(&self, claim: &str) -> Option<&str> {
        self.0.get(claim).and_then(Value::as_str)
    }

    /// Whether `claim` is `value`, or an array of strings holding it.
    pub fn has(&self, claim: &str, value: &str) -> bool {
        match self.0.get(claim) {
            Some(Value::String(s)) => s == value,
            Some(Value::Array(items)) => items.iter().any(|item| item.as_str() == Some(value)),
            _ => false,
        }
    }

    /// Whether the claims carry `claim` at all.
    pub fn contains(&self, claim: &str) -> bool {
        self.0.contains_key(claim)
    }
}

/// One mapping rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MappingRule {
    /// The only Principal kind the rule applies to.
    pub kind: PrincipalKind,
    pub claim: String,
    pub value: String,
    pub organization: OrganizationId,
    pub role: Role,
}

impl MappingRule {
    fn matches(&self, kind: PrincipalKind, claims: &UpstreamClaims) -> bool {
        self.kind == kind && claims.has(&self.claim, &self.value)
    }
}

/// A deployment's mapping rules.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MappingRules(Vec<MappingRule>);

impl MappingRules {
    pub fn new(rules: Vec<MappingRule>) -> Self {
        Self(rules)
    }

    /// What a Principal of `kind` with `claims` is granted in the declared
    /// `organizations`: the highest Role per Organization among the matching
    /// rules of that kind. Empty when no rule matches.
    pub fn scope(
        &self,
        kind: PrincipalKind,
        claims: &UpstreamClaims,
        organizations: &Organizations,
    ) -> Scope {
        Scope::issue(
            self.0
                .iter()
                .filter(|rule| rule.matches(kind, claims))
                .map(|rule| (rule.organization.clone(), rule.role)),
            organizations,
        )
    }

    /// The claims rules of `kind` read, each once, in rule order. A refusal
    /// can name the ones a Principal's claims lack (for example `groups`
    /// under Entra group overage).
    pub fn claims_read(&self, kind: PrincipalKind) -> Vec<&str> {
        let mut claims: Vec<&str> = Vec::new();
        for rule in self.0.iter().filter(|rule| rule.kind == kind) {
            if !claims.contains(&rule.claim.as_str()) {
                claims.push(&rule.claim);
            }
        }
        claims
    }
}
