//! Port of the pinned Fusion client's scope rules.
//!
//! Source: `crates/dbt-run-cache/src/auth/scope.rs` at
//! `dbt-labs/dbt-fusion@9977b6cbb1b761065536300037560d8e3c037011`.

use super::OrgChoice;

const RUNCACHE_ORG_SCOPE_PREFIX: &str = "runcache:scope:org:";
const RUNCACHE_APP_SCOPE_PREFIX: &str = "runcache:scope:app:";

/// `Scope`
#[derive(Debug, Clone)]
pub struct Scope {
    org_scopes: Vec<String>,
    app_scopes: Vec<String>,
}

/// `RunCacheServiceError::Auth(format!("invalid OAuth scope '{scope}'"))`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidOauthScope(pub String);

impl Scope {
    /// `Scope::from_string`: split on any whitespace, keep entries by prefix
    /// (with trailing colon) and refuse a kept entry with fewer than five
    /// fields or an empty fourth field.
    pub fn from_string(scope: &str) -> Result<Self, InvalidOauthScope> {
        let mut org_scopes = Vec::new();
        let mut app_scopes = Vec::new();
        for part in scope.split_whitespace() {
            if part.starts_with(RUNCACHE_ORG_SCOPE_PREFIX) {
                validate_scope_format(part)?;
                org_scopes.push(part.to_owned());
            } else if part.starts_with(RUNCACHE_APP_SCOPE_PREFIX) {
                validate_scope_format(part)?;
                app_scopes.push(part.to_owned());
            }
        }
        Ok(Self {
            org_scopes,
            app_scopes,
        })
    }

    /// `Scope::org_ids`
    pub fn org_ids(&self) -> Vec<String> {
        let mut ids = Vec::new();
        for s in &self.org_scopes {
            if let Some(id) = extract_org_id(s) {
                if !ids.contains(&id) {
                    ids.push(id);
                }
            }
        }
        ids
    }

    /// `Scope::is_org_id_in_scope`
    pub fn is_org_id_in_scope(&self, org_id: &str) -> bool {
        self.org_ids().iter().any(|id| id == org_id || id == "*")
    }

    /// `Scope::is_org_id_disabled`
    pub fn is_org_id_disabled(&self, org_id: &str) -> bool {
        if self.is_org_id_in_scope(org_id) {
            return false;
        }
        self.app_scopes
            .iter()
            .filter_map(|s| extract_org_id(s))
            .any(|id| id == org_id)
    }

    /// `Scope::disabled_org_ids`
    pub fn disabled_org_ids(&self) -> Vec<String> {
        let active = self.org_ids();
        let mut ids = Vec::new();
        for s in &self.app_scopes {
            if let Some(id) = extract_org_id(s) {
                if !active.contains(&id) && !ids.contains(&id) {
                    ids.push(id);
                }
            }
        }
        ids
    }
}

/// `determine_org_id`
pub fn determine_org_id(scope: &Scope, configured_org_id: Option<&str>) -> OrgChoice {
    if let Some(configured) = configured_org_id {
        if scope.is_org_id_in_scope(configured) {
            return OrgChoice::Use(configured.to_owned());
        }
        if scope.is_org_id_disabled(configured) {
            return OrgChoice::Disabled(configured.to_owned());
        }
        return OrgChoice::NoAccess;
    }
    let org_ids = scope.org_ids();
    match org_ids.as_slice() {
        [id] if id != "*" => OrgChoice::Use(id.clone()),
        [] => match scope.disabled_org_ids().as_slice() {
            [org_id] => OrgChoice::Disabled(org_id.clone()),
            _ => OrgChoice::NoOrganization,
        },
        _ => OrgChoice::Ambiguous,
    }
}

fn validate_scope_format(scope: &str) -> Result<(), InvalidOauthScope> {
    let parts: Vec<&str> = scope.split(':').collect();
    if parts.len() < 5 || parts[3].is_empty() {
        return Err(InvalidOauthScope(scope.to_owned()));
    }
    Ok(())
}

fn extract_org_id(scope: &str) -> Option<String> {
    let parts: Vec<&str> = scope.split(':').collect();
    if parts.len() >= 5 && !parts[3].is_empty() {
        Some(parts[3].to_owned())
    } else {
        None
    }
}
