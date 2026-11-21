//! Port of the pinned Python client's scope rules.
//!
//! Source: `query_cache_common/auth.py` from `query-cache-common==2.7.0`, the
//! exact dependency of `dbt-state==2.62.6` (`dbt-labs/dbt-state@b0f3030`),
//! and `_determine_org_id` in `clients/dbt_state/src/dbt_state/auth/sso.py`
//! of the same commit.

use super::OrgChoice;

const RUNCACHE_ORG_SCOPE_PREFIX: &str = "runcache:scope:org";
const LEGACY_ORG_SCOPE_PREFIX: &str = "conway:scope:org";
const ORG_SCOPE_PREFIXES: [&str; 2] = [RUNCACHE_ORG_SCOPE_PREFIX, LEGACY_ORG_SCOPE_PREFIX];
const RUNCACHE_APP_SCOPE_PREFIX: &str = "runcache:scope:app";

/// `Scope`: the `org` and `app` entries of a token's `scope` claim.
#[derive(Debug, Clone)]
pub struct Scope {
    org_scopes: Vec<String>,
    app_scopes: Vec<String>,
}

/// `ValueError(f"Invalid scope format: '{scope}'.")`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidScopeFormat(pub String);

impl Scope {
    /// `Scope.from_string`: split on single spaces, strip each piece, keep
    /// entries by prefix (no trailing colon), and refuse any kept entry with
    /// fewer than four colons.
    pub fn from_string(scope: &str) -> Result<Self, InvalidScopeFormat> {
        let scopes: Vec<String> = scope.split(' ').map(|s| py_strip(s).to_owned()).collect();
        let org_scopes: Vec<String> = scopes
            .iter()
            .filter(|s| ORG_SCOPE_PREFIXES.iter().any(|p| s.starts_with(p)))
            .cloned()
            .collect();
        let app_scopes: Vec<String> = scopes
            .iter()
            .filter(|s| s.starts_with(RUNCACHE_APP_SCOPE_PREFIX))
            .cloned()
            .collect();
        for scope in org_scopes.iter().chain(&app_scopes) {
            if scope.matches(':').count() < 4 {
                return Err(InvalidScopeFormat(scope.clone()));
            }
        }
        Ok(Self {
            org_scopes,
            app_scopes,
        })
    }

    /// `Scope.org_ids`: every Organization in an `org` entry, wildcards
    /// included, in first-seen order.
    pub fn org_ids(&self) -> Vec<String> {
        org_id_to_level(&self.org_scopes)
            .into_iter()
            .map(|(id, _)| id)
            .collect()
    }

    /// The Role (`level`) per `org` Organization; a repeated Organization
    /// keeps its last level, as a Python dict does.
    pub fn org_levels(&self) -> Vec<(String, String)> {
        org_id_to_level(&self.org_scopes)
    }

    /// `Scope.disabled_org_ids`: Organizations in `app` entries but in no
    /// `org` entry.
    pub fn disabled_org_ids(&self) -> Vec<String> {
        let active = self.org_ids();
        org_id_to_level(&self.app_scopes)
            .into_iter()
            .map(|(id, _)| id)
            .filter(|id| !active.contains(id))
            .collect()
    }

    /// `Scope.is_org_id_in_scope`
    pub fn is_org_id_in_scope(&self, org_id: &str) -> bool {
        let ids = self.org_ids();
        ids.iter().any(|id| id == org_id) || ids.iter().any(|id| id == "*")
    }

    /// `Scope.is_org_id_disabled`
    pub fn is_org_id_disabled(&self, org_id: &str) -> bool {
        if self.is_org_id_in_scope(org_id) {
            return false;
        }
        org_id_to_level(&self.app_scopes)
            .iter()
            .any(|(id, _)| id == org_id)
    }
}

/// `SSOAuth._determine_org_id`
pub fn determine_org_id(scope: &Scope, configured_org_id: Option<&str>) -> OrgChoice {
    // `if self._configured_org_id:`, so an empty string counts as unset.
    if let Some(configured) = configured_org_id.filter(|c| !c.is_empty()) {
        if scope.is_org_id_in_scope(configured) {
            return OrgChoice::Use(configured.to_owned());
        }
        if scope.is_org_id_disabled(configured) {
            return OrgChoice::Disabled(configured.to_owned());
        }
        return OrgChoice::NoAccess;
    }
    let org_ids = scope.org_ids();
    if org_ids.is_empty() {
        let disabled = scope.disabled_org_ids();
        if disabled.len() == 1 {
            return OrgChoice::Disabled(disabled[0].clone());
        }
        return OrgChoice::NoOrganization;
    }
    if org_ids.len() == 1 && org_ids[0] != "*" {
        return OrgChoice::Use(org_ids[0].clone());
    }
    OrgChoice::Ambiguous
}

/// `_org_id_to_level`: `{org_id: level}` over the entries with a non-empty
/// fourth field, first-seen key order, last-seen value.
fn org_id_to_level(scopes: &[String]) -> Vec<(String, String)> {
    let mut mapping: Vec<(String, String)> = Vec::new();
    for scope in scopes {
        // `_extract_org_id_and_level`
        let parts: Vec<&str> = scope.split(':').collect();
        let (org_id, level) = (parts[3], parts[4]);
        if org_id.is_empty() {
            continue;
        }
        match mapping.iter_mut().find(|(id, _)| id == org_id) {
            Some(entry) => entry.1 = level.to_owned(),
            None => mapping.push((org_id.to_owned(), level.to_owned())),
        }
    }
    mapping
}

/// Python's `str.strip()`: Unicode whitespace plus the four ASCII separator
/// controls (`\x1c`..`\x1f`) Python also counts as whitespace.
fn py_strip(s: &str) -> &str {
    s.trim_matches(|c: char| c.is_whitespace() || ('\x1c'..='\x1f').contains(&c))
}
