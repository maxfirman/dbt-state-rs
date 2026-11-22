//! The `scope` claim: which Organizations a token grants, with which Role.
//!
//! The format is the dbt client's runcache scope grammar: entries separated
//! by single spaces, each `runcache:scope:org:<ORG>:<role>` for an active
//! Organization or `runcache:scope:app:<ORG>:<role>` for one the Principal
//! belongs to. A token lists an active Organization under both kinds, as
//! hosted dbt State does, and a disabled one only as `app`, which the clients
//! report as "access ... has been disabled".
//!
//! [`Scope::parse`] accepts only that grammar: every entry must be well formed
//! and each Organization appears at most once per kind. Both pinned clients
//! read anything it accepts identically (`tests/scope_proptest.rs`).

use std::collections::BTreeMap;
use std::fmt;

use crate::{OrganizationId, Organizations, Role};

const ENTRY_PREFIX: &str = "runcache:scope:";
const ACTIVE_KIND: &str = "org";
const MEMBER_KIND: &str = "app";

/// The Organizations a token grants.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Scope {
    /// Active Organizations and the Role in each.
    active: BTreeMap<OrganizationId, Role>,
    /// Organizations listed only as `app` entries (disabled), with their Role.
    disabled: BTreeMap<OrganizationId, Role>,
}

/// A `scope` claim outside the issued grammar.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("malformed scope claim")]
pub struct ScopeError;

impl Scope {
    /// The scope that grants each of `grants` declared in `organizations`:
    /// the highest Role per Organization, active ones as `org` entries,
    /// disabled ones only as `app` entries. Undeclared Organizations are
    /// left out.
    pub fn issue(
        grants: impl IntoIterator<Item = (OrganizationId, Role)>,
        organizations: &Organizations,
    ) -> Self {
        let mut scope = Self::default();
        for (id, role) in grants {
            let target = if organizations.is_active(&id) {
                &mut scope.active
            } else if organizations.is_disabled(&id) {
                &mut scope.disabled
            } else {
                continue;
            };
            let best = target.entry(id).or_insert(role);
            *best = (*best).max(role);
        }
        scope
    }

    /// Read a `scope` claim. Refuses anything [`Scope::issue`] never
    /// produces.
    pub fn parse(claim: &str) -> Result<Self, ScopeError> {
        let mut active = BTreeMap::new();
        let mut members = BTreeMap::new();
        if !claim.is_empty() {
            for entry in claim.split(' ') {
                let fields = entry.strip_prefix(ENTRY_PREFIX).ok_or(ScopeError)?;
                let mut fields = fields.split(':');
                let (Some(kind), Some(id), Some(role), None) =
                    (fields.next(), fields.next(), fields.next(), fields.next())
                else {
                    return Err(ScopeError);
                };
                let id = OrganizationId::new(id).map_err(|_| ScopeError)?;
                let role: Role = role.parse().map_err(|_| ScopeError)?;
                let entries = match kind {
                    ACTIVE_KIND => &mut active,
                    MEMBER_KIND => &mut members,
                    _ => return Err(ScopeError),
                };
                if entries.insert(id, role).is_some() {
                    return Err(ScopeError);
                }
            }
        }
        members.retain(|id, _| !active.contains_key(id));
        Ok(Self {
            active,
            disabled: members,
        })
    }

    /// The Role this scope grants in `id`, if it is an active Organization of
    /// the scope.
    pub fn active_role(&self, id: &OrganizationId) -> Option<Role> {
        self.active.get(id).copied()
    }

    /// The active Organizations with their Roles, by id.
    pub fn active(&self) -> impl Iterator<Item = (&OrganizationId, Role)> {
        self.active.iter().map(|(id, role)| (id, *role))
    }

    /// The disabled Organizations (only `app` entries), by id.
    pub fn disabled(&self) -> impl Iterator<Item = &OrganizationId> {
        self.disabled.keys()
    }

    /// Whether the scope grants no Organization at all.
    pub fn is_empty(&self) -> bool {
        self.active.is_empty() && self.disabled.is_empty()
    }
}

/// The `scope` claim string.
impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut entries: Vec<(&OrganizationId, &str, Role)> = Vec::new();
        for (id, role) in &self.active {
            entries.push((id, MEMBER_KIND, *role));
            entries.push((id, ACTIVE_KIND, *role));
        }
        for (id, role) in &self.disabled {
            entries.push((id, MEMBER_KIND, *role));
        }
        entries.sort();
        for (i, (id, kind, role)) in entries.into_iter().enumerate() {
            if i > 0 {
                f.write_str(" ")?;
            }
            write!(f, "{ENTRY_PREFIX}{kind}:{id}:{role}")?;
        }
        Ok(())
    }
}
