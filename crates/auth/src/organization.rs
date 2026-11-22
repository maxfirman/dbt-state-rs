//! Organizations and Roles (GLOSSARY.md#access).

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

/// An operator-chosen Organization id: non-empty visible ASCII, without `:`,
/// and never the `*` wildcard.
///
/// `:` separates the fields of a `scope` entry and whitespace separates the
/// entries. The dbt clients read `*` as every Organization. Ids outside
/// visible ASCII could never be sent as `x-organization-id` metadata.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OrganizationId(String);

/// An Organization id that [`OrganizationId::new`] refuses.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "invalid Organization id {0:?}: use non-empty visible ASCII without ':' or whitespace, not '*'"
)]
pub struct InvalidOrganizationId(pub String);

impl OrganizationId {
    /// The id `id`, if it is a valid Organization id.
    pub fn new(id: &str) -> Result<Self, InvalidOrganizationId> {
        let valid =
            !id.is_empty() && id != "*" && id.chars().all(|c| c.is_ascii_graphic() && c != ':');
        if valid {
            Ok(Self(id.to_owned()))
        } else {
            Err(InvalidOrganizationId(id.to_owned()))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for OrganizationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The access a Principal holds in one Organization. Ordered: `Admin` is
/// higher than `Developer`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Role {
    Developer,
    Admin,
}

/// A Role name other than `developer` or `admin`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown Role {0:?}: use 'developer' or 'admin'")]
pub struct UnknownRole(pub String);

impl Role {
    /// The Role's name in config and in the `scope` claim.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Developer => "developer",
            Self::Admin => "admin",
        }
    }
}

impl FromStr for Role {
    type Err = UnknownRole;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        match name {
            "developer" => Ok(Self::Developer),
            "admin" => Ok(Self::Admin),
            other => Err(UnknownRole(other.to_owned())),
        }
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One Organization a deployment declares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Organization {
    pub id: OrganizationId,
    /// A disabled Organization is refused on every request and appears in
    /// issued tokens only as an `app` entry.
    pub disabled: bool,
}

/// The Organizations a deployment declares, each active or disabled.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Organizations(BTreeMap<OrganizationId, bool>);

/// Two declared Organizations share an id.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("Organization {0:?} is declared more than once")]
pub struct DuplicateOrganization(pub String);

impl Organizations {
    /// The declared `organizations`; ids must be distinct.
    pub fn new(
        organizations: impl IntoIterator<Item = Organization>,
    ) -> Result<Self, DuplicateOrganization> {
        let mut declared = BTreeMap::new();
        for organization in organizations {
            let id = organization.id.as_str().to_owned();
            if declared
                .insert(organization.id, organization.disabled)
                .is_some()
            {
                return Err(DuplicateOrganization(id));
            }
        }
        Ok(Self(declared))
    }

    /// Whether `id` is declared and not disabled.
    pub fn is_active(&self, id: &OrganizationId) -> bool {
        self.0.get(id) == Some(&false)
    }

    /// Whether `id` is declared and disabled.
    pub fn is_disabled(&self, id: &OrganizationId) -> bool {
        self.0.get(id) == Some(&true)
    }

    /// Every declared Organization, by id.
    pub fn iter(&self) -> impl Iterator<Item = Organization> + '_ {
        self.0.iter().map(|(id, disabled)| Organization {
            id: id.clone(),
            disabled: *disabled,
        })
    }
}
