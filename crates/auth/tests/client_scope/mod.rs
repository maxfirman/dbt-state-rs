//! Ports of the pinned dbt clients' `scope` claim rules, used as oracles for
//! the scope format the Token broker issues.
//!
//! Each port follows its source line by line, including the inputs it
//! refuses, and keeps that client's own quirks: they disagree on some inputs,
//! and an issued scope must satisfy both.

#![allow(dead_code)]

pub mod fusion;
pub mod python;

/// What a client concludes about which Organization to use
/// (`determine_org_id` / `_determine_org_id`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrgChoice {
    /// The client uses this Organization.
    Use(String),
    /// "access to organization ... has been disabled".
    Disabled(String),
    /// The configured Organization is not in the token.
    NoAccess,
    /// No Organization in the token.
    NoOrganization,
    /// Several (or a wildcard) Organization entries and none configured.
    Ambiguous,
}
