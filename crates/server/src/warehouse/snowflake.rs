//! Snowflake rules. Result copying is a zero-copy CLONE, and the copy logic
//! identity is the schema-abstracted namespace logic (see `fingerprint`).

use super::{CopyIdentity, NamespaceIdentity};

/// Verified live: the same model in two environments shares this identity.
pub(super) const NAMESPACE_IDENTITY: NamespaceIdentity = NamespaceIdentity::SchemaAbstracted {
    version: b"dbt-state-ns-logic-v4",
};

/// Every Snowflake submission can take part in a result copy, paired with its
/// equivalent by the schema-abstracted namespace logic identity.
pub(super) fn copy_identity() -> CopyIdentity {
    CopyIdentity::NamespaceLogic
}

/// Whether the root SQL or any query dependency calls a volatile function,
/// per the checked Snowflake parser and volatile-function set.
pub(super) fn has_volatile_sql<'a>(
    root: &str,
    dependencies: impl IntoIterator<Item = &'a str>,
) -> bool {
    crate::sql_norm::request_has_volatile_sql(root, dependencies)
}

/// The clone statement, matching the hosted service's formatting (leading
/// newline and indentation) and preserving the source object kind, which
/// Snowflake reports as e.g. "TRANSIENT TABLE".
pub(super) fn copy_ddl(source: &str, target: &str, source_table_type: Option<&str>) -> Vec<String> {
    let kind = match source_table_type.map(|s| s.to_ascii_uppercase()) {
        Some(t) if t.contains("TRANSIENT") => "TRANSIENT TABLE",
        _ => "TABLE",
    };
    vec![format!(
        "\n            CREATE OR REPLACE {kind} {target}\n            CLONE {source}\n            COPY GRANTS"
    )]
}

/// Whether the reported relation `name` is exactly the three-part relation
/// `parts` (database, schema, identifier): an unquoted identifier folds to
/// upper case and a quoted one is exact, on both sides.
pub(super) fn names_relation(name: &str, parts: [&str; 3]) -> bool {
    use crate::sql_norm::{canonical_identifier, split_object_name_components};
    let components = split_object_name_components(name);
    components.len() == parts.len()
        && parts.iter().all(|p| !p.trim().is_empty())
        && components
            .iter()
            .zip(parts)
            .all(|(c, p)| canonical_identifier(c) == canonical_identifier(p))
}
