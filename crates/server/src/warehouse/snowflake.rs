//! Snowflake rules. Result copying is a zero-copy CLONE, and the copy logic
//! identity is the schema-abstracted namespace logic (see `fingerprint`).

use super::NamespaceIdentity;

/// Verified live: the same model in two environments shares this identity.
pub(super) const NAMESPACE_IDENTITY: NamespaceIdentity = NamespaceIdentity::SchemaAbstracted {
    version: b"dbt-state-ns-logic-v4",
};

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
