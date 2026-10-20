//! Clone SQL generation for the dbt State CLONE decision.
//!
//! When the service decides a table can be cloned from another location (e.g. a
//! dev build deferring to prod), it returns the DDL the client should run. The
//! statements are dialect-specific but simple (a zero-copy CLONE), so we
//! template them directly rather than pulling in a SQL engine. Observed real
//! output for Snowflake (from captured golden traffic):
//!
//! ```sql
//! CREATE OR REPLACE TRANSIENT TABLE <target>
//! CLONE <source>
//! COPY GRANTS
//! ```

/// Generate the clone DDL statement(s) for a dialect. `source_table_type` is
/// the warehouse-reported type of the source (e.g. "TRANSIENT TABLE", "TABLE",
/// "VIEW") and governs the object kind we create.
pub fn clone_sqls(
    dialect: &str,
    source: &str,
    target: &str,
    source_table_type: Option<&str>,
) -> Result<Vec<String>, &'static str> {
    clone_sqls_with_properties(dialect, source, target, source_table_type, None)
}

/// Plan clone SQL only when the source kind and request options are supported.
/// Both RegisterClone and SQL-submit clone fallback use this path. Unsupported
/// options are rejected so the client can build normally instead of silently
/// creating a clone with different retention from the requested target.
pub fn clone_sqls_with_properties(
    dialect: &str,
    source: &str,
    target: &str,
    source_table_type: Option<&str>,
    properties: Option<&crate::query_cache::TableProperties>,
) -> Result<Vec<String>, &'static str> {
    if properties
        .is_some_and(|p| p.hours_to_expiration.is_some() || p.partition_expiration_days.is_some())
    {
        return Err("clone table properties are not implemented");
    }
    if source_table_type.is_some_and(|kind| kind.to_ascii_uppercase().contains("VIEW")) {
        return Err("cloning view sources is not implemented");
    }
    match crate::warehouse::Warehouse::from_dialect(dialect) {
        crate::warehouse::Warehouse::Snowflake => {
            Ok(vec![snowflake_clone(source, target, source_table_type)])
        }
        crate::warehouse::Warehouse::BigQuery => {
            // GoogleSQL's CREATE TABLE ... CLONE syntax has no OR REPLACE form.
            Ok(vec![format!("CREATE TABLE {target} CLONE {source}")])
        }
        crate::warehouse::Warehouse::Databricks => {
            // Managed Iceberg supports DEEP but not SHALLOW CLONE. A generic
            // "TABLE" type does not identify the storage format, so only an
            // explicitly Delta source can use this unverified template.
            let is_delta =
                source_table_type.is_some_and(|kind| kind.to_ascii_uppercase().contains("DELTA"));
            if !is_delta {
                return Err("Databricks shallow clone requires a known Delta source");
            }
            Ok(vec![format!(
                "CREATE TABLE {target} SHALLOW CLONE {source}"
            )])
        }
        _ => Err("clone DDL is not implemented for this dialect"),
    }
}

fn snowflake_clone(source: &str, target: &str, source_table_type: Option<&str>) -> String {
    // Preserve the source object kind. Snowflake reports e.g. "TRANSIENT TABLE".
    let kind = match source_table_type.map(|s| s.to_ascii_uppercase()) {
        Some(t) if t.contains("TRANSIENT") => "TRANSIENT TABLE",
        _ => "TABLE",
    };
    // Match the real service's formatting (leading newline + indentation).
    format!(
        "\n            CREATE OR REPLACE {kind} {target}\n            CLONE {source}\n            COPY GRANTS"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snowflake_transient_clone_matches_real_shape() {
        let sqls = clone_sqls(
            "snowflake",
            "\"DB\".\"PROD\".\"T\"",
            "\"DB\".\"DEV\".\"T\"",
            Some("TRANSIENT TABLE"),
        )
        .unwrap();
        assert_eq!(sqls.len(), 1);
        let s = &sqls[0];
        assert!(s.contains("CREATE OR REPLACE TRANSIENT TABLE \"DB\".\"DEV\".\"T\""));
        assert!(s.contains("CLONE \"DB\".\"PROD\".\"T\""));
        assert!(s.contains("COPY GRANTS"));
    }

    #[test]
    fn snowflake_plain_table_clone() {
        let sqls = clone_sqls("snowflake", "p.s.t", "d.s.t", Some("TABLE")).unwrap();
        assert!(sqls[0].contains("CREATE OR REPLACE TABLE"));
        assert!(!sqls[0].contains("TRANSIENT"));
    }

    #[test]
    fn documented_non_snowflake_shapes_and_capabilities() {
        assert_eq!(
            clone_sqls("bigquery", "s", "t", Some("TABLE")).unwrap(),
            ["CREATE TABLE t CLONE s"]
        );
        assert_eq!(
            clone_sqls("databricks", "s", "t", Some("DELTA TABLE")).unwrap(),
            ["CREATE TABLE t SHALLOW CLONE s"]
        );
        for kind in [None, Some("TABLE"), Some("ICEBERG TABLE")] {
            assert!(clone_sqls("databricks", "s", "t", kind).is_err());
        }
    }

    #[test]
    fn unsupported_clone_sources_and_dialects_do_not_emit_guessed_sql() {
        assert!(clone_sqls("snowflake", "s", "t", Some("VIEW")).is_err());
        assert!(clone_sqls("unknown", "s", "t", Some("TABLE")).is_err());
        assert!(clone_sqls("redshift", "s", "t", Some("TABLE")).is_err());
        assert!(clone_sqls_with_properties(
            "bigquery",
            "s",
            "t",
            Some("TABLE"),
            Some(&crate::query_cache::TableProperties {
                hours_to_expiration: Some(24),
                partition_expiration_days: None,
            }),
        )
        .is_err());
    }
}
