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
) -> Vec<String> {
    match dialect.to_ascii_lowercase().as_str() {
        "snowflake" => vec![snowflake_clone(source, target, source_table_type)],
        "bigquery" => vec![format!("CREATE OR REPLACE TABLE {target} CLONE {source}")],
        "databricks" | "spark" => vec![format!(
            "CREATE OR REPLACE TABLE {target} SHALLOW CLONE {source}"
        )],
        // Redshift / others have no zero-copy clone; fall back to CTAS.
        _ => vec![format!("CREATE TABLE {target} AS SELECT * FROM {source}")],
    }
}

fn snowflake_clone(source: &str, target: &str, source_table_type: Option<&str>) -> String {
    // Preserve the source object kind. Snowflake reports e.g. "TRANSIENT TABLE".
    let kind = match source_table_type.map(|s| s.to_ascii_uppercase()) {
        Some(t) if t.contains("TRANSIENT") => "TRANSIENT TABLE",
        Some(t) if t.contains("VIEW") => "VIEW",
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
        );
        assert_eq!(sqls.len(), 1);
        let s = &sqls[0];
        assert!(s.contains("CREATE OR REPLACE TRANSIENT TABLE \"DB\".\"DEV\".\"T\""));
        assert!(s.contains("CLONE \"DB\".\"PROD\".\"T\""));
        assert!(s.contains("COPY GRANTS"));
    }

    #[test]
    fn snowflake_plain_table_clone() {
        let sqls = clone_sqls("snowflake", "p.s.t", "d.s.t", Some("TABLE"));
        assert!(sqls[0].contains("CREATE OR REPLACE TABLE"));
        assert!(!sqls[0].contains("TRANSIENT"));
    }

    #[test]
    fn bigquery_and_databricks_variants() {
        assert!(clone_sqls("bigquery", "s", "t", None)[0].contains("CLONE s"));
        assert!(clone_sqls("databricks", "s", "t", None)[0].contains("SHALLOW CLONE"));
    }
}
