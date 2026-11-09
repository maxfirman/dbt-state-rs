//! Clone SQL generation for the dbt State CLONE decision.
//!
//! When the service decides a table can be cloned from another location (e.g. a
//! dev build deferring to prod), it returns the DDL the client should run. The
//! statements are owned by the warehouse rules
//! ([`Warehouse::result_copy_ddl`]); this is the dialect-string entry point.
//! Observed real output for Snowflake (from captured golden traffic):
//!
//! ```sql
//! CREATE OR REPLACE TRANSIENT TABLE <target>
//! CLONE <source>
//! COPY GRANTS
//! ```

use crate::warehouse::Warehouse;

/// Generate the clone DDL statement(s) for a dialect. `source_table_type` is
/// the warehouse-reported type of the source (e.g. "TRANSIENT TABLE", "TABLE",
/// "VIEW") and governs the object kind we create.
pub fn clone_sqls(
    dialect: &str,
    source: &str,
    target: &str,
    source_table_type: Option<&str>,
) -> Result<Vec<String>, &'static str> {
    Warehouse::from_dialect(dialect).result_copy_ddl(source, target, source_table_type, None)
}
