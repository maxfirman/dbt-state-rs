//! Pure workload analysis shared by Submit and Record. Warehouse adapters own
//! identifier meaning, supported transformations and copy eligibility; traversal,
//! input binding and identity framing live here. Opaque copy analysis retains
//! the strict identity. No State history or I/O enters this module.
pub(crate) mod binding;
pub(crate) mod databricks_v1;
pub mod identity;
pub(crate) mod inputs;
use crate::query_cache as qc;
use crate::warehouse::Warehouse;
use sqlparser::ast::{VisitMut, VisitorMut};
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;

pub(crate) struct AnalyzedWorkload {
    pub rendered_hash: String,
    pub template_hash: Option<String>,
    pub copy_hash: Option<String>,
    pub direct_provenance: Option<crate::copy_lifecycle::DirectCopyProvenance>,
    pub inputs: Vec<crate::store::InputTable>,
}

pub(crate) fn analyze(req: &qc::SqlExecution) -> AnalyzedWorkload {
    let warehouse = Warehouse::from_dialect(&req.dialect);
    AnalyzedWorkload {
        rendered_hash: identity::sql_logic_hash(req, None),
        template_hash: warehouse
            .template_body(true, req.dbt_node_state.as_ref())
            .map(|body| identity::sql_logic_hash(req, Some(body))),
        copy_hash: identity::copy_logic_hash(req),
        direct_provenance: databricks_v1::direct_copy_provenance(req),
        inputs: inputs::sql_inputs(req),
    }
}

/// Shared parse, visit and serialization path. Callers decide which AST nodes
/// and transformations have evidence for their warehouse.
pub(crate) fn canonicalized<V: VisitorMut>(
    sql: &str,
    dialect: &dyn Dialect,
    visitor: &mut V,
) -> Option<String> {
    let mut statements = Parser::parse_sql(dialect, sql).ok()?;
    for statement in &mut statements {
        if statement.visit(visitor).is_break() {
            return None;
        }
    }
    Some(
        statements
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; "),
    )
}
