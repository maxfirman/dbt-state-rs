//! Pure workload analysis shared by Submit and Record. Warehouse adapters own
//! identifier meaning, supported transformations and copy eligibility; traversal,
//! input binding and identity framing live here. Opaque copy analysis retains
//! the strict identity. No State history or I/O enters this module.
pub(crate) mod binding;
pub mod identity;
pub(crate) mod inputs;
pub(crate) mod physical_binding;
pub(crate) mod query;
use crate::query_cache as qc;
use crate::warehouse::Warehouse;
use sqlparser::ast::{VisitMut, VisitorMut};
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;

/// Why a workload is analyzed. It decides which identities the workload
/// publishes: a template-mode submit matches its unrendered template and is
/// never copied; a submit without SQL is never copied; a Record publishes a
/// template alternative only where the Warehouse rules admit one (#138).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Purpose {
    /// A `SubmitEnrichedSQL` submission (also speculative).
    Submit { compare_unrendered_code: bool },
    /// A `RecordExecutions` SQL execution.
    Record,
}

/// Everything identity-related a Submit or Record needs from one workload,
/// already limited to what its [`Purpose`] publishes.
pub(crate) struct AnalyzedWorkload {
    /// The strict rendered identity, in every purpose.
    pub rendered_hash: String,
    /// The unrendered-template identity, only when the purpose matches it.
    pub template_hash: Option<String>,
    /// The schema-abstracted Logical copy identity, only when copy-eligible.
    pub copy_hash: Option<String>,
    /// The physical binding identity, only for a copy-eligible Snowflake
    /// workload whose references all resolve ([`physical_binding`]).
    pub binding_hash: Option<String>,
    /// The complete copy binding proof, only for a copy-eligible rendered workload.
    pub copy_proof: Option<query::CopyProof>,
    /// Direct copy provenance, only for a copy-eligible rendered workload.
    /// Template mode cannot establish direct equivalence, even when missing
    /// signatures leave no usable template alternative.
    pub direct_provenance: Option<crate::copy_lifecycle::DirectCopyProvenance>,
    /// The genuine inputs the workload reported.
    pub inputs: Vec<crate::store::InputTable>,
    /// Whether the SQL reads a relation in the warehouse's dialect
    /// ([`Warehouse::sql_reads_relation`]); `None` when it is not one query.
    pub reads_relation: Option<bool>,
}

/// The one analysis of a workload, shared by Submit and Record.
pub(crate) fn analyze(req: &qc::SqlExecution, purpose: Purpose) -> AnalyzedWorkload {
    let warehouse = Warehouse::from_dialect(&req.dialect);
    let (template, copy) = match purpose {
        Purpose::Submit {
            compare_unrendered_code,
        } => (
            warehouse.template_body(compare_unrendered_code, req.dbt_node_state.as_ref()),
            !compare_unrendered_code && !req.sql.trim().is_empty(),
        ),
        Purpose::Record => (warehouse.record_template_body(req), true),
    };
    let rendered_copy = copy && template.is_none();
    let copy_proof = query::proof(req);
    let copy_hash = copy
        .then(|| {
            if warehouse == Warehouse::Databricks {
                crate::databricks_submit_copy::identity_from_proof(req, copy_proof.as_ref())
            } else {
                identity::copy_logic_hash(req)
            }
        })
        .flatten();
    let binding_hash = (copy_hash.is_some() && warehouse == Warehouse::Snowflake)
        .then(|| physical_binding::physical_binding(req, &|_| None))
        .flatten()
        .map(|binding| binding.hash);
    let direct_provenance = rendered_copy
        .then(|| crate::copy_lifecycle::direct_provenance(req, copy_proof.as_ref()))
        .flatten();
    AnalyzedWorkload {
        rendered_hash: identity::sql_logic_hash(req, None),
        template_hash: template.map(|body| identity::sql_logic_hash(req, Some(body))),
        copy_hash,
        binding_hash,
        copy_proof: rendered_copy.then_some(copy_proof).flatten(),
        direct_provenance,
        inputs: inputs::sql_inputs(req),
        reads_relation: warehouse.sql_reads_relation(&req.sql),
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
