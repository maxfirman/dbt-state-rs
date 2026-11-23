//! Snowflake data-test history by key (#200, ADR 0007 "Data-test history
//! key"): which relation each input of a data test stands for, and the
//! history key that follows from it.
//!
//! Each input read through a copy this server recorded is read as its
//! Stand-in ([`crate::stand_in`]); every other input stands for itself.
//! [`read_through_copies`] rewrites the submit's evidence.

use crate::execution::{self, ExecutionEvidence};
use crate::query_cache as qc;
use crate::store::Store;
use crate::warehouse::Warehouse;

/// For a data test of a Warehouse that keys data-test history (Snowflake),
/// read each input through the copies this server recorded
/// ([`crate::stand_in`]) and recompute the history key with each Stand-in's
/// relation read as its source. Its producers must be resolved first: a
/// Stand-in carries its source build's. A no-op for every other submit.
pub(crate) async fn read_through_copies(
    store: &Store,
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &mut ExecutionEvidence,
) -> sqlx::Result<()> {
    let keyed = evidence.execution_type == qc::ModelExecutionType::DbtDataTest as i32
        && Warehouse::from_dialect(&evidence.dialect).keys_data_test_history();
    if !keyed || evidence.input_tables.is_empty() {
        return Ok(());
    }
    let reads = crate::stand_in::input_reads(store, evidence).await?;
    let mut stand_ins = Vec::new();
    for (input, read) in evidence.input_tables.iter_mut().zip(reads) {
        if let Some(source) = read.stand_in {
            stand_ins.push(crate::analyzed_workload::identity::StandIn {
                copy: crate::sql_norm::canonical_identifiers(&input.name),
                source: crate::sql_norm::canonical_identifiers(&source.name),
            });
            *input = source;
        }
    }
    if !stand_ins.is_empty() {
        let sql = execution::sql_execution(req);
        evidence.data_test_key =
            crate::analyzed_workload::identity::data_test_history_key(&sql, &stand_ins);
    }
    Ok(())
}
