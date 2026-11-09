//! DuckDB rules. A result copy is a CTAS between the `prod` and `dev` schemas
//! of one database file, accepted only for sources inspected as
//! CTAS-compatible. Copy chains stop at two ancestors, so the clone-equivalent
//! lookup walks each candidate's copy provenance. See
//! `docs/duckdb-result-copy.md`.

use super::IncrementalCopyContract;
use crate::query_cache as qc;
use crate::query_cache::ModelExecutionType;
use crate::sql_identity::{duckdb_relation, DuckDbRelation};
use std::collections::HashMap;

/// The table type the client reports for a source it inspected as
/// CTAS-compatible.
const CTAS_TABLE_TYPE: &str = "DUCKDB CTAS TABLE";

/// Hosted DuckDB copy chains stop at two ancestors.
const MAX_COPY_DEPTH: i64 = 2;

/// The two ends of a copy, parsed as fully qualified local relations.
struct CopyRelations {
    source: DuckDbRelation,
    target: DuckDbRelation,
}

impl CopyRelations {
    fn parse(source: &str, target: &str) -> Result<Self, &'static str> {
        Ok(Self {
            source: duckdb_relation(source).ok_or("invalid source")?,
            target: duckdb_relation(target).ok_or("invalid target")?,
        })
    }

    /// The one DuckDB relation identity check, shared by the clone-equivalent
    /// lookup and the DDL: a copy reads another schema of the same database
    /// file.
    fn one_file_across_schemas(&self) -> bool {
        self.source.catalog() == self.target.catalog()
            && self.source.schema() != self.target.schema()
    }

    fn between_copy_environments(&self) -> bool {
        self.source.is_copy_environment() && self.target.is_copy_environment()
    }
}

pub(super) fn copy_relations_compatible(source: &str, target: &str) -> bool {
    CopyRelations::parse(source, target).is_ok_and(|ends| ends.one_file_across_schemas())
}

pub(super) fn copy_ddl(
    source: &str,
    target: &str,
    source_table_type: Option<&str>,
) -> Result<Vec<String>, &'static str> {
    if source_table_type != Some(CTAS_TABLE_TYPE) {
        return Err("DuckDB copy requires inspected CTAS-compatible source");
    }
    let ends = CopyRelations::parse(source, target)?;
    if !ends.one_file_across_schemas() || !ends.between_copy_environments() {
        return Err("DuckDB copies require one file and supported environments");
    }
    Ok(vec![format!(
        "CREATE OR REPLACE TABLE {target} AS SELECT * FROM {source}"
    )])
}

/// The requested limit, capped at [`MAX_COPY_DEPTH`]. The cast is preserved
/// from the original planning code: a negative request wraps to a limit no
/// chain reaches.
pub(super) fn copy_chain_depth_limit(requested: Option<i64>) -> usize {
    requested.unwrap_or(MAX_COPY_DEPTH).min(MAX_COPY_DEPTH) as usize
}

pub(super) fn copy_provenance_depth() -> usize {
    MAX_COPY_DEPTH as usize
}

pub(super) fn copy_source_evidence(last_modified_epoch: Option<i64>) -> Result<(), &'static str> {
    match last_modified_epoch {
        Some(_) => Ok(()),
        None => Err("DuckDB source requires recorded physical evidence"),
    }
}

const INCREMENTAL_CONTRACT_KEY: &str = "duckdb_incremental_contract";
const INCREMENTAL_CONTRACT: &str = "delete-insert-fixed-v1";
const INCREMENTAL_IDENTITY_KEY: &str = "duckdb_incremental_identity";
const INCREMENTAL_IDENTITY_VERSION: &str = "duckdb-incremental-init-v2:";

pub(super) fn declares_incremental_contract(values: &HashMap<String, String>) -> bool {
    values.get(INCREMENTAL_CONTRACT_KEY).map(String::as_str) == Some(INCREMENTAL_CONTRACT)
}

/// The pinned delete+insert contract with a current initialization identity,
/// for a source the client inspected as CTAS-compatible.
pub(super) fn incremental_copy_contract(req: &qc::CloneRequest) -> IncrementalCopyContract<'_> {
    match (
        ModelExecutionType::try_from(req.execution_type),
        req.labels.get(INCREMENTAL_IDENTITY_KEY),
    ) {
        (Ok(ModelExecutionType::DeleteInsert), Some(identity))
            if declares_incremental_contract(&req.labels)
                && identity.starts_with(INCREMENTAL_IDENTITY_VERSION)
                && req.clone_source_table_type.as_deref() == Some(CTAS_TABLE_TYPE) =>
        {
            IncrementalCopyContract::Declared { identity }
        }
        _ => IncrementalCopyContract::Missing,
    }
}
