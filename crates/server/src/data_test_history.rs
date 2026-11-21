//! Snowflake data-test history by key (#200, ADR 0007 "Data-test history
//! key"): which relation each input of a data test stands for, and the
//! history key that follows from it.
//!
//! A relation that is the Target relation of a copy this server recorded (a
//! Result copy on submit or a RegisterClone), unchanged since the copy,
//! stands for the confirmed build it was copied from. Hosted reads a dev
//! test over a copy of the prod upstream as the prod test (VERIFIED LIVE
//! 2026-11-21, `data_test_history.jsonl` F34, F31, F33, F35). Names or
//! schema abstraction never make two relations one: a dev relation with the
//! prod relation's name and epoch that was not copied is another input
//! (P48–P59). Every other input stands for itself.
//!
//! [`stand_in`] is pure; [`read_through_copies`] reads the copies and their
//! sources and rewrites the submit's evidence.

use crate::execution::{self, ExecutionEvidence};
use crate::query_cache as qc;
use crate::store::{ExecutionRow, InputTable, Producer, Store};
use crate::warehouse::Warehouse;
use std::collections::HashMap;

/// `read`, an input as the submit reports it, read as the build its
/// relation was copied from, when `copy` (the relation's current confirmed
/// state) is a copy this server recorded from `source`, and `read` reports
/// the copy unchanged since it was confirmed. `source`
/// must be the confirmed ordinary build the copy names, in the copy's
/// organization, warehouse family and namespace, with a known epoch; a copy
/// of a copy stands for itself. `None`: the input stands for itself.
pub(crate) fn stand_in(
    read: &InputTable,
    copy: &ExecutionRow,
    source: &ExecutionRow,
) -> Option<InputTable> {
    let copied_from = copy.clone_source_table.as_deref()?;
    let family =
        |row: &ExecutionRow| Warehouse::from_dialect(&row.dialect).family_key(&row.dialect);
    // Unchanged since the copy, by the rule that a Target relation is as
    // built (AGENTS.md reuse invariant 3): a known epoch no later than the
    // copy's confirmation, an upper bound for its write. Snowflake reports a
    // copy's metadata epoch slightly before its confirmation (F50, F76:
    // 1795290449321 against the copy's 1795290449575).
    let unchanged = copy.status == "confirmed"
        && read
            .last_modified_epoch
            .zip(copy.last_modified_epoch)
            .is_some_and(|(read, copied)| read <= copied);
    let copied_build = copy.clone_source_execution_id == Some(source.id)
        && source.status == "confirmed"
        && crate::copy_lifecycle::ordinary(source)
        && source.last_modified_epoch.is_some_and(|e| e > 0)
        && source.org_id == copy.org_id
        && family(source) == family(copy)
        && execution::non_blank_namespace(source.table_namespace.as_deref())
            == execution::non_blank_namespace(copy.table_namespace.as_deref())
        && Warehouse::physical_scope_key(&source.dialect, &source.target_table)
            == Warehouse::physical_scope_key(&copy.dialect, copied_from);
    (unchanged && copied_build).then(|| InputTable {
        name: source
            .raw_target_table
            .clone()
            .unwrap_or_else(|| source.target_table.clone()),
        last_modified_epoch: source.last_modified_epoch,
        producer: Producer::Build(source.logic_identity()),
        read_copy: Some(read.name.clone()),
    })
}

/// A relation's canonical name components (`sql_norm::canonical_identifier`).
fn components(name: &str) -> Vec<String> {
    crate::sql_norm::split_object_name_components(name)
        .iter()
        .map(|c| crate::sql_norm::canonical_identifier(c))
        .collect()
}

/// For a data test of a Warehouse that keys data-test history (Snowflake),
/// read each input through the copies this server recorded ([`stand_in`])
/// and recompute the history key with each stand-in's relation read as its
/// source. Its producers must be resolved first: a stand-in carries its
/// source build's. A no-op for every other submit.
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
    let names: Vec<String> = evidence
        .input_tables
        .iter()
        .map(|t| t.name.clone())
        .collect();
    let namespace = execution::non_blank_namespace(evidence.table_namespace.as_deref());
    let current = store
        .current_target_states(&evidence.org_id, &names, namespace, &evidence.dialect)
        .await?;
    let mut sources: HashMap<i64, Option<ExecutionRow>> = HashMap::new();
    let mut stand_ins = Vec::new();
    for input in evidence.input_tables.iter_mut() {
        let Some(copy) = current.get(&input.name) else {
            continue;
        };
        let Some(source_id) = copy.clone_source_execution_id else {
            continue;
        };
        if let std::collections::hash_map::Entry::Vacant(slot) = sources.entry(source_id) {
            slot.insert(store.execution_by_id(&evidence.org_id, source_id).await?);
        }
        let Some(source) = sources.get(&source_id).and_then(Option::as_ref) else {
            continue;
        };
        if let Some(read) = stand_in(input, copy, source) {
            stand_ins.push(crate::analyzed_workload::identity::StandIn {
                copy: components(&input.name),
                source: components(&read.name),
            });
            *input = read;
        }
    }
    if !stand_ins.is_empty() {
        let sql = execution::sql_execution(req);
        evidence.data_test_key =
            crate::analyzed_workload::identity::data_test_history_key(&sql, &stand_ins);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const COPY: &str = "\"DB\".\"DEV\".\"U\"";
    const SOURCE: &str = "\"DB\".\"PROD\".\"U\"";

    fn build(id: i64, target: &str, epoch: i64) -> ExecutionRow {
        ExecutionRow {
            id,
            org_id: "org".into(),
            target_table: target.into(),
            execution_type: 1,
            node_sql_hash: Some(format!("sql-{id}")),
            dialect: "snowflake".into(),
            table_namespace: Some("ns".into()),
            last_modified_epoch: Some(epoch),
            status: "confirmed".into(),
            ..ExecutionRow::default()
        }
    }

    fn copy_of(source: &ExecutionRow, epoch: i64) -> ExecutionRow {
        ExecutionRow {
            clone_source_table: Some(source.target_table.clone()),
            clone_source_execution_id: Some(source.id),
            ..build(2, COPY, epoch)
        }
    }

    #[test]
    fn an_unchanged_copy_stands_for_its_source_build() {
        let source = build(1, SOURCE, 100);
        let read = InputTable::at(COPY, Some(200));
        let stood = stand_in(&read, &copy_of(&source, 200), &source).expect("a stand-in");
        assert_eq!(stood.name, SOURCE);
        assert_eq!(stood.last_modified_epoch, Some(100));
        assert_eq!(stood.producer, Producer::Build(source.logic_identity()));
        assert_eq!(stood.read_copy.as_deref(), Some(COPY));
    }

    #[test]
    fn a_copy_changed_since_the_copy_stands_for_itself() {
        let source = build(1, SOURCE, 100);
        let copy = copy_of(&source, 200);
        assert_eq!(
            stand_in(&InputTable::at(COPY, Some(201)), &copy, &source),
            None
        );
        assert_eq!(stand_in(&InputTable::at(COPY, None), &copy, &source), None);
        assert!(
            stand_in(&InputTable::at(COPY, Some(199)), &copy, &source).is_some(),
            "a metadata epoch before the copy's confirmation is unchanged"
        );
    }

    #[test]
    fn only_the_recorded_ordinary_source_build_in_scope_is_stood_for() {
        let source = build(1, SOURCE, 100);
        let read = InputTable::at(COPY, Some(200));
        let copy = copy_of(&source, 200);
        let other_build = build(3, SOURCE, 100);
        assert_eq!(stand_in(&read, &copy, &other_build), None, "another build");
        let a_copy = ExecutionRow {
            clone_source_table: Some("\"DB\".\"CI\".\"U\"".into()),
            ..source.clone()
        };
        assert_eq!(stand_in(&read, &copy, &a_copy), None, "a copy of a copy");
        for scoped in [
            ExecutionRow {
                org_id: "other".into(),
                ..source.clone()
            },
            ExecutionRow {
                table_namespace: Some("other".into()),
                ..source.clone()
            },
            ExecutionRow {
                dialect: "databricks".into(),
                ..source.clone()
            },
            ExecutionRow {
                target_table: "\"DB\".\"PROD\".\"V\"".into(),
                ..source.clone()
            },
            ExecutionRow {
                last_modified_epoch: None,
                ..source.clone()
            },
            ExecutionRow {
                status: "pending".into(),
                ..source.clone()
            },
        ] {
            assert_eq!(stand_in(&read, &copy, &scoped), None, "{scoped:?}");
        }
        let ordinary = ExecutionRow {
            clone_source_table: None,
            ..copy.clone()
        };
        assert_eq!(stand_in(&read, &ordinary, &source), None, "not a copy");
    }
}
