//! Genuine input observations, including the characterized audit-output exclusions.
use crate::query_cache as qc;
use crate::store::InputTable;
fn is_own_audit_relation(req: &qc::SqlExecution, name: &str) -> bool {
    if !stores_failures(req) {
        return false;
    }
    let (Some(schema), Some(test)) = (
        req.default_schema.as_deref(),
        req.labels.get("dbt_node_name"),
    ) else {
        return false;
    };
    !mentioned_by_test(req, test)
        && crate::warehouse::Warehouse::from_dialect(&req.dialect).names_relation(
            name,
            &req.default_catalog,
            schema,
            test,
        )
}

/// The audit relation of a `store_failures` test with a configured alias,
/// recognized by elimination (#123, ADR 0008): the one reported table in the
/// test's own schema (`default_catalog`.`default_schema`, under the
/// warehouse's identifier rules, Snowflake only) that neither the test's SQL
/// nor any of its query dependencies mentions. Client 2.0.11 reports a table
/// only when the test's SQL or a view definition it sends as a query
/// dependency names it, or when it is the node's own relation
/// (dbt-fusion `collect_table_modified_infos`), so that table is the test's
/// own write. Hosted SKIPs the unchanged rerun
/// (explain_store_failures_audit_alias.jsonl entries 9, 10).
///
/// `None` when two or more reported tables qualify (counting one the
/// exact-name rule recognizes): the request is then ambiguous and every
/// table stays an input.
fn unnamed_audit_relation(req: &qc::SqlExecution) -> Option<&str> {
    if !stores_failures(req) {
        return None;
    }
    let schema = req.default_schema.as_deref()?;
    let warehouse = crate::warehouse::Warehouse::from_dialect(&req.dialect);
    if warehouse == crate::warehouse::Warehouse::Databricks {
        return warehouse.unread_test_audit(req);
    }
    let mut qualifying = req.tables.iter().filter(|t| {
        warehouse
            .relation_in_schema(&t.name, &req.default_catalog, schema)
            // An identifier with an embedded quote is spelled doubled in SQL,
            // which the mention check does not read; it stays an input.
            .is_some_and(|id| !id.is_empty() && !id.contains('"') && !mentioned_by_test(req, &id))
    });
    match (qualifying.next(), qualifying.next()) {
        (Some(only), None) => Some(only.name.as_str()),
        _ => None,
    }
}

/// Whether `req` is a data test whose semantic extras set
/// `store_failures: true`.
fn stores_failures(req: &qc::SqlExecution) -> bool {
    req.execution_type == qc::ModelExecutionType::DbtDataTest as i32
        && req
            .semantic_extras
            .get("store_failures")
            .and_then(|v| serde_json::from_str::<bool>(v).ok())
            == Some(true)
}

/// Whether the test's SQL or any of its query dependencies mentions
/// `identifier` ([`mentions_identifier`]).
fn mentioned_by_test(req: &qc::SqlExecution, identifier: &str) -> bool {
    std::iter::once(req.sql.as_str())
        .chain(req.query_dependencies.iter().map(|d| d.query.as_str()))
        .any(|sql| mentions_identifier(sql, identifier))
}

/// The reported tables that are the execution's inputs: all of them but a
/// `store_failures` test's own audit relation (#88, #123).
pub(crate) fn sql_inputs(req: &qc::SqlExecution) -> Vec<InputTable> {
    let unnamed = unnamed_audit_relation(req);
    req.tables
        .iter()
        .filter(|t| !is_own_audit_relation(req, &t.name) && Some(t.name.as_str()) != unnamed)
        .map(InputTable::from)
        .collect()
}

/// Whether `text` contains `identifier` as a whole word, ignoring ASCII case.
/// This over-approximates a reference (a comment or string literal also
/// counts), which only keeps a relation as an input.
fn mentions_identifier(text: &str, identifier: &str) -> bool {
    let is_word = |c: char| c.is_alphanumeric() || c == '_' || c == '$';
    let (text, identifier) = (text.to_lowercase(), identifier.to_lowercase());
    !identifier.is_empty()
        && text.match_indices(&identifier).any(|(at, _)| {
            let before = text[..at].chars().next_back();
            let after = text[at + identifier.len()..].chars().next();
            !before.is_some_and(is_word) && !after.is_some_and(is_word)
        })
}
