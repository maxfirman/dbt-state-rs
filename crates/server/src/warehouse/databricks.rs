//! Databricks identifier equivalence evidenced by the pinned #136 campaign.
//! Only plain ASCII components are admitted. SQL bytes outside a single input
//! name are preserved: this grants no formatting, expression or copy policy.
use sqlparser::ast::{
    Expr, GroupByExpr, ObjectName, ObjectNamePart, SelectFlavor, SelectItem, SetExpr, Spanned,
    Statement, TableFactor,
};
use sqlparser::dialect::DatabricksDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Location;

pub(super) const SQL_IDENTITY_VERSION: &[u8] = b"dbt-state-databricks-identifiers-v1";

fn component(value: &str) -> Option<String> {
    let mut chars = value.chars();
    let first = chars.next()?;
    (first.is_ascii_alphabetic() || first == '_').then_some(())?;
    chars
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
        .then(|| value.to_ascii_lowercase())
}

fn parts(name: &ObjectName) -> Option<Vec<String>> {
    name.0
        .iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(id) if matches!(id.quote_style, None | Some('`')) => {
                component(&id.value)
            }
            _ => None,
        })
        .collect()
}

fn rendered(parts: &[String]) -> String {
    parts
        .iter()
        .map(|p| format!("`{p}`"))
        .collect::<Vec<_>>()
        .join(".")
}

/// Fully qualified metadata only. Never infer a physical scope from defaults,
/// split quoted dots, remove escaped backticks or fold unknown identifier forms.
pub(super) fn physical_name(raw: &str) -> Option<String> {
    let statements =
        Parser::parse_sql(&DatabricksDialect {}, &format!("SELECT id FROM {raw}")).ok()?;
    let [Statement::Query(q)] = statements.as_slice() else {
        return None;
    };
    let SetExpr::Select(s) = q.body.as_ref() else {
        return None;
    };
    let [table] = s.from.as_slice() else {
        return None;
    };
    let TableFactor::Table {
        name,
        alias: None,
        args: None,
        ..
    } = &table.relation
    else {
        return None;
    };
    // Exact lexical roundtrip prevents suffix clauses/comments from becoming a
    // physical name. SQL parser serialization is not used for query identity.
    if name.to_string() != raw || !table.joins.is_empty() {
        return None;
    }
    let parts = parts(name)?;
    (parts.len() == 3).then(|| rendered(&parts))
}

fn byte_offset(sql: &str, at: Location) -> Option<usize> {
    let mut line = 1;
    let mut column = 1;
    for (offset, ch) in sql.char_indices() {
        if line == at.line && column == at.column {
            return Some(offset);
        }
        if ch == '\n' {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }
    (line == at.line && column == at.column).then_some(sql.len())
}

/// Resolve the sole input of a SELECT without CTEs/joins/subquery inputs.
/// Return None rather than serialize an uncharacterized AST. Output expressions,
/// whitespace, literals, aliases and every non-input byte remain untouched.
pub(super) fn resolved_sql(sql: &str, catalog: &str, schema: &str) -> Option<String> {
    let statements = Parser::parse_sql(&DatabricksDialect {}, sql).ok()?;
    let [Statement::Query(q)] = statements.as_slice() else {
        return None;
    };
    if q.with.is_some()
        || q.order_by.is_some()
        || q.limit_clause.is_some()
        || q.fetch.is_some()
        || !q.locks.is_empty()
        || q.for_clause.is_some()
        || q.settings.is_some()
        || q.format_clause.is_some()
        || !q.pipe_operators.is_empty()
    {
        return None;
    }
    let SetExpr::Select(s) = q.body.as_ref() else {
        return None;
    };
    // Defaults may be omitted from the identity only when every reference is
    // resolved. Functions/UDFs, nested queries and CTE bindings are outside this
    // evidenced slice and retain their complete original resolution context.
    if s.flavor != SelectFlavor::Standard
        || !s.optimizer_hints.is_empty()
        || s.distinct.is_some()
        || s.select_modifiers.is_some()
        || s.top.is_some()
        || s.exclude.is_some()
        || s.into.is_some()
        || !s.lateral_views.is_empty()
        || s.prewhere.is_some()
        || s.selection.is_some()
        || !s.connect_by.is_empty()
        || !matches!(&s.group_by, GroupByExpr::Expressions(e,m) if e.is_empty() && m.is_empty())
        || !s.cluster_by.is_empty()
        || !s.distribute_by.is_empty()
        || !s.sort_by.is_empty()
        || s.having.is_some()
        || !s.named_window.is_empty()
        || s.qualify.is_some()
        || s.value_table_mode.is_some()
        || s.projection
            .iter()
            .any(|p| !matches!(p, SelectItem::UnnamedExpr(Expr::Identifier(_))))
    {
        return None;
    }
    let [table] = s.from.as_slice() else {
        return None;
    };
    if !table.joins.is_empty() {
        return None;
    }
    let TableFactor::Table {
        name,
        alias: None,
        args: None,
        with_ordinality: false,
        with_hints,
        version: None,
        partitions,
        json_path: None,
        sample: None,
        index_hints,
        ..
    } = &table.relation
    else {
        return None;
    };
    if !with_hints.is_empty() || !partitions.is_empty() || !index_hints.is_empty() {
        return None;
    }
    let names = parts(name)?;
    let qualified = match names.as_slice() {
        [name] => vec![component(catalog)?, component(schema)?, name.clone()],
        [schema, name] => vec![component(catalog)?, schema.clone(), name.clone()],
        [catalog, schema, name] => vec![catalog.clone(), schema.clone(), name.clone()],
        _ => return None,
    };
    let span = name.span();
    let start = byte_offset(sql, span.start)?;
    let end = byte_offset(sql, span.end)?;
    if sql.get(start..end)? != name.to_string() {
        return None;
    }
    let mut normalized = sql.to_owned();
    normalized.replace_range(start..end, &rendered(&qualified));
    Some(normalized)
}
