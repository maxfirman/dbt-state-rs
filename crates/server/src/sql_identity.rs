//! SQL identity policy. Shared parsing/traversal feeds dialect-specific rules;
//! fingerprint framing and dependency bindings remain in `fingerprint`.
use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, ObjectNamePart, Query, Select, SelectFlavor, SelectItem,
    SetExpr, Statement, TableFactor, Value, VisitMut, VisitorMut,
};
use sqlparser::dialect::{Dialect, DuckDbDialect};
use sqlparser::parser::Parser;

use crate::warehouse::Warehouse;

/// Exact fallback is deliberately outside the validated representation. No
/// parser failure or unreviewed AST node supplies equivalence evidence.
pub fn normalized_sql(warehouse: Warehouse, sql: &str) -> String {
    match warehouse {
        Warehouse::Snowflake => crate::sql_norm::normalize_sql(sql),
        Warehouse::DuckDb => validated_duckdb_sql(sql).unwrap_or_else(|| sql.to_owned()),
        _ => sql.to_owned(),
    }
}

/// A validated strict representation, distinct from the exact fallback. This
/// does not grant cross-environment eligibility or abstract physical names.
pub fn validated_duckdb_sql(sql: &str) -> Option<String> {
    let mut subset = DuckDbSubset {
        seen_statement: false,
    };
    let result = canonicalized(sql, &DuckDbDialect {}, &mut subset)?;
    subset.seen_statement.then_some(result)
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

// Only simple SELECTs with named inputs, identifier projections or explicitly
// aliased expressions. DuckDB's implicit expression column names can include
// source spelling, so formatting those expressions must NOT authorize SKIP.
// Preserve every identifier's spelling/quoting, literal and physical qualifier.
struct DuckDbSubset {
    seen_statement: bool,
}
impl VisitorMut for DuckDbSubset {
    type Break = ();

    fn pre_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<()> {
        if !self.seen_statement && matches!(statement, Statement::Query(_)) {
            self.seen_statement = true;
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(())
        }
    }

    fn pre_visit_query(&mut self, q: &mut Query) -> ControlFlow<()> {
        if !matches!(q.body.as_ref(), SetExpr::Select(_))
            || q.with.is_some()
            || q.order_by.is_some()
            || q.limit_clause.is_some()
            || q.fetch.is_some()
            || !q.locks.is_empty()
            || q.for_clause.is_some()
            || q.settings.is_some()
            || q.format_clause.is_some()
            || !q.pipe_operators.is_empty()
        {
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, s: &mut Select) -> ControlFlow<()> {
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
            || !matches!(&s.group_by, GroupByExpr::Expressions(e, m) if e.is_empty() && m.is_empty())
            || !s.cluster_by.is_empty()
            || !s.distribute_by.is_empty()
            || !s.sort_by.is_empty()
            || s.having.is_some()
            || !s.named_window.is_empty()
            || s.qualify.is_some()
            || s.value_table_mode.is_some()
            || s.from.len() > 1
            || s.from.iter().any(|t| !t.joins.is_empty())
            || s.projection.iter().any(|item| {
                !matches!(
                    item,
                    SelectItem::ExprWithAlias { .. }
                        | SelectItem::UnnamedExpr(
                            Expr::Identifier(_) | Expr::CompoundIdentifier(_)
                        )
                )
            })
        {
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, table: &mut TableFactor) -> ControlFlow<()> {
        match table {
            TableFactor::Table {
                name,
                alias,
                args: None,
                with_hints,
                version: None,
                with_ordinality: false,
                partitions,
                json_path: None,
                sample: None,
                index_hints,
                ..
            } if with_hints.is_empty()
                && partitions.is_empty()
                && index_hints.is_empty()
                && name
                    .0
                    .iter()
                    .all(|part| matches!(part, ObjectNamePart::Identifier(_)))
                && alias
                    .as_ref()
                    .is_none_or(|a| a.columns.is_empty() && a.at.is_none()) =>
            {
                ControlFlow::Continue(())
            }
            _ => ControlFlow::Break(()),
        }
    }

    fn pre_visit_expr(&mut self, expression: &mut Expr) -> ControlFlow<()> {
        match expression {
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Nested(_) => {
                ControlFlow::Continue(())
            }
            Expr::Value(v) if matches!(v.value, Value::Number(_, _)) => ControlFlow::Continue(()),
            Expr::BinaryOp {
                op: BinaryOperator::Plus,
                ..
            } => ControlFlow::Continue(()),
            _ => ControlFlow::Break(()),
        }
    }
}
