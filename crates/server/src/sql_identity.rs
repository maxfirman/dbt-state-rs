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

/// A checked, fully qualified local relation. Components retain their parsed
/// spelling and boundaries, including dots within quoted identifiers.
pub struct DuckDbRelation {
    components: [String; 3],
}

impl DuckDbRelation {
    pub fn catalog(&self) -> &str {
        &self.components[0]
    }

    pub fn schema(&self) -> &str {
        &self.components[1]
    }

    pub fn name(&self) -> &str {
        &self.components[2]
    }

    pub fn is_state_schema(&self) -> bool {
        self.schema() == "_dbt_state"
    }

    pub(crate) fn into_components(self) -> Vec<String> {
        self.components.into()
    }
}

/// AST parsing checks quoting and component boundaries before these names are
/// interpolated into CTAS. Keep the original SQL for rendering, not raw values.
pub fn duckdb_relation(name: &str) -> Option<DuckDbRelation> {
    let statements =
        Parser::parse_sql(&DuckDbDialect {}, &format!("SELECT id FROM {name}")).ok()?;
    if statements.len() != 1 {
        return None;
    }
    let Statement::Query(q) = &statements[0] else {
        return None;
    };
    let SetExpr::Select(s) = q.body.as_ref() else {
        return None;
    };
    if s.from.len() != 1 || !s.from[0].joins.is_empty() {
        return None;
    }
    let TableFactor::Table {
        name: object,
        alias: None,
        args: None,
        ..
    } = &s.from[0].relation
    else {
        return None;
    };
    // Serialization roundtrip rejects trailing clauses/comments/statement text.
    if object.to_string() != name {
        return None;
    }
    let parts: Option<Vec<_>> = object
        .0
        .iter()
        .map(|p| match p {
            ObjectNamePart::Identifier(i) => Some(i.value.clone()),
            _ => None,
        })
        .collect();
    Some(DuckDbRelation {
        components: parts?.try_into().ok()?,
    })
}

pub fn duckdb_copy_inputs(sql: &str, catalog: &str) -> Option<Vec<Vec<String>>> {
    validated_duckdb_sql(sql)?;
    struct Inputs<'a> {
        catalog: &'a str,
        names: Vec<Vec<String>>,
    }
    impl VisitorMut for Inputs<'_> {
        type Break = ();
        fn pre_visit_relation(&mut self, name: &mut sqlparser::ast::ObjectName) -> ControlFlow<()> {
            match duckdb_relation(&name.to_string()) {
                Some(relation)
                    if relation.catalog() == self.catalog && !relation.is_state_schema() =>
                {
                    self.names.push(relation.into_components());
                    ControlFlow::Continue(())
                }
                _ => ControlFlow::Break(()),
            }
        }
    }
    let mut inputs = Inputs {
        catalog,
        names: Vec::new(),
    };
    canonicalized(sql, &DuckDbDialect {}, &mut inputs)?;
    Some(inputs.names)
}

/// Checked graph representation. SQL relation slots are framed separately from
/// their structured bindings, so no identifier spelling can impersonate a slot.
pub(crate) struct DuckDbCopyGraph {
    pub queries: Vec<DuckDbCopyQuery>,
    pub inputs: Vec<Vec<String>>,
}

// Field declaration order preserves the previous tuple's lexicographic sort,
// and fingerprint framing keeps these fields in exactly this order.
#[derive(Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct DuckDbCopyQuery {
    pub binding: Vec<String>,
    pub query: String,
    pub slots: Vec<Vec<String>>,
    pub catalog: String,
    pub schema: String,
}

pub(crate) fn duckdb_copy_graph(req: &crate::query_cache::SqlExecution) -> Option<DuckDbCopyGraph> {
    use sqlparser::ast::{Ident, ObjectName};
    use std::collections::{BTreeMap, BTreeSet};
    fn resolve(name: &ObjectName, catalog: &str, schema: &str) -> Option<Vec<String>> {
        let parts: Option<Vec<_>> = name
            .0
            .iter()
            .map(|p| match p {
                ObjectNamePart::Identifier(i) => Some(i.value.clone()),
                _ => None,
            })
            .collect();
        match parts?.as_slice() {
            [table] => Some(vec![catalog.into(), schema.into(), table.clone()]),
            [s, table] => Some(vec![catalog.into(), s.clone(), table.clone()]),
            [c, s, table] if c == catalog => Some(vec![c.clone(), s.clone(), table.clone()]),
            _ => None,
        }
    }
    fn folded(parts: &[String]) -> Vec<String> {
        parts.iter().map(|p| p.to_ascii_lowercase()).collect()
    }
    let env = req.default_schema.as_deref()?;
    let mut definitions = BTreeMap::new();
    let mut keys = BTreeSet::new();
    for dep in &req.query_dependencies {
        let relation = duckdb_relation(&dep.name)?;
        if relation.catalog() != req.default_catalog
            || relation.is_state_schema()
            || dep.default_catalog != req.default_catalog
            || dep.default_schema.is_empty()
        {
            return None;
        }
        let name = relation.into_components();
        if !keys.insert(folded(&name)) {
            return None;
        }
        definitions.insert(name, dep);
    }
    fn abstract_name(name: &[String], env: &str, view: bool) -> Vec<String> {
        let mut key = vec![if view { "view" } else { "physical" }.into()];
        key.extend(name.iter().cloned());
        if view && name[1] == env {
            key[2] = String::new();
            key.insert(1, "environment".into());
        }
        key
    }
    struct References<'a> {
        catalog: &'a str,
        schema: &'a str,
        env: &'a str,
        definitions: &'a BTreeMap<Vec<String>, &'a crate::query_cache::QueryDependency>,
        names: Vec<Vec<String>>,
        slots: Vec<Vec<String>>,
    }
    impl VisitorMut for References<'_> {
        type Break = ();
        fn pre_visit_relation(&mut self, name: &mut ObjectName) -> ControlFlow<()> {
            let Some(parts) = resolve(name, self.catalog, self.schema) else {
                return ControlFlow::Break(());
            };
            if parts[1] == "_dbt_state" {
                return ControlFlow::Break(());
            }
            // Case-insensitive alternatives are ambiguous; never guess a binding.
            if self
                .definitions
                .keys()
                .any(|key| folded(key) == folded(&parts) && key != &parts)
            {
                return ControlFlow::Break(());
            }
            let view = self.definitions.contains_key(&parts);
            self.slots.push(abstract_name(&parts, self.env, view));
            self.names.push(parts);
            *name = ObjectName::from(vec![Ident::with_quote('"', "relation_slot")]);
            ControlFlow::Continue(())
        }
    }
    let mut graph = DuckDbCopyGraph {
        queries: Vec::new(),
        inputs: Vec::new(),
    };
    let mut pending = vec![(
        None,
        req.sql.clone(),
        req.default_catalog.clone(),
        env.to_owned(),
    )];
    let mut visited = BTreeSet::new();
    while let Some((binding, sql, catalog, schema)) = pending.pop() {
        let mut statements = Parser::parse_sql(&DuckDbDialect {}, &sql).ok()?;
        if statements.len() != 1 {
            return None;
        }
        let query = match statements.pop()? {
            Statement::Query(q) => q.to_string(),
            Statement::CreateView(v)
                if binding.is_some()
                    && v.columns.is_empty()
                    && !v.materialized
                    && !v.temporary
                    && !v.secure
                    && !v.with_no_schema_binding
                    && v.params.is_none()
                    && v.to.is_none()
                    && v.cluster_by.is_empty() =>
            {
                v.query.to_string()
            }
            _ => return None,
        };
        validated_duckdb_sql(&query)?;
        let mut refs = References {
            catalog: &catalog,
            schema: &schema,
            env,
            definitions: &definitions,
            names: Vec::new(),
            slots: Vec::new(),
        };
        let query = canonicalized(&query, &DuckDbDialect {}, &mut refs)?;
        for name in refs.names {
            if let Some(dep) = definitions.get(&name) {
                if visited.insert(name.clone()) {
                    pending.push((
                        Some(name),
                        dep.query.clone(),
                        dep.default_catalog.clone(),
                        dep.default_schema.clone(),
                    ));
                } else {
                    // The validated subset has one input, so a repeated node is a cycle.
                    return None;
                }
            } else {
                graph.inputs.push(name);
            }
        }
        let key = binding
            .as_deref()
            .map(|n| abstract_name(n, env, true))
            .unwrap_or_default();
        graph.queries.push(DuckDbCopyQuery {
            binding: key,
            query,
            slots: refs.slots,
            catalog,
            schema: if schema == env {
                "environment:".into()
            } else {
                format!("schema:{schema}")
            },
        });
    }
    if visited.len() != definitions.len() {
        return None;
    }
    graph.queries.sort();
    Some(graph)
}
