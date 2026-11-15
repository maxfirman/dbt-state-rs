//! Checked dependency bindings. Identifier resolution and abstraction remain dialect policy.
use super::canonicalized;
use crate::sql_identity::{duckdb_relation, validated_duckdb_sql};
use sqlparser::ast::{ObjectNamePart, Statement, VisitorMut};
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;
use std::ops::ControlFlow;

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
