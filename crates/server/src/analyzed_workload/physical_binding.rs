//! Snowflake physical bindings (#264, ADR 0007 "Result copy over different
//! inputs"): which physical relation each relation reference of a workload
//! reads. A Result copy qualifies only when the submit, each copy it reads
//! taken as the build it stands for, binds every reference as the build it
//! would copy did. The schema-abstracted copy logic identity cannot tell:
//! it abstracts the schemas that decide what an unqualified or
//! schema-relative reference reads.
//!
//! Each relation reference in FROM or JOIN is resolved in the default
//! catalog and schema of the SQL that names it: the root SQL, or a query
//! dependency's definition. A CTE reference stays a CTE: a CTE is in scope
//! in its query's body and nested queries, in its later siblings and in its
//! own definition (`RECURSIVE` is optional on Snowflake). A table literal
//! (`TABLE('<name>')`, `IDENTIFIER('<name>')`) is resolved like a name. A
//! reference to a query dependency is bound to its definition, itself so
//! resolved, so two views of one definition bind alike and swapping
//! definitions between references does not. Anything else that could read
//! a relation (a one-part name of a later sibling CTE, a table function
//! outside the pure set, an unsupported FROM construct, a cycle of
//! dependencies, SQL that does not parse) leaves the workload without a
//! physical binding.
//!
//! Function and sequence names, which also resolve through the session's
//! schema, are not bound (uncaptured).

use crate::query_cache as qc;
use crate::sql_norm::{canonical_identifier, split_object_name_components};
use sha2::{Digest, Sha256};
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, ObjectName, ObjectNamePart,
    Query, Statement, TableFactor, Value, VisitMut, VisitorMut,
};
use sqlparser::dialect::SnowflakeDialect;
use sqlparser::parser::Parser;
use std::ops::ControlFlow;

/// The version of [`PhysicalBinding::hash`], a persisted fingerprint
/// (`executions.node_binding_hash`). Change it with any change to what it
/// frames.
const VERSION: &[u8] = b"dbt-state-physical-binding-v1";

/// Table functions that read no relation.
const PURE_TABLE_FUNCTIONS: &[&str] = &[
    "FLATTEN",
    "GENERATOR",
    "SPLIT_TO_TABLE",
    "STRTOK_SPLIT_TO_TABLE",
];

/// The placeholder a resolved reference is rewritten to before binding.
const PLACEHOLDER: &str = "__dbt_state_reference__";

/// A relation as its canonical components (`sql_norm::canonical_identifier`),
/// three parts once resolved.
pub(crate) type Relation = Vec<String>;

/// How a workload binds its relation references ([`physical_binding`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PhysicalBinding {
    /// The versioned identity of the SQL and of the query dependency
    /// definitions it reads, each reference resolved and bound.
    pub hash: String,
    /// Each base relation read (no query dependency), as resolved before
    /// substitution: what the workload must report as its inputs.
    pub base_reads: Vec<Relation>,
}

/// The physical binding of `req`, a Snowflake workload, with each resolved
/// base relation that `substitute` maps read as the mapped relation (a copy
/// read as the build it stands for); `None` when a reference cannot be
/// resolved.
pub(crate) fn physical_binding(
    req: &qc::SqlExecution,
    substitute: &dyn Fn(&[String]) -> Option<Relation>,
) -> Option<PhysicalBinding> {
    let dependencies: Vec<(Relation, &qc::QueryDependency)> = req
        .query_dependencies
        .iter()
        .map(|d| (canonical_name(&d.name), d))
        .collect();
    let mut binder = Binder {
        dependencies: &dependencies,
        substitute,
        base_reads: Vec::new(),
        stack: Vec::new(),
    };
    let schema = req.default_schema.as_deref().unwrap_or("");
    let root = binder.bind(&req.sql, &req.default_catalog, schema, false)?;
    let mut h = Sha256::new();
    field(&mut h, VERSION);
    field(&mut h, root.as_bytes());
    Some(PhysicalBinding {
        hash: hex::encode(h.finalize()),
        base_reads: binder.base_reads,
    })
}

fn field(h: &mut Sha256, bytes: &[u8]) {
    h.update((bytes.len() as u64).to_le_bytes());
    h.update(bytes);
}

/// `name` (dotted, possibly quoted) as canonical components.
pub(crate) fn canonical_name(name: &str) -> Relation {
    split_object_name_components(name)
        .iter()
        .map(|c| canonical_identifier(c))
        .collect()
}

/// `parts` as a quoted, dotted name.
fn quoted(parts: &[String]) -> String {
    parts
        .iter()
        .map(|p| format!("\"{}\"", p.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(".")
}

struct Binder<'a> {
    dependencies: &'a [(Relation, &'a qc::QueryDependency)],
    substitute: &'a dyn Fn(&[String]) -> Option<Relation>,
    base_reads: Vec<Relation>,
    /// The query dependencies being bound, to refuse a cycle.
    stack: Vec<usize>,
}

impl Binder<'_> {
    /// `sql`, one query (with `definition`, also a view declaration, whose
    /// query is bound and whose declared name is not), rendered with every
    /// reference resolved in `catalog`.`schema` and bound; `None` when one
    /// cannot be.
    fn bind(&mut self, sql: &str, catalog: &str, schema: &str, definition: bool) -> Option<String> {
        let mut statements = Parser::parse_sql(&SnowflakeDialect {}, sql).ok()?;
        let query: &mut Query = match statements.as_mut_slice() {
            [Statement::Query(query)] => query,
            [Statement::CreateView(view)] if definition => &mut view.query,
            _ => return None,
        };
        let mut references = References {
            catalog: canonical_identifier(catalog),
            schema: canonical_identifier(schema),
            frames: Vec::new(),
            resolved: Vec::new(),
            unbound: false,
        };
        let _ = query.visit(&mut references);
        if references.unbound {
            return None;
        }
        let mut bound = Vec::with_capacity(references.resolved.len());
        // Bind each reference: a query dependency to its definition, a base
        // relation to itself or its substitute.
        for relation in references.resolved {
            bound.push(
                match self
                    .dependencies
                    .iter()
                    .position(|(name, _)| *name == relation)
                {
                    Some(d) => {
                        if self.stack.contains(&d) {
                            return None;
                        }
                        self.stack.push(d);
                        let dependency = self.dependencies[d].1;
                        let definition = self.bind(
                            &dependency.query,
                            &dependency.default_catalog,
                            &dependency.default_schema,
                            true,
                        );
                        self.stack.pop();
                        let mut h = Sha256::new();
                        field(&mut h, definition?.as_bytes());
                        format!("\"__dbt_state_view__\".\"{}\"", hex::encode(h.finalize()))
                    }
                    None => {
                        let read = (self.substitute)(&relation).unwrap_or_else(|| relation.clone());
                        self.base_reads.push(relation);
                        quoted(&read)
                    }
                },
            );
        }
        let mut rendered = query.to_string();
        // Later placeholders first, so `…1` is not replaced inside `…10`.
        for (i, bound) in bound.iter().enumerate().rev() {
            rendered = rendered.replace(&format!("\"{PLACEHOLDER}{i}\""), bound);
        }
        Some(rendered)
    }
}

/// One query being visited: its own CTEs and the CTE scope of its body.
struct Frame {
    ctes: Vec<(*const Query, String)>,
    outer: Vec<String>,
    body: Vec<String>,
}

/// Resolves every relation reference, in visit order, and rewrites it to a
/// numbered placeholder.
struct References {
    catalog: String,
    schema: String,
    frames: Vec<Frame>,
    resolved: Vec<Relation>,
    unbound: bool,
}

impl References {
    fn resolve(&self, parts: Vec<String>) -> Option<Relation> {
        let resolved = match parts.len() {
            1 if !self.catalog.is_empty() && !self.schema.is_empty() => {
                vec![self.catalog.clone(), self.schema.clone(), parts[0].clone()]
            }
            2 if !self.catalog.is_empty() => {
                vec![self.catalog.clone(), parts[0].clone(), parts[1].clone()]
            }
            3 => parts,
            _ => return None,
        };
        resolved.iter().all(|p| !p.is_empty()).then_some(resolved)
    }

    /// What a one-part name means here: a CTE in scope (`Some(true)`), a
    /// name Snowflake may resolve to a later sibling CTE (`None`), or a
    /// relation (`Some(false)`).
    fn cte(&self, name: &str) -> Option<bool> {
        let frame = self.frames.last()?;
        if frame.body.iter().any(|cte| cte == name) {
            return Some(true);
        }
        let later = self
            .frames
            .iter()
            .any(|f| f.ctes.iter().any(|(_, cte)| cte == name));
        (!later).then_some(false)
    }

    /// Record `relation` and rewrite the reference to its placeholder.
    fn placeholder(&mut self, relation: Relation) -> ObjectName {
        let name = format!("{PLACEHOLDER}{}", self.resolved.len());
        self.resolved.push(relation);
        ObjectName(vec![ObjectNamePart::Identifier(Ident::with_quote(
            '"', name,
        ))])
    }
}

fn ident_key(id: &Ident) -> String {
    match id.quote_style {
        Some(_) => id.value.clone(),
        None => id.value.to_ascii_uppercase(),
    }
}

fn string_literal(v: &Value) -> Option<&str> {
    match v {
        Value::SingleQuotedString(s) => Some(s),
        _ => None,
    }
}

/// The literal a single-argument `IDENTIFIER(...)` call names.
fn identifier_literal(args: &[FunctionArg]) -> Option<&str> {
    match args {
        [FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(v)))] => string_literal(&v.value),
        _ => None,
    }
}

fn is_pure(name: &ObjectName) -> bool {
    matches!(name.0.as_slice(),
        [ObjectNamePart::Identifier(id)]
            if PURE_TABLE_FUNCTIONS.contains(&id.value.to_ascii_uppercase().as_str()))
}

/// What a `TABLE(<expr>)` factor reads: the relation a table literal names
/// (`Ok(Some)`), nothing for a pure table function (`Ok(None)`), or
/// something unknown (`Err`).
fn table_function(expr: &Expr) -> Result<Option<&str>, ()> {
    match expr {
        Expr::Value(v) => string_literal(&v.value).map(Some).ok_or(()),
        Expr::Function(f) => {
            let is_identifier = matches!(f.name.0.as_slice(),
                [ObjectNamePart::Identifier(id)] if id.value.eq_ignore_ascii_case("identifier"));
            if is_identifier {
                let FunctionArguments::List(list) = &f.args else {
                    return Err(());
                };
                identifier_literal(&list.args).map(Some).ok_or(())
            } else if is_pure(&f.name) {
                Ok(None)
            } else {
                Err(())
            }
        }
        _ => Err(()),
    }
}

impl VisitorMut for References {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        let this: *const Query = query;
        let outer = match self.frames.last() {
            None => Vec::new(),
            Some(parent) => match parent
                .ctes
                .iter()
                .position(|(cte, _)| std::ptr::eq(*cte, this))
            {
                // A CTE's definition sees the scope around its query, its
                // earlier siblings and itself.
                Some(i) => {
                    let mut scope = parent.outer.clone();
                    scope.extend(parent.ctes[..=i].iter().map(|(_, name)| name.clone()));
                    scope
                }
                None => parent.body.clone(),
            },
        };
        let ctes: Vec<(*const Query, String)> = query
            .with
            .as_ref()
            .map(|with| {
                with.cte_tables
                    .iter()
                    .map(|c| (&*c.query as *const Query, ident_key(&c.alias.name)))
                    .collect()
            })
            .unwrap_or_default();
        let mut body = outer.clone();
        body.extend(ctes.iter().map(|(_, name)| name.clone()));
        self.frames.push(Frame { ctes, outer, body });
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &mut Query) -> ControlFlow<()> {
        self.frames.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<()> {
        let relation = match factor {
            TableFactor::Table { name, args, .. } => {
                if args.is_some() {
                    if !is_pure(name) {
                        self.unbound = true;
                    }
                    return ControlFlow::Continue(());
                }
                let parts = match name.0.as_slice() {
                    [ObjectNamePart::Function(f)]
                        if f.name.value.eq_ignore_ascii_case("identifier") =>
                    {
                        identifier_literal(&f.args).map(canonical_name)
                    }
                    parts => parts
                        .iter()
                        .map(|p| match p {
                            ObjectNamePart::Identifier(id) => Some(ident_key(id)),
                            _ => None,
                        })
                        .collect(),
                };
                let Some(parts) = parts else {
                    self.unbound = true;
                    return ControlFlow::Continue(());
                };
                if let [only] = parts.as_slice() {
                    match self.cte(only) {
                        Some(true) => return ControlFlow::Continue(()),
                        Some(false) => {}
                        None => {
                            self.unbound = true;
                            return ControlFlow::Continue(());
                        }
                    }
                }
                match self.resolve(parts) {
                    Some(relation) => {
                        *name = self.placeholder(relation);
                        return ControlFlow::Continue(());
                    }
                    None => None,
                }
            }
            TableFactor::TableFunction { expr, alias } => match table_function(expr) {
                Ok(None) => return ControlFlow::Continue(()),
                Ok(Some(literal)) => self.resolve(canonical_name(literal)).map(|relation| {
                    let name = self.placeholder(relation);
                    (name, alias.take())
                }),
                Err(()) => None,
            }
            .map(|(name, alias)| {
                *factor = TableFactor::Table {
                    name,
                    alias,
                    args: None,
                    with_hints: Vec::new(),
                    version: None,
                    with_ordinality: false,
                    partitions: Vec::new(),
                    json_path: None,
                    sample: None,
                    index_hints: Vec::new(),
                };
            }),
            TableFactor::Function { name, .. } => is_pure(name).then_some(()),
            TableFactor::Derived { .. }
            | TableFactor::NestedJoin { .. }
            | TableFactor::UNNEST { .. }
            | TableFactor::Pivot { .. }
            | TableFactor::Unpivot { .. } => Some(()),
            _ => None,
        };
        if relation.is_none() {
            self.unbound = true;
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(sql: &str, schema: &str) -> qc::SqlExecution {
        qc::SqlExecution {
            dialect: "snowflake".into(),
            default_catalog: "DB".into(),
            default_schema: Some(schema.into()),
            sql: sql.into(),
            ..Default::default()
        }
    }

    fn binding(req: &qc::SqlExecution) -> Option<PhysicalBinding> {
        physical_binding(req, &|_| None)
    }

    fn reads(sql: &str, schema: &str) -> Option<Vec<String>> {
        binding(&request(sql, schema)).map(|b| b.base_reads.iter().map(|r| r.join(".")).collect())
    }

    #[test]
    fn references_resolve_in_the_default_catalog_and_schema() {
        assert_eq!(
            reads("select * from u cross join s.u cross join db2.s.u", "dev"),
            Some(vec!["DB.DEV.U".into(), "DB.S.U".into(), "DB2.S.U".into()])
        );
    }

    #[test]
    fn the_same_reads_bound_to_other_positions_differ() {
        let sql = "select local.v from u local cross join aaa.u a cross join zzz.u z";
        let prod = binding(&request(sql, "AAA")).unwrap();
        let dev = binding(&request(sql, "ZZZ")).unwrap();
        assert_ne!(prod.hash, dev.hash);
        // Fully qualified, the same SQL in two schemas binds alike.
        let fixed = "select v from db.aaa.u cross join db.zzz.u";
        assert_eq!(
            binding(&request(fixed, "AAA")).unwrap().hash,
            binding(&request(fixed, "ZZZ")).unwrap().hash
        );
    }

    #[test]
    fn a_substituted_copy_binds_as_its_source() {
        let source = binding(&request("select v from db.prod.u", "prod")).unwrap();
        let copy = physical_binding(&request("select v from db.dev.u", "dev"), &|r| {
            (r == ["DB", "DEV", "U"]).then(|| vec!["DB".into(), "PROD".into(), "U".into()])
        })
        .unwrap();
        assert_eq!(copy.hash, source.hash);
        assert_eq!(
            copy.base_reads,
            vec![vec!["DB".to_string(), "DEV".into(), "U".into()]]
        );
    }

    #[test]
    fn ctes_are_not_reads_and_recursion_needs_no_keyword() {
        assert_eq!(
            reads("with c as (select v from db.s.t) select v from c", "dev"),
            Some(vec!["DB.S.T".into()])
        );
        assert_eq!(
            reads(
                "with r(n) as (select n from db.aaa.fixed union all select n + 1 from r where n < 3) select n from r",
                "dev"
            ),
            Some(vec!["DB.AAA.FIXED".into()])
        );
        // A later sibling's name is resolved differently across warehouses.
        assert_eq!(
            reads(
                "with a as (select * from b), b as (select 1 as x) select * from a",
                "dev"
            ),
            None
        );
    }

    #[test]
    fn table_literals_are_reads_and_pure_table_functions_are_not() {
        assert_eq!(
            reads("select v from table('u')", "dev"),
            Some(vec!["DB.DEV.U".into()])
        );
        assert_eq!(
            reads("select v from identifier('s.u')", "dev"),
            Some(vec!["DB.S.U".into()])
        );
        assert_eq!(
            reads("select v from table(identifier('u'))", "dev"),
            Some(vec!["DB.DEV.U".into()])
        );
        assert_eq!(
            reads(
                "select f.value from table(flatten(input => array_construct(1))) f",
                "dev"
            ),
            Some(vec![])
        );
        assert_eq!(
            reads("select * from table(result_scan(last_query_id()))", "dev"),
            None
        );
        assert_eq!(reads("select * from table($1)", "dev"), None);
    }

    #[test]
    fn a_view_is_bound_to_its_definition_in_its_own_context() {
        let mut req = request("select l.v from db.p.v l cross join db.q.v r", "zzz");
        let view = |schema: &str| qc::QueryDependency {
            name: format!("DB.{schema}.V"),
            query: "select v from u".into(),
            default_catalog: "DB".into(),
            default_schema: schema.into(),
        };
        req.query_dependencies = vec![view("P"), view("Q")];
        let b = binding(&req).unwrap();
        assert_eq!(
            b.base_reads,
            vec![
                vec!["DB".to_string(), "P".into(), "U".into()],
                vec!["DB".into(), "Q".into(), "U".into()]
            ]
        );
        // The declared name of a view is no read.
        req.query_dependencies[0].query = "create or replace view v as (select 1 as v)".into();
        assert_eq!(
            binding(&req).unwrap().base_reads,
            vec![vec!["DB".to_string(), "Q".into(), "U".into()]]
        );
    }
}
