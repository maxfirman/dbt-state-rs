//! Snowflake physical bindings (#264, ADR 0007 "Result copy over different
//! inputs"): which physical relation each relation reference of a workload
//! reads. A Result copy qualifies only when the submit, each copy it reads
//! taken as the build it stands for, binds every reference as the build it
//! would copy did. The schema-abstracted copy logic identity cannot tell:
//! it abstracts the schemas that decide what an unqualified or
//! schema-relative reference reads.
//!
//! Each relation reference in FROM or JOIN is resolved as Snowflake
//! resolves it. In the root SQL, a partly qualified name is completed with
//! the request's default catalog and schema, and a one-part name goes
//! through the session's `SEARCH_PATH`, which no request carries, so it is
//! resolved only when the one relation the workload reports or defines
//! under that name is the one in the default schema. Inside a view the
//! search path is not used: its names resolve in the view's own database
//! and schema, whatever context the request declares for it. A CTE
//! reference stays a CTE: a CTE is in scope in its query's body and nested
//! queries, in its later siblings and in its own definition (`RECURSIVE` is
//! optional on Snowflake). A table literal (`TABLE('<name>')`,
//! `IDENTIFIER('<name>')`) is resolved like a name. A reference to a query
//! dependency is bound to its definition, itself so resolved, so two views
//! of one definition in one schema bind alike and swapping definitions
//! between references does not. SQL reading the database or schema it runs
//! in (`CURRENT_DATABASE()`, `CURRENT_SCHEMA()`, and in the root SQL
//! `SYS_CONTEXT('SNOWFLAKE$SESSION', 'DATABASE' | 'SCHEMA')`) is bound with
//! each value it reads: the session's for the root SQL, the view's own
//! inside a view. Anything else that could read a relation or a context the
//! request does not identify (a session database or schema the request
//! omits, a one-part name of a later sibling CTE, a table function outside
//! the pure set, an unsupported FROM construct, Time Travel,
//! `CURRENT_SCHEMAS()`, any other `SYS_CONTEXT`, a cycle of dependencies,
//! SQL that does not parse) leaves the workload without a physical binding. So do two definitions of one view (however
//! its name is spelled), a view's declared columns with a type or options,
//! and a traversal with a definition deeper than [`MAX_DEPTH`] on any path
//! or wider than [`MAX_VISITS`] references. A view's declared column names
//! are bound with its query: they rename its output. The bound SQL is
//! normalized as Snowflake identity is (`sql_norm::normalize_sql`), so an
//! unquoted identifier's case does not matter.
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
const VERSION: &[u8] = b"dbt-state-physical-binding-v4";

/// Table functions that read no relation.
const PURE_TABLE_FUNCTIONS: &[&str] = &[
    "FLATTEN",
    "GENERATOR",
    "SPLIT_TO_TABLE",
    "STRTOK_SPLIT_TO_TABLE",
];

/// The context values a query reads ([`References::context`]).
#[derive(Debug, Default, Clone, Copy)]
struct Context {
    /// The database it runs in: `CURRENT_DATABASE()`,
    /// `SYS_CONTEXT('SNOWFLAKE$SESSION', 'DATABASE')`.
    database: bool,
    /// The schema it runs in: `CURRENT_SCHEMA()`,
    /// `SYS_CONTEXT('SNOWFLAKE$SESSION', 'SCHEMA')`.
    schema: bool,
}

/// The most nested query dependency definitions bound
/// (`docs/architecture.md`, dependency traversal).
pub(crate) const MAX_DEPTH: usize = 32;

/// The most relation references bound in one workload, every definition
/// counted once.
pub(crate) const MAX_VISITS: usize = 1024;

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
    // One definition per view, named in full: two definitions of one view,
    // however spelled, leave which one the warehouse read unknown.
    for (i, (name, _)) in dependencies.iter().enumerate() {
        if name.len() != 3 || dependencies[..i].iter().any(|(other, _)| other == name) {
            return None;
        }
    }
    // What a one-part name could resolve to: each relation reported or
    // defined.
    let known: Vec<Relation> = req
        .tables
        .iter()
        .map(|t| canonical_name(&t.name))
        .chain(dependencies.iter().map(|(name, _)| name.clone()))
        .collect();
    let mut binder = Binder {
        dependencies: &dependencies,
        known: &known,
        substitute,
        base_reads: Vec::new(),
        stack: Vec::new(),
        definitions: vec![None; dependencies.len()],
        deepest: 0,
        visits: 0,
    };
    let catalog = canonical_identifier(&req.default_catalog);
    let schema = canonical_identifier(req.default_schema.as_deref().unwrap_or(""));
    let root = binder.bind(&req.sql, catalog, schema, false)?;
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
    /// Every relation reported or defined ([`References::resolve`]).
    known: &'a [Relation],
    substitute: &'a dyn Fn(&[String]) -> Option<Relation>,
    base_reads: Vec<Relation>,
    /// The query dependencies being bound, to refuse a cycle.
    stack: Vec<usize>,
    /// Each query dependency's bound definition, once bound, with the
    /// definition levels it spans (itself and the deepest below it).
    definitions: Vec<Option<(String, usize)>>,
    /// The deepest definition level reached so far ([`MAX_DEPTH`]).
    deepest: usize,
    /// The relation references bound so far ([`MAX_VISITS`]).
    visits: usize,
}

impl Binder<'_> {
    /// `sql`, one query, rendered with every reference resolved in
    /// `catalog`.`schema` (canonical components) and bound; `None` when one
    /// cannot be. `in_view` when `sql` defines a view (also a view
    /// declaration, whose query is bound and whose declared name is not);
    /// otherwise it is the root SQL, the only SQL whose one-part names
    /// resolve through the session's `SEARCH_PATH`.
    fn bind(
        &mut self,
        sql: &str,
        catalog: String,
        schema: String,
        in_view: bool,
    ) -> Option<String> {
        let mut statements = Parser::parse_sql(&SnowflakeDialect {}, sql).ok()?;
        let (query, columns): (&mut Query, String) = match statements.as_mut_slice() {
            [Statement::Query(query)] => (query, String::new()),
            [Statement::CreateView(declared)] if in_view => {
                // The declared column names rename the view's output.
                let columns: Option<Vec<String>> = declared
                    .columns
                    .iter()
                    .map(|c| {
                        (c.data_type.is_none() && c.options.is_none()).then(|| ident_key(&c.name))
                    })
                    .collect();
                let columns = columns?;
                (&mut declared.query, format!("({}) ", quoted(&columns)))
            }
            _ => return None,
        };
        let mut references = References {
            catalog,
            schema,
            known: (!in_view).then_some(self.known),
            frames: Vec::new(),
            resolved: Vec::new(),
            context: Context::default(),
            unbound: false,
        };
        let _ = query.visit(&mut references);
        self.visits += references.resolved.len();
        if references.unbound || self.visits > MAX_VISITS {
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
                    Some(d) => self.definition(d)?,
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
        // Each context value read, the session's in the root SQL and the
        // view's own inside a view; one the request does not supply leaves
        // no binding.
        let mut context = String::new();
        for (read, name, value) in [
            (references.context.database, "database", &references.catalog),
            (references.context.schema, "schema", &references.schema),
        ] {
            if read {
                if value.is_empty() {
                    return None;
                }
                context.push_str(&format!("{name} {} ", quoted(std::slice::from_ref(value))));
            }
        }
        Some(format!(
            "{context}{columns}{}",
            crate::sql_norm::normalize_sql(&rendered)
        ))
    }

    /// Query dependency `d` bound: its definition, bound once in the
    /// view's own schema, as an opaque relation; `None` on a cycle, when
    /// any of its definitions would lie deeper than [`MAX_DEPTH`] on this
    /// path, or when the definition cannot be bound.
    fn definition(&mut self, d: usize) -> Option<String> {
        // The definition levels above `d` on this path.
        let above = self.stack.len();
        if let Some((bound, levels)) = &self.definitions[d] {
            if above + levels > MAX_DEPTH {
                return None;
            }
            self.deepest = self.deepest.max(above + levels);
            return Some(bound.clone());
        }
        if self.stack.contains(&d) || above >= MAX_DEPTH {
            return None;
        }
        // Snowflake resolves a view's names in the view's own schema,
        // whatever context the request declares for it.
        let dependencies = self.dependencies;
        let (name, dependency) = &dependencies[d];
        let outer = std::mem::replace(&mut self.deepest, above + 1);
        self.stack.push(d);
        let definition = self.bind(&dependency.query, name[0].clone(), name[1].clone(), true);
        self.stack.pop();
        let levels = self.deepest - above;
        self.deepest = self.deepest.max(outer);
        let mut h = Sha256::new();
        field(&mut h, definition?.as_bytes());
        let bound = format!("\"__dbt_state_view__\".\"{}\"", hex::encode(h.finalize()));
        self.definitions[d] = Some((bound.clone(), levels));
        Some(bound)
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
struct References<'a> {
    catalog: String,
    schema: String,
    /// Every relation reported or defined, for the root SQL's one-part
    /// names; `None` inside a view.
    known: Option<&'a [Relation]>,
    frames: Vec<Frame>,
    resolved: Vec<Relation>,
    /// The context values the SQL reads.
    context: Context,
    unbound: bool,
}

impl References<'_> {
    fn resolve(&self, parts: Vec<String>) -> Option<Relation> {
        let resolved = match parts.len() {
            1 if !self.catalog.is_empty() && !self.schema.is_empty() => {
                let resolved = vec![self.catalog.clone(), self.schema.clone(), parts[0].clone()];
                // In the root SQL, through `SEARCH_PATH`: only when the one
                // relation of that name the workload reports or defines is
                // the default schema's. A view's are its schema's only.
                if let Some(known) = self.known {
                    let mut named = known.iter().filter(|known| known.last() == parts.last());
                    if named.next() != Some(&resolved) || named.any(|other| *other != resolved) {
                        return None;
                    }
                }
                resolved
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
        [arg] => literal_arg(arg),
        _ => None,
    }
}

/// `arg` when it is a string literal.
fn literal_arg(arg: &FunctionArg) -> Option<&str> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(v))) => string_literal(&v.value),
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

impl VisitorMut for References<'_> {
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

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        let Expr::Function(f) = expr else {
            return ControlFlow::Continue(());
        };
        let [ObjectNamePart::Identifier(id)] = f.name.0.as_slice() else {
            return ControlFlow::Continue(());
        };
        match id.value.to_ascii_uppercase().as_str() {
            "CURRENT_DATABASE" => self.context.database = true,
            "CURRENT_SCHEMA" => self.context.schema = true,
            // The search path, which no request carries.
            "CURRENT_SCHEMAS" => self.unbound = true,
            "SYS_CONTEXT" => {
                // The session's database or schema, in the root SQL. Any
                // other property, namespace or call, or one inside a view,
                // reads a context the request does not identify.
                let property = match &f.args {
                    FunctionArguments::List(list) => match list.args.as_slice() {
                        [namespace, property] => literal_arg(namespace)
                            .filter(|n| n.eq_ignore_ascii_case("SNOWFLAKE$SESSION"))
                            .and(literal_arg(property))
                            .map(str::to_ascii_uppercase),
                        _ => None,
                    },
                    _ => None,
                };
                match property.as_deref() {
                    Some("DATABASE") if self.known.is_some() => self.context.database = true,
                    Some("SCHEMA") if self.known.is_some() => self.context.schema = true,
                    _ => self.unbound = true,
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<()> {
        let relation = match factor {
            TableFactor::Table {
                name,
                args,
                version,
                ..
            } => {
                // Time Travel (`AT`, `BEFORE`, `CHANGES`) reads a version
                // the recorded epochs do not identify: an `OFFSET` is
                // relative to the query's own time.
                if version.is_some() {
                    self.unbound = true;
                    return ControlFlow::Continue(());
                }
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

    /// A Snowflake request in catalog `DB` and `schema`, reporting `tables`.
    fn request(sql: &str, schema: &str, tables: &[&str]) -> qc::SqlExecution {
        qc::SqlExecution {
            dialect: "snowflake".into(),
            default_catalog: "DB".into(),
            default_schema: Some(schema.into()),
            sql: sql.into(),
            tables: tables
                .iter()
                .map(|name| qc::TableModifiedInfo {
                    name: (*name).into(),
                    last_modified_epoch: Some(1),
                })
                .collect(),
            ..Default::default()
        }
    }

    fn binding(req: &qc::SqlExecution) -> Option<PhysicalBinding> {
        physical_binding(req, &|_| None)
    }

    fn reads(req: &qc::SqlExecution) -> Option<Vec<String>> {
        binding(req).map(|b| b.base_reads.iter().map(|r| r.join(".")).collect())
    }

    fn view(name: &str, query: &str, schema: &str) -> qc::QueryDependency {
        qc::QueryDependency {
            name: name.into(),
            query: query.into(),
            default_catalog: "DB".into(),
            default_schema: schema.into(),
        }
    }

    #[test]
    fn references_resolve_in_the_default_catalog_and_schema() {
        let req = request(
            "select * from u cross join s.t cross join db2.s.w",
            "dev",
            &["DB.DEV.U", "DB.S.T", "DB2.S.W"],
        );
        assert_eq!(
            reads(&req),
            Some(vec!["DB.DEV.U".into(), "DB.S.T".into(), "DB2.S.W".into()])
        );
    }

    #[test]
    fn a_one_part_name_needs_one_candidate_in_the_default_schema() {
        // Unreported, or reported only in another schema: the search path
        // could resolve it anywhere.
        assert_eq!(reads(&request("select * from u", "dev", &[])), None);
        assert_eq!(reads(&request("select * from u", "dev", &["DB.S.U"])), None);
        // Two relations of that name: the search path decides (review of #266).
        assert_eq!(
            reads(&request(
                "select l.v from u l cross join aaa.u a cross join zzz.u z",
                "aaa",
                &["DB.AAA.U", "DB.ZZZ.U"]
            )),
            None
        );
    }

    #[test]
    fn the_same_reads_bound_to_other_positions_differ() {
        let sql = "select l.v from db.aaa.u l cross join db.zzz.u z";
        let swapped = "select l.v from db.zzz.u l cross join db.aaa.u z";
        let tables = ["DB.AAA.U", "DB.ZZZ.U"];
        assert_ne!(
            binding(&request(sql, "aaa", &tables)).unwrap().hash,
            binding(&request(swapped, "aaa", &tables)).unwrap().hash
        );
        // Fully qualified, the same SQL in two schemas binds alike.
        assert_eq!(
            binding(&request(sql, "aaa", &tables)).unwrap().hash,
            binding(&request(sql, "zzz", &tables)).unwrap().hash
        );
    }

    #[test]
    fn an_unquoted_identifiers_case_does_not_change_the_binding() {
        let tables = ["DB.AAA.FIXED"];
        assert_eq!(
            binding(&request("select v from db.aaa.fixed", "zzz", &tables))
                .unwrap()
                .hash,
            binding(&request("select V from DB.AAA.FIXED", "zzz", &tables))
                .unwrap()
                .hash
        );
        assert_ne!(
            binding(&request("select \"v\" from db.aaa.fixed", "zzz", &tables))
                .unwrap()
                .hash,
            binding(&request("select v from db.aaa.fixed", "zzz", &tables))
                .unwrap()
                .hash
        );
    }

    #[test]
    fn a_substituted_copy_binds_as_its_source() {
        let source = binding(&request("select v from db.prod.u", "prod", &["DB.PROD.U"])).unwrap();
        let copy = physical_binding(
            &request("select v from db.dev.u", "dev", &["DB.DEV.U"]),
            &|r| (r == ["DB", "DEV", "U"]).then(|| vec!["DB".into(), "PROD".into(), "U".into()]),
        )
        .unwrap();
        assert_eq!(copy.hash, source.hash);
        assert_eq!(
            copy.base_reads,
            vec![vec!["DB".to_string(), "DEV".into(), "U".into()]]
        );
    }

    #[test]
    fn ctes_are_not_reads_and_recursion_needs_no_keyword() {
        let req = |sql| request(sql, "dev", &["DB.S.T", "DB.AAA.FIXED"]);
        assert_eq!(
            reads(&req("with c as (select v from db.s.t) select v from c")),
            Some(vec!["DB.S.T".into()])
        );
        assert_eq!(
            reads(&req(
                "with r(n) as (select n from db.aaa.fixed union all select n + 1 from r where n < 3) select n from r"
            )),
            Some(vec!["DB.AAA.FIXED".into()])
        );
        // A later sibling's name is resolved differently across warehouses.
        assert_eq!(
            reads(&req(
                "with a as (select * from b), b as (select 1 as x) select * from a"
            )),
            None
        );
    }

    #[test]
    fn table_literals_are_reads_and_pure_table_functions_are_not() {
        let req = |sql| request(sql, "dev", &["DB.DEV.U", "DB.S.W"]);
        assert_eq!(
            reads(&req("select v from table('u')")),
            Some(vec!["DB.DEV.U".into()])
        );
        assert_eq!(
            reads(&req("select v from identifier('s.w')")),
            Some(vec!["DB.S.W".into()])
        );
        assert_eq!(
            reads(&req("select v from table(identifier('u'))")),
            Some(vec!["DB.DEV.U".into()])
        );
        assert_eq!(
            reads(&req(
                "select f.value from table(flatten(input => array_construct(1))) f"
            )),
            Some(vec![])
        );
        assert_eq!(
            reads(&req("select * from table(result_scan(last_query_id()))")),
            None
        );
        assert_eq!(reads(&req("select * from table($1)")), None);
    }

    #[test]
    fn a_view_is_bound_to_its_definition_in_its_own_context() {
        let mut req = request(
            "select l.v from db.p.v l cross join db.q.v r",
            "zzz",
            &["DB.P.U", "DB.Q.U"],
        );
        req.query_dependencies = vec![
            view("DB.P.V", "select v from db.p.u", "P"),
            view("DB.Q.V", "select v from db.q.u", "Q"),
        ];
        assert_eq!(reads(&req), Some(vec!["DB.P.U".into(), "DB.Q.U".into()]));
        // The declared name of a view is no read.
        req.query_dependencies[0].query = "create or replace view v as (select 1 as v)".into();
        assert_eq!(reads(&req), Some(vec!["DB.Q.U".into()]));
    }

    #[test]
    fn a_views_names_resolve_in_its_own_schema_only() {
        // Inside a view the search path is not used: `u` is the view
        // schema's even with another `U` reported (fifth review of #266).
        let mut req = request(
            "select v from db.shared.v",
            "zzz",
            &["DB.SHARED.U", "DB.OTHER.U"],
        );
        req.query_dependencies = vec![view(
            "DB.SHARED.V",
            "create view V as select l.v from u l cross join other.u o",
            "SHARED",
        )];
        assert_eq!(
            reads(&req),
            Some(vec!["DB.SHARED.U".into(), "DB.OTHER.U".into()])
        );
        // The context the request declares for a view does not change it.
        let declared = binding(&req).unwrap().hash;
        req.query_dependencies[0].default_schema = "OTHER".into();
        assert_eq!(binding(&req).unwrap().hash, declared);
    }

    #[test]
    fn the_database_and_schema_a_query_returns_are_bound() {
        let in_view = |schema: &str, body: &str| {
            let mut req = request(
                &format!("select v from db.{schema}.v"),
                schema,
                &["DB.RAW.U"],
            );
            req.query_dependencies = vec![view(&format!("DB.{schema}.V"), body, schema)];
            binding(&req).map(|b| b.hash)
        };
        // Inside a view, the view's own schema.
        let body = "create view V as select current_schema() as v from db.raw.u";
        assert!(in_view("PROD", body).is_some());
        assert_ne!(in_view("PROD", body), in_view("DEV", body));
        let plain = "create view V as select v from db.raw.u";
        assert_eq!(in_view("PROD", plain), in_view("DEV", plain));
        // In the root SQL, the session's, each value only when read.
        let root =
            |schema: &str, sql: &str| binding(&request(sql, schema, &["DB.RAW.U"])).map(|b| b.hash);
        for sql in [
            "select current_schema() as v from db.raw.u",
            "select sys_context('SNOWFLAKE$SESSION', 'SCHEMA') as v from db.raw.u",
        ] {
            assert!(root("PROD", sql).is_some(), "{sql}");
            assert_ne!(root("PROD", sql), root("DEV", sql), "{sql}");
        }
        for sql in [
            "select current_database() as v from db.raw.u",
            "select sys_context('snowflake$session', 'database') as v from db.raw.u",
        ] {
            assert!(root("PROD", sql).is_some(), "{sql}");
            assert_eq!(root("PROD", sql), root("DEV", sql), "{sql}");
        }
        // A value the request does not supply has no binding.
        let mut omitted = request(
            "select current_schema() as v from db.raw.u",
            "PROD",
            &["DB.RAW.U"],
        );
        omitted.default_schema = None;
        assert_eq!(binding(&omitted), None);
        omitted.sql = "select current_database() as v from db.raw.u".into();
        assert!(binding(&omitted).is_some());
        omitted.default_catalog = String::new();
        assert_eq!(binding(&omitted), None);
        // The search path, any other session context, and the session's
        // context inside a view are unknown.
        for sql in [
            "select current_schemas() as v from db.raw.u",
            "select sys_context('SNOWFLAKE$SESSION', 'SCHEMAS') as v from db.raw.u",
            "select sys_context('SNOWFLAKE$SESSION', 'ROLE') as v from db.raw.u",
            "select sys_context('SNOWFLAKE$CURRENT', 'SCHEMA') as v from db.raw.u",
            "select sys_context('SNOWFLAKE$SESSION', $p) as v from db.raw.u",
        ] {
            assert_eq!(root("PROD", sql), None, "{sql}");
        }
        assert_eq!(
            in_view(
                "PROD",
                "create view V as select sys_context('SNOWFLAKE$SESSION', 'SCHEMA') as v from db.raw.u"
            ),
            None
        );
    }

    #[test]
    fn time_travel_has_no_binding() {
        let req = |sql: &str| request(sql, "dev", &["DB.RAW.U"]);
        assert!(binding(&req("select v from db.raw.u")).is_some());
        for clause in [
            "at (offset => -60)",
            "at (timestamp => '2026-11-23 00:00:00'::timestamp_ltz)",
            "before (statement => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726')",
            "changes (information => default) at (offset => -60)",
        ] {
            let sql = format!("select v from db.raw.u {clause}");
            // It parses: the clause, not the parser, declines.
            assert!(
                Parser::parse_sql(&SnowflakeDialect {}, &sql).is_ok(),
                "{clause}"
            );
            assert_eq!(binding(&req(&sql)), None, "{clause}");
        }
    }

    #[test]
    fn a_views_declared_columns_rename_its_output() {
        let with_columns = |columns: &str| {
            let mut req = request("select x from db.s.v", "s", &[]);
            req.query_dependencies = vec![view(
                "DB.S.V",
                &format!("create view v ({columns}) as select 1 as a, 2 as b"),
                "S",
            )];
            binding(&req).map(|b| b.hash)
        };
        assert!(with_columns("x, y").is_some());
        assert_ne!(with_columns("x, y"), with_columns("y, x"));
        // A declared type or option has no binding.
        assert_eq!(with_columns("x int, y"), None);
    }

    #[test]
    fn two_definitions_of_one_view_have_no_binding() {
        let mut req = request("select v from db.s.v", "s", &[]);
        req.query_dependencies = vec![
            view("DB.S.V", "select 'dummy' as v", "S"),
            view("\"DB\".\"S\".\"V\"", "select 'other' as v", "S"),
        ];
        assert_eq!(binding(&req), None);
        // A view named in fewer than three parts has none either.
        req.query_dependencies = vec![view("S.V", "select 1 as v", "S")];
        assert_eq!(binding(&req), None);
    }

    #[test]
    fn traversal_is_bounded_and_binds_each_definition_once() {
        // A chain of MAX_DEPTH views binds; one more does not.
        let chain = |depth: usize| {
            let mut req = request("select v from db.s.v0", "s", &[]);
            req.query_dependencies = (0..depth)
                .map(|i| {
                    let body = if i + 1 == depth {
                        "select 1 as v".to_string()
                    } else {
                        format!("select v from db.s.v{}", i + 1)
                    };
                    view(&format!("DB.S.V{i}"), &body, "S")
                })
                .collect();
            binding(&req)
        };
        assert!(chain(MAX_DEPTH).is_some());
        assert_eq!(chain(MAX_DEPTH + 1), None);
        // A view bound shallow first is still too deep on a longer path:
        // the limit does not depend on traversal order.
        let shared_after = |links: usize| {
            let mut req = request("select * from db.s.a cross join db.s.c0", "s", &[]);
            req.query_dependencies = vec![
                view("DB.S.A", "select v from db.s.b", "S"),
                view("DB.S.B", "select 1 as v", "S"),
            ];
            req.query_dependencies.extend((0..links).map(|i| {
                let next = if i + 1 == links {
                    "a".to_string()
                } else {
                    format!("c{}", i + 1)
                };
                view(
                    &format!("DB.S.C{i}"),
                    &format!("select v from db.s.{next}"),
                    "S",
                )
            }));
            binding(&req)
        };
        assert!(shared_after(MAX_DEPTH - 2).is_some());
        assert_eq!(shared_after(MAX_DEPTH - 1), None);
        // A diamond of shared views, each read twice per level: linear
        // with each definition bound once, exponential otherwise.
        let mut req = request("select * from db.s.d0 a cross join db.s.d0 b", "s", &[]);
        req.query_dependencies = (0..30)
            .map(|i| {
                let body = if i == 29 {
                    "select 1 as v".to_string()
                } else {
                    format!(
                        "select * from db.s.d{n} a cross join db.s.d{n} b",
                        n = i + 1
                    )
                };
                view(&format!("DB.S.D{i}"), &body, "S")
            })
            .collect();
        assert!(binding(&req).is_some());
        // Too many references in all has no binding.
        let wide = (0..=MAX_VISITS)
            .map(|_| "db.s.t")
            .collect::<Vec<_>>()
            .join(" cross join ");
        assert_eq!(
            binding(&request(&format!("select 1 from {wide}"), "s", &["DB.S.T"])),
            None
        );
    }
}
