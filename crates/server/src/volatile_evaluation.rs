//! Volatile evaluation (GLOSSARY; #67, ADR 0005 "Amended by #67").
//!
//! When a submit requests Volatile evaluation (`tolerate_nondeterminism =
//! false`, which current clients send for `evaluate_volatile_sql: true`), the
//! State service renders each Volatile function in the SQL to a value when it
//! decides, and reports it with the decision in `transformed_nodes_by_query`.
//! The Warehouse still executes the original SQL; no official client reads
//! the map, so it is telemetry. Values are rendered only in the cells a
//! Warehouse was captured rendering in ([`Cell`]), which on Snowflake include
//! evaluation off, a reuse and a Result copy; every other submit returns an
//! empty map.
//!
//! This module is pure. The submit decision layer calls [`evaluate`] after the
//! decision is made, with the [`Sources`] the values are drawn from, and the
//! Warehouse supplies the [`Rules`] for the functions it has captures for
//! ([`crate::warehouse::Warehouse::volatile_rules`]). A function without a
//! captured rule renders nothing, and so does SQL that does not parse.
//!
//! Rendered values are never part of an identity, fingerprint, Upstream
//! producer or Reuse qualification: the decision is made before they exist.
//! They are persisted with the decision for Explain only.

use sqlparser::ast::{Expr, FunctionArguments, ObjectNamePart, Visit, Visitor};
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;
use std::ops::ControlFlow;

use crate::decision::DecisionReason;
use crate::query_cache as qc;
use crate::reuse_candidate::RerunEvidence;

/// What rendered values are drawn from. Injected by the gRPC adapter, so the
/// pure decision engine and this module stay free of I/O.
#[derive(Debug, Clone, Copy)]
pub struct Sources {
    /// The server clock in Unix microseconds, UTC. The test-only replay clock
    /// sets it in tests (`crate::Clock::Replay`).
    pub now_micros: i64,
    /// A uniform random number in `[0, 1)`.
    pub random: fn() -> f64,
    /// A random (version 4) UUID.
    pub uuid: fn() -> uuid::Uuid,
}

impl Sources {
    /// The server clock and the operating system's random source.
    pub fn system(now_micros: i64) -> Self {
        Self {
            now_micros,
            random: rand::random::<f64>,
            uuid: uuid::Uuid::new_v4,
        }
    }
}

/// One Warehouse's captured rendering rules and the dialect its SQL is
/// parsed in to find them.
#[derive(Debug, Clone, Copy)]
pub struct Rules {
    pub dialect: &'static dyn Dialect,
    pub functions: &'static [Rule],
    /// The cells hosted was captured rendering values in. A submit in any
    /// other cell renders nothing.
    pub cells: &'static [Cell],
}

/// A kind of submit and decision hosted was captured rendering values in.
#[derive(Debug, Clone, Copy)]
pub struct Cell {
    pub execution_type: qc::ModelExecutionType,
    /// Whether the submit requested Volatile evaluation ([`requested`]).
    pub evaluation_requested: bool,
    pub outcome: Outcome,
    /// The SQL the values come from.
    pub queries: Queries,
    /// The function forms a cell was captured with, one per submit
    /// ([`Cell::with_functions`]). `None`: any rule.
    functions: Option<&'static [Rule]>,
    without_upstream_tables: bool,
}

/// A [`Cell`], for the Warehouses' const cell lists.
pub const fn cell(
    execution_type: qc::ModelExecutionType,
    evaluation_requested: bool,
    outcome: Outcome,
    queries: Queries,
) -> Cell {
    Cell {
        execution_type,
        evaluation_requested,
        outcome,
        queries,
        functions: None,
        without_upstream_tables: false,
    }
}

impl Cell {
    /// Narrow the cell to the function forms it was captured with: it then
    /// renders only SQL that calls exactly one distinct form of `functions`
    /// and no other function, as a single query without query dependencies
    /// (the Databricks cells, #213). Calling one listed form beside another,
    /// listed or not, is uncaptured and renders nothing.
    pub const fn with_functions(mut self, functions: &'static [Rule]) -> Self {
        self.functions = Some(functions);
        self
    }

    pub const fn without_upstream_tables(mut self) -> Self {
        self.without_upstream_tables = true;
        self
    }
}

/// The decisions a [`Cell`] covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// A build for this reason only ([`Decided::Build`]).
    BuildFor(DecisionReason),
    /// A build refused at Reuse qualification for its volatile SQL alone
    /// ([`Decided::RefusedVolatileSql`]), whatever else is against the
    /// refused build.
    RefusedVolatileSql,
    /// [`Outcome::RefusedVolatileSql`] in an unchanged rerun only: nothing
    /// but the volatile SQL is against the refused build
    /// ([`Decided::RefusedVolatileSql`] with [`RerunEvidence::Unchanged`]).
    RefusedVolatileSqlUnchangedRerun,
    /// The same refusal after the Target relation or the sole input
    /// changed, as the Warehouse established it from the input's State
    /// (`Warehouse::changed_volatile_rerun`, #213).
    RefusedVolatileSqlChangedRerun,
    /// A reuse (`skip_execution`).
    Reuse,
    /// A Result copy (`ready_to_clone`) into a Target relation that does not
    /// exist. A copy over an existing Target relation is not covered.
    CopyIntoAbsentTarget,
}

impl Outcome {
    fn covers(self, decided: Decided) -> bool {
        match (self, decided) {
            (Self::RefusedVolatileSql, Decided::RefusedVolatileSql { .. })
            | (
                Self::RefusedVolatileSqlUnchangedRerun,
                Decided::RefusedVolatileSql {
                    rerun: RerunEvidence::Unchanged,
                },
            )
            | (
                Self::RefusedVolatileSqlChangedRerun,
                Decided::RefusedVolatileSql {
                    rerun: RerunEvidence::TargetChanged | RerunEvidence::InputModifiedExternally,
                },
            )
            | (Self::Reuse, Decided::Reuse)
            | (
                Self::CopyIntoAbsentTarget,
                Decided::Copy {
                    target_exists: false,
                },
            ) => true,
            (Self::BuildFor(captured), Decided::Build(reason)) => captured == reason,
            _ => false,
        }
    }
}

/// What a submit was decided: values are only ever rendered for a build, a
/// reuse or a Result copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Decided {
    /// A build for this reason. A build refused at Reuse qualification for
    /// any cause other than [`Decided::RefusedVolatileSql`]'s is
    /// `Build(NoReuseCandidate(FailedReuseQualification))`.
    Build(DecisionReason),
    /// A build refused at Reuse qualification for its volatile SQL alone,
    /// compared by its rendered SQL
    /// ([`crate::reuse_candidate::refuses_volatile_sql_alone`]).
    RefusedVolatileSql {
        rerun: RerunEvidence,
    },
    Reuse,
    /// A Result copy, and whether the decision saw the Target relation exist.
    Copy {
        target_exists: bool,
    },
}

/// The SQL a [`Cell`]'s values come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Queries {
    /// The root SQL, keyed by the Target relation as sent, or for a data
    /// test without one by [`TARGET_QUERY_KEY`].
    Own,
    /// Query dependency definitions, keyed by the dependency's name.
    Dependencies,
    /// Both: values in the root SQL and in a query dependency definition,
    /// each keyed as for [`Queries::Own`] and [`Queries::Dependencies`].
    OwnAndDependencies,
}

/// Whether `req` requests Volatile evaluation: current clients send
/// `tolerate_nondeterminism = false` for `evaluate_volatile_sql: true`.
pub fn requested(req: &qc::SubmitEnrichedSqlRequest) -> bool {
    !req.tolerate_nondeterminism
}

/// The outer map key hosted reports a data test's own values under: a data
/// test has no Target relation (`explain_volatile_evaluation.jsonl` entries
/// 16 and 37).
pub const TARGET_QUERY_KEY: &str = "__target_query__";

/// A Volatile function hosted was captured rendering, and how.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rule {
    /// The builtin's name, lower case. Matches an unquoted, unqualified call
    /// written as [`Rule::call`], in any case.
    pub name: &'static str,
    pub call: Call,
    /// The function text hosted reports as the inner map key (`RAND()`).
    pub key: &'static str,
    pub value: Value,
}

/// The call form a [`Rule`] matches. Each form is matched only as captured:
/// hosted keys some forms of one builtin differently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Call {
    /// `name()`, an empty argument list.
    Empty,
    /// `name` without parentheses (`current_date`).
    Parenless,
    /// `name(…)` with exactly these arguments as the parser prints them,
    /// in any case (`1, 100, random()`).
    Arguments(&'static str),
}

/// How a rule renders its value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Value {
    /// A uniform random number in `[0, 1)`, as the shortest decimal that
    /// round-trips (`0.2867523899434695`).
    UnitRandom,
    /// The server clock in UTC to the microsecond, without an offset
    /// (`2026-10-12 21:52:38.577832`).
    UtcTimestampMicros,
    /// The server clock in UTC to the microsecond, with the UTC offset
    /// (`2026-11-17 16:48:51.782884+00:00`).
    UtcTimestampMicrosWithOffset,
    /// The server clock's UTC time of day to the microsecond (`16:48:51.782956`).
    UtcTimeMicros,
    /// The server clock's UTC date (`2026-11-17`).
    UtcDate,
    /// A random UUID in lower case (`8f162c59-9bda-412e-b5de-d0d22af57bf9`).
    Uuid,
    /// The same value on every render: hosted's value for a generator called
    /// with a constant seed (`RANDSTR(8, 1234)` -> `9XChaf68`).
    Constant(&'static str),
}

impl Value {
    fn render(self, sources: &Sources) -> String {
        let clock = |format| {
            chrono::DateTime::from_timestamp_micros(sources.now_micros)
                .unwrap_or_default()
                .format(format)
                .to_string()
        };
        match self {
            Self::UnitRandom => (sources.random)().to_string(),
            Self::UtcTimestampMicros => clock("%Y-%m-%d %H:%M:%S%.6f"),
            Self::UtcTimestampMicrosWithOffset => clock("%Y-%m-%d %H:%M:%S%.6f+00:00"),
            Self::UtcTimeMicros => clock("%H:%M:%S%.6f"),
            Self::UtcDate => clock("%Y-%m-%d"),
            Self::Uuid => (sources.uuid)().hyphenated().to_string(),
            Self::Constant(value) => value.to_owned(),
        }
    }
}

/// One rendered Volatile function: the query it was rendered for (the
/// request's Target relation as sent), the function text and the value.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RenderedValue {
    pub query: String,
    pub function: String,
    pub value: String,
}

/// The values one decision rendered, in the order the functions first
/// appear in the SQL. Never empty.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct Rendered(Vec<RenderedValue>);

impl Rendered {
    pub fn values(&self) -> &[RenderedValue] {
        &self.0
    }

    /// The wire `transformed_nodes_by_query` map.
    pub fn to_wire(&self) -> std::collections::HashMap<String, qc::NodeFuncMapping> {
        let mut map = std::collections::HashMap::<String, qc::NodeFuncMapping>::new();
        for v in &self.0 {
            map.entry(v.query.clone())
                .or_default()
                .node_to_func
                .insert(v.function.clone(), v.value.clone());
        }
        map
    }
}

/// What a decision records about its Volatile evaluation, for Explain only
/// (ADR 0005, "Amended by #67"). The default, no evidence, is every decision
/// other than a SQL submit's and one recorded before migrations 0027 and
/// 0028; it prints no volatile lines.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Evidence {
    /// Whether the SQL submit requested Volatile evaluation ([`requested`],
    /// migration 0028).
    pub requested: Option<bool>,
    /// The values rendered, as returned (migration 0027). Empty when none
    /// were, and for unreadable stored values.
    pub values: Vec<RenderedValue>,
}

impl Evidence {
    /// The evidence of a decided SQL submit that rendered `rendered`.
    pub fn of_submit(req: &qc::SubmitEnrichedSqlRequest, rendered: Option<&Rendered>) -> Self {
        Self {
            requested: Some(requested(req)),
            values: rendered.map(|r| r.0.clone()).unwrap_or_default(),
        }
    }

    /// The evidence stored in `node_decisions.rendered_volatile_values` and
    /// `node_decisions.volatile_evaluation_requested`.
    pub fn stored(values: Option<serde_json::Value>, requested: Option<bool>) -> Self {
        Self {
            requested,
            values: values
                .and_then(|v| serde_json::from_value(v).ok())
                .unwrap_or_default(),
        }
    }

    /// The `node_decisions.rendered_volatile_values` value: NULL when
    /// nothing was rendered.
    pub fn stored_values(&self) -> Option<serde_json::Value> {
        (!self.values.is_empty())
            .then(|| serde_json::to_value(&self.values).expect("rendered volatile values"))
    }
}

/// Render the Volatile functions of `req`'s SQL that `rules` has a rule
/// for, when `req` and its decision fall in a captured [`Cell`]. `None` when
/// nothing was rendered.
///
/// The root SQL is keyed by the Target relation as sent, or for a data test
/// without one by [`TARGET_QUERY_KEY`]; each query dependency definition by
/// the dependency's name, in request order (`explain_volatile_evaluation.jsonl`
/// entries 10 and 16). Values in both a node's own SQL and a query dependency
/// are listed dependencies first, as hosted printed them
/// (`explain_volatile_cells.jsonl` entries 59 and 159; the wire map has no
/// order). Each function is rendered once per query however often it is
/// called. SQL that does not parse renders nothing, and so does a data test
/// with a Target relation.
pub(crate) fn evaluate(
    req: &qc::SubmitEnrichedSqlRequest,
    rules: Option<Rules>,
    decided: Decided,
    sources: &Sources,
) -> Option<Rendered> {
    let rules = rules?;
    let target = req.target_table.as_deref().filter(|t| !t.is_empty());
    // A data test with a Target relation (stored failures) is uncaptured.
    let own = if req.execution_type == qc::ModelExecutionType::DbtDataTest as i32 {
        if target.is_some() {
            return None;
        }
        Some(TARGET_QUERY_KEY)
    } else {
        target
    };
    let root = called_rules(rules, &req.sql);
    let own_calls: Vec<_> = own
        .into_iter()
        .flat_map(|query| root.rules.iter().copied().map(move |rule| (query, rule)))
        .collect();
    let dependency_calls: Vec<_> = req
        .query_dependencies
        .iter()
        .flat_map(|dep| {
            // A dependency's shape is never read: a `Cell::with_functions`
            // cell, the only reader of `unchecked`, admits no query
            // dependencies at all.
            called_rules(rules, &dep.query)
                .rules
                .into_iter()
                .map(|rule| (dep.name.as_str(), rule))
        })
        .collect();
    let queries = match (own_calls.is_empty(), dependency_calls.is_empty()) {
        (false, true) => Queries::Own,
        (true, false) => Queries::Dependencies,
        (false, false) => Queries::OwnAndDependencies,
        (true, true) => return None,
    };
    let count = own_calls.len() + dependency_calls.len();
    let calls = dependency_calls.into_iter().chain(own_calls);
    let captured = rules.cells.iter().any(|cell| {
        cell.execution_type as i32 == req.execution_type
            && cell.evaluation_requested == requested(req)
            && cell.outcome.covers(decided)
            && cell.queries == queries
            // `Cell::with_functions`: exactly one distinct listed form, in
            // one checked query without dependencies.
            && cell.functions.is_none_or(|functions| {
                !root.unchecked
                    && req.query_dependencies.is_empty()
                    && count == 1
                    && calls.clone().all(|(_, rule)| functions.contains(rule))
            })
            && (!cell.without_upstream_tables
                || req
                    .tables
                    .iter()
                    .all(|table| Some(&table.name) == req.target_table.as_ref())
                    && crate::sql_identity::reads_relation(&req.sql, rules.dialect) == Some(false))
    });
    if !captured {
        return None;
    }
    let mut values: Vec<RenderedValue> = Vec::new();
    for (query, rule) in calls {
        if !values
            .iter()
            .any(|v| v.query == query && v.function == rule.key)
        {
            values.push(RenderedValue {
                query: query.to_owned(),
                function: rule.key.to_owned(),
                value: rule.value.render(sources),
            });
        }
    }
    Some(Rendered(values))
}

/// The rules one query calls.
struct Calls {
    /// Each rule the SQL calls, once, first call first.
    rules: Vec<&'static Rule>,
    /// Whether the SQL contains an unrecognized function or is outside the
    /// checked single-query shape (it does not parse, or is not one query).
    unchecked: bool,
}

/// The rules `sql` calls.
fn called_rules(rules: Rules, sql: &str) -> Calls {
    let Ok(statements) = Parser::parse_sql(rules.dialect, sql) else {
        return Calls {
            rules: Vec::new(),
            unchecked: true,
        };
    };
    let mut finder = Finder {
        rules: rules.functions,
        found: Vec::new(),
        unchecked: !matches!(statements.as_slice(), [sqlparser::ast::Statement::Query(_)]),
    };
    let _ = statements.visit(&mut finder);
    Calls {
        rules: finder.found,
        unchecked: finder.unchecked,
    }
}

/// Collects the rules whose function the SQL calls, first call first.
struct Finder {
    rules: &'static [Rule],
    found: Vec<&'static Rule>,
    unchecked: bool,
}

impl Visitor for Finder {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        if let Some(rule) = called_rule(self.rules, expr) {
            if !self.found.contains(&rule) {
                self.found.push(rule);
            }
        } else if matches!(expr, Expr::Function(_)) {
            self.unchecked = true;
        }
        ControlFlow::Continue(())
    }
}

/// The rule `expr` calls: an unquoted, unqualified `name` in the call form
/// a rule names, without clauses. Comments and string literals never reach
/// the visitor, and a quoted name is a user object.
fn called_rule(rules: &'static [Rule], expr: &Expr) -> Option<&'static Rule> {
    let (actual_name, args) = call_form(expr)?;
    rules
        .iter()
        .find(|rule| matches_form(actual_name, args.as_deref(), rule.name, rule.call))
}

pub(crate) fn matches_call(expr: &Expr, name: &str, call: Call) -> bool {
    call_form(expr)
        .is_some_and(|(actual_name, args)| matches_form(actual_name, args.as_deref(), name, call))
}

fn call_form(expr: &Expr) -> Option<(&str, Option<String>)> {
    let Expr::Function(f) = expr else {
        return None;
    };
    let [ObjectNamePart::Identifier(name)] = f.name.0.as_slice() else {
        return None;
    };
    if name.quote_style.is_some()
        || f.uses_odbc_syntax
        || !matches!(f.parameters, FunctionArguments::None)
        || f.filter.is_some()
        || f.null_treatment.is_some()
        || f.over.is_some()
        || !f.within_group.is_empty()
    {
        return None;
    }
    let arguments = match &f.args {
        FunctionArguments::None => None,
        FunctionArguments::List(args)
            if args.duplicate_treatment.is_none() && args.clauses.is_empty() =>
        {
            Some(
                args.args
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", "),
            )
        }
        _ => return None,
    };
    Some((&name.value, arguments))
}

fn matches_form(actual_name: &str, args: Option<&str>, name: &str, call: Call) -> bool {
    actual_name.eq_ignore_ascii_case(name)
        && match (call, args) {
            (Call::Empty, Some(args)) => args.is_empty(),
            (Call::Parenless, None) => true,
            (Call::Arguments(expected), Some(args)) => {
                !expected.is_empty() && args.eq_ignore_ascii_case(expected)
            }
            _ => false,
        }
}
