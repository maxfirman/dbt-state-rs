//! Volatile evaluation (GLOSSARY; #67, ADR 0005 "Amended by #67").
//!
//! When a submit requests Volatile evaluation (`tolerate_nondeterminism =
//! false`, which current clients send for `evaluate_volatile_sql: true`), the
//! State service renders each Volatile function in the SQL to a value when it
//! decides, and reports it with the decision in `transformed_nodes_by_query`.
//! The Warehouse still executes the original SQL; no official client reads
//! the map, so it is telemetry. Values are rendered only in the cells a
//! Warehouse was captured rendering in ([`Cell`]), which on Snowflake include
//! evaluation off and a reuse; every other submit returns an empty map.
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
    /// The SQL the values come from. Values from both a node's own SQL and a
    /// query dependency are captured in no cell.
    pub queries: Queries,
}

/// The decisions a [`Cell`] covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// A build (`ready_to_execute`) for any reason.
    AnyBuild,
    /// A build for this reason only.
    BuildFor(DecisionReason),
    /// A reuse (`skip_execution`).
    Reuse,
}

impl Outcome {
    fn covers(self, decided: Decided) -> bool {
        match (self, decided) {
            (Self::AnyBuild, Decided::Build(_)) | (Self::Reuse, Decided::Reuse) => true,
            (Self::BuildFor(captured), Decided::Build(reason)) => captured == reason,
            _ => false,
        }
    }
}

/// What a submit was decided: values are only ever rendered for a build or
/// a reuse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decided {
    Build(DecisionReason),
    Reuse,
}

/// The SQL a [`Cell`]'s values come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Queries {
    /// The root SQL, keyed by the Target relation as sent, or for a data
    /// test without one by [`TARGET_QUERY_KEY`].
    Own,
    /// Query dependency definitions, keyed by the dependency's name.
    Dependencies,
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
#[derive(Debug, Clone, Copy)]
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

/// Render the Volatile functions of `req`'s SQL that `rules` has a rule
/// for, when `req` and its decision fall in a captured [`Cell`]. `None` when
/// nothing was rendered.
///
/// The root SQL is keyed by the Target relation as sent, or for a data test
/// without one by [`TARGET_QUERY_KEY`]; each query dependency definition by
/// the dependency's name, in request order (`explain_volatile_evaluation.jsonl`
/// entries 10 and 16). Each function is rendered once per query however often
/// it is called. SQL that does not parse renders nothing, and so does a
/// submit with values in both its own SQL and a query dependency.
pub fn evaluate(
    req: &qc::SubmitEnrichedSqlRequest,
    rules: Option<Rules>,
    decided: Decided,
    sources: &Sources,
) -> Option<Rendered> {
    let rules = rules?;
    let captured = |queries| {
        rules.cells.iter().any(|cell| {
            cell.execution_type as i32 == req.execution_type
                && cell.evaluation_requested == requested(req)
                && cell.outcome.covers(decided)
                && cell.queries == queries
        })
    };
    let target = req.target_table.as_deref().filter(|t| !t.is_empty());
    // A data test with a Target relation (stored failures) is uncaptured.
    let own = if req.execution_type == qc::ModelExecutionType::DbtDataTest as i32 {
        target.is_none().then_some(TARGET_QUERY_KEY)
    } else {
        target
    };
    let render = |queries: &mut dyn Iterator<Item = (&str, &str)>| {
        let mut values: Vec<RenderedValue> = Vec::new();
        for (query, sql) in queries {
            for rule in called_rules(rules, sql) {
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
        }
        values
    };
    let own_values = render(&mut own.map(|query| (query, req.sql.as_str())).into_iter());
    let dependency_values = render(
        &mut req
            .query_dependencies
            .iter()
            .map(|d| (d.name.as_str(), d.query.as_str())),
    );
    let values = match (own_values.is_empty(), dependency_values.is_empty()) {
        (false, true) if captured(Queries::Own) => own_values,
        (true, false) if captured(Queries::Dependencies) => dependency_values,
        _ => return None,
    };
    Some(Rendered(values))
}

/// The rules `sql` calls, first call first; none when it does not parse.
fn called_rules(rules: Rules, sql: &str) -> Vec<&'static Rule> {
    let Ok(statements) = Parser::parse_sql(rules.dialect, sql) else {
        return Vec::new();
    };
    let mut finder = Finder {
        rules: rules.functions,
        found: Vec::new(),
    };
    let _ = statements.visit(&mut finder);
    finder.found
}

/// Collects the rules whose function the SQL calls, first call first.
struct Finder {
    rules: &'static [Rule],
    found: Vec<&'static Rule>,
}

impl Visitor for Finder {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        if let Some(rule) = called_rule(self.rules, expr) {
            if !self.found.iter().any(|r| r.key == rule.key) {
                self.found.push(rule);
            }
        }
        ControlFlow::Continue(())
    }
}

/// The rule `expr` calls: an unquoted, unqualified `name` in the call form
/// a rule names, without clauses. Comments and string literals never reach
/// the visitor, and a quoted name is a user object.
fn called_rule(rules: &'static [Rule], expr: &Expr) -> Option<&'static Rule> {
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
    rules.iter().find(|rule| {
        name.value.eq_ignore_ascii_case(rule.name)
            && match (rule.call, arguments.as_deref()) {
                (Call::Empty, Some(args)) => args.is_empty(),
                (Call::Parenless, None) => true,
                (Call::Arguments(expected), Some(args)) => {
                    !expected.is_empty() && args.eq_ignore_ascii_case(expected)
                }
                _ => false,
            }
    })
}
