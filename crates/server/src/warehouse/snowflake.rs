//! Snowflake rules. Result copying is a zero-copy CLONE, and the copy logic
//! identity is the schema-abstracted namespace logic (see `fingerprint`).

use super::{CopyIdentity, NamespaceIdentity};
use sqlparser::dialect::SnowflakeDialect;

/// Volatile evaluation (#67): the function forms hosted was captured
/// rendering, with hosted's key and value format for each. `CURRENT_TIMESTAMP()`
/// first in #193 (`clone_fallback_volatile.jsonl` entries 0 and 2); the forms
/// of #191/#194 (`explain_volatile_evaluation.jsonl` entry 6, the forms of
/// `explain_volatile_evaluation_functions.jsonl` entries 13–24); and the call
/// forms of #202/#203 (`explain_volatile_cells.jsonl` entries 21–36 and
/// 131–139): a parenless `current_time` or `localtime`, `current_timestamp(3)`,
/// and `uniform`, `randstr` and `normal` with the captured arguments.
///
/// Several forms fold into one key, and a key is rendered once per submit.
/// Values are rendered only in the captured cells below; every other
/// execution type, decision reason, evaluation setting and source of values
/// renders nothing. A form without a rule renders nothing: `zipf` and the
/// `seqN` generators and functions outside the detector set such as
/// `current_user()` (hosted renders none, #202 entries 20, 25 and 31), and
/// calls with other arguments are uncaptured.
pub(super) const VOLATILE_RULES: crate::volatile_evaluation::Rules = {
    use crate::decision::{DecisionReason, NoCandidate};
    use crate::query_cache::ModelExecutionType::{
        Append, DbtCustom, DbtDataTest, Full, Merge, Snapshot, View,
    };
    use crate::volatile_evaluation::{cell, Call, Outcome, Queries, RerunForm, Rule, Rules, Value};
    const fn rule(name: &'static str, call: Call, key: &'static str, value: Value) -> Rule {
        Rule {
            name,
            call,
            key,
            value,
        }
    }
    // The captured build reasons. A build for any other reason (upstream
    // data or producer changes, an External modification with evaluation
    // off, an execution type change, a Reuse qualification refusal for any
    // cause but volatile SQL compared by its rendered SQL, ...) renders
    // nothing.
    const FIRST_BUILD: Outcome = Outcome::BuildFor(DecisionReason::FirstBuild);
    const REFUSED_VOLATILE_SQL: Outcome = Outcome::RefusedVolatileSql;
    const QUERY_CHANGED: Outcome = Outcome::BuildFor(DecisionReason::NoReuseCandidate(
        NoCandidate::IdentityMismatch,
    ));
    Rules {
        dialect: &SnowflakeDialect {},
        functions: &[
            rule(
                "current_timestamp",
                Call::Empty,
                "CURRENT_TIMESTAMP()",
                Value::UtcTimestampMicros,
            ),
            rule(
                "current_timestamp",
                Call::Parenless,
                "CURRENT_TIMESTAMP()",
                Value::UtcTimestampMicros,
            ),
            // A precision argument is kept in the key; the value still has six
            // fractional digits (#202 entries 21 and 135).
            rule(
                "current_timestamp",
                Call::Arguments("3"),
                "CURRENT_TIMESTAMP(3)",
                Value::UtcTimestampMicros,
            ),
            rule(
                "localtimestamp",
                Call::Empty,
                "CURRENT_TIMESTAMP()",
                Value::UtcTimestampMicros,
            ),
            // Hosted prints a parenless `localtimestamp` without parentheses
            // (run 2 entry 14), unlike a parenless `current_timestamp`.
            rule(
                "localtimestamp",
                Call::Parenless,
                "CURRENT_TIMESTAMP",
                Value::UtcTimestampMicros,
            ),
            rule(
                "systimestamp",
                Call::Empty,
                "CURRENT_TIMESTAMP()",
                Value::UtcTimestampMicros,
            ),
            rule(
                "getdate",
                Call::Empty,
                "CURRENT_TIMESTAMP()",
                Value::UtcTimestampMicros,
            ),
            rule(
                "sysdate",
                Call::Empty,
                "SYSDATE()",
                Value::UtcTimestampMicrosWithOffset,
            ),
            rule(
                "current_time",
                Call::Empty,
                "CURRENT_TIME",
                Value::UtcTimeMicros,
            ),
            rule(
                "localtime",
                Call::Empty,
                "CURRENT_TIME",
                Value::UtcTimeMicros,
            ),
            // Parenless forms fold into the same key (#202 `explain_volatile_cells.jsonl`
            // entries 23 and 28).
            rule(
                "current_time",
                Call::Parenless,
                "CURRENT_TIME",
                Value::UtcTimeMicros,
            ),
            rule(
                "localtime",
                Call::Parenless,
                "CURRENT_TIME",
                Value::UtcTimeMicros,
            ),
            rule("current_date", Call::Empty, "CURRENT_DATE", Value::UtcDate),
            rule(
                "current_date",
                Call::Parenless,
                "CURRENT_DATE",
                Value::UtcDate,
            ),
            rule("random", Call::Empty, "RANDOM()", Value::UnitRandom),
            // The bounds are ignored: a decimal in [0, 1), and the nested
            // `random()` is rendered under its own key (run 2 entry 18).
            rule(
                "uniform",
                Call::Arguments("1, 100, random()"),
                "UNIFORM(1, 100, RANDOM())",
                Value::UnitRandom,
            ),
            // Constant seeds: the same value in every captured render (run 1
            // entries 6, 8, 33, 34, 50), not the value Snowflake computes.
            rule(
                "randstr",
                Call::Arguments("8, 1234"),
                "RANDSTR(8, 1234)",
                Value::Constant("9XChaf68"),
            ),
            rule(
                "normal",
                Call::Arguments("0, 1, 1234"),
                "NORMAL(0, 1, 1234)",
                Value::Constant("1.0542196419272387"),
            ),
            // #202 `explain_volatile_cells.jsonl` entries 33/134 (`fra`) and
            // 36/131 (`frb`). With a `random()` generator hosted renders a
            // unit decimal whatever the outer function, as for
            // `uniform(1, 100, random())`.
            rule(
                "uniform",
                Call::Arguments("1, 10, random()"),
                "UNIFORM(1, 10, RANDOM())",
                Value::UnitRandom,
            ),
            rule(
                "randstr",
                Call::Arguments("8, random()"),
                "RANDSTR(8, RANDOM())",
                Value::UnitRandom,
            ),
            rule(
                "normal",
                Call::Arguments("0, 1, random()"),
                "NORMAL(0, 1, RANDOM())",
                Value::UnitRandom,
            ),
            // With a constant generator, hosted's value on both runs.
            rule(
                "randstr",
                Call::Arguments("5, 1234"),
                "RANDSTR(5, 1234)",
                Value::Constant("9XCha"),
            ),
            rule(
                "normal",
                Call::Arguments("5, 2, 42"),
                "NORMAL(5, 2, 42)",
                Value::Constant("4.711819340844143"),
            ),
            rule(
                "uniform",
                Call::Arguments("1, 100, 1234"),
                "UNIFORM(1, 100, 1234)",
                Value::Constant("96"),
            ),
            rule("uuid_string", Call::Empty, "UUID_STRING()", Value::Uuid),
        ],
        cells: &[
            // Evaluation on, a table: its first builds (run 1 entry 6, run 2
            // entries 13–24, `clone_fallback_volatile.jsonl` entries 0 and 2),
            // a rerun refused at Reuse qualification, unchanged (run 1 entry
            // 33), after an External modification, over an upstream table
            // or a query dependency (`explain_volatile_cells.jsonl` entries
            // 137, 166, 147), and a query-changed rebuild (entry 142).
            cell(Full, true, FIRST_BUILD, Queries::Own),
            cell(Full, true, REFUSED_VOLATILE_SQL, Queries::Own),
            cell(Full, true, QUERY_CHANGED, Queries::Own),
            // Evaluation on, a table's reuse: volatile SQL is reused under
            // `compare_unrendered_code` with an unchanged template (#206,
            // maintainer decision 1 on #201; `explain_volatile_cells.jsonl`
            // `ecu`, entry 140) and in the checked `uniform(1, 100, 1234)`
            // shape (`checked_volatile_reuse`, maintainer decision on
            // #211; `explain_volatile_generator_merge.jsonl` entry 28).
            cell(Full, true, Outcome::Reuse, Queries::Own),
            // A table over a volatile view, first build and rerun (run 1
            // entries 10 and 35; `explain_volatile_cells.jsonl` entries 72
            // and 168, over an upstream table too).
            cell(Full, true, FIRST_BUILD, Queries::Dependencies),
            cell(Full, true, REFUSED_VOLATILE_SQL, Queries::Dependencies),
            // A data test without a Target relation: first execution, rerun
            // (run 1 entries 16 and 37) and after its logic changed
            // (`explain_volatile_cells.jsonl` entry 148).
            cell(DbtDataTest, true, FIRST_BUILD, Queries::Own),
            cell(DbtDataTest, true, REFUSED_VOLATILE_SQL, Queries::Own),
            cell(DbtDataTest, true, QUERY_CHANGED, Queries::Own),
            // Evaluation off: a table's first build, query-changed rebuild
            // and reuses (run 1 entries 8, 34 and 50;
            // `explain_volatile_cells.jsonl` entry 133), and a view's first
            // build and reuse (run 1 entries 7 and 32).
            cell(Full, false, FIRST_BUILD, Queries::Own),
            cell(Full, false, QUERY_CHANGED, Queries::Own),
            cell(Full, false, Outcome::Reuse, Queries::Own),
            cell(View, false, FIRST_BUILD, Queries::Own),
            cell(View, false, Outcome::Reuse, Queries::Own),
            // Evaluation on: an APPEND incremental, a snapshot and a custom
            // materialization, first build and unchanged rerun
            // (`explain_volatile_cells.jsonl` entries 71/167, 69/165 and
            // 24/143, #205). Their other rebuild reasons, and evaluation off,
            // are uncaptured.
            cell(Append, true, FIRST_BUILD, Queries::Own),
            cell(Append, true, REFUSED_VOLATILE_SQL, Queries::Own),
            cell(Snapshot, true, FIRST_BUILD, Queries::Own),
            cell(Snapshot, true, REFUSED_VOLATILE_SQL, Queries::Own),
            cell(DbtCustom, true, FIRST_BUILD, Queries::Own),
            cell(DbtCustom, true, REFUSED_VOLATILE_SQL, Queries::Own),
            // Evaluation on: a keyed MERGE incremental over a source table,
            // its first build (#211, `explain_volatile_generator_merge.jsonl`
            // entry 7), and its rerun refused at Reuse qualification with its
            // Target relation physically as built and one recorded source
            // whose epoch is known, unchanged (entry 31) or after its data
            // drifted (entry 41; `reuse_candidate::sole_source_rerun`). Every
            // other refusal (no upstream, a dbt-built or unrecorded producer,
            // an unknown or moved-back epoch, a written Target relation even
            // under `ignore_external_modifications`) is uncaptured.
            cell(Merge, true, FIRST_BUILD, Queries::Own),
            cell(
                Merge,
                true,
                Outcome::RefusedVolatileSqlIn(RerunForm::SoleSourceMerge),
                Queries::Own,
            ),
            // Combinations (#204, `explain_volatile_cells.jsonl`). Evaluation
            // on, first build and rerun refused at Reuse qualification: a
            // table with values of its own and in a view dependency (entries
            // 59 and 159); a data test over a volatile view (52, 144); a view
            // (19, 141).
            cell(Full, true, FIRST_BUILD, Queries::OwnAndDependencies),
            cell(
                Full,
                true,
                REFUSED_VOLATILE_SQL,
                Queries::OwnAndDependencies,
            ),
            cell(DbtDataTest, true, FIRST_BUILD, Queries::Dependencies),
            cell(
                DbtDataTest,
                true,
                REFUSED_VOLATILE_SQL,
                Queries::Dependencies,
            ),
            cell(View, true, FIRST_BUILD, Queries::Own),
            cell(View, true, REFUSED_VOLATILE_SQL, Queries::Own),
            // Evaluation off, first build and reuse: a data test (60, 146);
            // a table over a volatile view (58, 158).
            cell(DbtDataTest, false, FIRST_BUILD, Queries::Own),
            cell(DbtDataTest, false, Outcome::Reuse, Queries::Own),
            cell(Full, false, FIRST_BUILD, Queries::Dependencies),
            cell(Full, false, Outcome::Reuse, Queries::Dependencies),
            // Evaluation off: a table's Result copy into its absent dev
            // Target relation, keyed by that relation and rendered at the dev
            // decision (#207; `explain_volatile_cells.jsonl` entry 207).
            cell(Full, false, Outcome::CopyIntoAbsentTarget, Queries::Own),
        ],
    }
};

/// The generator hosted reuses in a source-less FULL model projecting only
/// it and literals (#211, `explain_volatile_generator_merge.jsonl` `gu`,
/// entries 28 and 35; maintainer decision on #211).
const REUSED_GENERATOR: (&str, crate::volatile_evaluation::Call) = (
    "uniform",
    crate::volatile_evaluation::Call::Arguments("1, 100, 1234"),
);

/// Whether `sql` has `gu`'s captured shape: one `SELECT` without `FROM` or
/// any other clause, each projection a number or string literal or
/// [`REUSED_GENERATOR`], which is called at least once. Any other expression,
/// whatever its spelling (`CURRENT_USER`, `CURRENT_USER()`, `FLOOR(1.5)`), is
/// outside it.
fn isolated_generator_projection(sql: &str) -> bool {
    use crate::volatile_evaluation::matches_call;
    use sqlparser::ast::{Expr, Value};
    let (name, call) = REUSED_GENERATOR;
    let mut generator = false;
    let skeleton = super::projection_skeleton(&SnowflakeDialect {}, sql, |expr| {
        let called = matches_call(expr, name, call);
        generator |= called;
        called
            || matches!(expr, Expr::Value(value)
                if matches!(value.value, Value::Number(_, false) | Value::SingleQuotedString(_)))
    });
    generator && skeleton.is_some_and(|query| query == "SELECT checked_projection")
}

/// Whether Reuse qualification admits `req`'s volatile SQL (maintainer
/// decision on #211): hosted reuses the unchanged rerun of `gu`, an ordinary
/// FULL model with Volatile evaluation requested and template comparison
/// off, without query dependencies, reporting no relation but its own Target
/// relation, whose SQL is [`isolated_generator_projection`] and so reads no
/// relation (`explain_volatile_generator_merge.jsonl` entries 28 and 35).
/// The ordinary scope, identity, Target relation and freshness checks still
/// decide the reuse. Any other shape, the same call over an upstream or in
/// another node kind, and other generator arguments keep the refusal
/// (uncaptured).
pub(super) fn checked_volatile_reuse(req: &crate::query_cache::SubmitEnrichedSqlRequest) -> bool {
    req.execution_type == crate::query_cache::ModelExecutionType::Full as i32
        && crate::volatile_evaluation::requested(req)
        && !req.compare_unrendered_code
        && req.query_dependencies.is_empty()
        && req
            .tables
            .iter()
            .all(|table| Some(&table.name) == req.target_table.as_ref())
        && isolated_generator_projection(&req.sql)
}

/// How Explain describes Snowflake's captured volatile decisions beyond the
/// shared forms: the checked reuse ([`checked_volatile_reuse`]) lists its
/// rendered values above `the model query has not changed`, with no
/// tolerance line (`explain_volatile_generator_merge.jsonl` entry 35). With
/// evaluation requested, a reuse compared by its rendered SQL is only ever
/// that one. A reuse under template comparison (#206) prints no volatile
/// lines (`explain_volatile_cells.jsonl` entry 198), also when its macros
/// changed, whether its compiled SQL changed or not.
pub(super) const VOLATILE_EXPLAIN_FORMS: crate::explain::VolatileForms =
    crate::explain::VolatileForms {
        requested_reuse: true,
        ..crate::explain::VolatileForms::NONE
    };

/// The captured form of a rerun of `refused` that Reuse qualification
/// refused, with Volatile evaluation requested (#211,
/// `docs/reports/snowflake-volatile-generator-merge-2026-11-24.md`):
/// - [`RerunForm::SoleSourceMerge`]: a MERGE over one source table, its
///   Target relation physically as built (`reuse_candidate::sole_source_rerun`;
///   entries 31 and 41, Explain 37 and 45);
/// - [`RerunForm::IsolatedGenerator`]: a FULL model without upstreams or
///   query dependencies whose SQL reads no relation, its Target relation
///   physically as built, projecting only literals and [`REUSED_GENERATOR`]
///   ([`isolated_generator_projection`]; entry 28, Explain 35). Against an
///   ordinary build this shape qualifies ([`checked_volatile_reuse`]), so
///   the decision keeps the form only for a rerun refused over a confirmed
///   Result copy (a refusal under template comparison is not one for
///   volatile SQL alone, and keeps no form).
///
/// [`RerunForm::SoleSourceMerge`]: crate::volatile_evaluation::RerunForm::SoleSourceMerge
/// [`RerunForm::IsolatedGenerator`]: crate::volatile_evaluation::RerunForm::IsolatedGenerator
pub(super) fn volatile_rerun_form(
    req: &crate::query_cache::SubmitEnrichedSqlRequest,
    evidence: &crate::execution::ExecutionEvidence,
    refused: &crate::store::ExecutionRow,
) -> Option<crate::volatile_evaluation::RerunForm> {
    use crate::query_cache::ModelExecutionType::{Full, Merge};
    use crate::reuse_candidate::{own_target_as_built, sole_source_rerun};
    use crate::volatile_evaluation::{requested, RerunForm};
    if !requested(req) {
        return None;
    }
    let target = evidence.target_table.as_str();
    let inputs = &evidence.input_tables;
    match crate::query_cache::ModelExecutionType::try_from(req.execution_type).ok()? {
        Merge
            if sole_source_rerun(
                req.execution_type,
                target,
                inputs,
                &evidence.inputs_without_state,
                refused,
            ) =>
        {
            Some(RerunForm::SoleSourceMerge)
        }
        Full if req.query_dependencies.is_empty()
            && evidence.sql_reads_relation == Some(false)
            && inputs.iter().all(|t| t.name == target)
            && refused.input_tables.iter().all(|t| t.name == target)
            && own_target_as_built(target, inputs, refused)
            && isolated_generator_projection(&req.sql) =>
        {
            Some(RerunForm::IsolatedGenerator)
        }
        _ => None,
    }
}

/// Verified live: the same model in two environments shares this identity.
pub(super) const NAMESPACE_IDENTITY: NamespaceIdentity = NamespaceIdentity::SchemaAbstracted {
    version: b"dbt-state-ns-logic-v4",
};

/// Every Snowflake submission can take part in a result copy, paired with its
/// equivalent by the schema-abstracted namespace logic identity.
pub(super) fn copy_identity() -> CopyIdentity {
    CopyIdentity::NamespaceLogic
}

/// Whether the root SQL or any query dependency calls a volatile function,
/// per the checked Snowflake parser and volatile-function set.
pub(super) fn has_volatile_sql<'a>(
    root: &str,
    dependencies: impl IntoIterator<Item = &'a str>,
) -> bool {
    crate::sql_norm::request_has_volatile_sql(root, dependencies)
}

/// The clone statement, matching the hosted service's formatting (leading
/// newline and indentation) and preserving the source object kind, which
/// Snowflake reports as e.g. "TRANSIENT TABLE".
pub(super) fn copy_ddl(source: &str, target: &str, source_table_type: Option<&str>) -> Vec<String> {
    let kind = match source_table_type.map(|s| s.to_ascii_uppercase()) {
        Some(t) if t.contains("TRANSIENT") => "TRANSIENT TABLE",
        _ => "TABLE",
    };
    vec![format!(
        "\n            CREATE OR REPLACE {kind} {target}\n            CLONE {source}\n            COPY GRANTS"
    )]
}

/// Whether the reported relation `name` is exactly the three-part relation
/// `parts` (database, schema, identifier): an unquoted identifier folds to
/// upper case and a quoted one is exact, on both sides.
pub(super) fn names_relation(name: &str, parts: [&str; 3]) -> bool {
    use crate::sql_norm::{canonical_identifier, split_object_name_components};
    let components = split_object_name_components(name);
    components.len() == parts.len()
        && parts.iter().all(|p| !p.trim().is_empty())
        && components
            .iter()
            .zip(parts)
            .all(|(c, p)| canonical_identifier(c) == canonical_identifier(p))
}

/// The resolved identifier of the reported relation `name` when it is a
/// three-part relation in `parts` (database, schema), under the same folding
/// rules as [`names_relation`].
pub(super) fn relation_in_schema(name: &str, parts: [&str; 2]) -> Option<String> {
    use crate::sql_norm::{canonical_identifier, split_object_name_components};
    let components = split_object_name_components(name);
    let [database, schema, identifier] = components.as_slice() else {
        return None;
    };
    (parts.iter().all(|p| !p.trim().is_empty())
        && canonical_identifier(database) == canonical_identifier(parts[0])
        && canonical_identifier(schema) == canonical_identifier(parts[1]))
    .then(|| canonical_identifier(identifier))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #211: only `gu`'s captured shape counts as the isolated generator:
    /// literals and `uniform(1, 100, 1234)` in one `SELECT` with no other
    /// clause. Another expression of any spelling, a `FROM` (also of the
    /// model's own Target relation) or another clause rules it out.
    #[test]
    fn isolated_generator_projection_is_gus_captured_shape_only() {
        for sql in [
            "select 1 as id, uniform(1, 100, 1234) as u",
            "select 1 as id, 'x' as v, UNIFORM(1, 100, 1234) as u",
            "select uniform(1, 100, 1234)",
        ] {
            assert!(isolated_generator_projection(sql), "{sql}");
        }
        for sql in [
            "select 1 as id",
            "select 1 as id, uniform(1, 100, 1234) as u, current_user as w",
            "select 1 as id, uniform(1, 100, 1234) as u, current_user() as w",
            "select 1 as id, uniform(1, 100, 1234) as u, floor(1.5) as w",
            "select 1 as id, uniform(1, 100, 1234) as u, current_timestamp(6) as w",
            "select 1 as id, uniform(1, 100, 1234) as u, seq4() as w",
            "select 1 as id, uniform(1, 100, 1234) as u, random() as w",
            "select 1 as id, uniform(1, 100, 1234) as u, id as w",
            "select 1 as id, uniform(1, 100, 1234) + 1 as u",
            "select 1 as id, uniform(1, 100, 99) as u",
            "select 1 as id, uniform(1, 100, 1234) as u from \"DB\".\"S\".\"GU\"",
            "select 1 as id, uniform(1, 100, 1234) as u where 1 = 1",
            "select 1 as id, uniform(1, 100, 1234) as u limit 1",
            "with c as (select 1) select uniform(1, 100, 1234) as u",
            "select uniform(1, 100, 1234) union all select 1",
            "select uniform(1, 100, 1234); select 1",
            "not sql",
        ] {
            assert!(!isolated_generator_projection(sql), "{sql}");
        }
    }
}
