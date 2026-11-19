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
/// execution type, decision and evaluation setting, and values in both a
/// node's own SQL and a query dependency, render nothing. A form without a
/// rule renders nothing: `zipf` and the `seqN` generators and functions
/// outside the detector set such as `current_user()` (hosted renders none,
/// #202 entries 20, 25 and 31), and calls with other arguments are uncaptured.
pub(super) const VOLATILE_RULES: crate::volatile_evaluation::Rules = {
    use crate::query_cache::ModelExecutionType::{DbtDataTest, Full, View};
    use crate::volatile_evaluation::{Call, Cell, Outcome, Queries, Rule, Rules, Value};
    const fn rule(name: &'static str, call: Call, key: &'static str, value: Value) -> Rule {
        Rule {
            name,
            call,
            key,
            value,
        }
    }
    const fn cell(
        execution_type: crate::query_cache::ModelExecutionType,
        evaluation_requested: bool,
        outcome: Outcome,
        queries: Queries,
    ) -> Cell {
        Cell {
            execution_type,
            evaluation_requested,
            outcome,
            queries,
        }
    }
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
            // Evaluation on: a table's first builds (run 1 entry 6, run 2
            // entries 13–24, `clone_fallback_volatile.jsonl` entries 0 and 2)
            // and its unchanged rerun (run 1 entry 33).
            cell(Full, true, Outcome::AnyBuild, Queries::Own),
            // A table over a volatile view, first build and rerun (run 1
            // entries 10 and 35).
            cell(Full, true, Outcome::AnyBuild, Queries::Dependencies),
            // A data test without a Target relation, first execution and
            // rerun (run 1 entries 16 and 37).
            cell(DbtDataTest, true, Outcome::AnyBuild, Queries::Own),
            // Evaluation off: a table's first build and reuses (run 1
            // entries 8, 34 and 50), and a view's (entries 7 and 32).
            cell(Full, false, Outcome::AnyBuild, Queries::Own),
            cell(Full, false, Outcome::Reuse, Queries::Own),
            cell(View, false, Outcome::AnyBuild, Queries::Own),
            cell(View, false, Outcome::Reuse, Queries::Own),
        ],
    }
};

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
