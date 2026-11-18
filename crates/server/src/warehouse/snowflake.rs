//! Snowflake rules. Result copying is a zero-copy CLONE, and the copy logic
//! identity is the schema-abstracted namespace logic (see `fingerprint`).

use super::{CopyIdentity, NamespaceIdentity};
use sqlparser::dialect::SnowflakeDialect;

/// Volatile evaluation (#67): the function forms hosted was captured
/// rendering, with hosted's key and value format for each. `CURRENT_TIMESTAMP()`
/// first in #193 (`clone_fallback_volatile.jsonl` entries 0 and 2); every other
/// form in #191/#194 (`explain_volatile_evaluation.jsonl` entry 6, the forms of
/// `explain_volatile_evaluation_functions.jsonl` entries 13–24).
///
/// Several forms fold into one key, and a key is rendered once per submit.
/// Hosted renders values with evaluation off as well, and on a reuse (run 1
/// entries 8, 34, 50). A form without a rule renders nothing: `zipf` and the
/// `seqN` generators (hosted renders none), a parenless `current_time` or
/// `localtime`, and calls with other arguments are uncaptured.
pub(super) const VOLATILE_RULES: crate::volatile_evaluation::Rules = {
    use crate::volatile_evaluation::{Call, Rule, Rules, Value};
    const fn rule(name: &'static str, call: Call, key: &'static str, value: Value) -> Rule {
        Rule {
            name,
            call,
            key,
            value,
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
            rule("uuid_string", Call::Empty, "UUID_STRING()", Value::Uuid),
        ],
        evaluation_off: true,
        on_skip: true,
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
