//! Restricted Snowflake AST canonicalization for versioned fingerprints.
//! Preserve cast kinds, parameters and qualified function identities. Unsupported
//! syntax stays byte-for-byte identical; other dialects bypass this module.
//! Individual positive equivalences are captured in SQL_NORMALIZATION.md, not
//! proof of a complete semantic equivalence catalog.

use std::ops::ControlFlow;

use sqlparser::ast::{CastKind, DataType, Expr, Ident, ObjectNamePart, VisitMut, VisitorMut};
use sqlparser::dialect::SnowflakeDialect;
use sqlparser::parser::Parser;

/// Normalize SQL to a canonical string for the match-key hash. Parser-backed
/// with exact preservation on parser failure.
pub fn normalize_sql(sql: &str) -> String {
    normalize_sql_inner(sql, false)
}

/// Like [`normalize_sql`] but ABSTRACTS the schema component of three-part
/// object names (`catalog.schema.table` → `catalog.<ns>.table`). This yields a
/// logic identity that is identical for the SAME model built in different
/// environments (e.g. prod vs a dev clone), which differ only by the physical
/// schema qualifier in their rendered SQL. VERIFIED LIVE against
/// api.state.dbt.com: the hosted service skips a dev submit whose rendered SQL
/// differs from the prod source ONLY in the schema qualifier, and executes when
/// the projection/logic genuinely changes (see experiments/clone_logic_
/// sensitivity.sh and docs/correctness-handoff.md). This is scoped to
/// cross-environment (namespace-grouped) reuse; the strict `normalize_sql`
/// remains the primary same-target identity.
pub fn normalize_sql_namespace_abstracted(sql: &str) -> String {
    normalize_sql_inner(sql, true)
}

/// Schema-abstract a dependency's physical object NAME so the (reference →
/// definition) binding can be keyed across environments.
///
/// A dependency reference like `"CDO"."PROD"."STG_ORDERS"` differs from its dev
/// clone `"CDO"."DEV"."STG_ORDERS"` ONLY in the middle (schema) component. To
/// bind each dependency reference to its definition in the cross-environment
/// (namespace-abstracted) identity WITHOUT reintroducing an environment-specific
/// difference, we abstract the schema component of the name exactly as
/// `normalize_sql_namespace_abstracted` does for in-SQL relations, and
/// case-fold unquoted parts.
///
/// Rules (mirroring the in-SQL relation visitor):
///   * Split into dot-separated parts, each optionally double-quoted.
///   * Only a THREE-part name has its middle component replaced with the schema
///     placeholder; one- and two-part names are left intact (a two-part
///     `schema.table` is ambiguous and abstracting it risks conflation).
///   * An UNQUOTED part is lowercased; a quoted part keeps its exact case (the
///     quotes themselves are normalized away so `"V1"` and `V1` compare equal
///     only when case matches — Snowflake uppercases unquoted idents, but we do
///     not resolve that here; the goal is a STABLE cross-env key, and the client
///     sends consistently-quoted names within one project).
///
/// This is a string-level transform (dependency names are not full SQL
/// statements, so the AST path does not apply) and is intentionally
/// conservative: when a name does not look like a dotted identifier it is
/// returned lowercased-but-otherwise-verbatim, which can only ever make two
/// genuinely different names LESS likely to collide (safe: over-execute).
pub fn abstract_object_name_schema(name: &str) -> String {
    let parts: Vec<&str> = name.split('.').collect();
    let unquote = |p: &str| -> String {
        let t = p.trim();
        if t.len() >= 2 && t.starts_with('"') && t.ends_with('"') {
            // Quoted: strip the surrounding quotes, un-double embedded quotes,
            // preserve case.
            t[1..t.len() - 1].replace("\"\"", "\"")
        } else {
            // Unquoted: Snowflake-style case-insensitive — lowercase.
            t.to_ascii_lowercase()
        }
    };
    if parts.len() == 3 {
        format!(
            "{}.{}.{}",
            unquote(parts[0]),
            SCHEMA_PLACEHOLDER,
            unquote(parts[2])
        )
    } else {
        parts
            .iter()
            .map(|p| unquote(p))
            .collect::<Vec<_>>()
            .join(".")
    }
}

fn normalize_sql_inner(sql: &str, abstract_schema: bool) -> String {
    match Parser::parse_sql(&SnowflakeDialect {}, sql) {
        Ok(mut stmts) => {
            let mut canon = Canonicalizer { abstract_schema };
            for s in stmts.iter_mut() {
                let _ = s.visit(&mut canon);
            }
            stmts
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
                .join("; ")
        }
        // Preserve unsupported syntax exactly. A partial tokenizer cannot prove
        // equivalence for dialect extensions, escaped strings or malformed SQL.
        _ => sql.to_string(),
    }
}

/// Placeholder substituted for the schema component under schema abstraction.
const SCHEMA_PLACEHOLDER: &str = "__dbt_state_ns__";

/// Canonical spelling for a type-name synonym, else `None` (leave as written).
/// Small, documented set verified live (Snowflake): string family and numeric
/// family. Unknown types fall through — anything not unified here simply
/// over-executes (safe), matching our "accept parser differences" stance.
fn canon_type(dt: &DataType) -> Option<DataType> {
    use DataType::*;
    // Snowflake STRING family → VARCHAR.
    let is_string = matches!(dt, Text | String(None) | Varchar(None));
    // Snowflake NUMBER family → NUMERIC. NUMBER/NUMERIC/DECIMAL with no
    // precision are equivalent; INT/INTEGER/BIGINT/SMALLINT are NUMBER(38,0).
    let is_number = matches!(
        dt,
        Int(None)
            | Integer(None)
            | BigInt(None)
            | SmallInt(None)
            | TinyInt(None)
            | Numeric(sqlparser::ast::ExactNumberInfo::None)
            | Decimal(sqlparser::ast::ExactNumberInfo::None)
            | Dec(sqlparser::ast::ExactNumberInfo::None)
    ) || matches!(dt, Custom(name, args)
    if args.is_empty()
        && name.0.len() == 1
        && matches!(
            name.0[0].as_ident().filter(|i| i.quote_style.is_none()).map(|i| i.value.to_ascii_lowercase()).as_deref(),
            Some("number") | Some("numeric") | Some("decimal") | Some("int")
                | Some("integer") | Some("bigint") | Some("smallint") | Some("tinyint")
        ));
    if is_string {
        Some(Varchar(None))
    } else if is_number {
        Some(Numeric(sqlparser::ast::ExactNumberInfo::None))
    } else {
        None
    }
}

/// Canonical spelling for a function-name synonym, else `None`. Small verified
/// set (Snowflake). Unknown functions fall through (over-execute = safe).
fn canon_function(name: &str) -> Option<&'static str> {
    match name.to_ascii_lowercase().as_str() {
        "nvl" | "ifnull" => Some("coalesce"),
        _ => None,
    }
}

/// AST pass applying the dialect-aware canonicalizations the hosted service does.
struct Canonicalizer {
    /// When true, replace the schema component of three-part object names so the
    /// same model in different environments shares a logic identity.
    abstract_schema: bool,
}
impl VisitorMut for Canonicalizer {
    type Break = ();

    fn pre_visit_relation(&mut self, name: &mut sqlparser::ast::ObjectName) -> ControlFlow<()> {
        // A fully-qualified `catalog.schema.table` reference abstracts its
        // middle (schema) component. Two-part (`schema.table`) and one-part
        // names are left intact: a bare `schema.table` is ambiguous and
        // abstracting it would risk conflating genuinely different relations.
        if self.abstract_schema && name.0.len() == 3 {
            if let ObjectNamePart::Identifier(id) = &mut name.0[1] {
                id.value = SCHEMA_PLACEHOLDER.to_string();
                id.quote_style = None;
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_ident(&mut self, id: &mut Ident) -> ControlFlow<()> {
        // Fold case of UNQUOTED identifiers/keywords; leave quoted idents alone.
        if id.quote_style.is_none() {
            id.value = id.value.to_ascii_lowercase();
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<()> {
        match e {
            // `x::T` ≡ `CAST(x AS T)` — unify to CAST, and canonicalize the type.
            Expr::Cast {
                kind, data_type, ..
            } => {
                if *kind == CastKind::DoubleColon {
                    *kind = CastKind::Cast;
                }
                if let Some(c) = canon_type(data_type) {
                    *data_type = c;
                }
            }
            // Function-name synonyms (nvl/ifnull → coalesce, …).
            Expr::Function(f) if f.name.0.len() == 1 => {
                if let Some(ObjectNamePart::Identifier(id)) = f.name.0.last_mut() {
                    if id.quote_style.is_none() {
                        if let Some(c) = canon_function(&id.value) {
                            id.value = c.to_string();
                        }
                    }
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// Legacy lexical helper; not used for persisted fingerprints.
/// Strips comments, collapses inter-token whitespace, case-folds keywords and
/// unquoted identifiers (preserving string literals and quoted identifiers),
/// canonicalizes `!=`→`<>`, drops trailing commas/semicolons. Does NOT do any
/// AST-level synonym canonicalization. Equivalent inputs under these rules
/// produce identical output.
pub fn normalize_sql_lexer(sql: &str) -> String {
    // This legacy helper only recognizes ASCII syntax. Preserve other inputs
    // rather than slicing through a UTF-8 character or changing its identity.
    if !sql.is_ascii() {
        return sql.to_string();
    }
    let mut out: Vec<String> = Vec::new();
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    let n = bytes.len();

    // Accumulator for a run of "bareword" chars (identifiers, keywords, numbers)
    // that are case-folded. Operators/punctuation are emitted as single-char
    // tokens so adjacent words never merge across them.
    let mut word = String::new();
    macro_rules! flush_word {
        () => {
            if !word.is_empty() {
                out.push(std::mem::take(&mut word).to_ascii_lowercase());
            }
        };
    }

    while i < n {
        let c = bytes[i] as char;

        // Line comment: -- … \n
        if c == '-' && i + 1 < n && bytes[i + 1] == b'-' {
            flush_word!();
            i += 2;
            while i < n && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }

        // Block comment: /* … */
        if c == '/' && i + 1 < n && bytes[i + 1] == b'*' {
            flush_word!();
            i += 2;
            while i + 1 < n && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(n);
            continue;
        }

        // Single-quoted string literal: preserve verbatim (incl. whitespace and
        // case). SQL doubles the quote to escape: '' inside '…'.
        if c == '\'' {
            flush_word!();
            let start = i;
            i += 1;
            while i < n {
                if bytes[i] == b'\'' {
                    if i + 1 < n && bytes[i + 1] == b'\'' {
                        i += 2; // escaped quote
                        continue;
                    }
                    i += 1; // closing quote
                    break;
                }
                i += 1;
            }
            out.push(sql[start..i.min(n)].to_string());
            continue;
        }

        // Double-quoted identifier: preserve verbatim (case-sensitive, distinct
        // from the unquoted form). "" escapes a quote.
        if c == '"' {
            flush_word!();
            let start = i;
            i += 1;
            while i < n {
                if bytes[i] == b'"' {
                    if i + 1 < n && bytes[i + 1] == b'"' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            out.push(sql[start..i.min(n)].to_string());
            continue;
        }

        // Whitespace: token separator.
        if c.is_ascii_whitespace() {
            flush_word!();
            i += 1;
            continue;
        }

        // Bareword chars: letters, digits, underscore, dollar, dot (part of
        // identifiers/numbers/qualified names) accumulate into a case-folded
        // word.
        if c.is_ascii_alphanumeric() || c == '_' || c == '$' || c == '.' {
            word.push(c);
            i += 1;
            continue;
        }

        // Multi-char and single-char operators / punctuation. Match the longest
        // known operator first so e.g. `!=`, `<>`, `<=`, `>=`, `||`, `::` are
        // single tokens (needed for synonym canonicalization below).
        flush_word!();
        let two = if i + 1 < n { &sql[i..i + 2] } else { "" };
        const TWO_CHAR_OPS: &[&str] = &[
            "!=", "<>", "<=", ">=", "||", "::", "->", "==", "<<", ">>", ":=",
        ];
        if TWO_CHAR_OPS.contains(&two) {
            out.push(two.to_string());
            i += 2;
        } else {
            out.push(c.to_string());
            i += 1;
        }
    }
    flush_word!();

    canonicalize_tokens(&mut out);
    out.join(" ")
}

/// Post-lexing canonicalization of the token sequence that is SAFE to do
/// lexically (no parser needed). Covers the live-verified operator-synonym and
/// trailing-noise equivalences:
///   - `!=` ≡ `<>`  (canonical `<>`)
///   - `==` ≡ `=`   (some dialects)
///   - drop a trailing `;`
///   - drop a trailing comma that immediately precedes a clause keyword
///     (`from`/`where`/`group`/`order`/`having`/`qualify`/`limit`) or EOF —
///     i.e. a dangling SELECT-list comma.
///
/// NOT done here (requires a real parser + dialect catalog, so left as a
/// documented over-execute gap): cast `::`↔`cast()`, type-name synonyms
/// (varchar/text), function-name synonyms (coalesce/nvl), optional `AS`.
fn canonicalize_tokens(toks: &mut Vec<String>) {
    // Operator synonyms.
    for t in toks.iter_mut() {
        match t.as_str() {
            "!=" => *t = "<>".to_string(),
            "==" => *t = "=".to_string(),
            _ => {}
        }
    }
    // Drop trailing semicolon(s).
    while toks.last().map(|t| t == ";").unwrap_or(false) {
        toks.pop();
    }
    // Drop a dangling comma before a clause keyword or EOF.
    const CLAUSE_KW: &[&str] = &[
        "from", "where", "group", "order", "having", "qualify", "limit", "window",
    ];
    let mut cleaned: Vec<String> = Vec::with_capacity(toks.len());
    for (idx, t) in toks.iter().enumerate() {
        if t == "," {
            let next = toks.get(idx + 1).map(|s| s.as_str());
            let drop = match next {
                None => true,                        // trailing comma at EOF
                Some(kw) => CLAUSE_KW.contains(&kw), // comma before a clause kw
            };
            if drop {
                continue;
            }
        }
        cleaned.push(t.clone());
    }
    *toks = cleaned;
}

#[cfg(test)]
mod tests {
    use super::{normalize_sql, normalize_sql_lexer};

    fn eq(a: &str, b: &str) -> bool {
        normalize_sql(a) == normalize_sql(b)
    }

    #[test]
    fn meaningful_cast_and_function_changes_remain_distinct() {
        for (a, b) in [
            ("select cast(x as integer)", "select try_cast(x as integer)"),
            (
                "select cast(x as varchar(1))",
                "select cast(x as varchar(100))",
            ),
            (
                "select cast(x as numeric(10,0))",
                "select cast(x as numeric(10,2))",
            ),
            ("select s.nvl(x,0)", "select s.coalesce(x,0)"),
        ] {
            assert!(
                !eq(a, b),
                "different behavior must retain different fingerprints: {a} / {b}"
            );
        }
    }

    proptest::proptest! {
        #[test]
        fn arbitrary_utf8_never_panics(sql in ".{0,256}") {
            let _ = normalize_sql(&sql);
            let _ = normalize_sql_lexer(&sql);
        }
    }

    #[test]
    fn unsupported_unicode_sql_is_preserved() {
        assert_eq!(normalize_sql("select é @@"), "select é @@");
    }

    // ---- Parser-backed canonicalization (full statements) ----

    #[test]
    fn comments_whitespace_case_optional_as_canonicalized() {
        assert!(eq("select 1 as a", "-- hi\nselect 1 as a"));
        assert!(eq("select 1 as a", "select /* block */ 1 as a"));
        assert!(eq("select /*+ hint */ a from t", "select a from t"));
        assert!(eq("select a from t", "SELECT A FROM T"));
        assert!(eq("select customer_id from t", "select CUSTOMER_ID from t"));
        assert!(eq("select   a  from   t", "select a from t"));
        assert!(eq("select 1 as k", "select 1 k")); // optional AS
        assert!(eq("select 1 from t", "select 1 from t;")); // trailing ;
    }

    #[test]
    fn operator_and_cast_synonyms_canonicalized() {
        assert!(eq(
            "select 1 from t where a != 0",
            "select 1 from t where a <> 0"
        ));
        assert!(eq(
            "select x::varchar from t",
            "select cast(x as varchar) from t"
        ));
    }

    #[test]
    fn type_and_function_synonyms_canonicalized() {
        // String family.
        assert!(eq(
            "select cast(x as varchar) from t",
            "select cast(x as text) from t"
        ));
        assert!(eq(
            "select cast(x as varchar) from t",
            "select cast(x as string) from t"
        ));
        // Number family (incl. Snowflake NUMBER which parses as a custom type).
        assert!(eq(
            "select cast(x as int) from t",
            "select cast(x as number) from t"
        ));
        assert!(eq(
            "select cast(x as integer) from t",
            "select cast(x as numeric) from t"
        ));
        // Function synonyms.
        assert!(eq(
            "select coalesce(a, b) from t",
            "select nvl(a, b) from t"
        ));
        assert!(eq(
            "select coalesce(a, b) from t",
            "select ifnull(a, b) from t"
        ));
    }

    #[test]
    fn string_literals_and_quoted_identifiers_preserved() {
        // String content (whitespace + case) is significant.
        assert!(!eq("select 'a b' from t", "select 'a  b' from t"));
        assert!(!eq("select 'abc' from t", "select 'ABC' from t"));
        // Quoted identifiers are distinct from unquoted and case-sensitive.
        assert!(!eq(
            "select \"customer_id\" from t",
            "select customer_id from t"
        ));
        assert!(!eq("select \"a\" from t", "select \"A\" from t"));
    }

    #[test]
    fn semantics_are_not_canonicalized() {
        // Semantically equivalent but the hosted service EXECUTES — we must too
        // (it is NOT a logical plan).
        assert!(!eq(
            "select a from t group by 1",
            "select a from t group by a"
        ));
        assert!(!eq(
            "select 1 from t where a is not null",
            "select 1 from t where (a is not null)"
        ));
        assert!(!eq(
            "select 1 from t where a is not null",
            "select 1 from t where not (a is null)"
        ));
        assert!(!eq(
            "select 1 from t where a = 1",
            "select 1 from t where a = 1.0"
        ));
        assert!(!eq("select a, b from t", "select b, a from t"));
    }

    #[test]
    fn genuine_changes_differ() {
        assert!(!eq("select a from t", "select a, b from t"));
        assert!(!eq(
            "select a from t where x = 1",
            "select a from t where x = 2"
        ));
    }

    // ---- SQL-semantics negative coverage (over-execute is the safe default) --

    /// Only the small verified synonym set is unified. A quoted function name is
    /// a DIFFERENT (case-sensitive, possibly user-defined) identifier and must
    /// NOT be folded into the builtin synonym.
    #[test]
    fn quoted_function_name_is_not_synonym_folded() {
        assert!(
            !eq(
                "select \"NVL\"(a, 0) from t",
                "select coalesce(a, 0) from t"
            ),
            "a quoted \"NVL\" is not the builtin nvl and must stay distinct"
        );
    }

    /// UDF SHADOWING: a schema-qualified call (`myschema.nvl(...)`) is a
    /// user/overloaded function, NOT the unqualified builtin, so it must not be
    /// canonicalized to `coalesce`. Guards against false reuse across a UDF that
    /// shadows a builtin name.
    #[test]
    fn qualified_udf_shadowing_a_builtin_stays_distinct() {
        assert!(!eq(
            "select myschema.nvl(a, 0) from t",
            "select coalesce(a, 0) from t"
        ));
        assert!(!eq(
            "select myschema.nvl(a, 0) from t",
            "select myschema.coalesce(a, 0) from t"
        ));
    }

    /// A function OUTSIDE the verified synonym set is left as written — two
    /// genuinely different functions must not collide, and a function is not
    /// confused with a same-named column.
    #[test]
    fn unknown_functions_are_not_unified() {
        assert!(!eq(
            "select zeroifnull(a) from t",
            "select coalesce(a, 0) from t"
        ));
        assert!(!eq(
            "select nullif(a, b) from t",
            "select coalesce(a, b) from t"
        ));
    }

    /// CAST negatives beyond the verified families: a precision/scale difference
    /// and a genuinely different target type must stay distinct, and TRY_CAST is
    /// not CAST (different failure semantics).
    #[test]
    fn cast_type_differences_stay_distinct() {
        assert!(!eq(
            "select cast(x as varchar(10)) from t",
            "select cast(x as varchar(20)) from t"
        ));
        assert!(!eq(
            "select cast(x as number(10,2)) from t",
            "select cast(x as number(10,4)) from t"
        ));
        assert!(!eq(
            "select cast(x as int) from t",
            "select cast(x as float) from t"
        ));
        assert!(!eq(
            "select cast(x as date) from t",
            "select cast(x as timestamp) from t"
        ));
        assert!(!eq(
            "select try_cast(x as int) from t",
            "select cast(x as int) from t"
        ));
    }

    /// COLLATE / session-sensitive constructs are preserved: a COLLATE clause
    /// changes comparison/sort semantics and must remain distinct.
    #[test]
    fn collation_is_significant() {
        assert!(!eq(
            "select a from t order by a",
            "select a from t order by a collate 'en-ci'"
        ));
        assert!(!eq(
            "select a collate 'en-ci' from t",
            "select a collate 'fr' from t"
        ));
    }

    /// DISTINCT, LIMIT and ORDER BY direction all change result semantics and
    /// must stay distinct (the normalizer is syntactic, not a logical planner).
    #[test]
    fn result_shaping_clauses_are_significant() {
        assert!(!eq("select a from t", "select distinct a from t"));
        assert!(!eq("select a from t", "select a from t limit 10"));
        assert!(!eq(
            "select a from t order by a asc",
            "select a from t order by a desc"
        ));
        assert!(!eq("select a from t", "select a from t order by a"));
    }

    /// JOIN kind is significant (inner vs left changes the result set).
    #[test]
    fn join_kind_is_significant() {
        assert!(!eq(
            "select a from t join u on t.id = u.id",
            "select a from t left join u on t.id = u.id"
        ));
    }

    /// Qualified vs unqualified builtin that IS in the synonym set: an
    /// unqualified `nvl` folds to `coalesce`, but the aggregate/count family and
    /// other builtins are untouched (we only unified a tiny set).
    #[test]
    fn only_verified_synonyms_fold() {
        // Verified: unqualified nvl/ifnull -> coalesce.
        assert!(eq(
            "select nvl(a, 0) from t",
            "select coalesce(a, 0) from t"
        ));
        assert!(eq(
            "select ifnull(a, 0) from t",
            "select coalesce(a, 0) from t"
        ));
        // NOT unified: count(*) vs count(1) are left as written (over-execute).
        assert!(!eq("select count(*) from t", "select count(1) from t"));
    }

    // ---- Schema-abstracted (cross-environment) normalization ----
    use super::normalize_sql_namespace_abstracted as nsabs;

    fn nseq(a: &str, b: &str) -> bool {
        nsabs(a) == nsabs(b)
    }

    /// VERIFIED LIVE: the SAME model in prod vs a dev clone differs ONLY in the
    /// schema qualifier of three-part names, and the hosted service treats them
    /// as the same logic (skips the dev submit after a clone). Schema
    /// abstraction must collapse that difference.
    #[test]
    fn schema_abstraction_collapses_prod_and_dev() {
        let prod =
            "select order_id from CDO_DEMO_DB.prod_state_demo.stg_orders where x > '1900-01-01'";
        let dev =
            "select order_id from CDO_DEMO_DB.dev_clone_demo.stg_orders where x > '1900-01-01'";
        assert!(
            nseq(prod, dev),
            "prod/dev same model (schema-only diff) must share abstracted identity"
        );
    }

    /// VERIFIED LIVE: a genuine projection change (added column) EXECUTES even
    /// across environments — schema abstraction must NOT hide real logic change.
    #[test]
    fn schema_abstraction_preserves_logic_change() {
        let base = "select order_id from CDO_DEMO_DB.dev_clone_demo.stg_orders";
        let changed =
            "select order_id, upper(customer_id) as c from CDO_DEMO_DB.dev_clone_demo.stg_orders";
        assert!(
            !nseq(base, changed),
            "a real projection change must remain distinct under abstraction"
        );
    }

    /// Catalog and table components must still distinguish relations; only the
    /// schema (middle) component is abstracted.
    #[test]
    fn schema_abstraction_keeps_catalog_and_table_significant() {
        assert!(!nseq("select a from db1.s.t", "select a from db2.s.t"));
        assert!(!nseq("select a from db.s.t1", "select a from db.s.t2"));
    }

    /// Two-part names are NOT abstracted (ambiguous catalog.schema vs
    /// schema.table), so a schema-only change there still differs.
    #[test]
    fn schema_abstraction_leaves_two_part_names_intact() {
        assert!(!nseq("select a from prod.t", "select a from dev.t"));
    }

    /// The strict (non-abstracted) normalizer must STILL distinguish prod/dev —
    /// abstraction is an additive cross-environment identity, not a replacement.
    #[test]
    fn strict_normalizer_still_distinguishes_schema() {
        let prod = "select a from db.prod.t";
        let dev = "select a from db.dev.t";
        assert_ne!(
            super::normalize_sql(prod),
            super::normalize_sql(dev),
            "strict identity must remain schema-sensitive"
        );
    }

    #[test]
    fn empty_and_whitespace_only() {
        assert_eq!(normalize_sql(""), "");
        assert_eq!(normalize_sql("   \n\t "), "");
        assert_eq!(normalize_sql("-- only a comment"), "");
    }

    #[test]
    fn unparseable_sql_preserves_exact_identity() {
        let a = normalize_sql("~~ not valid sql @@ -- c");
        let b = normalize_sql("~~ not valid sql @@");
        assert_eq!(a, "~~ not valid sql @@ -- c");
        assert_eq!(b, "~~ not valid sql @@");
        assert_ne!(a, b, "unsupported syntax must not be partially rewritten");
    }

    // ---- Lexer fallback (exercised directly) ----

    #[test]
    fn lexer_fallback_rules() {
        let leq = |a: &str, b: &str| normalize_sql_lexer(a) == normalize_sql_lexer(b);
        assert!(leq("select 1", "-- c\nselect 1"));
        assert!(leq("SELECT A", "select a"));
        assert!(leq("a != 0", "a <> 0"));
        assert!(leq("select 1", "select 1;"));
        // String content preserved even in the fallback.
        assert!(!leq("'a b'", "'a  b'"));
        // No semantic canonicalization in the fallback.
        assert!(!leq("group by 1", "group by a"));
    }
}
