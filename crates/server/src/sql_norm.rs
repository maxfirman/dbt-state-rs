//! SQL lexical normalization — mirrors the dbt State server's *token-stream*
//! SQL normalization — mirrors the dbt State server's SQL *canonicalization*
//! (characterized live; see `experiments/SQL_NORMALIZATION.md`).
//!
//! The server parses SQL into a dialect AST and compares a canonical
//! re-rendering: it canonicalizes names/operators/casts/type+function synonyms
//! and drops syntactic noise (comments, whitespace, case, trailing commas/
//! semicolons, optional `AS`), but performs NO semantic simplification (parens,
//! group-by ordinals, CTE-vs-inline, boolean rewrites, numeric-literal forms,
//! and token order all remain significant). It is NOT a logical plan.
//!
//! We reproduce this with `sqlparser` (apache/datafusion-sqlparser-rs, Snowflake
//! dialect): parse → a canonicalizing AST pass (lowercase unquoted identifiers,
//! rewrite `::` casts to `CAST(..)`, map a small, documented set of type- and
//! function-name synonyms to a canonical spelling) → `Display`. parse+Display
//! already gives us case/whitespace/comment/optional-AS/`!=`↔`<>`/`;` for free
//! while preserving the semantic structure the hosted service preserves.
//!
//! When `sqlparser` cannot parse the input (dialect features it doesn't support),
//! we FALL BACK to a conservative lexer-level normalizer (`normalize_sql_lexer`)
//! so robustness is never worse than the previous implementation.

use std::ops::ControlFlow;

use sqlparser::ast::{CastKind, DataType, Expr, Ident, ObjectNamePart, VisitMut, VisitorMut};
use sqlparser::dialect::SnowflakeDialect;
use sqlparser::parser::Parser;

/// Normalize SQL to a canonical string for the match-key hash. Parser-backed
/// with a lexer fallback (see module docs).
pub fn normalize_sql(sql: &str) -> String {
    match Parser::parse_sql(&SnowflakeDialect {}, sql) {
        Ok(mut stmts) if !stmts.is_empty() => {
            let mut canon = Canonicalizer;
            for s in stmts.iter_mut() {
                let _ = s.visit(&mut canon);
            }
            stmts
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
                .join("; ")
        }
        // Empty parse (e.g. whitespace/comment-only) or parse error: fall back
        // to the lexer-level normalizer (robust, conservative).
        _ => normalize_sql_lexer(sql),
    }
}

/// Canonical spelling for a type-name synonym, else `None` (leave as written).
/// Small, documented set verified live (Snowflake): string family and numeric
/// family. Unknown types fall through — anything not unified here simply
/// over-executes (safe), matching our "accept parser differences" stance.
fn canon_type(dt: &DataType) -> Option<DataType> {
    use DataType::*;
    // Snowflake STRING family → VARCHAR.
    let is_string = matches!(
        dt,
        Text | String(_)
            | Varchar(_)
            | Nvarchar(_)
            | Char(_)
            | Character(_)
            | CharVarying(_)
            | CharacterVarying(_)
            | Clob(_)
    );
    // Snowflake NUMBER family → NUMERIC. NUMBER/NUMERIC/DECIMAL with no
    // precision are equivalent; INT/INTEGER/BIGINT/SMALLINT are NUMBER(38,0).
    let is_number = matches!(
        dt,
        Int(_)
            | Integer(_)
            | BigInt(_)
            | SmallInt(_)
            | TinyInt(_)
            | Numeric(_)
            | Decimal(_)
            | Dec(_)
    ) || matches!(dt, Custom(name, args)
    if args.is_empty()
        && name.0.len() == 1
        && matches!(
            name.0[0].as_ident().map(|i| i.value.to_ascii_lowercase()).as_deref(),
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
struct Canonicalizer;
impl VisitorMut for Canonicalizer {
    type Break = ();

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
                *kind = CastKind::Cast;
                if let Some(c) = canon_type(data_type) {
                    *data_type = c;
                }
            }
            // Function-name synonyms (nvl/ifnull → coalesce, …).
            Expr::Function(f) => {
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

/// Conservative lexer-level fallback used when `sqlparser` can't parse the SQL.
/// Strips comments, collapses inter-token whitespace, case-folds keywords and
/// unquoted identifiers (preserving string literals and quoted identifiers),
/// canonicalizes `!=`→`<>`, drops trailing commas/semicolons. Does NOT do any
/// AST-level synonym canonicalization. Equivalent inputs under these rules
/// produce identical output.
pub fn normalize_sql_lexer(sql: &str) -> String {
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

    #[test]
    fn empty_and_whitespace_only() {
        assert_eq!(normalize_sql(""), "");
        assert_eq!(normalize_sql("   \n\t "), "");
        assert_eq!(normalize_sql("-- only a comment"), "");
    }

    #[test]
    fn unparseable_sql_falls_back_to_lexer_and_still_normalizes() {
        // A fragment sqlparser rejects as a statement must still normalize via
        // the lexer fallback (comments/case/whitespace), never panic.
        let a = normalize_sql("~~ not valid sql @@ -- c");
        let b = normalize_sql("~~ not valid sql @@");
        assert_eq!(a, b, "comment stripped via lexer fallback");
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
