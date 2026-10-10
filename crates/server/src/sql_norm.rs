//! SQL lexical normalization — mirrors the dbt State server's *token-stream*
//! comparison (verified live; see `experiments/SQL_NORMALIZATION.md`).
//!
//! The hosted service does NOT compare logical plans. It lexes the SQL and
//! compares a normalized token stream. Reproduced rules (each live-verified):
//!   1. strip comments: `-- …` to end-of-line, and `/* … */` blocks;
//!   2. collapse whitespace BETWEEN tokens, but PRESERVE whitespace inside
//!      string literals;
//!   3. case-fold keywords and UNQUOTED identifiers, but PRESERVE the case of
//!      string literals and treat quoted identifiers verbatim (distinct);
//!   4. do NOT canonicalize semantics — parens, group-by ordinals, CTE-vs-
//!      inline, predicate rewrites, numeric-literal forms and token ORDER all
//!      remain significant.
//!
//! The output is a canonical single-space-joined token string suitable for
//! hashing. It is deliberately lexical, not semantic.

/// Normalize SQL to a canonical token string (lexer-level). Equivalent inputs
/// under rules 1–3 above produce identical output; anything the hosted service
/// treats as a real change (rule 4) produces different output.
pub fn normalize_sql(sql: &str) -> String {
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
    use super::normalize_sql;

    fn eq(a: &str, b: &str) -> bool {
        normalize_sql(a) == normalize_sql(b)
    }

    #[test]
    fn comments_are_stripped() {
        assert!(eq("select 1", "-- hi\nselect 1"));
        assert!(eq("select 1", "select /* block */ 1"));
    }

    #[test]
    fn keyword_and_unquoted_identifier_case_folded() {
        assert!(eq("select a from t", "SELECT A FROM T"));
        assert!(eq("select customer_id", "select CUSTOMER_ID"));
    }

    #[test]
    fn inter_token_whitespace_collapsed() {
        assert!(eq("select   a  ,   b", "select a, b"));
        assert!(eq("select\n\ta\nfrom t", "select a from t"));
    }

    #[test]
    fn string_literal_whitespace_and_case_preserved() {
        // Whitespace INSIDE a string literal is significant.
        assert!(!eq("select 'a b'", "select 'a  b'"));
        // Case INSIDE a string literal is significant.
        assert!(!eq("select 'abc'", "select 'ABC'"));
    }

    #[test]
    fn quoted_identifier_is_distinct_from_unquoted() {
        assert!(!eq("select \"customer_id\"", "select customer_id"));
        // Quoted identifier case is preserved (distinct).
        assert!(!eq("select \"a\"", "select \"A\""));
    }

    #[test]
    fn semantics_are_not_canonicalized() {
        // These are semantically equivalent but must remain DIFFERENT tokens
        // (the hosted service executes on each — it is not a logical plan).
        assert!(!eq("group by customer_id", "group by 1"));
        assert!(!eq("where x is not null", "where (x is not null)"));
        assert!(!eq("where x is not null", "where not (x is null)"));
        assert!(!eq("1 = 1", "1 = 1.0"));
        assert!(!eq("select a, b", "select b, a"));
    }

    #[test]
    fn empty_and_whitespace_only() {
        assert_eq!(normalize_sql(""), "");
        assert_eq!(normalize_sql("   \n\t "), "");
    }

    #[test]
    fn operator_synonyms_canonicalized() {
        // != ≡ <> (verified live).
        assert!(eq("where a != 0", "where a <> 0"));
        // multi-char operators tokenize as one unit.
        assert_eq!(normalize_sql("a<=b"), normalize_sql("a <= b"));
        assert!(!eq("a < b", "a <= b"));
    }

    #[test]
    fn trailing_comma_and_semicolon_dropped() {
        assert!(eq("select a, b from t", "select a, b, from t"));
        assert!(eq("select 1", "select 1;"));
        assert!(eq("select 1", "select 1 ;"));
        // A comma that is NOT dangling must be kept (would change meaning).
        assert!(!eq("select a, b", "select a b"));
    }

    #[test]
    fn optimizer_hint_stripped_like_comment() {
        assert!(eq("select /*+ no_merge */ a from t", "select a from t"));
    }

    /// DOCUMENTED PARSER-GAP: the hosted service canonicalizes these via a
    /// dialect AST (cast shorthand, type synonyms, function synonyms, optional
    /// AS) and SKIPs; our lexer does NOT, so these remain DIFFERENT and we
    /// over-execute (safe-directional). Pins the known boundary so a future
    /// parser-based implementation has an explicit target to flip.
    #[test]
    fn parser_level_synonyms_are_a_known_gap() {
        assert!(
            !eq("cast(x as varchar)", "x::varchar"),
            "cast shorthand: gap"
        );
        assert!(
            !eq("cast(x as varchar)", "cast(x as text)"),
            "type synonym: gap"
        );
        assert!(!eq("coalesce(a, b)", "nvl(a, b)"), "function synonym: gap");
        assert!(!eq("10 as k", "10 k"), "optional AS: gap");
    }
}
