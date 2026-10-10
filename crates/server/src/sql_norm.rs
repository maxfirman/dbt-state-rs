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

        // Anything else (operators, punctuation: ( ) , = < > + * etc.) is its
        // own token so it both separates words and stays significant.
        flush_word!();
        out.push(c.to_string());
        i += 1;
    }
    flush_word!();

    out.join(" ")
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
}
