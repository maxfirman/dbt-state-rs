//! Databricks identifier policy (#136) and bounded SQL identity (#137).
//! Checked formatting, table-alias binding and explicit literal projections
//! preserve output identifiers and CTE names. Other syntax remains exact;
//! no Result copy or freshness policy follows from SQL identity.
use sqlparser::ast::{
    Expr, GroupByExpr, ObjectName, ObjectNamePart, SelectFlavor, SelectItem, SetExpr, Spanned,
    Statement, TableFactor,
};
use sqlparser::dialect::DatabricksDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Location, Token, Tokenizer, Whitespace};

pub(super) const SQL_IDENTITY_VERSION: &[u8] = b"dbt-state-databricks-sql-v3";

fn component(value: &str) -> Option<String> {
    let mut chars = value.chars();
    let first = chars.next()?;
    (first.is_ascii_alphabetic() || first == '_').then_some(())?;
    chars
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
        .then(|| value.to_ascii_lowercase())
}

fn parts(name: &ObjectName) -> Option<Vec<String>> {
    name.0
        .iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(id) if matches!(id.quote_style, None | Some('`')) => {
                component(&id.value)
            }
            _ => None,
        })
        .collect()
}

fn rendered(parts: &[String]) -> String {
    parts
        .iter()
        .map(|p| format!("`{p}`"))
        .collect::<Vec<_>>()
        .join(".")
}

/// Fully qualified metadata only. Never infer a physical scope from defaults,
/// split quoted dots, remove escaped backticks or fold unknown identifier forms.
pub(super) fn physical_name(raw: &str) -> Option<String> {
    let raw_parts: Vec<_> = raw.split('.').collect();
    let [catalog, schema, name] = raw_parts.as_slice() else {
        return None;
    };
    let names: Option<Vec<_>> = [catalog, schema, name]
        .into_iter()
        .map(|part| {
            let value = if part.starts_with('`') && part.ends_with('`') {
                part.get(1..part.len().checked_sub(1)?)?
            } else {
                part
            };
            component(value)
        })
        .collect();
    Some(rendered(&names?))
}

fn byte_offset(sql: &str, at: Location) -> Option<usize> {
    let mut line = 1;
    let mut column = 1;
    for (offset, ch) in sql.char_indices() {
        if line == at.line && column == at.column {
            return Some(offset);
        }
        if ch == '\n' {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }
    (line == at.line && column == at.column).then_some(sql.len())
}

/// Resolve the sole input of a SELECT without CTEs/joins/subquery inputs.
/// Return None rather than serialize an uncharacterized AST. Output expressions,
/// whitespace, literals, aliases and every non-input byte remain untouched.
pub(super) struct ResolvedSql {
    pub sql: String,
    pub input: String,
    pub start: usize,
    pub end: usize,
}

pub(super) fn resolved_sql(sql: &str, catalog: &str, schema: &str) -> Option<ResolvedSql> {
    let statements = Parser::parse_sql(&DatabricksDialect {}, sql).ok()?;
    let [Statement::Query(q)] = statements.as_slice() else {
        return None;
    };
    if q.with.is_some()
        || q.order_by.is_some()
        || q.limit_clause.is_some()
        || q.fetch.is_some()
        || !q.locks.is_empty()
        || q.for_clause.is_some()
        || q.settings.is_some()
        || q.format_clause.is_some()
        || !q.pipe_operators.is_empty()
    {
        return None;
    }
    let SetExpr::Select(s) = q.body.as_ref() else {
        return None;
    };
    // Defaults may be omitted from the identity only when every reference is
    // resolved. Functions/UDFs, nested queries and CTE bindings are outside this
    // evidenced slice and retain their complete original resolution context.
    if s.flavor != SelectFlavor::Standard
        || !s.optimizer_hints.is_empty()
        || s.distinct.is_some()
        || s.select_modifiers.is_some()
        || s.top.is_some()
        || s.exclude.is_some()
        || s.into.is_some()
        || !s.lateral_views.is_empty()
        || s.prewhere.is_some()
        || s.selection.is_some()
        || !s.connect_by.is_empty()
        || !matches!(&s.group_by, GroupByExpr::Expressions(e,m) if e.is_empty() && m.is_empty())
        || !s.cluster_by.is_empty()
        || !s.distribute_by.is_empty()
        || !s.sort_by.is_empty()
        || s.having.is_some()
        || !s.named_window.is_empty()
        || s.qualify.is_some()
        || s.value_table_mode.is_some()
        || s.projection
            .iter()
            .any(|p| !matches!(p, SelectItem::UnnamedExpr(Expr::Identifier(_))))
    {
        return None;
    }
    let [table] = s.from.as_slice() else {
        return None;
    };
    if !table.joins.is_empty() {
        return None;
    }
    let TableFactor::Table {
        name,
        alias: None,
        args: None,
        with_ordinality: false,
        with_hints,
        version: None,
        partitions,
        json_path: None,
        sample: None,
        index_hints,
        ..
    } = &table.relation
    else {
        return None;
    };
    if !with_hints.is_empty() || !partitions.is_empty() || !index_hints.is_empty() {
        return None;
    }
    let names = parts(name)?;
    let qualified = match names.as_slice() {
        [name] => vec![component(catalog)?, component(schema)?, name.clone()],
        [schema, name] => vec![component(catalog)?, schema.clone(), name.clone()],
        [catalog, schema, name] => vec![catalog.clone(), schema.clone(), name.clone()],
        _ => return None,
    };
    let span = name.span();
    let start = byte_offset(sql, span.start)?;
    let end = byte_offset(sql, span.end)?;
    if sql.get(start..end)? != name.to_string() {
        return None;
    }
    let mut normalized = sql.to_owned();
    normalized.replace_range(start..end, &rendered(&qualified));
    Some(ResolvedSql {
        sql: normalized,
        input: rendered(&qualified),
        start,
        end,
    })
}

/// Formatting is evidenced only for the existing identifier-only direct SELECT.
/// The typed input has already passed resolved_sql's complete grammar check.
/// Preserve comments carrying hint/directive syntax rather than treating them
/// as the ordinary comments in the hosted capture. Keep original source spans
/// and resolved physical input separate from the serialized identity.
pub(super) fn formatted_direct_sql(resolved: &ResolvedSql) -> String {
    let Ok(tokens) = Tokenizer::new(&DatabricksDialect {}, &resolved.sql)
        .with_unescape(false)
        .tokenize()
    else {
        return resolved.sql.clone();
    };
    formatted_tokens(tokens).unwrap_or_else(|| resolved.sql.clone())
}

// Rendering tokenizers retain raw escape spelling; AST parsing above is only
// grammar validation. Unescaping here would silently widen string identity.
fn formatted_tokens(tokens: Vec<Token>) -> Option<String> {
    if tokens.iter().any(|token| match token {
        Token::Whitespace(Whitespace::MultiLineComment(body))
        | Token::Whitespace(Whitespace::SingleLineComment { comment: body, .. }) => {
            body.starts_with('+') || body.starts_with('!')
        }
        _ => false,
    }) {
        return None;
    }
    // Keep token boundaries and punctuation rather than serialize the AST:
    // parser serialization could erase an uncharacterized semicolon or other
    // spelling distinction. Only whitespace/comments and these two keywords
    // have positive evidence for this grammar.
    Some(
        tokens
            .into_iter()
            .filter_map(|token| match token {
                Token::Whitespace(_) => None,
                Token::Word(word)
                    if word.quote_style.is_none()
                        && (word.value.eq_ignore_ascii_case("select")
                            || word.value.eq_ignore_ascii_case("from")) =>
                {
                    Some(word.value.to_ascii_uppercase())
                }
                token => Some(token.to_string()),
            })
            .collect::<Vec<_>>()
            .join(" "),
    )
}

/// Alpha-renaming of one evidenced TABLE alias, with or without AS. Keep its
/// presence, source name, output column identifiers and bound qualifiers.
/// Unbound qualifiers and all other grammar retain exact SQL/context.
pub(super) fn table_alias_sql(sql: &str, catalog: &str, schema: &str) -> Option<String> {
    let statements = Parser::parse_sql(&DatabricksDialect {}, sql).ok()?;
    let [Statement::Query(query)] = statements.as_slice() else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let [input] = select.from.as_slice() else {
        return None;
    };
    let TableFactor::Table {
        alias: Some(alias), ..
    } = &input.relation
    else {
        return None;
    };
    if alias.name.quote_style.is_some()
        || component(&alias.name.value).is_none()
        || !alias.columns.is_empty()
        || alias.at.is_some()
    {
        return None;
    }
    let mut locations = vec![alias.name.span.start];
    let mut projection = Vec::new();
    for item in &select.projection {
        let SelectItem::UnnamedExpr(Expr::CompoundIdentifier(ids)) = item else {
            return None;
        };
        let [qualifier, column] = ids.as_slice() else {
            return None;
        };
        if qualifier != &alias.name {
            return None;
        }
        locations.push(qualifier.span.start);
        projection.push(SelectItem::UnnamedExpr(Expr::Identifier(column.clone())));
    }
    // Reuse the existing strict SELECT grammar checker after erasing only the
    // independently checked alias binding. This does not grant resolution or
    // default-context removal for the original aliased query.
    let mut checked = query.clone();
    let SetExpr::Select(checked_select) = checked.body.as_mut() else {
        return None;
    };
    checked_select.projection = projection;
    let TableFactor::Table {
        alias: checked_alias,
        ..
    } = &mut checked_select.from[0].relation
    else {
        return None;
    };
    *checked_alias = None;
    resolved_sql(&checked.to_string(), catalog, schema)?;
    let tokens = Tokenizer::new(&DatabricksDialect {}, sql)
        .with_unescape(false)
        .tokenize_with_location()
        .ok()?;
    let alias_index = tokens
        .iter()
        .position(|token| token.span.start == alias.name.span.start)?;
    let as_index = if alias.explicit {
        let previous = (0..alias_index)
            .rev()
            .find(|i| !matches!(tokens[*i].token, Token::Whitespace(_)))?;
        match &tokens[previous].token {
            Token::Word(word)
                if word.quote_style.is_none() && word.value.eq_ignore_ascii_case("as") =>
            {
                Some(previous)
            }
            _ => return None,
        }
    } else {
        None
    };
    let mut replacements = 0;
    let normalized = tokens
        .into_iter()
        .enumerate()
        .filter_map(|(index, mut token)| {
            if Some(index) == as_index {
                return None;
            }
            if locations.contains(&token.span.start) {
                if let Token::Word(word) = &mut token.token {
                    word.value = "__dbt_state_table_alias".into();
                    replacements += 1;
                }
            }
            Some(token.token)
        })
        .collect();
    if replacements != locations.len() {
        return None;
    }
    formatted_tokens(normalized)
}

/// Checked explicit projections: identifiers or CAST of a literal to INT or
/// STRING, with named outputs. Keep every literal token and output identifier.
pub(super) fn literal_projection_sql(sql: &str, catalog: &str, schema: &str) -> Option<String> {
    use sqlparser::ast::{CastKind, DataType, Ident, Value};
    let statements = Parser::parse_sql(&DatabricksDialect {}, sql).ok()?;
    let [Statement::Query(query)] = statements.as_slice() else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let mut protected = Vec::new();
    let mut explicit = false;
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(Expr::Identifier(id)) => protected.push(id.span.start),
            SelectItem::ExprWithAlias { expr, alias } => {
                explicit = true;
                protected.push(alias.span.start);
                match expr {
                    Expr::Identifier(id) => protected.push(id.span.start),
                    Expr::Cast {
                        kind: CastKind::Cast,
                        expr,
                        data_type: DataType::Int(None) | DataType::String(None),
                        format: None,
                    } if matches!(expr.as_ref(), Expr::Value(v) if matches!(v.value, Value::Number(_, false) | Value::SingleQuotedString(_) | Value::Null)) =>
                        {}
                    _ => return None,
                }
            }
            _ => return None,
        }
    }
    if !explicit {
        return None;
    }
    let [input] = select.from.as_slice() else {
        return None;
    };
    let TableFactor::Table {
        name, alias: None, ..
    } = &input.relation
    else {
        return None;
    };
    for part in &name.0 {
        let ObjectNamePart::Identifier(id) = part else {
            return None;
        };
        protected.push(id.span.start);
    }
    let mut checked = query.clone();
    let SetExpr::Select(checked_select) = checked.body.as_mut() else {
        return None;
    };
    checked_select.projection = vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident::new(
        "checked_projection",
    )))];
    resolved_sql(&checked.to_string(), catalog, schema)?;
    let tokens = Tokenizer::new(&DatabricksDialect {}, sql)
        .with_unescape(false)
        .tokenize_with_location()
        .ok()?;
    let normalized = tokens
        .into_iter()
        .map(|mut token| {
            if !protected.contains(&token.span.start) {
                if let Token::Word(word) = &mut token.token {
                    if word.quote_style.is_none()
                        && ["cast", "as", "int", "string", "null"]
                            .iter()
                            .any(|keyword| word.value.eq_ignore_ascii_case(keyword))
                    {
                        word.value = word.value.to_ascii_uppercase();
                    }
                }
            }
            token.token
        })
        .collect();
    formatted_tokens(normalized)
}

/// Formatting/comments for one simple CTE. Names and binding-bearing tokens
/// remain exact: there is no alpha-renaming, definition elimination or inlining.
pub(super) fn cte_formatting_sql(sql: &str, catalog: &str, schema: &str) -> Option<String> {
    let statements = Parser::parse_sql(&DatabricksDialect {}, sql).ok()?;
    let [Statement::Query(query)] = statements.as_slice() else {
        return None;
    };
    let with = query.with.as_ref()?;
    let [cte] = with.cte_tables.as_slice() else {
        return None;
    };
    if with.recursive
        || cte.from.is_some()
        || cte.materialized.is_some()
        || cte.alias.name.quote_style.is_some()
        || component(&cte.alias.name.value).is_none()
        || !cte.alias.columns.is_empty()
        || cte.alias.at.is_some()
    {
        return None;
    }
    let SetExpr::Select(body) = query.body.as_ref() else {
        return None;
    };
    let [input] = body.from.as_slice() else {
        return None;
    };
    let TableFactor::Table { name, .. } = &input.relation else {
        return None;
    };
    let [ObjectNamePart::Identifier(reference)] = name.0.as_slice() else {
        return None;
    };
    if reference != &cte.alias.name {
        return None;
    }
    let SetExpr::Select(definition) = cte.query.body.as_ref() else {
        return None;
    };
    let [source] = definition.from.as_slice() else {
        return None;
    };
    let TableFactor::Table {
        name: source_name, ..
    } = &source.relation
    else {
        return None;
    };
    // No self/later-sibling or default-context ambiguity in the definition.
    if parts(source_name)?.len() != 3 {
        return None;
    }
    resolved_sql(&cte.query.to_string(), catalog, schema)?;
    let mut checked = query.clone();
    checked.with = None;
    resolved_sql(&checked.to_string(), catalog, schema)?;
    formatted_tokens(
        Tokenizer::new(&DatabricksDialect {}, sql)
            .with_unescape(false)
            .tokenize()
            .ok()?,
    )
}
