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

pub(super) const SQL_IDENTITY_VERSION: &[u8] = b"dbt-state-databricks-sql-v4";

/// Volatile evaluation (#67, #192): hosted renders `rand()` as `RAND()` with
/// a decimal in [0, 1) (`databricks_submit_copy_controls.jsonl` entry 85).
/// Other Databricks Volatile functions are uncaptured and render nothing.
pub(super) const VOLATILE_RULES: crate::volatile_evaluation::Rules =
    crate::volatile_evaluation::Rules {
        dialect: &DatabricksDialect {},
        data_tests_and_dependencies: false,
        functions: &[crate::volatile_evaluation::Rule {
            name: "rand",
            call: crate::volatile_evaluation::Call::Empty,
            key: "RAND()",
            value: crate::volatile_evaluation::Value::UnitRandom,
        }],
        // Evaluation off and reuse are uncaptured on Databricks.
        evaluation_off: false,
        on_skip: false,
    };

/// Extra bounds on #144's source-less ordinary-submit rejection. The shared
/// relation-read proof deliberately treats literal table functions as reading
/// no named relation; they are uncharacterized here and must stay untouched.
/// Only the captured literal/CAST/comparison shapes and derived SELECT/UNION
/// inputs qualify. This supplies neither SQL equivalence nor audit elimination.
pub(super) fn literal_audit_query(sql: &str) -> bool {
    use sqlparser::ast::{
        BinaryOperator, CastKind, Select, SetOperator, SetQuantifier, Value, Visit, Visitor,
    };
    use std::ops::ControlFlow;

    fn literal_set(expr: &SetExpr) -> bool {
        match expr {
            SetExpr::Select(_) => true,
            SetExpr::SetOperation {
                op: SetOperator::Union,
                set_quantifier: SetQuantifier::All,
                left,
                right,
            } => literal_set(left) && literal_set(right),
            _ => false,
        }
    }

    struct LiteralAudit;
    impl Visitor for LiteralAudit {
        type Break = ();

        fn pre_visit_query(&mut self, query: &sqlparser::ast::Query) -> ControlFlow<()> {
            if literal_set(&query.body)
                && query.with.is_none()
                && query.order_by.is_none()
                && query.limit_clause.is_none()
                && query.fetch.is_none()
                && query.locks.is_empty()
                && query.for_clause.is_none()
                && query.settings.is_none()
                && query.format_clause.is_none()
                && query.pipe_operators.is_empty()
            {
                ControlFlow::Continue(())
            } else {
                ControlFlow::Break(())
            }
        }

        fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<()> {
            if select.flavor == SelectFlavor::Standard
                && select.optimizer_hints.is_empty()
                && select.distinct.is_none()
                && select.select_modifiers.is_none()
                && select.top.is_none()
                && select.exclude.is_none()
                && select.into.is_none()
                && select.lateral_views.is_empty()
                && select.prewhere.is_none()
                && select.connect_by.is_empty()
                && matches!(&select.group_by, GroupByExpr::Expressions(e, m) if e.is_empty() && m.is_empty())
                && select.cluster_by.is_empty()
                && select.distribute_by.is_empty()
                && select.sort_by.is_empty()
                && select.having.is_none()
                && select.named_window.is_empty()
                && select.qualify.is_none()
                && select.value_table_mode.is_none()
                && select.from.len() <= 1
                && select.from.iter().all(|table| table.joins.is_empty())
                && select.projection.iter().all(|item| {
                    matches!(
                        item,
                        SelectItem::UnnamedExpr(_) | SelectItem::ExprWithAlias { .. }
                    )
                })
            {
                ControlFlow::Continue(())
            } else {
                ControlFlow::Break(())
            }
        }

        fn pre_visit_table_factor(&mut self, table: &TableFactor) -> ControlFlow<()> {
            if matches!(table, TableFactor::Derived { .. }) {
                ControlFlow::Continue(())
            } else {
                ControlFlow::Break(())
            }
        }

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
            let literal = match expr {
                Expr::Identifier(_) | Expr::Nested(_) => true,
                Expr::Value(value) => matches!(
                    value.value,
                    Value::Number(_, _) | Value::SingleQuotedString(_) | Value::Null
                ),
                Expr::Cast {
                    kind: CastKind::Cast,
                    format: None,
                    ..
                } => true,
                Expr::BinaryOp {
                    op: BinaryOperator::Eq | BinaryOperator::And,
                    ..
                } => true,
                _ => false,
            };
            if literal {
                ControlFlow::Continue(())
            } else {
                ControlFlow::Break(())
            }
        }
    }

    let Ok(statements) = Parser::parse_sql(&DatabricksDialect {}, sql) else {
        return false;
    };
    let [statement @ Statement::Query(_)] = statements.as_slice() else {
        return false;
    };
    statement.visit(&mut LiteralAudit).is_continue()
}

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

/// #144's direct-read proof is separate from SQL identity. A stored-failure
/// output is the sole reported relation in the test schema other than the
/// positively resolved input. Unsupported syntax retains every reported input.
pub(super) fn unread_test_audit(req: &crate::query_cache::SqlExecution) -> Option<&str> {
    use sqlparser::ast::{BinaryOperator, Value, Visit, Visitor};
    use std::ops::ControlFlow;

    struct PurePredicate;
    impl Visitor for PurePredicate {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
            let supported = match expr {
                Expr::Identifier(_) | Expr::Nested(_) => true,
                Expr::Value(value) => matches!(value.value, Value::Number(_, _) | Value::Null),
                Expr::BinaryOp { op, .. } => matches!(
                    op,
                    BinaryOperator::Eq
                        | BinaryOperator::NotEq
                        | BinaryOperator::Lt
                        | BinaryOperator::LtEq
                        | BinaryOperator::Gt
                        | BinaryOperator::GtEq
                        | BinaryOperator::And
                        | BinaryOperator::Or
                ),
                _ => false,
            };
            if supported {
                ControlFlow::Continue(())
            } else {
                ControlFlow::Break(())
            }
        }
    }
    if req.tables.len() != 2 || !req.query_dependencies.is_empty() {
        return None;
    }
    let schema = req.default_schema.as_deref()?;
    let mut statements = Parser::parse_sql(&DatabricksDialect {}, &req.sql).ok()?;
    let [Statement::Query(query)] = statements.as_mut_slice() else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_mut() else {
        return None;
    };
    let predicate = select.selection.take()?;
    if predicate.visit(&mut PurePredicate).is_break() {
        return None;
    }
    // The unchanged identity resolver proves the entire remaining direct SELECT,
    // including rejection of CTEs, joins, subqueries, UDFs and table functions.
    let input = resolved_sql(&statements[0].to_string(), &req.default_catalog, schema)?.input;
    let catalog = component(&req.default_catalog)?;
    let schema = component(schema)?;
    let prefix = format!("`{catalog}`.`{schema}`.");
    let canonical: Vec<_> = req
        .tables
        .iter()
        .map(|t| physical_name(&t.name))
        .collect::<Option<_>>()?;
    let source = canonical.iter().position(|name| *name == input)?;
    let audit = 1 - source;
    (canonical[audit] != input && canonical[audit].starts_with(&prefix))
        .then_some(req.tables[audit].name.as_str())
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

/// Explain-only leaf traversal of whole dependency bindings. Physical evidence
/// is never reordered or rewritten: an unknown/ambiguous/cyclic graph supplies
/// no display order. Each definition resolves under its own reported defaults.
pub(super) fn explain_input_order(
    req: &crate::query_cache::SubmitEnrichedSqlRequest,
) -> Option<Vec<String>> {
    fn walk(
        sql: &str,
        catalog: &str,
        schema: &str,
        dependencies: &[crate::query_cache::QueryDependency],
        stack: &mut Vec<String>,
        output: &mut Vec<String>,
        budget: &mut usize,
    ) -> Option<()> {
        // Display enrichment must stay bounded for adversarial dependency
        // DAGs as well as cycles. Overflow retains the submitted ordering.
        if stack.len() >= 32 || *budget == 0 {
            return None;
        }
        let statements = Parser::parse_sql(&DatabricksDialect {}, sql).ok()?;
        let [Statement::Query(query)] = statements.as_slice() else {
            return None;
        };
        if query.with.is_some() {
            return None;
        }
        let mut names = Vec::new();
        let _ = sqlparser::ast::visit_relations(&statements, |name| {
            names.push(name.clone());
            std::ops::ControlFlow::<()>::Continue(())
        });
        for name in names {
            *budget = budget.checked_sub(1)?;
            let components = parts(&name)?;
            let qualified = match components.as_slice() {
                [name] => vec![component(catalog)?, component(schema)?, name.clone()],
                [schema, name] => vec![component(catalog)?, schema.clone(), name.clone()],
                [catalog, schema, name] => vec![catalog.clone(), schema.clone(), name.clone()],
                _ => return None,
            };
            let physical = rendered(&qualified);
            let mut matching = dependencies
                .iter()
                .filter(|d| physical_name(&d.name).as_ref() == Some(&physical));
            if let Some(dependency) = matching.next() {
                if matching.next().is_some() || stack.contains(&physical) {
                    return None;
                }
                stack.push(physical);
                walk(
                    &dependency.query,
                    &dependency.default_catalog,
                    &dependency.default_schema,
                    dependencies,
                    stack,
                    output,
                    budget,
                )?;
                stack.pop();
            } else if !output.contains(&physical) {
                output.push(physical);
            }
        }
        Some(())
    }
    let mut output = Vec::new();
    walk(
        &req.sql,
        &req.default_catalog,
        req.default_schema.as_deref().unwrap_or(""),
        &req.query_dependencies,
        &mut Vec::new(),
        &mut output,
        &mut 1024,
    )?;
    Some(output)
}

/// Checked effects only; symbolic columns are handled by the shared binder.
/// Only the explicit system.builtin namespace proves routine resolution; bare
/// names can be shadowed by SESSION PATH. COUNT is BIGINT; MIN/MAX preserve
/// numeric input. Arithmetic and casts
/// are admitted only for in-range literal expressions, so ANSI overflow and
/// coercion settings cannot change the outcome. SUM(0) cannot overflow.
pub(crate) fn expression_effect(
    e: &sqlparser::ast::Expr,
    child: &impl Fn(&sqlparser::ast::Expr) -> Option<crate::analyzed_workload::query::Scalar>,
) -> Option<crate::analyzed_workload::query::Scalar> {
    use crate::analyzed_workload::query::Scalar;
    use sqlparser::ast::*;
    match e {
        Expr::Value(v) => match &v.value {
            Value::Number(s, false) if s.parse::<i64>().is_ok() => Some(Scalar::Integer),
            Value::Boolean(_) => Some(Scalar::Boolean),
            Value::Null => Some(Scalar::Null),
            _ => None,
        },
        Expr::BinaryOp { left, op, right } => {
            let l = child(left)?;
            let r = child(right)?;
            match op {
                BinaryOperator::And | BinaryOperator::Or if l == Scalar::Boolean && r == l => {
                    Some(Scalar::Boolean)
                }
                BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq
                    if l == r && matches!(l, Scalar::Integer | Scalar::Boolean) =>
                {
                    Some(Scalar::Boolean)
                }
                BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
                | BinaryOperator::Divide
                    if l == Scalar::Integer
                        && r == Scalar::Integer
                        && literal_integer(e).is_some() =>
                {
                    Some(Scalar::Integer)
                }
                _ => None,
            }
        }
        Expr::UnaryOp { op, expr } => match op {
            UnaryOperator::Not if child(expr)? == Scalar::Boolean => Some(Scalar::Boolean),
            UnaryOperator::Plus | UnaryOperator::Minus
                if child(expr) == Some(Scalar::Integer) && literal_integer(e).is_some() =>
            {
                Some(Scalar::Integer)
            }
            _ => None,
        },
        Expr::Cast {
            kind: CastKind::Cast,
            expr,
            data_type,
            format: None,
        } => {
            child(expr)?;
            let v = literal_integer(expr)?;
            match data_type {
                DataType::Int(None) | DataType::Integer(None) if i32::try_from(v).is_ok() => {
                    Some(Scalar::Integer)
                }
                DataType::BigInt(None) => Some(Scalar::Integer),
                DataType::Decimal(ExactNumberInfo::PrecisionAndScale(p, s))
                    if *p <= 38
                        && *s >= 0
                        && (*s as u64) <= *p
                        && v.to_string().trim_start_matches('-').len() as u64
                            <= p - (*s as u64) =>
                {
                    Some(Scalar::Decimal(*p, *s))
                }
                _ => None,
            }
        }
        Expr::Case {
            operand: None,
            conditions,
            else_result,
            ..
        } => {
            let mut result = else_result
                .as_ref()
                .map(|e| child(e))
                .unwrap_or(Some(Scalar::Null))?;
            for arm in conditions {
                if child(&arm.condition)? != Scalar::Boolean {
                    return None;
                }
                let value = child(&arm.result)?;
                if result == Scalar::Null {
                    result = value;
                } else if value != Scalar::Null && value != result {
                    return None;
                }
            }
            // Unknown branches might hide incompatible warehouse types.
            (result != Scalar::Unknown).then_some(result)
        }
        Expr::Function(f) => {
            if f.name.0.len() != 3
                || f.uses_odbc_syntax
                || f.parameters != FunctionArguments::None
                || !f.within_group.is_empty()
                || f.filter.is_some()
                || f.null_treatment.is_some()
                || f.over.is_some()
            {
                return None;
            }
            let resolved = f
                .name
                .0
                .iter()
                .map(|p| match p {
                    ObjectNamePart::Identifier(i) if i.quote_style.is_none() => {
                        Some(i.value.to_ascii_lowercase())
                    }
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            if resolved[0] != "system" || resolved[1] != "builtin" {
                return None;
            }
            let ObjectNamePart::Identifier(n) = &f.name.0[2] else {
                return None;
            };
            if n.quote_style.is_some() {
                return None;
            }
            let FunctionArguments::List(args) = &f.args else {
                return None;
            };
            if args.duplicate_treatment.is_some()
                || !args.clauses.is_empty()
                || args.args.len() != 1
            {
                return None;
            }
            let FunctionArg::Unnamed(arg) = &args.args[0] else {
                return None;
            };
            match (n.value.to_ascii_lowercase().as_str(), arg) {
                ("count", FunctionArgExpr::Wildcard) => Some(Scalar::Integer),
                ("count", FunctionArgExpr::Expr(e)) => {
                    child(e)?;
                    Some(Scalar::Integer)
                }
                ("min" | "max", FunctionArgExpr::Expr(e)) if child(e)? == Scalar::Integer => {
                    Some(Scalar::Integer)
                }
                ("sum", FunctionArgExpr::Expr(e))
                    if child(e)? == Scalar::Integer && literal_integer(e) == Some(0) =>
                {
                    Some(Scalar::Integer)
                }
                _ => None,
            }
        }
        _ => None,
    }
}
fn literal_integer(e: &sqlparser::ast::Expr) -> Option<i64> {
    use sqlparser::ast::*;
    match e {
        Expr::Value(v) => match &v.value {
            Value::Number(n, false) => n.parse().ok(),
            _ => None,
        },
        Expr::Nested(e) => literal_integer(e),
        Expr::Cast { expr, .. } => literal_integer(expr),
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => literal_integer(expr),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => literal_integer(expr)?
            .checked_neg()
            .filter(|v| i32::try_from(*v).is_ok()),
        Expr::BinaryOp { left, op, right } => {
            let l = literal_integer(left)?;
            let r = literal_integer(right)?;
            match op {
                BinaryOperator::Plus => l.checked_add(r).filter(|v| i32::try_from(*v).is_ok()),
                BinaryOperator::Minus => l.checked_sub(r).filter(|v| i32::try_from(*v).is_ok()),
                BinaryOperator::Multiply => l.checked_mul(r).filter(|v| i32::try_from(*v).is_ok()),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Copy analysis uses the same checked ASCII/backtick identifier meaning as
/// physical State history. Defaults participate only when resolution needs them.
pub(crate) fn copy_identifier(id: &sqlparser::ast::Ident) -> Option<String> {
    if !matches!(id.quote_style, None | Some('`')) {
        return None;
    }
    component(&id.value)
}
pub(crate) fn copy_components(name: &sqlparser::ast::ObjectName) -> Option<Vec<String>> {
    name.0
        .iter()
        .map(|p| match p {
            sqlparser::ast::ObjectNamePart::Identifier(i) => copy_identifier(i),
            _ => None,
        })
        .collect()
}
pub(crate) fn copy_relation(
    name: &sqlparser::ast::ObjectName,
    catalog: &str,
    schema: &str,
) -> Option<String> {
    let names = copy_components(name)?;
    let resolved = match names.as_slice() {
        [t] => vec![component(catalog)?, component(schema)?, t.clone()],
        [s, t] => vec![component(catalog)?, s.clone(), t.clone()],
        [c, s, t] => vec![c.clone(), s.clone(), t.clone()],
        _ => return None,
    };
    Some(resolved.join("."))
}
pub(crate) fn copy_name(sql: &str) -> Option<sqlparser::ast::ObjectName> {
    use sqlparser::{dialect::DatabricksDialect, parser::Parser, tokenizer::Token};
    let mut parser = Parser::new(&DatabricksDialect {}).try_with_sql(sql).ok()?;
    let name = parser.parse_object_name(false).ok()?;
    (parser.peek_token().token == Token::EOF).then_some(name)
}
pub(crate) fn copy_query(sql: &str) -> Option<Box<sqlparser::ast::Query>> {
    use sqlparser::{ast::Statement, dialect::DatabricksDialect, parser::Parser};
    let mut statements = Parser::parse_sql(&DatabricksDialect {}, sql).ok()?;
    if statements.len() != 1 {
        return None;
    }
    match statements.pop()? {
        Statement::Query(q) => Some(q),
        _ => None,
    }
}
