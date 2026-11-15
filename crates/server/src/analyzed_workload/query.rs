//! Structural analysis with lexical bindings. ASTs are retained exactly: no
//! algebraic transformations or schema erasure. Effects are warehouse policy.
use crate::warehouse::databricks::{
    copy_components as parts, copy_identifier as ident, copy_name, copy_query as parse,
    copy_relation as resolve,
};
use crate::{query_cache as qc, warehouse::Warehouse};
use sqlparser::ast::*;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct CopyProof {
    pub version: u8,
    pub identity: String,
    /// Physical leaves and each dependency's reference, definition and context.
    pub bindings: Vec<String>,
    pub effects: String,
}
impl CopyProof {
    /// Origin domains share one framing of the same checked logical identity.
    pub(crate) fn for_origin(&self, domain: &str) -> String {
        use sha2::{Digest, Sha256};
        hex::encode(Sha256::digest(format!("{domain}:{}", self.identity)))
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Scalar {
    Unknown,
    Integer,
    Decimal(u64, i64),
    Boolean,
    Null,
}
type Columns = Option<Vec<(String, Scalar)>>;
type Scope = BTreeMap<String, Columns>;
struct Analysis<'a> {
    req: &'a qc::SqlExecution,
    definitions: BTreeMap<String, &'a qc::QueryDependency>,
    active: BTreeSet<String>,
    used: BTreeSet<String>,
    leaves: BTreeSet<String>,
    bindings: Vec<String>,
    visits: usize,
}
impl Analysis<'_> {
    fn table(
        &mut self,
        table: &TableFactor,
        ctes: &Scope,
        catalog: &str,
        schema: &str,
        depth: usize,
    ) -> Option<(String, Columns)> {
        let TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
        } = table
        else {
            return None;
        };
        if args.is_some()
            || !with_hints.is_empty()
            || version.is_some()
            || *with_ordinality
            || !partitions.is_empty()
            || json_path.is_some()
            || sample.is_some()
            || !index_hints.is_empty()
        {
            return None;
        }
        self.visits += 1;
        if self.visits > 1024 {
            return None;
        }
        let p = parts(name)?;
        let columns = if p.len() == 1 && ctes.contains_key(&p[0]) {
            ctes.get(&p[0])?.clone()
        } else {
            let key = resolve(name, catalog, schema)?;
            if key
                == Warehouse::physical_scope_key(
                    &self.req.dialect,
                    self.req.target_table.as_deref()?,
                )
            {
                return None;
            }
            if let Some(dep) = self.definitions.get(&key).copied() {
                if depth >= 32 || !self.active.insert(key.clone()) {
                    return None;
                }
                self.used.insert(key.clone());
                self.bindings.push(
                    serde_json::to_string(&(
                        &key,
                        &dep.query,
                        &dep.default_catalog,
                        &dep.default_schema,
                    ))
                    .ok()?,
                );
                let result = self.query(
                    &*parse(&dep.query)?,
                    &Scope::new(),
                    &dep.default_catalog,
                    &dep.default_schema,
                    depth + 1,
                )?;
                self.active.remove(&key);
                result
            } else {
                self.leaves.insert(key.clone());
                self.bindings.push(key);
                None
            }
        };
        let label = if let Some(a) = alias {
            if a.at.is_some() || !a.columns.is_empty() {
                return None;
            }
            ident(&a.name)?
        } else {
            p.last()?.clone()
        };
        Some((label, columns))
    }
    fn query(
        &mut self,
        q: &Query,
        inherited: &Scope,
        catalog: &str,
        schema: &str,
        depth: usize,
    ) -> Option<Columns> {
        if q.order_by.is_some()
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
        let mut ctes = inherited.clone();
        if let Some(w) = &q.with {
            if w.recursive {
                return None;
            }
            // Reserve all names: forward references must not resolve as tables.
            let mut future = BTreeSet::new();
            for c in &w.cte_tables {
                let n = ident(&c.alias.name)?;
                if ctes.contains_key(&n) || !future.insert(n) {
                    return None;
                }
            }
            for c in &w.cte_tables {
                if c.from.is_some()
                    || c.materialized.is_some()
                    || c.alias.at.is_some()
                    || c.alias.columns.iter().any(|c| c.data_type.is_some())
                {
                    return None;
                }
                let body = c.query.clone();
                // Bare forward/self references are not physical relations.
                struct Future<'a>(&'a BTreeSet<String>);
                impl Visitor for Future<'_> {
                    type Break = ();
                    fn pre_visit_relation(&mut self, n: &ObjectName) -> std::ops::ControlFlow<()> {
                        if parts(n).is_some_and(|p| p.len() == 1 && self.0.contains(&p[0])) {
                            std::ops::ControlFlow::Break(())
                        } else {
                            std::ops::ControlFlow::Continue(())
                        }
                    }
                }
                if body.visit(&mut Future(&future)).is_break() {
                    return None;
                }
                let mut cols = self.query(&body, &ctes, catalog, schema, depth)?;
                if !c.alias.columns.is_empty() {
                    let out = cols.as_mut()?;
                    if out.len() != c.alias.columns.len() {
                        return None;
                    }
                    for (entry, name) in out.iter_mut().zip(&c.alias.columns) {
                        entry.0 = ident(&name.name)?;
                    }
                }
                let n = ident(&c.alias.name)?;
                future.remove(&n);
                ctes.insert(n, cols);
            }
        }
        let SetExpr::Select(s) = q.body.as_ref() else {
            return None;
        };
        if !s.optimizer_hints.is_empty()
            || s.distinct.is_some()
            || s.select_modifiers.is_some()
            || s.top.is_some()
            || s.exclude.is_some()
            || s.into.is_some()
            || !s.lateral_views.is_empty()
            || s.prewhere.is_some()
            || !s.connect_by.is_empty()
            || !s.cluster_by.is_empty()
            || !s.distribute_by.is_empty()
            || !s.sort_by.is_empty()
            || !s.named_window.is_empty()
            || s.qualify.is_some()
            || s.value_table_mode.is_some()
            || s.flavor != SelectFlavor::Standard
        {
            return None;
        }
        if s.from.len() > 1 {
            return None;
        }
        let mut scope = Scope::new();
        for t in &s.from {
            let (n, c) = self.table(&t.relation, &ctes, catalog, schema, depth)?;
            scope.insert(n, c);
            for j in &t.joins {
                if j.global {
                    return None;
                }
                let (n, c) = self.table(&j.relation, &ctes, catalog, schema, depth)?;
                if scope.insert(n, c).is_some() {
                    return None;
                }
                let constraint = match &j.join_operator {
                    JoinOperator::Join(c)
                    | JoinOperator::Inner(c)
                    | JoinOperator::Left(c)
                    | JoinOperator::LeftOuter(c) => c,
                    _ => return None,
                };
                let JoinConstraint::On(e) = constraint else {
                    return None;
                };
                if expression(e, &scope)? != Scalar::Boolean {
                    return None;
                }
            }
        }
        if let Some(e) = &s.selection {
            if expression(e, &scope)? != Scalar::Boolean {
                return None;
            }
        }
        match &s.group_by {
            GroupByExpr::Expressions(es, mods) if mods.is_empty() => {
                for e in es {
                    // A numeric GROUP BY item can be a projection ordinal,
                    // not a literal grouping key. Do not guess its binding.
                    let mut plain = e;
                    while let Expr::Nested(inner) = plain {
                        plain = inner;
                    }
                    if matches!(plain,Expr::Value(v) if matches!(v.value,Value::Number(..))) {
                        return None;
                    }
                    if !matches!(
                        expression(e, &scope)?,
                        Scalar::Integer | Scalar::Boolean | Scalar::Null
                    ) {
                        return None;
                    }
                }
            }
            _ => return None,
        }
        if let Some(e) = &s.having {
            if expression(e, &scope)? != Scalar::Boolean {
                return None;
            }
        }
        let mut out = Vec::new();
        for p in &s.projection {
            match p {
                SelectItem::ExprWithAlias { expr, alias } => {
                    out.push((ident(alias)?, expression(expr, &scope)?))
                }
                SelectItem::UnnamedExpr(e) => {
                    let n = match e {
                        Expr::Identifier(i) => ident(i)?,
                        Expr::CompoundIdentifier(p) => ident(p.last()?)?,
                        _ => return None,
                    };
                    out.push((n, expression(e, &scope)?));
                }
                SelectItem::Wildcard(options)
                    if s.projection.len() == 1
                        && s.from.len() == 1
                        && s.from[0].joins.is_empty()
                        && options.to_string().is_empty() =>
                {
                    return Some(None)
                }
                _ => return None,
            }
        }
        Some(Some(out))
    }
}
fn column(name: &str, cols: &Columns) -> Option<Scalar> {
    match cols {
        None => Some(Scalar::Unknown),
        Some(c) => {
            let mut matches = c.iter().filter(|(n, _)| n == name);
            let result = matches.next()?.1.clone();
            if matches.next().is_some() {
                None
            } else {
                Some(result)
            }
        }
    }
}
fn expression(e: &Expr, scope: &Scope) -> Option<Scalar> {
    match e {
        Expr::Identifier(i) => {
            let n = ident(i)?;
            if matches!(
                n.as_str(),
                "current_user"
                    | "localtimestamp"
                    | "current_time"
                    | "current_date"
                    | "current_timestamp"
                    | "session_user"
                    | "user"
                    | "current_catalog"
                    | "current_schema"
            ) {
                return None;
            }
            if scope.len() == 1 {
                column(&n, scope.values().next()?)
            } else {
                let mut found = scope.values().map(|c| column(&n, c)).collect::<Vec<_>>();
                if found.iter().any(|v| matches!(v, Some(Scalar::Unknown))) {
                    return None;
                }
                let mut hits = found.drain(..).flatten();
                let result = hits.next()?;
                if hits.next().is_some() {
                    None
                } else {
                    Some(result)
                }
            }
        }
        Expr::CompoundIdentifier(p) if p.len() == 2 => {
            column(&ident(&p[1])?, scope.get(&ident(&p[0])?)?)
        }
        Expr::Nested(e) => expression(e, scope),
        Expr::IsNull(e) | Expr::IsNotNull(e) => {
            expression(e, scope)?;
            Some(Scalar::Boolean)
        }
        _ => crate::warehouse::databricks::expression_effect(e, &|e| expression(e, scope)),
    }
}

pub(crate) fn proof(req: &qc::SqlExecution) -> Option<CopyProof> {
    use sha2::{Digest, Sha256};
    if Warehouse::from_dialect(&req.dialect) != Warehouse::Databricks
        || !matches!(req.execution_type, 1 | 3 | 7)
    {
        return None;
    }
    let state = req.dbt_node_state.as_ref()?;
    let signature = |s: &Option<String>| {
        s.as_ref()
            .is_some_and(|s| s.len() == 32 && s.bytes().all(|c| c.is_ascii_hexdigit()))
    };
    if !matches!(
        (req.execution_type, state.resource_type.as_str()),
        (1 | 3, "model") | (7, "snapshot")
    ) || state.node_unique_id.is_empty()
        || !signature(&state.node_body_hash)
        || !signature(&state.node_configs_hash)
    {
        return None;
    }
    let target = Warehouse::physical_scope_key(&req.dialect, req.target_table.as_deref()?);
    let target_parts = parts(&copy_name(req.target_table.as_deref()?)?)?;
    if target_parts.len() != 3 {
        return None;
    }
    let mut a = Analysis {
        req,
        definitions: BTreeMap::new(),
        active: BTreeSet::new(),
        used: BTreeSet::new(),
        leaves: BTreeSet::new(),
        bindings: Vec::new(),
        visits: 0,
    };
    for dep in &req.query_dependencies {
        let n = copy_name(&dep.name)?;
        let key = resolve(&n, &dep.default_catalog, &dep.default_schema)?;
        if a.definitions.insert(key, dep).is_some() {
            return None;
        }
    }
    a.query(
        &*parse(&req.sql)?,
        &Scope::new(),
        &req.default_catalog,
        req.default_schema.as_deref().unwrap_or(""),
        0,
    )?;
    if a.used.len() != a.definitions.len() {
        return None;
    }
    let mut reported = BTreeMap::new();
    for t in &req.tables {
        let n = copy_name(&t.name)?;
        let key = resolve(
            &n,
            &req.default_catalog,
            req.default_schema.as_deref().unwrap_or(""),
        )?;
        if key == target {
            continue;
        }
        if let Some(old) = reported.insert(key, t.last_modified_epoch) {
            if old != t.last_modified_epoch {
                return None;
            }
        }
    }
    if a.leaves != reported.keys().cloned().collect() {
        return None;
    }
    let extras: BTreeMap<_, _> = req.semantic_extras.iter().collect();
    let effects = "databricks-checked-scalars-v2";
    let framed = serde_json::to_vec(&(
        "databricks-analyzed-workload-v2",
        &req.sql,
        &req.dialect,
        req.execution_type,
        &req.default_catalog,
        (&target_parts[0], &target_parts[2]),
        extras,
        &state.node_unique_id,
        &state.node_body_hash,
        &state.node_configs_hash,
        &state.node_macros_hash,
        &state.node_contract_hash,
        &a.bindings,
        effects,
    ))
    .ok()?;
    Some(CopyProof {
        version: 2,
        identity: hex::encode(Sha256::digest(framed)),
        bindings: a.bindings,
        effects: effects.into(),
    })
}
