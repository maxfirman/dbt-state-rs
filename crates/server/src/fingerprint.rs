//! Versioned evidence shared by submit and batch recording. Length framing
//! prevents concatenation ambiguity; unknown dialects retain exact SQL bytes.
use crate::query_cache as qc;
use sha2::{Digest, Sha256};
use std::collections::HashMap;

fn normalized(dialect: &str, sql: &str) -> String {
    if dialect.eq_ignore_ascii_case("snowflake") {
        crate::sql_norm::normalize_sql(sql)
    } else {
        sql.to_owned()
    }
}

/// Schema-abstracted normalization for the cross-environment (namespace-scoped)
/// logic identity. The SAME model built in different environments differs only
/// in the physical schema qualifier of its three-part names; abstracting that
/// yields a shared identity (verified live — see sql_norm docs).
fn normalized_ns(dialect: &str, sql: &str) -> String {
    if dialect.eq_ignore_ascii_case("snowflake") {
        crate::sql_norm::normalize_sql_namespace_abstracted(sql)
    } else {
        sql.to_owned()
    }
}

fn field(h: &mut Sha256, bytes: &[u8]) {
    h.update((bytes.len() as u64).to_le_bytes());
    h.update(bytes);
}

fn extras(h: &mut Sha256, values: &HashMap<String, String>) {
    let mut pairs: Vec<_> = values.iter().collect();
    pairs.sort();
    field(h, &(pairs.len() as u64).to_le_bytes());
    for (key, value) in pairs {
        field(h, key.as_bytes());
        field(h, value.as_bytes());
    }
}

/// `template` is the existing template-comparison mode, whose exact hosted
/// truth table remains unresolved. Its namespace cannot collide with SQL mode.
pub fn sql_logic_hash(req: &qc::SqlExecution, template: Option<&str>) -> String {
    let mut h = Sha256::new();
    field(&mut h, b"dbt-state-sql-v2");
    field(&mut h, req.dialect.to_ascii_lowercase().as_bytes());
    field(&mut h, req.default_catalog.as_bytes());
    field(
        &mut h,
        req.default_schema.as_deref().unwrap_or("").as_bytes(),
    );
    match template {
        Some(body) => {
            field(&mut h, b"template");
            field(&mut h, body.as_bytes());
        }
        None => {
            field(&mut h, b"rendered");
            field(&mut h, normalized(&req.dialect, &req.sql).as_bytes());
        }
    }
    extras(&mut h, &req.semantic_extras);
    let mut dependencies: Vec<_> = req
        .query_dependencies
        .iter()
        .map(|d| {
            (
                d.name.as_str(),
                normalized(&req.dialect, &d.query),
                d.default_catalog.as_str(),
                d.default_schema.as_str(),
            )
        })
        .collect();
    dependencies.sort();
    field(&mut h, &(dependencies.len() as u64).to_le_bytes());
    for (name, query, catalog, schema) in dependencies {
        for value in [name, query.as_str(), catalog, schema] {
            field(&mut h, value.as_bytes());
        }
    }
    hex::encode(h.finalize())
}

/// Cross-environment logic identity, scoped by `table_namespace`.
///
/// The same dbt model built in different environments (e.g. prod and a dev
/// clone) produces rendered SQL that differs ONLY in the physical schema
/// qualifier of its three-part object names, and dependency definitions that
/// differ only likewise. This hash abstracts those schema components (and omits
/// the physical default_schema and per-dependency physical names/schemas) so
/// the identity is identical across environments. It is used ONLY for
/// namespace-grouped cross-environment reuse (e.g. adopting a cloned object);
/// the strict `sql_logic_hash` remains the primary same-target identity.
///
/// VERIFIED LIVE (api.state.dbt.com, Fusion client, Snowflake): a dev submit
/// whose rendered SQL differs from the prod source only by schema is treated as
/// the same logic (skips after a clone), while a genuine projection change
/// executes. See experiments/clone_logic_sensitivity.sh.
pub fn namespace_logic_hash(req: &qc::SqlExecution) -> String {
    let mut h = Sha256::new();
    field(&mut h, b"dbt-state-ns-logic-v1");
    field(&mut h, req.dialect.to_ascii_lowercase().as_bytes());
    // Intentionally NO default_catalog/default_schema: those are physical and
    // differ across environments. The abstracted SQL carries the logic.
    field(&mut h, normalized_ns(&req.dialect, &req.sql).as_bytes());
    extras(&mut h, &req.semantic_extras);
    // Dependencies: compare only the schema-abstracted query text, sorted. The
    // physical dependency NAME and its catalog/schema are environment-specific
    // and excluded; the abstracted definition carries the dependency's logic.
    let mut dependency_queries: Vec<String> = req
        .query_dependencies
        .iter()
        .map(|d| normalized_ns(&req.dialect, &d.query))
        .collect();
    dependency_queries.sort();
    field(&mut h, &(dependency_queries.len() as u64).to_le_bytes());
    for query in dependency_queries {
        field(&mut h, query.as_bytes());
    }
    hex::encode(h.finalize())
}

pub fn seed_logic_hash(
    dialect: &str,
    catalog: &str,
    semantic_extras: &HashMap<String, String>,
) -> String {
    let mut h = Sha256::new();
    field(&mut h, b"dbt-state-seed-v2");
    field(&mut h, dialect.to_ascii_lowercase().as_bytes());
    field(&mut h, catalog.as_bytes());
    extras(&mut h, semantic_extras);
    hex::encode(h.finalize())
}
