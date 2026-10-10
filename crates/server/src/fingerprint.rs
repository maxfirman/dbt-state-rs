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
