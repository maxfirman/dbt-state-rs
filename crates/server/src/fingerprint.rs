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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_cache as qc;

    fn dep(name: &str, query: &str, catalog: &str, schema: &str) -> qc::QueryDependency {
        qc::QueryDependency {
            name: name.into(),
            query: query.into(),
            default_catalog: catalog.into(),
            default_schema: schema.into(),
        }
    }

    fn exec(sql: &str, deps: Vec<qc::QueryDependency>) -> qc::SqlExecution {
        qc::SqlExecution {
            target_table: Some("\"DB\".\"S\".\"T\"".into()),
            dialect: "snowflake".into(),
            default_catalog: "DB".into(),
            default_schema: Some("S".into()),
            execution_type: 1,
            sql: sql.into(),
            tables: vec![],
            query_dependencies: deps,
            semantic_extras: HashMap::new(),
            labels: HashMap::new(),
            dbt_node_state: None,
            table_namespace: Some("ns".into()),
            from_speculative_submit: false,
        }
    }

    fn strict(e: &qc::SqlExecution) -> String {
        sql_logic_hash(e, None)
    }

    // ---- STRICT identity (same-target reuse) is dependency-sensitive ---------

    /// Identical dependency sets produce the same strict fingerprint.
    #[test]
    fn strict_identical_dependencies_match() {
        let a = exec(
            "select * from v",
            vec![dep("\"DB\".\"S\".\"V\"", "select 1 as id", "DB", "S")],
        );
        let b = exec(
            "select * from v",
            vec![dep("\"DB\".\"S\".\"V\"", "select 1 as id", "DB", "S")],
        );
        assert_eq!(strict(&a), strict(&b));
    }

    /// A changed dependency DEFINITION (query text) changes the strict hash:
    /// different physical data behind the same reference must not be reused.
    #[test]
    fn strict_changed_dependency_query_differs() {
        let a = exec(
            "select * from v",
            vec![dep("\"DB\".\"S\".\"V\"", "select 1 as id", "DB", "S")],
        );
        let b = exec(
            "select * from v",
            vec![dep("\"DB\".\"S\".\"V\"", "select 2 as id", "DB", "S")],
        );
        assert_ne!(
            strict(&a),
            strict(&b),
            "changed view definition must differ"
        );
    }

    /// The strict identity distinguishes the physical dependency NAME and its
    /// resolution catalog/schema — same-target reuse must see different physical
    /// inputs as different.
    #[test]
    fn strict_distinguishes_physical_dependency_name_and_context() {
        let base = exec(
            "select * from v",
            vec![dep("\"DB\".\"S\".\"V\"", "select 1 as id", "DB", "S")],
        );
        let diff_name = exec(
            "select * from v",
            vec![dep("\"DB\".\"S\".\"W\"", "select 1 as id", "DB", "S")],
        );
        let diff_schema = exec(
            "select * from v",
            vec![dep("\"DB\".\"S\".\"V\"", "select 1 as id", "DB", "OTHER")],
        );
        let diff_catalog = exec(
            "select * from v",
            vec![dep("\"DB\".\"S\".\"V\"", "select 1 as id", "OTHER", "S")],
        );
        assert_ne!(
            strict(&base),
            strict(&diff_name),
            "physical dep name matters"
        );
        assert_ne!(strict(&base), strict(&diff_schema), "dep schema matters");
        assert_ne!(strict(&base), strict(&diff_catalog), "dep catalog matters");
    }

    /// Dependency ORDER must not change identity (sets, not sequences).
    #[test]
    fn dependency_order_is_irrelevant() {
        let d1 = dep("\"DB\".\"S\".\"A\"", "select 1 as id", "DB", "S");
        let d2 = dep("\"DB\".\"S\".\"B\"", "select 2 as id", "DB", "S");
        let a = exec("select * from a join b", vec![d1.clone(), d2.clone()]);
        let b = exec("select * from a join b", vec![d2, d1]);
        assert_eq!(strict(&a), strict(&b), "dependency order must not matter");
        assert_eq!(
            namespace_logic_hash(&a),
            namespace_logic_hash(&b),
            "dependency order must not matter (abstracted)"
        );
    }

    // ---- NAMESPACE-ABSTRACTED identity (cross-environment reuse) -------------

    /// The abstracted identity EXCLUDES the physical dependency name and its
    /// catalog/schema: the SAME model's view in prod vs a dev clone (different
    /// physical name/schema, schema-abstracted-equivalent query) shares identity.
    /// This is the deliberate cross-environment behavior (verified live).
    #[test]
    fn ns_abstracts_physical_dependency_name_and_context() {
        let prod = exec(
            "select order_id from CDO.prod.stg_orders",
            vec![dep(
                "\"CDO\".\"PROD\".\"STG_ORDERS\"",
                "create or replace view STG_ORDERS as (select * from CDO.prod.raw_orders)",
                "CDO",
                "prod",
            )],
        );
        let dev = exec(
            "select order_id from CDO.dev.stg_orders",
            vec![dep(
                "\"CDO\".\"DEV\".\"STG_ORDERS\"",
                "create or replace view STG_ORDERS as (select * from CDO.dev.raw_orders)",
                "CDO",
                "dev",
            )],
        );
        assert_eq!(
            namespace_logic_hash(&prod),
            namespace_logic_hash(&dev),
            "same model across envs (schema-only diff) shares abstracted identity"
        );
        // ...but the STRICT identity still distinguishes them (schema-sensitive).
        assert_ne!(
            strict(&prod),
            strict(&dev),
            "strict identity stays environment-specific"
        );
    }

    /// NEGATIVE: the abstracted identity still distinguishes a genuine change to
    /// the dependency's LOGIC (not just its environment qualifier). Different
    /// physical dependencies with genuinely different query logic must NOT
    /// collide, even though physical names are excluded.
    #[test]
    fn ns_distinguishes_dependency_logic_change() {
        let a = exec(
            "select order_id from CDO.dev.stg_orders",
            vec![dep(
                "\"CDO\".\"DEV\".\"STG_ORDERS\"",
                "create or replace view STG_ORDERS as (select * from CDO.dev.raw_orders)",
                "CDO",
                "dev",
            )],
        );
        // The view now filters rows — a genuine logic change in the dependency.
        let b = exec(
            "select order_id from CDO.dev.stg_orders",
            vec![dep(
                "\"CDO\".\"DEV\".\"STG_ORDERS\"",
                "create or replace view STG_ORDERS as (select * from CDO.dev.raw_orders where ordered_at > '2020-01-01')",
                "CDO",
                "dev",
            )],
        );
        assert_ne!(
            namespace_logic_hash(&a),
            namespace_logic_hash(&b),
            "a dependency LOGIC change must remain distinct under abstraction"
        );
    }

    /// NEGATIVE: a different NUMBER of dependencies must differ (length framing
    /// prevents a dropped dependency from colliding with a reordered one).
    #[test]
    fn ns_distinguishes_dependency_set_size() {
        let one = exec(
            "select * from a",
            vec![dep("\"DB\".\"S\".\"A\"", "select 1 as id", "DB", "S")],
        );
        let two = exec(
            "select * from a join b",
            vec![
                dep("\"DB\".\"S\".\"A\"", "select 1 as id", "DB", "S"),
                dep("\"DB\".\"S\".\"B\"", "select 2 as id", "DB", "S"),
            ],
        );
        assert_ne!(namespace_logic_hash(&one), namespace_logic_hash(&two));
        assert_ne!(strict(&one), strict(&two));
    }

    /// NEGATIVE: the root SQL's own logic change is distinguished under
    /// abstraction (abstraction only collapses the schema qualifier, not the
    /// projection).
    #[test]
    fn ns_distinguishes_root_projection_change() {
        let a = exec("select order_id from CDO.dev.t", vec![]);
        let b = exec("select order_id, customer_id from CDO.dev.t", vec![]);
        assert_ne!(namespace_logic_hash(&a), namespace_logic_hash(&b));
    }

    /// The template (compare_unrendered_code) mode has a DISTINCT namespace from
    /// rendered mode: the same body bytes under template mode must not collide
    /// with rendered SQL that happens to hash the same.
    #[test]
    fn template_and_rendered_modes_do_not_collide() {
        let e = exec("select 1 as id", vec![]);
        let rendered = sql_logic_hash(&e, None);
        let template = sql_logic_hash(&e, Some("some-body-hash"));
        assert_ne!(rendered, template, "template vs rendered must not collide");
    }
}
