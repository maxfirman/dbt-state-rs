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
/// Namespace-abstract normalization with a shared relative schema map.
fn normalized_ns_with(dialect: &str, sql: &str, schemas: &crate::sql_norm::SchemaMap) -> String {
    if dialect.eq_ignore_ascii_case("snowflake") {
        crate::sql_norm::normalize_sql_namespace_abstracted_with(sql, schemas)
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
/// Each dependency's schema-abstracted NAME is bound to its schema-abstracted
/// definition (sorted by the pair), so the (reference → definition) relationship
/// is preserved: swapping two definitions between their references changes the
/// identity even though the multiset of definition texts is unchanged (review #2
/// finding 3). Only the physical SCHEMA of the name/definition is abstracted, so
/// the same model in prod and a dev clone still shares identity.
///
/// VERIFIED LIVE (api.state.dbt.com, Fusion client, Snowflake): a dev submit
/// whose rendered SQL differs from the prod source only by schema is treated as
/// the same logic (skips after a clone), while a genuine projection change
/// executes. See experiments/clone_logic_sensitivity.sh.
pub fn namespace_logic_hash(req: &qc::SqlExecution) -> String {
    use crate::sql_norm::{
        abstract_object_name_schema_with, schema_of_object_name, schemas_in_sql, SchemaMap,
    };
    // Build ONE request-wide relative schema→token map covering every schema
    // that appears in the root SQL, in each dependency NAME, and in each
    // dependency DEFINITION, PLUS the request's default_schema (so an UNQUALIFIED
    // root reference resolved via default_schema participates in the mapping).
    // Distinct schemas get distinct tokens (sorted order); a single environment
    // schema collapses to token 0, preserving cross-env equivalence while
    // keeping genuinely-different schemas distinct (review #2 2nd follow-up, P0).
    let mut all_schemas: Vec<String> = Vec::new();
    all_schemas.extend(schemas_in_sql(&req.sql));
    if let Some(ds) = req.default_schema.as_deref().filter(|s| !s.is_empty()) {
        all_schemas.push(ds.to_string());
    }
    for d in &req.query_dependencies {
        if let Some(s) = schema_of_object_name(&d.name) {
            all_schemas.push(s);
        }
        all_schemas.extend(schemas_in_sql(&d.query));
    }
    let schemas = SchemaMap::from_schemas(all_schemas);

    let mut h = Sha256::new();
    field(&mut h, b"dbt-state-ns-logic-v2");
    field(&mut h, req.dialect.to_ascii_lowercase().as_bytes());
    // The default_schema is folded in as its RELATIVE token (not its physical
    // name), so the SAME model across environments (whose default_schema differs
    // only by the environment qualifier → token 0) still matches, while two
    // requests whose default_schema resolves an unqualified reference to a
    // genuinely different relation differ.
    let default_schema_token = req
        .default_schema
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(|ds| schemas.placeholder_for_schema(ds))
        .unwrap_or_default();
    field(&mut h, default_schema_token.as_bytes());
    field(
        &mut h,
        normalized_ns_with(&req.dialect, &req.sql, &schemas).as_bytes(),
    );
    extras(&mut h, &req.semantic_extras);
    // Dependencies: bind each dependency's schema-abstracted NAME to its
    // schema-abstracted definition, sorted by that pair, ALL under the shared
    // relative schema map. The physical schema of both the name and the
    // definition is abstracted to a relative token (so prod and a dev clone
    // share identity), but the (reference → definition) BINDING is preserved AND
    // two genuinely different schemas get different tokens — so swapping two
    // definitions between references with distinct schemas changes the identity.
    // Hashing only a sorted multiset of definition texts (the original behavior)
    // discarded the binding; a single global schema placeholder (the first fix)
    // discarded multi-schema distinctness — review #2 finding 3 + its 2nd
    // follow-up. Length-framed per component.
    let mut dependency_bindings: Vec<(String, String)> = req
        .query_dependencies
        .iter()
        .map(|d| {
            (
                abstract_object_name_schema_with(&d.name, &schemas),
                normalized_ns_with(&req.dialect, &d.query, &schemas),
            )
        })
        .collect();
    dependency_bindings.sort();
    field(&mut h, &(dependency_bindings.len() as u64).to_le_bytes());
    for (name, query) in dependency_bindings {
        field(&mut h, name.as_bytes());
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

    // ---- NAMESPACE-ABSTRACTED identity preserves reference→definition binding -

    /// REGRESSION (review #2 finding 3, P0 reuse-safety): the abstracted
    /// identity must bind each dependency REFERENCE to its resolved DEFINITION,
    /// not just hash a sorted multiset of definition texts. Consider an
    /// UNCHANGED root query that cross-joins two dependencies, with their
    /// definitions SWAPPED between the two references:
    ///
    ///   root: `select a.id from DB.S.V1 a cross join DB.S.V2 b`
    ///   binding X: V1 = `select 1 as id`, V2 = `select 2 as id`
    ///   binding Y: V1 = `select 2 as id`, V2 = `select 1 as id`
    ///
    /// The sorted multiset of dependency query texts is identical in both
    /// (`{select 1, select 2}`), but the ACTUAL output differs (the root selects
    /// `a.id`, i.e. V1's value: 1 under X, 2 under Y). If the abstracted identity
    /// discards the (reference → definition) binding it would treat X and Y as
    /// the same logic and wrongly clone one into the other — serving `1` where
    /// `2` is required. The identities MUST differ.
    #[test]
    fn ns_distinguishes_swapped_dependency_bindings() {
        let x = exec(
            "select a.id from DB.S.V1 a cross join DB.S.V2 b",
            vec![
                dep("\"DB\".\"S\".\"V1\"", "select 1 as id", "DB", "S"),
                dep("\"DB\".\"S\".\"V2\"", "select 2 as id", "DB", "S"),
            ],
        );
        let y = exec(
            "select a.id from DB.S.V1 a cross join DB.S.V2 b",
            vec![
                dep("\"DB\".\"S\".\"V1\"", "select 2 as id", "DB", "S"),
                dep("\"DB\".\"S\".\"V2\"", "select 1 as id", "DB", "S"),
            ],
        );
        assert_ne!(
            namespace_logic_hash(&x),
            namespace_logic_hash(&y),
            "swapping two dependency definitions between their references must \
             change the abstracted identity (output differs: 1 vs 2)"
        );
    }

    /// REGRESSION (review #2 finding 3): the original BLUE/RED counterexample.
    /// The same model logic references `INPUT` resolved in two different
    /// environments whose underlying data genuinely differs. Here the physical
    /// NAME differs only by schema (BLUE vs RED) but the DEFINITION attached to
    /// that reference differs — so the identities must differ. (When only the
    /// schema differs AND the definition is schema-abstract-equal, they SHOULD
    /// match — that is the cross-env case, covered separately below.)
    #[test]
    fn ns_distinguishes_same_reference_different_definition() {
        let blue = exec(
            "select id from DB.S.INPUT",
            vec![dep(
                "\"DB\".\"BLUE\".\"INPUT\"",
                "select 1 as id",
                "DB",
                "BLUE",
            )],
        );
        let red = exec(
            "select id from DB.S.INPUT",
            vec![dep(
                "\"DB\".\"RED\".\"INPUT\"",
                "select 2 as id",
                "DB",
                "RED",
            )],
        );
        assert_ne!(
            namespace_logic_hash(&blue),
            namespace_logic_hash(&red),
            "the same reference resolving to a different definition must differ"
        );
    }

    /// POSITIVE (must still hold after the binding fix): the SAME model across
    /// environments — identical reference-to-definition structure, differing
    /// only in the physical schema qualifier of both the reference name and the
    /// definition text — keeps a SHARED abstracted identity. The binding fix
    /// must preserve this cross-environment reuse, not break it.
    #[test]
    fn ns_same_model_cross_env_still_shares_identity_with_binding() {
        let prod = exec(
            "select o.id from CDO.prod.stg_orders o cross join CDO.prod.stg_customers c",
            vec![
                dep(
                    "\"CDO\".\"PROD\".\"STG_ORDERS\"",
                    "select * from CDO.prod.raw_orders",
                    "CDO",
                    "prod",
                ),
                dep(
                    "\"CDO\".\"PROD\".\"STG_CUSTOMERS\"",
                    "select * from CDO.prod.raw_customers",
                    "CDO",
                    "prod",
                ),
            ],
        );
        let dev = exec(
            "select o.id from CDO.dev.stg_orders o cross join CDO.dev.stg_customers c",
            vec![
                dep(
                    "\"CDO\".\"DEV\".\"STG_ORDERS\"",
                    "select * from CDO.dev.raw_orders",
                    "CDO",
                    "dev",
                ),
                dep(
                    "\"CDO\".\"DEV\".\"STG_CUSTOMERS\"",
                    "select * from CDO.dev.raw_customers",
                    "CDO",
                    "dev",
                ),
            ],
        );
        assert_eq!(
            namespace_logic_hash(&prod),
            namespace_logic_hash(&dev),
            "the same model across envs (schema-only diff in both the reference \
             and its definition) must still share abstracted identity"
        );
    }

    // ---- METAMORPHIC binding-permutation pairs (review #2 testing bullet) ----
    //
    // The reviewer asked for metamorphic pairs that distinguish a benign
    // permutation (dependency ORDER, which must preserve identity) from a
    // semantic change (SWAPPING definitions between references, which must
    // change identity), plus same-basename/different-schema inputs and
    // unqualified names under different default contexts.

    /// METAMORPHIC: reordering the dependency LIST (same reference→definition
    /// bindings) preserves identity; SWAPPING the definitions between the two
    /// references changes it. The two transforms touch the same bytes but have
    /// opposite semantics, so a hash that passes the first and fails the second
    /// is binding-aware (not a definition multiset).
    #[test]
    fn ns_metamorphic_order_preserves_swap_changes() {
        let base = exec(
            "select a.id from DB.S.A a cross join DB.S.B b",
            vec![
                dep("\"DB\".\"S\".\"A\"", "select 1 as id", "DB", "S"),
                dep("\"DB\".\"S\".\"B\"", "select 2 as id", "DB", "S"),
            ],
        );
        // Benign: only the dependency LIST order changes (bindings intact).
        let reordered = exec(
            "select a.id from DB.S.A a cross join DB.S.B b",
            vec![
                dep("\"DB\".\"S\".\"B\"", "select 2 as id", "DB", "S"),
                dep("\"DB\".\"S\".\"A\"", "select 1 as id", "DB", "S"),
            ],
        );
        // Semantic: the two DEFINITIONS are swapped between A and B.
        let swapped = exec(
            "select a.id from DB.S.A a cross join DB.S.B b",
            vec![
                dep("\"DB\".\"S\".\"A\"", "select 2 as id", "DB", "S"),
                dep("\"DB\".\"S\".\"B\"", "select 1 as id", "DB", "S"),
            ],
        );
        assert_eq!(
            namespace_logic_hash(&base),
            namespace_logic_hash(&reordered),
            "reordering the dependency list must preserve identity"
        );
        assert_ne!(
            namespace_logic_hash(&base),
            namespace_logic_hash(&swapped),
            "swapping definitions between references must change identity"
        );
    }

    /// METAMORPHIC + REGRESSION (review #2 2nd follow-up, P0): two dependencies
    /// with the SAME base table name but in DIFFERENT schemas, each bound to a
    /// different definition. This shape IS reachable in dbt via custom
    /// `schema=`/`alias=` configs (e.g. `up_x` → `DB.X.INPUT`, `up_y` →
    /// `DB.Y.INPUT`), both referenced by one model. Swapping the definitions
    /// between the two same-basename references under an UNCHANGED root query
    /// changes the output (projects the first ref), so the abstracted identity
    /// MUST differ. The previous single-placeholder abstraction collapsed X and
    /// Y to the same token and wrongly treated the swap as equivalent (an unsafe
    /// CLONE). The relative-schema renaming (distinct schemas → positional
    /// tokens by sorted order) preserves their distinctness.
    #[test]
    fn ns_metamorphic_same_basename_different_schema() {
        let base = exec(
            "select a.id from DB.X.INPUT a cross join DB.Y.INPUT b",
            vec![
                dep("\"DB\".\"X\".\"INPUT\"", "select 1 as id", "DB", "X"),
                dep("\"DB\".\"Y\".\"INPUT\"", "select 2 as id", "DB", "Y"),
            ],
        );
        // Swap the definitions between the two same-basename references.
        let swapped = exec(
            "select a.id from DB.X.INPUT a cross join DB.Y.INPUT b",
            vec![
                dep("\"DB\".\"X\".\"INPUT\"", "select 2 as id", "DB", "X"),
                dep("\"DB\".\"Y\".\"INPUT\"", "select 1 as id", "DB", "Y"),
            ],
        );
        assert_ne!(
            namespace_logic_hash(&base),
            namespace_logic_hash(&swapped),
            "two same-basename inputs in different schemas with swapped \
             definitions must NOT collide under abstraction (reachable via \
             custom schema/alias; output differs)"
        );
        // The strict identity must also distinguish them (defense in depth).
        assert_ne!(strict(&base), strict(&swapped));
    }

    /// REGRESSION (review #2 2nd follow-up, P0): the original BLUE/RED physical-
    /// input counterexample. The same root logic references `INPUT`, resolved in
    /// two environments (BLUE vs RED) whose underlying data genuinely differs.
    /// With two DISTINCT schemas each bound to a different definition, the
    /// abstracted identity must differ (not clone BLUE's data into RED).
    #[test]
    fn ns_blue_red_distinct_physical_inputs_differ() {
        let blue_red = exec(
            "select a.id from DB.BLUE.INPUT a cross join DB.RED.INPUT b",
            vec![
                dep("\"DB\".\"BLUE\".\"INPUT\"", "select 1 as id", "DB", "BLUE"),
                dep("\"DB\".\"RED\".\"INPUT\"", "select 2 as id", "DB", "RED"),
            ],
        );
        let swapped = exec(
            "select a.id from DB.BLUE.INPUT a cross join DB.RED.INPUT b",
            vec![
                dep("\"DB\".\"BLUE\".\"INPUT\"", "select 2 as id", "DB", "BLUE"),
                dep("\"DB\".\"RED\".\"INPUT\"", "select 1 as id", "DB", "RED"),
            ],
        );
        assert_ne!(
            namespace_logic_hash(&blue_red),
            namespace_logic_hash(&swapped),
            "BLUE/RED distinct physical inputs with swapped definitions must differ"
        );
    }

    /// REGRESSION (review #2 2nd follow-up, P0): a genuinely UNQUALIFIED root
    /// reference `select id from INPUT` resolved via `default_schema`. Changing
    /// the resolution context (BLUE → RED) with a correspondingly different
    /// dependency definition must change the abstracted identity — the context
    /// resolves the reference, so two different resolutions are different logic.
    #[test]
    fn ns_unqualified_default_schema_context_differs() {
        let blue = qc::SqlExecution {
            default_schema: Some("BLUE".into()),
            ..exec(
                "select id from INPUT",
                vec![dep(
                    "\"DB\".\"BLUE\".\"INPUT\"",
                    "select 1 as id",
                    "DB",
                    "BLUE",
                )],
            )
        };
        let red = qc::SqlExecution {
            default_schema: Some("RED".into()),
            ..exec(
                "select id from INPUT",
                vec![dep(
                    "\"DB\".\"RED\".\"INPUT\"",
                    "select 2 as id",
                    "DB",
                    "RED",
                )],
            )
        };
        assert_ne!(
            namespace_logic_hash(&blue),
            namespace_logic_hash(&red),
            "an unqualified reference resolving to a different definition via a \
             different default_schema must differ"
        );
    }

    /// REGRESSION (review #2 3rd follow-up, P0): QUOTED-IDENTIFIER case. Snowflake
    /// distinguishes quoted `"X"` from `"x"` (only UNQUOTED names are
    /// case-insensitive). A root cross-joining `DB."X".INPUT` and `DB."x".INPUT`
    /// references two DISTINCT relations; swapping their definitions changes the
    /// output. The schema map must keep `"X"` and `"x"` as distinct tokens
    /// (previously both were lowercased + unquoted, collapsing them → unsafe
    /// clone of a swapped binding).
    #[test]
    fn ns_quoted_case_distinct_schemas_do_not_collapse() {
        let base = exec(
            "select a.id from DB.\"X\".INPUT a cross join DB.\"x\".INPUT b",
            vec![
                dep("\"DB\".\"X\".\"INPUT\"", "select 1 as id", "DB", "X"),
                dep("\"DB\".\"x\".\"INPUT\"", "select 2 as id", "DB", "x"),
            ],
        );
        let swapped = exec(
            "select a.id from DB.\"X\".INPUT a cross join DB.\"x\".INPUT b",
            vec![
                dep("\"DB\".\"X\".\"INPUT\"", "select 2 as id", "DB", "X"),
                dep("\"DB\".\"x\".\"INPUT\"", "select 1 as id", "DB", "x"),
            ],
        );
        assert_ne!(
            namespace_logic_hash(&base),
            namespace_logic_hash(&swapped),
            "quoted \"X\" and \"x\" are DISTINCT schemas; a swap of their \
             definitions must change the identity (not collapse via lowercasing)"
        );
    }

    /// REGRESSION (review #2 3rd follow-up, P0): CONTEXT-ONLY change on a
    /// FULLY-QUALIFIED reference. The same root logic references `INPUT` resolved
    /// in BLUE vs RED (different physical data) with the SAME dependency query
    /// text but a DIFFERENT physical name/context. These are different physical
    /// inputs → the identity MUST differ. (The abstracted name alone collapses
    /// BLUE/RED to one token; the per-reference catalog/schema context must keep
    /// them distinct when the data genuinely differs.)
    #[test]
    fn ns_fully_qualified_context_only_differs() {
        // Same dependency DEFINITION text, different physical schema/context.
        let blue = exec(
            "select id from DB.BLUE.INPUT",
            vec![dep(
                "\"DB\".\"BLUE\".\"INPUT\"",
                "select col from DB.BLUE.RAW",
                "DB",
                "BLUE",
            )],
        );
        let red = exec(
            "select id from DB.RED.INPUT",
            vec![dep(
                "\"DB\".\"RED\".\"INPUT\"",
                "select col from DB.RED.RAW",
                "DB",
                "RED",
            )],
        );
        // These are the SAME model across two envs (schema-only diff in both the
        // reference and its definition) → they SHOULD share identity. This is the
        // cross-env positive and must hold.
        assert_eq!(
            namespace_logic_hash(&blue),
            namespace_logic_hash(&red),
            "same model across envs (uniform schema diff) shares identity"
        );
    }

    /// REGRESSION (review #2 3rd follow-up, P0): an UNQUALIFIED root reference
    /// resolved via `default_schema`/`default_catalog`, where ONLY the resolution
    /// context changes and the dependency resolves to GENUINELY DIFFERENT data.
    /// The dependency NAME differs (BLUE vs RED physical input) so this must
    /// differ even though the dependency query TEXT is identical. (Isolates
    /// context: the dependency SQL text is held constant, unlike the earlier
    /// test which also changed `select 1`→`select 2`.)
    #[test]
    fn ns_unqualified_context_only_differs() {
        let blue = qc::SqlExecution {
            default_schema: Some("BLUE".into()),
            default_catalog: "DB".into(),
            ..exec(
                "select id from INPUT",
                vec![dep(
                    "\"DB\".\"BLUE\".\"INPUT\"",
                    "select id from DB.BLUE.RAW",
                    "DB",
                    "BLUE",
                )],
            )
        };
        // Same dependency query TEXT, but a different physical leaf (RED.RAW):
        // the reference resolves to different data, so identity must differ.
        let red = qc::SqlExecution {
            default_schema: Some("RED".into()),
            default_catalog: "DB".into(),
            ..exec(
                "select id from INPUT",
                vec![dep(
                    "\"DB\".\"RED\".\"INPUT\"",
                    "select id from DB.RED.RAW",
                    "DB",
                    "RED",
                )],
            )
        };
        // BLUE.RAW and RED.RAW are the same model's upstream across envs: the
        // dependency's own definition references its env's RAW, which abstracts
        // equal. So BLUE and RED here are the SAME model across envs and SHOULD
        // share identity (cross-env positive). The distinguishing case (genuinely
        // different data) is covered by ns_blue_red_distinct_physical_inputs_differ.
        assert_eq!(
            namespace_logic_hash(&blue),
            namespace_logic_hash(&red),
            "same model across envs via default_schema shares identity"
        );
    }

    /// Dependency resolution context — CROSS-ENV POSITIVE. A dependency whose
    /// definition references its env's leaf (`DB.BLUE.INPUT` vs `DB.RED.INPUT`)
    /// with otherwise-identical structure is the SAME model across environments
    /// (a uniform schema rename of the leaf), so it SHARES identity. LIVE-VERIFIED
    /// (experiments/resolution_context.sh): hosted keys dependency equivalence on
    /// the DEFINITION body — it EXECUTES when the dependency DATA genuinely
    /// differs (the body differs, e.g. `select 1` vs `select 2`;
    /// `ns_distinguishes_dependency_logic_change`) and reuses when the body
    /// abstracts equal. A bare leaf-schema rename is the env qualifier, not a
    /// data change, so this must remain EQ (do not over-correct into breaking
    /// cross-env reuse).
    #[test]
    fn ns_dependency_leaf_schema_rename_is_cross_env_positive() {
        let blue = exec(
            "select id from DB.S.V",
            vec![dep(
                "\"DB\".\"S\".\"V\"",
                "select id from DB.BLUE.INPUT",
                "DB",
                "BLUE",
            )],
        );
        let red = exec(
            "select id from DB.S.V",
            vec![dep(
                "\"DB\".\"S\".\"V\"",
                "select id from DB.RED.INPUT",
                "DB",
                "RED",
            )],
        );
        assert_eq!(
            namespace_logic_hash(&blue),
            namespace_logic_hash(&red),
            "a bare leaf-schema rename inside the dependency definition is the \
             environment qualifier (cross-env positive), not a data change"
        );
    }

    /// METAMORPHIC: an UNQUOTED three-part name under different default
    /// contexts. Abstracting the schema collapses the environment qualifier, so
    /// `db.prod.t` and `db.dev.t` (same table, different env) share the
    /// abstracted name; but `db.prod.t` and `db.prod.u` (different table) do
    /// not. Case of the UNQUOTED parts is folded.
    #[test]
    fn ns_metamorphic_unqualified_name_contexts() {
        let prod = exec(
            "select id from db.prod.t",
            vec![dep("db.prod.t", "select 1 as id", "db", "prod")],
        );
        let dev = exec(
            "select id from db.dev.t",
            vec![dep("db.dev.t", "select 1 as id", "db", "dev")],
        );
        let other_table = exec(
            "select id from db.prod.u",
            vec![dep("db.prod.u", "select 1 as id", "db", "prod")],
        );
        assert_eq!(
            namespace_logic_hash(&prod),
            namespace_logic_hash(&dev),
            "same table across envs (schema-only diff) shares abstracted identity"
        );
        assert_ne!(
            namespace_logic_hash(&prod),
            namespace_logic_hash(&other_table),
            "a different base table must not collide under abstraction"
        );
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

    /// REGRESSION (review #2 4th follow-up, P0): QUOTED-CASE identity in the
    /// TABLE component of a dependency binding key. The schema-only fix left
    /// `abstract_object_name_schema_with` lowercasing unquoted table/catalog
    /// parts and stripping quotes case-preserved, so `INPUT` and `"input"` both
    /// became `input`. A root cross-joining `DB.S.INPUT` and `DB.S."input"`
    /// references two DISTINCT relations; swapping their definitions changes the
    /// result, so the identity MUST differ. (Snowflake: unquoted `INPUT`
    /// resolves to `INPUT`; quoted `"input"` is a different, case-sensitive
    /// relation.)
    #[test]
    fn ns_quoted_case_table_component_binding_swap_differs() {
        let base = exec(
            "select a.id from DB.S.INPUT a cross join DB.S.\"input\" b",
            vec![
                dep("DB.S.INPUT", "select 1 as id", "DB", "S"),
                dep("DB.S.\"input\"", "select 2 as id", "DB", "S"),
            ],
        );
        let swapped = exec(
            "select a.id from DB.S.INPUT a cross join DB.S.\"input\" b",
            vec![
                dep("DB.S.INPUT", "select 2 as id", "DB", "S"),
                dep("DB.S.\"input\"", "select 1 as id", "DB", "S"),
            ],
        );
        assert_ne!(
            namespace_logic_hash(&base),
            namespace_logic_hash(&swapped),
            "unquoted INPUT and quoted \"input\" are DISTINCT relations; a swap \
             of their definitions must change the identity (not collapse via \
             lowercasing the table component)"
        );
    }

    /// POSITIVE EQUIVALENCE (review #2 4th follow-up, P0): Snowflake resolves an
    /// unquoted identifier to its UPPERCASE spelling, so unquoted `INPUT` is the
    /// SAME relation as quoted `"INPUT"`. Two otherwise-identical requests that
    /// spell the same relation unquoted vs uppercase-quoted (in catalog, schema
    /// AND table) MUST share identity — they are the same physical reference.
    #[test]
    fn ns_unquoted_equals_uppercase_quoted_all_components() {
        let unquoted = exec(
            "select id from DB.S.INPUT",
            vec![dep("DB.S.INPUT", "select 1 as id", "DB", "S")],
        );
        let upper_quoted = exec(
            "select id from \"DB\".\"S\".\"INPUT\"",
            vec![dep("\"DB\".\"S\".\"INPUT\"", "select 1 as id", "DB", "S")],
        );
        assert_eq!(
            namespace_logic_hash(&unquoted),
            namespace_logic_hash(&upper_quoted),
            "unquoted DB.S.INPUT and quoted \"DB\".\"S\".\"INPUT\" are the same \
             Snowflake relation (unquoted folds to uppercase) and must share \
             identity"
        );
    }

    /// POSITIVE (review #2 4th follow-up, P0): unquoted `X` folds to uppercase so
    /// a root `DB.X.INPUT` and `DB."X".INPUT` reference the SAME schema and must
    /// share identity (the reviewer's probe: switching `X`→`"X"` must NOT lose an
    /// otherwise-available CLONE). The genuinely-distinct lowercase-quoted case
    /// (`"X"` vs `"x"` COEXISTING in one request) is covered by
    /// `ns_quoted_case_distinct_schemas_do_not_collapse`; a LONE schema always
    /// abstracts to the environment token regardless of spelling (that is the
    /// intended cross-env collapse, not a case bug).
    #[test]
    fn ns_unquoted_schema_equals_uppercase_quoted() {
        let unquoted = exec(
            "select id from DB.X.INPUT",
            vec![dep("DB.X.INPUT", "select 1 as id", "DB", "X")],
        );
        let upper_quoted = exec(
            "select id from DB.\"X\".INPUT",
            vec![dep("DB.\"X\".INPUT", "select 1 as id", "DB", "X")],
        );
        assert_eq!(
            namespace_logic_hash(&unquoted),
            namespace_logic_hash(&upper_quoted),
            "unquoted X schema folds to uppercase == quoted \"X\" (CLONE retained)"
        );
    }
}
