//! Versioned evidence shared by submit and batch recording. Length framing
//! prevents concatenation ambiguity; unknown dialects retain exact SQL bytes.
//! This module computes identities only: the Warehouse rules choose each
//! version tag, which SQL transformation applies and whether a submission is
//! copy-eligible.
use crate::query_cache as qc;
use crate::warehouse::{CopyIdentity, NamespaceIdentity, Warehouse};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// Schema-abstracted normalization for the cross-environment (namespace-scoped)
/// logic identity. The SAME model built in different environments differs only
/// in the physical schema qualifier of its three-part names; abstracting that
/// yields a shared identity (verified live — see sql_norm docs).
/// Namespace-abstract normalization with a shared relative schema map. Only
/// a [`NamespaceIdentity::SchemaAbstracted`] warehouse reaches it.
fn normalized_ns_with(sql: &str, schemas: &crate::sql_norm::SchemaMap) -> String {
    crate::sql_norm::normalize_sql_namespace_abstracted_with(sql, schemas)
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
    let version = Warehouse::from_dialect(&req.dialect).sql_identity_version();
    sql_logic_hash_with_version(req, template, version)
}

/// The versioned core of [`sql_logic_hash`]. The ONLY difference an engine
/// version makes is the leading `version_tag`; everything else is fixed.
///
/// IMPORTANT (review #2 10th follow-up): this is PARAMETERIZED PRODUCTION
/// HASHING, NOT an independently frozen implementation of a prior algorithm. It
/// shares today's request conversion, normalization, field framing and hashing,
/// so it only reproduces an older version's digest as long as those shared
/// functions are unchanged. It is useful as a TAG-ROLLBACK GUARD — reverting the
/// production tag makes a regression that calls this with the current tag's
/// predecessor collide — but it is NOT a durable historical oracle. The durable
/// oracle is the frozen fixture of real old-server bytes
/// (`golden/upgrade/prefix_clone_366be09.json`), which is independent of this
/// code. Do not treat a `b"dbt-state-sql-v3"` call here as an authoritative v3
/// digest.
pub fn sql_logic_hash_with_version(
    req: &qc::SqlExecution,
    template: Option<&str>,
    version_tag: &[u8],
) -> String {
    let mut h = Sha256::new();
    field(&mut h, version_tag);
    field(&mut h, req.dialect.to_ascii_lowercase().as_bytes());
    let warehouse = match (Warehouse::from_dialect(&req.dialect), template) {
        // Template/configuration policy is a separately characterized slice.
        (Warehouse::Databricks, Some(_)) => Warehouse::Other,
        (warehouse, _) => warehouse,
    };
    let inferred_schema = warehouse.target_schema(req.target_table.as_deref());
    let schema = req
        .default_schema
        .as_deref()
        .or(inferred_schema.as_deref())
        .unwrap_or("");
    let (sql, catalog, schema) = warehouse.sql_identity(&req.sql, &req.default_catalog, schema);
    field(&mut h, catalog.as_bytes());
    field(&mut h, schema.as_bytes());
    match template {
        Some(body) => {
            field(&mut h, b"template");
            field(&mut h, body.as_bytes());
        }
        None => {
            field(&mut h, b"rendered");
            field(&mut h, sql.as_bytes());
        }
    }
    extras(&mut h, &req.semantic_extras);
    let mut dependencies: Vec<_> = req
        .query_dependencies
        .iter()
        .map(|d| {
            let (query, catalog, schema) =
                warehouse.sql_identity(&d.query, &d.default_catalog, &d.default_schema);
            (d.name.as_str(), query, catalog, schema)
        })
        .collect();
    dependencies.sort();
    field(&mut h, &(dependencies.len() as u64).to_le_bytes());
    for (name, query, catalog, schema) in dependencies {
        for value in [name, query.as_str(), catalog.as_str(), schema.as_str()] {
            field(&mut h, value.as_bytes());
        }
    }
    hex::encode(h.finalize())
}

/// The copy logic identity of a submission the Warehouse rules make
/// copy-eligible ([`Warehouse::copy_identity`]), or `None`. A checked copy
/// graph binds each structured view reference to its checked definition and
/// context. Only declared prod/dev view bindings substitute environments;
/// physical base inputs retain their exact names.
pub fn copy_logic_hash(req: &qc::SqlExecution) -> Option<String> {
    match Warehouse::from_dialect(&req.dialect).copy_identity(req)? {
        CopyIdentity::NamespaceLogic => Some(namespace_logic_hash(req)),
        CopyIdentity::CopyGraph { version, graph } => Some(copy_graph_hash(req, version, graph)),
    }
}

fn copy_graph_hash(
    req: &qc::SqlExecution,
    version: &[u8],
    graph: crate::sql_identity::DuckDbCopyGraph,
) -> String {
    let mut h = Sha256::new();
    field(&mut h, version);
    field(&mut h, &req.execution_type.to_le_bytes());
    extras(&mut h, &req.semantic_extras);
    field(&mut h, &(graph.queries.len() as u64).to_le_bytes());
    for crate::sql_identity::DuckDbCopyQuery {
        binding,
        query,
        slots,
        catalog,
        schema,
    } in graph.queries
    {
        field(&mut h, &(binding.len() as u64).to_le_bytes());
        for part in binding {
            field(&mut h, part.as_bytes());
        }
        field(&mut h, query.as_bytes());
        field(&mut h, &(slots.len() as u64).to_le_bytes());
        for slot in slots {
            field(&mut h, &(slot.len() as u64).to_le_bytes());
            for part in slot {
                field(&mut h, part.as_bytes());
            }
        }
        field(&mut h, catalog.as_bytes());
        field(&mut h, schema.as_bytes());
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
    let version = match Warehouse::from_dialect(&req.dialect).namespace_identity() {
        NamespaceIdentity::SchemaAbstracted { version } => version,
        NamespaceIdentity::Exact { version } => {
            // This public helper is also useful for comparing identities in tests.
            // Give unverified dialects an exact, context-sensitive identity rather
            // than applying Snowflake's lossy identifier/schema interpretation.
            let mut h = Sha256::new();
            field(&mut h, version);
            field(&mut h, sql_logic_hash(req, None).as_bytes());
            return hex::encode(h.finalize());
        }
    };
    schema_abstracted_hash(req, version, &request_schemas(req), None)
}

/// The copy logic identity of `req` read as if it ran where its copy source
/// was built: schema `from` (the submit's Target relation schema) reads as
/// schema `to` (the copy source's), every other schema as itself. Equal to
/// the source build's copy logic identity exactly when the submit, so
/// substituted, has the source's logic, each dependency name still bound to
/// its definition (AGENTS.md invariant 4).
///
/// VERIFIED LIVE 2026-11-11 (explain_post_copy_identity.jsonl, #93): hosted
/// reuses a dev-clone pre-step copy whose post-copy submit reads a fixed
/// prod-schema source (byte-identical SQL) or a view resolved per target
/// whose definition reads that source. The request-relative schema tokens of
/// [`namespace_logic_hash`] tell both apart from the prod build, because the
/// fixed schema is the prod default schema but not the dev one.
///
/// `None` unless the Warehouse rules give the dialect a schema-abstracted
/// namespace logic copy identity (Snowflake): other dialects keep exact SQL.
pub fn rehomed_copy_logic_hash(req: &qc::SqlExecution, from: &str, to: &str) -> Option<String> {
    let version = rehomable_version(req)?;
    Some(schema_abstracted_hash(
        req,
        version,
        &request_schemas(req),
        Some((from, to)),
    ))
}

/// The copy logic identity of `req` rehomed ([`rehomed_copy_logic_hash`])
/// from schema `from` (the submit's Target relation schema) into each other
/// schema the request names, where that differs from its own copy logic
/// identity. The submit-time equivalent search looks these up, and admits a
/// candidate matched by one only when the candidate's Target relation is in
/// that schema (#107). Empty unless the dialect has a schema-abstracted
/// namespace logic copy identity (Snowflake).
///
/// VERIFIED LIVE 2026-11-11: hosted finds the prod build as the equivalent
/// of a dev submit whose byte-identical SQL reads a fixed prod-schema source
/// (explain_post_copy_identity.jsonl entries 18-19, explain_copy_probes.jsonl
/// entries 13-14, explain_incremental_submit_copy.jsonl entries 9-10).
pub fn rehomed_copy_logic_hashes(req: &qc::SqlExecution, from: &str) -> Vec<RehomedLogic> {
    use crate::sql_norm::canonical_schema_key;
    let Some(version) = rehomable_version(req) else {
        return Vec::new();
    };
    let schemas = request_schemas(req);
    let own = schema_abstracted_hash(req, version, &schemas, None);
    let from_key = canonical_schema_key(from);
    let mut targets: Vec<String> = schemas
        .iter()
        .map(|s| canonical_schema_key(s))
        .filter(|key| *key != from_key)
        .collect();
    targets.sort();
    targets.dedup();
    targets
        .into_iter()
        .filter_map(|schema| {
            let hash =
                schema_abstracted_hash(req, version, &schemas, Some((from, &quoted(&schema))));
            let mut named: Vec<String> = schemas
                .iter()
                .map(|s| canonical_schema_key(s))
                .map(|key| if key == from_key { schema.clone() } else { key })
                .collect();
            named.sort();
            named.dedup();
            (hash != own).then_some(RehomedLogic {
                schema,
                hash,
                named,
            })
        })
        .collect()
}

/// A copy logic identity of a submit read as if it ran in `schema`
/// ([`rehomed_copy_logic_hashes`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RehomedLogic {
    /// The canonical key of the schema the submit is read in
    /// (`sql_norm::canonical_schema_key`).
    pub schema: String,
    pub hash: String,
    /// The canonical key of every schema the submit so read names, sorted:
    /// the schemas a build with this identity resolved its references in.
    /// The identity abstracts them, so only the build's recorded upstream
    /// reads can show which they were (PR #127 review).
    pub named: Vec<String>,
}

/// `key`, a canonical schema key, spelled so that it canonicalizes to itself.
fn quoted(key: &str) -> String {
    format!("\"{}\"", key.replace('"', "\"\""))
}

/// The schema-abstracted version tag when the Warehouse rules give `req` a
/// namespace logic copy identity that rehoming applies to.
fn rehomable_version(req: &qc::SqlExecution) -> Option<&'static [u8]> {
    let warehouse = Warehouse::from_dialect(&req.dialect);
    let CopyIdentity::NamespaceLogic = warehouse.copy_identity(req)? else {
        return None;
    };
    match warehouse.namespace_identity() {
        NamespaceIdentity::SchemaAbstracted { version } => Some(version),
        NamespaceIdentity::Exact { .. } => None,
    }
}

/// Every schema the request names: in the root SQL, in each dependency NAME
/// and DEFINITION, PLUS the request's default_schema (so an UNQUALIFIED root
/// reference resolved via default_schema participates in the mapping).
fn request_schemas(req: &qc::SqlExecution) -> Vec<String> {
    use crate::sql_norm::{schema_of_object_name, schemas_in_sql};
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
    all_schemas
}

/// The schema-abstracted body of [`namespace_logic_hash`] over the request's
/// schemas ([`request_schemas`]), optionally rehomed
/// ([`rehomed_copy_logic_hash`]).
fn schema_abstracted_hash(
    req: &qc::SqlExecution,
    version: &[u8],
    all_schemas: &[String],
    rehome: Option<(&str, &str)>,
) -> String {
    use crate::sql_norm::{abstract_object_name_components_with, SchemaMap};
    // ONE request-wide relative schema→token map covering every schema the
    // request names. Distinct schemas get distinct tokens (sorted order); a
    // single environment schema collapses to token 0, preserving cross-env
    // equivalence while keeping genuinely-different schemas distinct (review
    // #2 2nd follow-up, P0).
    let all_schemas = all_schemas.iter().cloned();
    let schemas = match rehome {
        Some((from, to)) => SchemaMap::rehomed(all_schemas, from, to),
        None => SchemaMap::from_schemas(all_schemas),
    };

    let mut h = Sha256::new();
    field(&mut h, version);
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
    field(&mut h, normalized_ns_with(&req.sql, &schemas).as_bytes());
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
    // Dependencies: bind each dependency's schema-abstracted NAME to its
    // schema-abstracted definition. The NAME is a STRUCTURED COMPONENT SEQUENCE
    // (abstract_object_name_components_with), hashed component-by-component with
    // explicit length framing — NOT a rejoined string — so a component that
    // itself contains a `.` (or matches the schema-placeholder spelling) cannot
    // collide across a boundary with a differently-split name (review #2 6th
    // follow-up P0). Sorted by (components, query) so the binding multiset is
    // order-independent while each (reference → definition) pair is preserved.
    let mut dependency_bindings: Vec<(Vec<String>, String)> = req
        .query_dependencies
        .iter()
        .map(|d| {
            (
                abstract_object_name_components_with(&d.name, &schemas),
                normalized_ns_with(&d.query, &schemas),
            )
        })
        .collect();
    dependency_bindings.sort();
    field(&mut h, &(dependency_bindings.len() as u64).to_le_bytes());
    for (components, query) in dependency_bindings {
        // Frame the component COUNT, then each component (each already length-
        // framed by `field`), then the definition — an unambiguous encoding.
        field(&mut h, &(components.len() as u64).to_le_bytes());
        for c in &components {
            field(&mut h, c.as_bytes());
        }
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

    #[test]
    fn bigquery_case_sensitive_dependency_bindings_stay_distinct() {
        let dep = |name: &str, query: &str| qc::QueryDependency {
            name: name.into(),
            query: query.into(),
            default_catalog: "project".into(),
            default_schema: "dataset".into(),
        };
        let a = qc::SqlExecution {
            dialect: "bigquery".into(),
            default_catalog: "project".into(),
            default_schema: Some("dataset".into()),
            sql: "select a.id, b.id from `project.dataset.Table` a cross join `project.dataset.table` b".into(),
            query_dependencies: vec![
                dep("`project.dataset.Table`", "select 1 as id"),
                dep("`project.dataset.table`", "select 2 as id"),
            ],
            ..Default::default()
        };
        let mut b = a.clone();
        b.query_dependencies[0].query = "select 2 as id".into();
        b.query_dependencies[1].query = "select 1 as id".into();
        assert_ne!(sql_logic_hash(&a, None), sql_logic_hash(&b, None));
        assert_ne!(namespace_logic_hash(&a), namespace_logic_hash(&b));
    }

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

    /// CROSS-ENV POSITIVE (review #2 4th follow-up, P1 #2 — was the misleadingly
    /// named `ns_fully_qualified_context_only_differs`). The same model built in
    /// BLUE vs RED (a uniform schema rename of BOTH the reference and its
    /// definition) is the SAME model across environments and MUST share identity
    /// — this is the cross-environment reuse the service exists to provide.
    ///
    /// NOTE on the open resolution-context question (do NOT over-correct this
    /// into inequality): a uniform schema rename is NOT a proof that two leaves
    /// hold the same data, but in the LIVE world any genuine data difference
    /// advances the leaf's physical `last_modified_epoch`, which the FRESHNESS
    /// path handles — it is NOT a namespace-logic distinction. Live measurement
    /// (experiments/resolution_context.sh) could not isolate a context-only
    /// change from upstream freshness through Fusion: rebuilding the leaf with
    /// different data advanced its freshness and the consumer EXECUTED on that
    /// basis, with the dependency NAME/schema/text byte-identical. The pure
    /// context-only namespace policy therefore remains UNRESOLVED (documented in
    /// the handoff as potential-unsafe), and this test asserts only the
    /// well-established cross-env positive.
    #[test]
    fn ns_uniform_schema_rename_is_cross_env_positive() {
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

    /// CROSS-ENV POSITIVE via `default_schema` (review #2 4th follow-up, P1 #2 —
    /// was the misleadingly named `ns_unqualified_context_only_differs`). An
    /// UNQUALIFIED root reference resolved through a different `default_schema`
    /// where the dependency is the SAME model across environments (its definition
    /// references its env's RAW, which abstracts equal) MUST share identity.
    /// As with the fully-qualified case, a genuine data difference would in the
    /// live world advance the leaf's freshness (handled separately); the
    /// context-only namespace policy stays UNRESOLVED. This asserts the
    /// cross-env positive only.
    #[test]
    fn ns_unqualified_uniform_schema_rename_is_cross_env_positive() {
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
        // Same dependency query TEXT; RED is the same model's env-renamed leaf
        // (RED.RAW), i.e. a uniform schema rename, not a data change.
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

    /// REGRESSION (review #2 5th follow-up, P0 #2): a QUOTED component with an
    /// EMBEDDED DOT must stay one case-sensitive component. `abstract_object_name_schema_with`
    /// split on every `.`, so `"X.Y"` and `"x.y"` fragmented and uppercased into
    /// the same string → a swapped binding on dotted quoted schemas CLONED
    /// unsafely. These are valid, distinct Snowflake relations.
    #[test]
    fn ns_dotted_quoted_schema_binding_swap_differs() {
        let base = exec(
            "select a.id from DB.\"X.Y\".INPUT a cross join DB.\"x.y\".INPUT b",
            vec![
                dep(
                    "\"DB\".\"X.Y\".\"INPUT\"",
                    "select 1 as id",
                    "DB",
                    "\"X.Y\"",
                ),
                dep(
                    "\"DB\".\"x.y\".\"INPUT\"",
                    "select 2 as id",
                    "DB",
                    "\"x.y\"",
                ),
            ],
        );
        let swapped = exec(
            "select a.id from DB.\"X.Y\".INPUT a cross join DB.\"x.y\".INPUT b",
            vec![
                dep(
                    "\"DB\".\"X.Y\".\"INPUT\"",
                    "select 2 as id",
                    "DB",
                    "\"X.Y\"",
                ),
                dep(
                    "\"DB\".\"x.y\".\"INPUT\"",
                    "select 1 as id",
                    "DB",
                    "\"x.y\"",
                ),
            ],
        );
        assert_ne!(
            namespace_logic_hash(&base),
            namespace_logic_hash(&swapped),
            "quoted \"X.Y\" and \"x.y\" are DISTINCT case-sensitive schemas; a \
             swapped binding must change the identity (the dot is inside the quotes)"
        );
    }

    /// REGRESSION (review #2 5th follow-up, P0 #2): a quoted TABLE component with
    /// an embedded dot likewise stays one component and keeps its case.
    #[test]
    fn ns_dotted_quoted_table_binding_swap_differs() {
        let base = exec(
            "select a.id from DB.S.X a cross join DB.S.Y b",
            vec![
                dep("\"DB\".\"S\".\"T.a\"", "select 1 as id", "DB", "S"),
                dep("\"DB\".\"S\".\"t.a\"", "select 2 as id", "DB", "S"),
            ],
        );
        let swapped = exec(
            "select a.id from DB.S.X a cross join DB.S.Y b",
            vec![
                dep("\"DB\".\"S\".\"T.a\"", "select 2 as id", "DB", "S"),
                dep("\"DB\".\"S\".\"t.a\"", "select 1 as id", "DB", "S"),
            ],
        );
        assert_ne!(
            namespace_logic_hash(&base),
            namespace_logic_hash(&swapped),
            "quoted \"T.a\" and \"t.a\" are distinct relations; a swapped binding \
             must change the identity"
        );
    }

    /// Escaped embedded quote in a component: `"a""b"` is the single identifier
    /// `a"b`, distinct from `"A""B"` (`A"B`). A swap must change the identity.
    #[test]
    fn ns_escaped_quote_component_binding_swap_differs() {
        let base = exec(
            "select a.id from DB.S.X a cross join DB.S.Y b",
            vec![
                dep("\"DB\".\"S\".\"a\"\"b\"", "select 1 as id", "DB", "S"),
                dep("\"DB\".\"S\".\"A\"\"B\"", "select 2 as id", "DB", "S"),
            ],
        );
        let swapped = exec(
            "select a.id from DB.S.X a cross join DB.S.Y b",
            vec![
                dep("\"DB\".\"S\".\"a\"\"b\"", "select 2 as id", "DB", "S"),
                dep("\"DB\".\"S\".\"A\"\"B\"", "select 1 as id", "DB", "S"),
            ],
        );
        assert_ne!(
            namespace_logic_hash(&base),
            namespace_logic_hash(&swapped),
            "escaped-quote components a\"b vs A\"B are distinct; a swap must differ"
        );
    }

    /// REGRESSION (review #2 6th follow-up, P0): COMPONENT-BOUNDARY collision.
    /// Abstracting then REJOINING components with literal `.` lost boundaries
    /// when a canonical component itself contained a `.` (or matched the schema
    /// placeholder spelling). `"DB.__dbt_state_ns__0"."S"."T"` and
    /// `"DB"."S"."__dbt_state_ns__0.T"` both rejoined to
    /// `DB.__dbt_state_ns__0.__dbt_state_ns__0.T`, so a swap of their definitions
    /// conflated → unsafe clone. These are DISTINCT valid Snowflake relations;
    /// a swap must change the identity. The binding key must preserve every
    /// component boundary (structured, length-framed), not a rejoined string.
    #[test]
    fn ns_component_boundary_collision_binding_swap_differs() {
        let n1 = "\"DB.__dbt_state_ns__0\".\"S\".\"T\"";
        let n2 = "\"DB\".\"S\".\"__dbt_state_ns__0.T\"";
        let root = "select a.id from \"DB.__dbt_state_ns__0\".\"S\".\"T\" a cross join \"DB\".\"S\".\"__dbt_state_ns__0.T\" b";
        let base = exec(
            root,
            vec![
                dep(n1, "select 1 as id", "DB", "S"),
                dep(n2, "select 2 as id", "DB", "S"),
            ],
        );
        let swapped = exec(
            root,
            vec![
                dep(n1, "select 2 as id", "DB", "S"),
                dep(n2, "select 1 as id", "DB", "S"),
            ],
        );
        assert_ne!(
            namespace_logic_hash(&base),
            namespace_logic_hash(&swapped),
            "placeholder-like dependency names that span a component boundary are \
             DISTINCT relations; a swap must change the identity (no rejoin collision)"
        );
    }

    /// Control: the two placeholder-like names are themselves distinct bindings
    /// even without a swap — an identical submit matches itself (sanity that we
    /// did not simply make everything differ).
    #[test]
    fn ns_component_boundary_identical_submit_matches() {
        let n1 = "\"DB.__dbt_state_ns__0\".\"S\".\"T\"";
        let n2 = "\"DB\".\"S\".\"__dbt_state_ns__0.T\"";
        let root = "select a.id from \"DB.__dbt_state_ns__0\".\"S\".\"T\" a cross join \"DB\".\"S\".\"__dbt_state_ns__0.T\" b";
        let a = exec(
            root,
            vec![
                dep(n1, "select 1 as id", "DB", "S"),
                dep(n2, "select 2 as id", "DB", "S"),
            ],
        );
        let b = exec(
            root,
            vec![
                dep(n1, "select 1 as id", "DB", "S"),
                dep(n2, "select 2 as id", "DB", "S"),
            ],
        );
        assert_eq!(
            namespace_logic_hash(&a),
            namespace_logic_hash(&b),
            "identical bindings must share identity"
        );
    }
    /// The namespace identity follows the Warehouse rules for every spelling
    /// they accept: a padded Snowflake dialect abstracts the schemas of its
    /// SQL too, so the same model across environments shares one identity.
    #[test]
    fn ns_padded_snowflake_dialect_abstracts_sql_schemas() {
        let padded = |sql: &str, schema: &str| qc::SqlExecution {
            dialect: " snowflake ".into(),
            default_schema: Some(schema.into()),
            ..exec(sql, vec![])
        };
        assert_eq!(
            namespace_logic_hash(&padded("select order_id from CDO.prod.t", "prod")),
            namespace_logic_hash(&padded("select order_id from CDO.dev.t", "dev")),
        );
    }

    // ---- REHOMED identity (adopting a RegisterClone copy, #93) ----------------

    fn in_schema(
        default_schema: &str,
        sql: &str,
        deps: Vec<qc::QueryDependency>,
    ) -> qc::SqlExecution {
        qc::SqlExecution {
            default_schema: Some(default_schema.into()),
            ..exec(sql, deps)
        }
    }

    /// VERIFIED LIVE 2026-11-11 (explain_post_copy_identity.jsonl `c93sa`, #76's
    /// `c76rsnap`): the prod build and the dev post-copy submit read the same
    /// fixed `PROD` source with byte-identical SQL; only the default schema
    /// differs. Hosted reuses the copy. The schema-abstracted identity tells
    /// them apart (the fixed `PROD` is the prod default schema but not the dev
    /// one); read in the copy source's schema, the submit IS the source build.
    #[test]
    fn rehomed_identity_matches_a_build_that_read_a_fixed_schema() {
        let prod = in_schema("prod", "select id from DB.prod.src", vec![]);
        let dev = in_schema("dev", "select id from DB.prod.src", vec![]);
        assert_ne!(namespace_logic_hash(&prod), namespace_logic_hash(&dev));
        assert_eq!(
            rehomed_copy_logic_hash(&dev, "\"DEV\"", "\"PROD\""),
            Some(namespace_logic_hash(&prod))
        );
    }

    /// VERIFIED LIVE 2026-11-11 (explain_post_copy_identity.jsonl `c93sv`): a
    /// view resolved per target (its name differs by environment) whose
    /// definition reads the fixed `PROD` source in both. Hosted reuses the copy.
    #[test]
    fn rehomed_identity_matches_a_per_target_view_over_a_fixed_schema() {
        let view = |env: &str, upper: &str| {
            in_schema(
                env,
                &format!("select id from DB.{env}.upv"),
                vec![dep(
                    &format!("\"DB\".\"{upper}\".\"UPV\""),
                    "select id from DB.prod.src",
                    "DB",
                    upper,
                )],
            )
        };
        let (prod, dev) = (view("prod", "PROD"), view("dev", "DEV"));
        assert_ne!(namespace_logic_hash(&prod), namespace_logic_hash(&dev));
        assert_eq!(
            rehomed_copy_logic_hash(&dev, "dev", "prod"),
            Some(namespace_logic_hash(&prod))
        );
    }

    /// Rehoming keeps the logic identity binding each dependency name to its
    /// definition (AGENTS.md invariant 4): swapping two definitions between
    /// their references is still a different logic.
    #[test]
    fn rehomed_identity_keeps_dependency_bindings() {
        let bound = |env: &str, upper: &str, a: &str, b: &str| {
            in_schema(
                env,
                &format!("select x.id from DB.{env}.a x cross join DB.{env}.b y"),
                vec![
                    dep(&format!("\"DB\".\"{upper}\".\"A\""), a, "DB", upper),
                    dep(&format!("\"DB\".\"{upper}\".\"B\""), b, "DB", upper),
                ],
            )
        };
        let prod = bound("prod", "PROD", "select 1 as id", "select 2 as id");
        let same = bound("dev", "DEV", "select 1 as id", "select 2 as id");
        let swapped = bound("dev", "DEV", "select 2 as id", "select 1 as id");
        let source = Some(namespace_logic_hash(&prod));
        assert_eq!(rehomed_copy_logic_hash(&same, "DEV", "PROD"), source);
        assert_ne!(rehomed_copy_logic_hash(&swapped, "DEV", "PROD"), source);
    }

    /// A genuine logic change is not hidden by rehoming, and only the given
    /// schema is rehomed: another schema keeps its physical name.
    #[test]
    fn rehomed_identity_distinguishes_logic_and_other_schemas() {
        let prod = in_schema("prod", "select id from DB.prod.src", vec![]);
        let source = Some(namespace_logic_hash(&prod));
        let changed = in_schema("dev", "select id, 1 as x from DB.prod.src", vec![]);
        assert_ne!(rehomed_copy_logic_hash(&changed, "dev", "prod"), source);
        let elsewhere = in_schema("dev", "select id from DB.other.src", vec![]);
        assert_ne!(rehomed_copy_logic_hash(&elsewhere, "dev", "prod"), source);
    }

    /// Only a schema-abstracted copy identity (Snowflake) is rehomed: other
    /// dialects keep exact SQL and do no automatic cross-target reuse.
    #[test]
    fn rehomed_identity_is_snowflake_only() {
        for dialect in ["duckdb", "bigquery", "postgres"] {
            let e = qc::SqlExecution {
                dialect: dialect.into(),
                ..in_schema("dev", "select id from DB.prod.src", vec![])
            };
            assert_eq!(
                rehomed_copy_logic_hash(&e, "dev", "prod"),
                None,
                "{dialect}"
            );
        }
    }

    /// The submit-time equivalent search (#107) reads the submit in each
    /// other schema it names. VERIFIED LIVE 2026-11-11
    /// (explain_post_copy_identity.jsonl entries 18-19): read in the fixed
    /// prod schema it reads, the dev submit is the prod build. A submit that
    /// names only its own schema has no other reading.
    #[test]
    fn rehomed_identities_read_the_submit_in_each_other_schema_it_names() {
        let prod = in_schema("prod", "select id from DB.prod.src", vec![]);
        let dev = in_schema("dev", "select id from DB.prod.src", vec![]);
        assert_eq!(
            rehomed_copy_logic_hashes(&dev, "\"DEV\""),
            vec![RehomedLogic {
                schema: "PROD".into(),
                hash: namespace_logic_hash(&prod),
                named: vec!["PROD".into()],
            }]
        );
        let own = in_schema("dev", "select id from DB.dev.src", vec![]);
        assert!(rehomed_copy_logic_hashes(&own, "dev").is_empty());
        let duckdb = qc::SqlExecution {
            dialect: "duckdb".into(),
            ..dev
        };
        assert!(rehomed_copy_logic_hashes(&duckdb, "dev").is_empty());
    }

    /// A dependency in the dev schema whose definition differs from the prod
    /// build's is not made equivalent by reading the submit in the prod
    /// schema: the rehomed identity keeps each dependency name bound to its
    /// definition (AGENTS.md invariant 4). The same definition is.
    #[test]
    fn rehomed_identities_keep_a_different_dev_dependency_definition_apart() {
        let build = |env: &str, upper: &str, definition: &str| {
            in_schema(
                env,
                &format!("select s.id from DB.prod.src s join DB.{env}.v v on s.id = v.id"),
                vec![dep(
                    &format!("\"DB\".\"{upper}\".\"V\""),
                    definition,
                    "DB",
                    upper,
                )],
            )
        };
        let prod = namespace_logic_hash(&build("prod", "PROD", "select id from DB.prod.src"));
        let readings = |e: &qc::SqlExecution| -> Vec<String> {
            rehomed_copy_logic_hashes(e, "DEV")
                .into_iter()
                .map(|r| r.hash)
                .collect()
        };
        let same = build("dev", "DEV", "select id from DB.prod.src");
        assert!(readings(&same).contains(&prod), "same definition");
        for changed in [
            "select id from DB.prod.src where id > 0",
            "select id + 1 as id from DB.prod.src",
            "select id from DB.prod.other",
        ] {
            let dev = build("dev", "DEV", changed);
            assert_ne!(namespace_logic_hash(&dev), prod, "{changed}");
            assert!(!readings(&dev).contains(&prod), "{changed}");
        }
    }
}

#[cfg(test)]
mod pinned_values {
    //! Persisted history stays valid only while these bytes are unchanged.
    //! The expected values were produced by the implementation before the
    //! Warehouse rules took over copy eligibility and version tags (#49).
    use super::*;
    use crate::query_cache as qc;

    fn duckdb_copy() -> qc::SqlExecution {
        qc::SqlExecution {
            target_table: Some("\"db\".\"dev\".\"result\"".into()),
            dialect: "duckdb".into(),
            default_catalog: "db".into(),
            default_schema: Some("dev".into()),
            execution_type: 1,
            sql: "select id, amount + 1 as amount from db.dev.stg".into(),
            tables: vec![qc::TableModifiedInfo {
                name: "db.raw.input".into(),
                last_modified_epoch: Some(10),
            }],
            query_dependencies: vec![qc::QueryDependency {
                name: "\"db\".\"dev\".\"stg\"".into(),
                query: "select id, amount from db.raw.input".into(),
                default_catalog: "db".into(),
                default_schema: "dev".into(),
            }],
            semantic_extras: [("duckdb_copy_contract".into(), "rows-schema-v1".into())].into(),
            table_namespace: Some("ns".into()),
            ..Default::default()
        }
    }

    fn snowflake() -> qc::SqlExecution {
        qc::SqlExecution {
            target_table: Some("\"CDO\".\"DEV\".\"ORDERS\"".into()),
            dialect: "snowflake".into(),
            default_catalog: "CDO".into(),
            default_schema: Some("dev".into()),
            execution_type: 1,
            sql: "select order_id from CDO.dev.stg_orders".into(),
            query_dependencies: vec![qc::QueryDependency {
                name: "\"CDO\".\"DEV\".\"STG_ORDERS\"".into(),
                query: "select * from CDO.dev.raw_orders".into(),
                default_catalog: "CDO".into(),
                default_schema: "dev".into(),
            }],
            table_namespace: Some("ns".into()),
            ..Default::default()
        }
    }

    #[test]
    fn fingerprint_values_are_unchanged() {
        let duck = duckdb_copy();
        let snow = snowflake();
        let bigquery = qc::SqlExecution {
            dialect: "bigquery".into(),
            ..snowflake()
        };
        let incremental = qc::SqlExecution {
            execution_type: qc::ModelExecutionType::DeleteInsert as i32,
            semantic_extras: [
                ("duckdb_copy_contract".into(), "rows-schema-v1".into()),
                (
                    "duckdb_incremental_contract".into(),
                    "delete-insert-fixed-v1".into(),
                ),
            ]
            .into(),
            ..duckdb_copy()
        };
        let values = [
            sql_logic_hash(&duck, None),
            sql_logic_hash(&duck, Some("body")),
            namespace_logic_hash(&duck),
            copy_logic_hash(&duck).unwrap(),
            copy_logic_hash(&incremental).unwrap(),
            sql_logic_hash(&snow, None),
            sql_logic_hash(&snow, Some("body")),
            namespace_logic_hash(&snow),
            copy_logic_hash(&snow).unwrap(),
            sql_logic_hash(&bigquery, None),
            namespace_logic_hash(&bigquery),
            seed_logic_hash("duckdb", "db", &duck.semantic_extras),
        ];
        assert_eq!(
            values,
            [
                // DuckDB strict, template, namespace, copy, incremental copy.
                "7bde339e739600bac9cafa38413233a4e4b23a81a770359de9e863dc23f4e56c",
                "e36181b1e5b75cc008807592d7847670e86a767ba88c30e6f8d1106cf6ae7a4e",
                "5faea4ddd8bdb097d7d920670ace91be89638d7ed467dc13c5c4e13e157eec9d",
                "77f2bf0a8d306e1c5ddde3d49e30f07e25e0976f31d2fd9af049c86867fca970",
                "90431d289fe50934e1edbf40ef8cdc5dd041a6bd1a1737559c884a26675df58e",
                // Snowflake strict, template, namespace, copy (the namespace logic).
                "d4f5ca8d45dcaa64ea35899e7afd5c47b05290450af6bb4f7911200c637d2fcc",
                "b1e089ed79d71846563ee7e1806fbd2e68ff208c0712000f42beac12792659d6",
                "ceffd1ab5732bb1f5d68c103e19e7912ce36ba1ae714541a37f1a0bc06ac927e",
                "ceffd1ab5732bb1f5d68c103e19e7912ce36ba1ae714541a37f1a0bc06ac927e",
                // BigQuery strict, exact namespace.
                "1158a6f0b167d472afa6027fb20f71073fb6c44642fd14ce815868f678b3bf84",
                "dae8e52a36ade6fc17d57ab28a5a4349bdcf0a06c12a21d6d645e544aa3e9378",
                // Seed.
                "dc8d7a80a4804272c0ba6917819f177b28f93010878892b713ba5a8886a3a758",
            ]
        );
        assert_eq!(copy_logic_hash(&bigquery), None);
    }
}
