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
    // Alias evidence covers rendered SQL; template/configuration equivalence
    // remains a separate characterized slice.
    let dialect = if template.is_none() {
        Warehouse::rendered_identity_dialect(&req.dialect)
    } else {
        req.dialect.to_ascii_lowercase()
    };
    field(&mut h, dialect.as_bytes());
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
            if Warehouse::from_dialect(&req.dialect) == Warehouse::Databricks {
                // Hosted controls equate uppercase/lowercase 32-hex body digests.
                field(&mut h, body.to_ascii_lowercase().as_bytes());
            } else {
                field(&mut h, body.as_bytes());
            }
        }
        None => {
            field(&mut h, b"rendered");
            field(&mut h, sql.as_bytes());
        }
    }
    if template.is_some() && Warehouse::from_dialect(&req.dialect) == Warehouse::Databricks {
        // Actual config edits and crafted one-axis controls establish the two
        // alternatives: rendered SQL + exact runtime extras, or declared body
        // + exact client config signature. No JSON or hash-string normalization.
        // Runtime construction enables this branch only with both signatures.
        field(&mut h, b"declared-config-signature");
        match req
            .dbt_node_state
            .as_ref()
            .and_then(|state| state.node_configs_hash.as_deref())
        {
            Some(signature) if !signature.is_empty() => {
                field(&mut h, signature.to_ascii_lowercase().as_bytes())
            }
            _ => field(&mut h, b""),
        }
        match req
            .dbt_node_state
            .as_ref()
            .and_then(|state| state.node_macros_hash.as_deref())
        {
            Some(signature) => {
                field(&mut h, b"macros-present");
                field(&mut h, signature.as_bytes());
            }
            None => field(&mut h, b"macros-absent"),
        }
    } else {
        extras(&mut h, &req.semantic_extras);
    }
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
        CopyIdentity::DatabricksSubmit(hash) => Some(hash),
        CopyIdentity::CopyGraph { version, graph } => Some(copy_graph_hash(req, version, graph)),
    }
}

fn copy_graph_hash(
    req: &qc::SqlExecution,
    version: &[u8],
    graph: super::binding::DuckDbCopyGraph,
) -> String {
    let mut h = Sha256::new();
    field(&mut h, version);
    field(&mut h, &req.execution_type.to_le_bytes());
    extras(&mut h, &req.semantic_extras);
    field(&mut h, &(graph.queries.len() as u64).to_le_bytes());
    for super::binding::DuckDbCopyQuery {
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
mod tests;

#[cfg(test)]
mod pinned_values;
