//! Warehouse-specific interpretation of the dialect string on the wire.
//!
//! A parser accepting a dialect is not evidence that two SQL statements or
//! physical relations are equivalent. Snowflake's schema abstraction and the
//! Databricks identifier subset have hosted evidence; other SQL remains exact,
//! except DuckDB's validated formatting subset. DuckDB copying
//! uses separate request eligibility rather than this warehouse-wide capability.
//!
//! Warehouse-specific rules dispatch to the per-warehouse submodules
//! ([`snowflake`], [`duckdb`]); the other variants get conservative defaults
//! (ADR 0004: shared machinery with explicit warehouse rules).

pub(crate) mod databricks;
mod duckdb;
mod snowflake;

use crate::store::ExecutionRow;

/// The strict SQL identity's version tag for warehouses without their own.
const SQL_IDENTITY_VERSION: &[u8] = b"dbt-state-sql-v4";

/// The exact namespace identity's version tag.
const EXACT_NAMESPACE_IDENTITY_VERSION: &[u8] = b"dbt-state-exact-ns-v1";
/// The version tag of the data-test history key (#200) of a Warehouse that
/// [keys data-test history](Warehouse::keys_data_test_history). New with
/// #200: no data test recorded before it has a key, so all of them cold-miss
/// once.
pub(crate) const DATA_TEST_HISTORY_KEY_VERSION: &[u8] = b"dbt-state-data-test-key-v1";

/// The namespace logic identity a warehouse records
/// ([`Warehouse::namespace_identity`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamespaceIdentity {
    /// The strict identity under its own tag: no cross-environment
    /// equivalence.
    Exact { version: &'static [u8] },
    /// Physical schema qualifiers are abstracted, so the same model built in
    /// two environments shares one identity.
    SchemaAbstracted { version: &'static [u8] },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Warehouse {
    Snowflake,
    DuckDb,
    BigQuery,
    Databricks,
    Redshift,
    Other,
}

impl Warehouse {
    /// Hosted SQL distinguishes an omitted target from a present empty name.
    /// Preserve other warehouses' established target-less test conventions.
    pub(crate) fn rejects_empty_sql_target(self, target: Option<&str>) -> bool {
        matches!(self, Self::Snowflake | Self::Databricks) && target == Some("")
    }

    pub fn from_dialect(dialect: &str) -> Self {
        // dbt can append dialect options, for example
        // "redshift, normalization_strategy = lowercase". Uncharacterized
        // option strings keep their full fingerprint framing and cannot inherit the
        // Snowflake reuse rules merely because the family name matches.
        // #159 captured this exact Spark option spelling. Other option strings
        // retain their conservative policy until independently characterized.
        if dialect == "spark, normalization_strategy = lowercase" {
            return Self::Databricks;
        }
        let trimmed = dialect.trim();
        let family = trimmed
            .split(',')
            .next()
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        match family.as_str() {
            "snowflake" if trimmed.eq_ignore_ascii_case("snowflake") => Self::Snowflake,
            "duckdb" if trimmed.eq_ignore_ascii_case("duckdb") => Self::DuckDb,
            "bigquery" if trimmed.eq_ignore_ascii_case("bigquery") => Self::BigQuery,
            "databricks" | "spark"
                if dialect.trim_matches(' ').eq_ignore_ascii_case("databricks")
                    || dialect.trim_matches(' ').eq_ignore_ascii_case("spark") =>
            {
                Self::Databricks
            }
            "redshift" => Self::Redshift,
            _ => Self::Other,
        }
    }

    /// Exact rendered SQL dialect aliases captured in #159. Case, padding and
    /// arbitrary options are not alias evidence.
    fn rendered_dialect_alias(dialect: &str) -> Option<&'static str> {
        match dialect {
            "databricks" | "spark" | "spark, normalization_strategy = lowercase" => {
                Some("databricks")
            }
            _ => None,
        }
    }

    pub(crate) fn rendered_identity_dialect(dialect: &str) -> String {
        Self::rendered_dialect_alias(dialect)
            .map(str::to_owned)
            .unwrap_or_else(|| dialect.to_ascii_lowercase())
    }

    pub(crate) fn rendered_dialect_aliases(left: &str, right: &str) -> bool {
        matches!(
            (Self::rendered_dialect_alias(left), Self::rendered_dialect_alias(right)),
            (Some(left), Some(right)) if left == right
        )
    }

    /// Source-less rendered FULL/VIEW query diagnostics have captured evidence.
    /// No diagnostic can become a physical reuse/copy candidate.
    fn permits_literal_query_diagnostic(
        self,
        req: &crate::query_cache::SubmitEnrichedSqlRequest,
    ) -> bool {
        self == Self::Databricks
            && matches!(req.execution_type, 1 | 10)
            && !req.compare_unrendered_code
            && req.query_dependencies.is_empty()
            && self.sql_reads_relation(&req.sql) == Some(false)
            && req.target_table.as_ref().is_some_and(|target| {
                !target.is_empty()
                    && (req.tables.is_empty()
                        || req.tables.len() == 1
                            && req.tables[0].name == *target
                            && req.tables[0].last_modified_epoch.is_some())
            })
    }

    pub(crate) fn permits_target_query_diagnostic(
        self,
        req: &crate::query_cache::SubmitEnrichedSqlRequest,
    ) -> bool {
        req.table_namespace.is_none() && self.permits_literal_query_diagnostic(req)
    }

    pub(crate) fn permits_other_target_query_diagnostic(
        self,
        req: &crate::query_cache::SubmitEnrichedSqlRequest,
    ) -> bool {
        self.permits_literal_query_diagnostic(req)
            && (req.tables.len() == 1
                || req.tables.is_empty()
                    && req.allow_clones == Some(true)
                    && req.execution_type == 1
                    && req.clone_chain_depth_limit == Some(0))
            && (req.execution_type == 10 || req.clone_chain_depth_limit == Some(0))
    }

    /// #151 reports a refused same-query snapshot optimization. This only
    /// admits a diagnostic lookup; physical copy eligibility stays disabled.
    pub(crate) fn permits_snapshot_refusal_diagnostic(
        self,
        req: &crate::query_cache::SubmitEnrichedSqlRequest,
    ) -> bool {
        if self != Self::Databricks
            || req.execution_type != crate::query_cache::ModelExecutionType::Snapshot as i32
            || req.compare_unrendered_code
            || !req.tolerate_nondeterminism
            || req.ignore_external_modifications
            || !req.query_dependencies.is_empty()
            || req.freshness_tolerance_seconds != 0
            || req.stale_upstream_policy != 0
            || req.allow_clones != Some(true)
            || req.clone_chain_depth_limit != Some(0)
            || req.semantic_extras.len() != 1
            || req.semantic_extras.get("unique_key").map(String::as_str) != Some("\"id\"")
            || req
                .table_namespace
                .as_deref()
                .is_none_or(|ns| ns.trim().is_empty())
            || req
                .dbt_node_state
                .as_ref()
                .is_none_or(|state| state.resource_type != "snapshot")
        {
            return false;
        }
        let Some(target) = req
            .target_table
            .as_deref()
            .and_then(databricks::physical_name)
        else {
            return false;
        };
        let Some(resolved) = databricks::resolved_sql(
            &req.sql,
            &req.default_catalog,
            req.default_schema.as_deref().unwrap_or(""),
        ) else {
            return false;
        };
        if resolved.input == target || !(1..=2).contains(&req.tables.len()) {
            return false;
        }
        let mut names = std::collections::HashSet::new();
        req.tables.iter().all(|table| {
            table.last_modified_epoch.is_some_and(|epoch| epoch > 0)
                && databricks::physical_name(&table.name).is_some_and(|name| {
                    (name == target || name == resolved.input) && names.insert(name)
                })
        }) && names.contains(&resolved.input)
    }

    pub(crate) fn permits_absent_target_history_diagnostic(
        self,
        req: &crate::query_cache::SubmitEnrichedSqlRequest,
    ) -> bool {
        self.permits_literal_query_diagnostic(req)
            && req.execution_type == crate::query_cache::ModelExecutionType::Full as i32
            && req.clone_chain_depth_limit == Some(0)
            && req.allow_clones == Some(true)
            && req.tables.is_empty()
            && req
                .table_namespace
                .as_deref()
                .is_some_and(|scope| !scope.is_empty())
    }

    pub(crate) fn permits_absent_scope_definition_diagnostic(
        self,
        req: &crate::query_cache::SubmitEnrichedSqlRequest,
    ) -> bool {
        req.table_namespace
            .as_deref()
            .is_some_and(|namespace| !namespace.is_empty())
            && req.tables.len() == 1
            && self.permits_literal_query_diagnostic(req)
    }

    /// Hosted Snowflake quotes backticks as literal identifier characters in
    /// this crafted three-component metadata spelling. Presentation never
    /// changes the physical key, persisted raw name or own-input comparison.
    pub(crate) fn explain_target_name(self, name: &str) -> String {
        let parts: Vec<_> = name.split('.').collect();
        if self == Self::Snowflake
            && parts.len() == 3
            && parts.iter().all(|part| {
                part.strip_prefix('`')
                    .and_then(|p| p.strip_suffix('`'))
                    .is_some_and(|inner| {
                        !inner.is_empty()
                            && inner
                                .bytes()
                                .all(|c| c.is_ascii_alphanumeric() || c == b'_')
                    })
            })
        {
            parts
                .iter()
                .map(|part| format!("\"{part}\""))
                .collect::<Vec<_>>()
                .join(".")
        } else {
            name.to_owned()
        }
    }

    pub fn family_key(self, dialect: &str) -> String {
        match self {
            Self::Snowflake => "snowflake".into(),
            Self::DuckDb => "duckdb".into(),
            Self::BigQuery => "bigquery".into(),
            Self::Databricks => "databricks".into(),
            Self::Redshift => "redshift".into(),
            Self::Other => {
                let family = dialect
                    .split(',')
                    .next()
                    .unwrap_or("")
                    .trim_matches(' ')
                    .to_ascii_lowercase();
                if family == "spark" {
                    "databricks".into()
                } else {
                    family
                }
            }
        }
    }

    /// Captured Databricks Explain timestamps use the submit instant. This
    /// value is display data and cannot replace an unknown physical epoch.
    pub(crate) fn explain_submit_clock(self, now_ms: i64) -> Option<i64> {
        (self == Self::Databricks).then_some(now_ms)
    }

    /// Hosted #150 reports generic rebuild text for MICRO when both its
    /// Target and a genuine source advanced. The caller must establish the
    /// captured known-positive evidence, matching identity and zero Lag/ANY;
    /// this chooses wording only and grants no reuse qualification.
    pub(crate) fn modified_target_uses_upstream_description(self, execution_type: i32) -> bool {
        self == Self::Databricks
            && execution_type == crate::query_cache::ModelExecutionType::Microbatch as i32
    }

    pub fn normalized_sql(self, sql: &str) -> String {
        crate::sql_identity::normalized_sql(self, sql)
    }

    /// Missing node state cannot supply a template identity. Databricks can
    /// still use exact rendered evidence; explicit empty bodies are errors.
    pub(crate) fn template_body(
        self,
        enabled: bool,
        state: Option<&crate::query_cache::DbtNodeState>,
    ) -> Option<&str> {
        if !enabled {
            return None;
        }
        let body = state.and_then(|s| s.node_body_hash.as_deref());
        if self == Self::Databricks {
            state?
                .node_configs_hash
                .as_deref()
                .filter(|signature| !signature.is_empty())?;
            body.filter(|signature| !signature.is_empty())
        } else {
            Some(body.unwrap_or(""))
        }
    }

    /// Actual pinned write-only evidence covers managed FULL targets only.
    pub(crate) fn record_template_body(
        self,
        req: &crate::query_cache::SqlExecution,
    ) -> Option<&str> {
        if self != Self::Databricks
            || req.execution_type != crate::query_cache::ModelExecutionType::Full as i32
            || req
                .target_table
                .as_deref()
                .and_then(databricks::physical_name)
                .is_none()
            || !req.query_dependencies.is_empty()
            || req
                .tables
                .iter()
                .any(|table| Some(table.name.as_str()) != req.target_table.as_deref())
            || req.dbt_node_state.as_ref().is_some_and(|state| {
                [
                    state.node_body_hash.as_deref(),
                    state.node_configs_hash.as_deref(),
                    state.node_macros_hash.as_deref(),
                ]
                .into_iter()
                .flatten()
                .any(|signature| {
                    signature.len() != 32 || !signature.bytes().all(|b| b.is_ascii_hexdigit())
                })
            })
        {
            return None;
        }
        self.template_body(true, req.dbt_node_state.as_ref())
    }

    /// Explain's template comparison for a resolved submit whose strict
    /// rendered identity is `rendered_hash`.
    pub(crate) fn template_comparison(
        self,
        req: &crate::query_cache::SubmitEnrichedSqlRequest,
        rendered_hash: &str,
    ) -> Option<crate::explain::TemplateComparison> {
        if self != Self::Databricks {
            return None;
        }
        let state = req.dbt_node_state.as_ref();
        let enabled = req.compare_unrendered_code && state.is_some();
        let display_upstream_order = if req.query_dependencies.is_empty() {
            Vec::new()
        } else {
            databricks::explain_input_order(req).unwrap_or_default()
        };
        let rendered_query = (req.execution_type
            == crate::query_cache::ModelExecutionType::Full as i32)
            .then(|| {
                let mut outer = crate::execution::sql_execution(req);
                outer.query_dependencies.clear();
                crate::explain::RenderedQuery {
                    outer_hash: crate::analyzed_workload::identity::sql_logic_hash(&outer, None),
                    dependency_names: req
                        .query_dependencies
                        .iter()
                        .map(|dep| dep.name.clone())
                        .collect(),
                    literal_dependencies: req
                        .query_dependencies
                        .iter()
                        .all(|dep| self.sql_reads_relation(&dep.query) == Some(false)),
                }
            });
        if !enabled && display_upstream_order.is_empty() && rendered_query.is_none() {
            return None;
        }
        Some(crate::explain::TemplateComparison {
            rendered_query,
            enabled,
            display_upstream_order,
            body: state.and_then(|s| s.node_body_hash.clone()),
            config: state.and_then(|s| s.node_configs_hash.clone()),
            macros: state.and_then(|s| s.node_macros_hash.clone()),
            rendered_hash: rendered_hash.to_owned(),
        })
    }

    pub(crate) fn invalid_template_signature(
        self,
        req: &crate::query_cache::SubmitEnrichedSqlRequest,
    ) -> bool {
        self == Self::Databricks
            && req.compare_unrendered_code
            && req.dbt_node_state.as_ref().is_some_and(|state| {
                [
                    state.node_body_hash.as_deref(),
                    state.node_configs_hash.as_deref(),
                    state.node_macros_hash.as_deref(),
                ]
                .into_iter()
                .flatten()
                .any(|signature| {
                    signature.len() != 32 || !signature.bytes().all(|b| b.is_ascii_hexdigit())
                })
            })
    }

    pub(crate) fn invalid_dependency_definition(
        self,
        req: &crate::query_cache::SubmitEnrichedSqlRequest,
    ) -> bool {
        self == Self::Databricks && req.query_dependencies.iter().any(|d| d.query.is_empty())
    }

    /// Bounded #144 ordinary-submit error, not an audit-input exclusion.
    /// Native aliased/default-name histories and historical C/D/E controls
    /// reject a reported audit table for source-less literal test SQL. Keep
    /// genuine/unknown reads, other contexts and uncharacterized modes intact.
    pub(crate) fn invalid_source_less_audit_input(
        self,
        req: &crate::query_cache::SubmitEnrichedSqlRequest,
    ) -> bool {
        use crate::query_cache::ModelExecutionType;

        if self != Self::Databricks
            || req.execution_type != ModelExecutionType::DbtDataTest as i32
            || req.target_table.is_some()
            || !req.compare_unrendered_code
            || req
                .semantic_extras
                .get("store_failures")
                .map(String::as_str)
                != Some("true")
            || req
                .semantic_extras
                .get("store_failures_as")
                .map(String::as_str)
                != Some("\"table\"")
            || !req.query_dependencies.is_empty()
        {
            return false;
        }
        let [table] = req.tables.as_slice() else {
            return false;
        };
        if !table.last_modified_epoch.is_some_and(|epoch| epoch > 0) {
            return false;
        }
        let Some(schema) = req.default_schema.as_deref() else {
            return false;
        };
        let Some(context) = databricks::physical_name(&format!(
            "{}.{}.__audit_context",
            req.default_catalog, schema
        )) else {
            return false;
        };
        let Some(input) = databricks::physical_name(&table.name) else {
            return false;
        };
        context.rsplit_once('.').map(|(scope, _)| scope)
            == input.rsplit_once('.').map(|(scope, _)| scope)
            && context
                .split('.')
                .nth(1)
                .is_some_and(|schema| schema.trim_matches('`').ends_with("_dbt_test__audit"))
            && self.sql_reads_relation(&req.sql) == Some(false)
            && databricks::literal_audit_query(&req.sql)
    }

    /// Resolve demonstrated metadata spellings once before history, producer,
    /// freshness and own-target processing. The caller retains its raw request
    /// for wire DDL; SQL text itself remains unmodified.
    pub(crate) fn resolve_submit(
        self,
        req: &crate::query_cache::SubmitEnrichedSqlRequest,
    ) -> std::borrow::Cow<'_, crate::query_cache::SubmitEnrichedSqlRequest> {
        if self != Self::Databricks {
            return std::borrow::Cow::Borrowed(req);
        }
        let mut resolved = req.clone();
        // #138: exact repeated whole bindings add no evidence. Do this before
        // resolving physical aliases, so mixed spellings or conflicting
        // definitions/contexts retain the existing ambiguity guard.
        let mut unique = Vec::new();
        resolved.query_dependencies.retain(|dependency| {
            if unique.contains(dependency) {
                false
            } else {
                unique.push(dependency.clone());
                true
            }
        });
        // #140: only exact, known repeated current evidence is redundant.
        // Check raw spellings before resolving aliases; conflicting/unknown
        // entries and the target's own evidence must remain ambiguous.
        if req.execution_type == 1
            && !req.compare_unrendered_code
            && req.query_dependencies.is_empty()
            && req
                .target_table
                .as_deref()
                .and_then(databricks::physical_name)
                .is_some()
        {
            let redundant: Vec<_> = req
                .tables
                .iter()
                .map(|table| {
                    let physical = databricks::physical_name(&table.name);
                    physical.as_ref().is_some_and(|name| {
                        req.target_table
                            .as_deref()
                            .and_then(databricks::physical_name)
                            .as_ref()
                            != Some(name)
                            && table.last_modified_epoch.is_some()
                            && req
                                .tables
                                .iter()
                                .filter(|other| {
                                    databricks::physical_name(&other.name).as_ref() == Some(name)
                                })
                                .all(|other| other == table)
                    })
                })
                .collect();
            let mut seen = std::collections::HashSet::new();
            let mut index = 0;
            resolved.tables.retain(|table| {
                let keep = !redundant[index] || seen.insert(table.name.clone());
                index += 1;
                keep
            });
        }
        self.resolve_names(
            &mut resolved.target_table,
            &mut resolved.tables,
            &mut resolved.query_dependencies,
        );
        std::borrow::Cow::Owned(resolved)
    }

    pub(crate) fn resolve_record(
        self,
        req: &crate::query_cache::SqlExecution,
    ) -> std::borrow::Cow<'_, crate::query_cache::SqlExecution> {
        if self != Self::Databricks {
            return std::borrow::Cow::Borrowed(req);
        }
        let mut resolved = req.clone();
        self.resolve_names(
            &mut resolved.target_table,
            &mut resolved.tables,
            &mut resolved.query_dependencies,
        );
        std::borrow::Cow::Owned(resolved)
    }

    fn resolve_names(
        self,
        target: &mut Option<String>,
        tables: &mut [crate::query_cache::TableModifiedInfo],
        deps: &mut [crate::query_cache::QueryDependency],
    ) {
        let canonical = |name: &mut String| {
            if let Some(resolved) = databricks::physical_name(name) {
                *name = resolved;
            }
        };
        if let Some(target) = target {
            canonical(target);
        }
        for table in tables {
            canonical(&mut table.name);
        }
        for dep in deps {
            canonical(&mut dep.name);
        }
    }

    /// Decode raw persisted names into physical decision evidence while
    /// retaining their original target spelling for later output consumers.
    pub(crate) fn resolve_execution_row(self, row: &mut ExecutionRow) {
        if self != Self::Databricks {
            return;
        }
        if let Some(target) = databricks::physical_name(&row.target_table) {
            if target != row.target_table {
                row.raw_target_table = Some(row.target_table.clone());
                row.target_table = target;
            }
        }
        for input in &mut row.input_tables {
            if let Some(name) = databricks::physical_name(&input.name) {
                input.name = name;
            }
        }
    }

    /// A resolved query no longer depends on defaults; unsupported SQL retains
    /// both exact bytes and its original context. No copy eligibility is added.
    pub(crate) fn sql_identity(
        self,
        sql: &str,
        catalog: &str,
        schema: &str,
    ) -> (String, String, String) {
        if self == Self::Databricks {
            if let Some(resolved) = databricks::resolved_sql(sql, catalog, schema) {
                return (
                    databricks::formatted_direct_sql(&resolved),
                    String::new(),
                    String::new(),
                );
            }
            if let Some(normalized) = databricks::table_alias_sql(sql, catalog, schema) {
                return (normalized, catalog.into(), schema.into());
            }
            if let Some(normalized) = databricks::literal_projection_sql(sql, catalog, schema) {
                return (normalized, catalog.into(), schema.into());
            }
            if let Some(normalized) = databricks::cte_formatting_sql(sql, catalog, schema) {
                return (normalized, catalog.into(), schema.into());
            }
            // #138's actual Jinja-comment edit emits an extra outer newline
            // around an otherwise unsupported VALUES query. Strip only SQL
            // whitespace outside the complete statement; unknown interior
            // syntax, literals, directives and contexts retain exact bytes.
            return (
                sql.trim_matches([' ', '\t', '\r', '\n']).to_owned(),
                catalog.into(),
                schema.into(),
            );
        }
        (self.normalized_sql(sql), catalog.into(), schema.into())
    }

    pub(crate) fn target_schema(self, target: Option<&str>) -> Option<String> {
        if self != Self::Databricks {
            return None;
        }
        let name = databricks::physical_name(target?)?;
        Some(name.split('.').nth(1)?.trim_matches('`').to_owned())
    }

    /// A request whose rendered identity matches the latest build only when
    /// its unqualified input is read in the target's context. This proves the
    /// observed context-only mismatch without trusting client body hashes.
    pub(crate) fn changed_resolution_context(
        self,
        req: &crate::query_cache::SubmitEnrichedSqlRequest,
        latest: &ExecutionRow,
    ) -> Option<String> {
        if self != Self::Databricks
            || req.compare_unrendered_code
            || req.execution_type != crate::query_cache::ModelExecutionType::Full as i32
            || latest.clone_source_table.is_some()
            || !req.tables.iter().any(|t| {
                Some(&t.name) == req.target_table.as_ref() && t.last_modified_epoch.is_some()
            })
            || latest.execution_type != req.execution_type
        {
            return None;
        }
        let target = databricks::physical_name(req.target_table.as_deref()?)?;
        let components: Vec<_> = target.split('.').map(|s| s.trim_matches('`')).collect();
        let inferred = self.target_schema(req.target_table.as_deref());
        let schema = req
            .default_schema
            .as_deref()
            .or(inferred.as_deref())
            .unwrap_or("");
        let current = databricks::resolved_sql(&req.sql, &req.default_catalog, schema)?;
        let mut target_context = req.clone();
        target_context.default_catalog = components[0].into();
        target_context.default_schema = Some(components[1].into());
        let prior =
            databricks::resolved_sql(&req.sql, &target_context.default_catalog, components[1])?;
        if prior.input == current.input {
            return None;
        }
        let hash = crate::analyzed_workload::identity::sql_logic_hash(
            &crate::execution::sql_execution(&target_context),
            None,
        );
        (latest.node_sql_hash.as_deref() == Some(hash.as_str())).then_some(current.input)
    }

    /// Prove a changed input binding against a prior direct-input build by
    /// reconstructing that exact full identity. This is Explain evidence only;
    /// it never qualifies the prior build for reuse.
    pub(crate) fn changed_input_binding(
        self,
        req: &crate::query_cache::SubmitEnrichedSqlRequest,
        latest: &ExecutionRow,
    ) -> bool {
        if self != Self::Databricks
            || req.compare_unrendered_code
            || req.execution_type != crate::query_cache::ModelExecutionType::Full as i32
            || latest.execution_type != req.execution_type
            || latest.clone_source_table.is_some()
        {
            return false;
        }
        let target_key =
            Self::physical_scope_key(&req.dialect, req.target_table.as_deref().unwrap_or(""));
        let previous: Vec<_> = latest
            .input_tables
            .iter()
            .filter(|t| Self::physical_scope_key(&req.dialect, &t.name) != target_key)
            .collect();
        let [previous] = previous.as_slice() else {
            return false;
        };
        let Some(name) = databricks::physical_name(&previous.name) else {
            return false;
        };
        let inferred = self.target_schema(req.target_table.as_deref());
        let schema = req
            .default_schema
            .as_deref()
            .or(inferred.as_deref())
            .unwrap_or("");
        let Some(current) = databricks::resolved_sql(&req.sql, &req.default_catalog, schema) else {
            return false;
        };
        if current.input == name && req.query_dependencies.is_empty() {
            return false;
        }
        let mut prior = req.clone();
        prior.sql = format!(
            "{}{}{}",
            &req.sql[..current.start],
            name,
            &req.sql[current.end..]
        );
        prior.query_dependencies.clear();
        latest.node_sql_hash.as_deref()
            == Some(
                crate::analyzed_workload::identity::sql_logic_hash(
                    &crate::execution::sql_execution(&prior),
                    None,
                )
                .as_str(),
            )
    }

    pub(crate) fn resolved_input(
        self,
        req: &crate::query_cache::SubmitEnrichedSqlRequest,
    ) -> Option<String> {
        if self != Self::Databricks {
            return None;
        }
        let inferred = self.target_schema(req.target_table.as_deref());
        let schema = req
            .default_schema
            .as_deref()
            .or(inferred.as_deref())
            .unwrap_or("");
        Some(databricks::resolved_sql(&req.sql, &req.default_catalog, schema)?.input)
    }

    /// Native Databricks RegisterClone leaves the optional
    /// `execution_decision_id` present but empty in `ready_to_clone`.
    pub(crate) fn clone_response_hides_decision_id(self) -> bool {
        self == Self::Databricks
    }

    /// Storage's physical history key (migration 0020). Raw names remain in
    /// target_table for DDL/Explain; the key never changes identity hashes.
    pub(crate) fn physical_scope_key(dialect: &str, name: &str) -> String {
        let family = dialect.split(',').next().unwrap_or("").trim_matches(' ');
        if family.eq_ignore_ascii_case("databricks") || family.eq_ignore_ascii_case("spark") {
            if let Some(name) = databricks::physical_name(name) {
                return name.replace('`', "");
            }
        }
        name.to_owned()
    }

    /// Whether the namespace logic identity abstracts physical schemas, so
    /// the same model can be reused across environments.
    pub fn supports_namespace_reuse(self) -> bool {
        matches!(
            self.namespace_identity(),
            NamespaceIdentity::SchemaAbstracted { .. }
        )
    }

    // ── Fingerprint version tags ────────────────────────────────────────────
    //
    // Each tag leads its fingerprint. A change to a warehouse's identity
    // semantics takes a new tag, so older history cold-misses instead of being
    // reinterpreted (ADR 0004).

    /// The version tag of the strict SQL identity, in both rendered and
    /// template mode.
    pub fn sql_identity_version(self) -> &'static [u8] {
        match self {
            Self::DuckDb => duckdb::SQL_IDENTITY_VERSION,
            Self::Databricks => databricks::SQL_IDENTITY_VERSION,
            _ => SQL_IDENTITY_VERSION,
        }
    }

    /// How the namespace (cross-environment) logic identity reads this
    /// warehouse's SQL, and its version tag.
    pub fn namespace_identity(self) -> NamespaceIdentity {
        match self {
            Self::Snowflake => snowflake::NAMESPACE_IDENTITY,
            _ => NamespaceIdentity::Exact {
                version: EXACT_NAMESPACE_IDENTITY_VERSION,
            },
        }
    }

    /// Whether this Warehouse's data-test State history is keyed by the
    /// data-test history key (#200, ADR 0007 "Data-test history key"):
    /// Snowflake, as captured (`data_test_history.jsonl`). Every other
    /// Warehouse keeps the history of each test unique id (Databricks and
    /// DuckDB, uncaptured).
    pub(crate) fn keys_data_test_history(self) -> bool {
        matches!(self, Self::Snowflake)
    }

    pub fn has_volatile_sql<'a>(
        self,
        root: &str,
        dependencies: impl IntoIterator<Item = &'a str>,
    ) -> bool {
        match self {
            Self::Snowflake => snowflake::has_volatile_sql(root, dependencies),
            // Until each dialect has a checked parser and volatile-function set,
            // suppress the SQL-submit clone fallback. A normal build remains
            // available and can still be reused on a later strict match.
            _ => true,
        }
    }

    pub(crate) fn checked_volatile_reuse(
        self,
        req: &crate::query_cache::SubmitEnrichedSqlRequest,
    ) -> bool {
        self == Self::Databricks && databricks::checked_volatile_reuse(req)
    }

    pub(crate) fn volatile_producer_input(
        self,
        req: &crate::query_cache::SubmitEnrichedSqlRequest,
    ) -> Option<String> {
        (self == Self::Databricks)
            .then(|| databricks::volatile_producer_input(req))
            .flatten()
    }

    /// Whether a model whose SQL calls a Volatile function, with Volatile
    /// evaluation requested, is reused by its unchanged unrendered code under
    /// `compare_unrendered_code` instead of being refused at Reuse
    /// qualification (maintainer decision 1 on #201, #206). Captured on
    /// Snowflake only (`explain_volatile_cells.jsonl` `ecu`, entries 140 and
    /// 198); every other Warehouse keeps the refusal (Databricks has no
    /// checked Volatile function detector).
    pub(crate) fn reuses_volatile_sql_by_unrendered_code(self) -> bool {
        self == Self::Snowflake
    }

    /// The Volatile functions this warehouse has captured rendering rules
    /// for (Volatile evaluation, #67). `None`: nothing is rendered.
    pub fn volatile_rules(self) -> Option<crate::volatile_evaluation::Rules> {
        match self {
            Self::Databricks => Some(databricks::VOLATILE_RULES),
            Self::Snowflake => Some(snowflake::VOLATILE_RULES),
            _ => None,
        }
    }

    // ── Result copy rules ───────────────────────────────────────────────────
    //
    // A result copy populates a target from an existing result instead of
    // executing its transformation (see `result_copy`). Warehouses without
    // their own rules get conservative defaults: no copy unless DDL exists,
    // no relation or provenance requirements beyond it.

    /// Whether a SQL submission can take part in a result copy, as source or
    /// target, and the identity that pairs it with an equivalent. `None`: it
    /// is never copied. Eligibility is explicit: formatting validation alone
    /// never grants copying.
    pub(crate) fn copy_identity(
        self,
        req: &crate::query_cache::SqlExecution,
    ) -> Option<CopyIdentity> {
        match self {
            Self::Snowflake => Some(snowflake::copy_identity()),
            Self::DuckDb => duckdb::copy_identity(req),
            Self::Databricks => {
                crate::databricks_submit_copy::identity(req).map(CopyIdentity::DatabricksSubmit)
            }
            _ => None,
        }
    }

    /// Whether a submit of this execution type may be answered with a
    /// submit-time Result copy (`ready_to_clone`). Hosted 2.0.11 copies only
    /// FULL on submit (maintainer decision 2026-11-11, #86, #92, #101): it
    /// rebuilds views, snapshots, custom nodes and incrementals whose
    /// equivalent exists under another name, so every warehouse but DuckDB
    /// copies FULL only. The one exception is a seed (VALUES) on Snowflake,
    /// which hosted copies on submit (maintainer decision 2026-11-11, #91;
    /// `result_copy::seed_lookup`). DuckDB keeps its own checked subset,
    /// which hosted does not engage: FULL, or delete+insert under the pinned
    /// incremental contract (docs/duckdb-identity-and-copy.md); it reloads
    /// seeds.
    pub(crate) fn submit_copy_admits(
        self,
        execution_type: i32,
        semantic_extras: &std::collections::HashMap<String, String>,
    ) -> bool {
        use crate::query_cache::ModelExecutionType;
        match self {
            Self::DuckDb => duckdb::copy_admits(execution_type, semantic_extras),
            Self::Snowflake => {
                execution_type == ModelExecutionType::Full as i32
                    || execution_type == ModelExecutionType::Values as i32
            }
            _ => execution_type == ModelExecutionType::Full as i32,
        }
    }

    /// The statements that copy `source` into `target`, or why no copy can be
    /// produced. `source_table_type` is the warehouse-reported kind of the
    /// source. Requested retention options and view sources are refused for
    /// every warehouse so the client builds normally instead of silently
    /// creating a copy unlike the requested target.
    pub fn result_copy_ddl(
        self,
        source: &str,
        target: &str,
        source_table_type: Option<&str>,
        properties: Option<&crate::query_cache::TableProperties>,
    ) -> Result<Vec<String>, &'static str> {
        if properties.is_some_and(|p| {
            p.hours_to_expiration.is_some() || p.partition_expiration_days.is_some()
        }) {
            return Err("clone table properties are not implemented");
        }
        if source_table_type.is_some_and(|kind| kind.to_ascii_uppercase().contains("VIEW")) {
            return Err("cloning view sources is not implemented");
        }
        match self {
            Self::Snowflake => Ok(snowflake::copy_ddl(source, target, source_table_type)),
            Self::DuckDb => duckdb::copy_ddl(source, target, source_table_type),
            Self::BigQuery => {
                // GoogleSQL's CREATE TABLE ... CLONE syntax has no OR REPLACE form.
                Ok(vec![format!("CREATE TABLE {target} CLONE {source}")])
            }
            Self::Databricks => {
                // Native default-format requests omit source type. This DDL
                // attempts a warehouse-validated shallow copy; unknown metadata
                // does not establish Delta identity or authorize adoption.
                // Explicit incompatible formats remain unsupported.
                if source_table_type.is_some_and(|kind| {
                    !matches!(kind.to_ascii_uppercase().as_str(), "DELTA" | "DELTA TABLE")
                }) {
                    return Err("Databricks shallow clone source type is not supported");
                }
                Ok(vec![format!(
                    "\n        CREATE OR REPLACE TABLE {target} SHALLOW CLONE {source}\n        "
                )])
            }
            Self::Redshift | Self::Other => Err("clone DDL is not implemented for this dialect"),
        }
    }

    /// Whether `source` and `target` can be the two ends of one copy. The
    /// clone-equivalent lookup skips candidates that fail this; the DDL
    /// applies the same check.
    pub fn copy_relations_compatible(self, source: &str, target: &str) -> bool {
        match self {
            Self::DuckDb => duckdb::copy_relations_compatible(source, target),
            _ => true,
        }
    }

    /// The copy-chain depth at which a submit stops copying an equivalent and
    /// executes instead (the chain-limit Execute), given the limit the client
    /// requested. `None`: this warehouse does not limit copy chains.
    pub fn copy_chain_depth_limit(self, requested: Option<i64>) -> Option<usize> {
        match self {
            Self::DuckDb => Some(duckdb::copy_chain_depth_limit(requested)),
            _ => None,
        }
    }

    /// `Some(max)`: the clone-equivalent lookup must walk each candidate's
    /// copy provenance (a store read), and a candidate with more than `max`
    /// copy ancestors is ineligible. `None`: copy depth is not tracked.
    pub fn copy_provenance_depth(self) -> Option<usize> {
        match self {
            Self::DuckDb => Some(duckdb::copy_provenance_depth()),
            _ => None,
        }
    }

    /// Physical evidence the submit fallback requires of a copy source beyond
    /// its DDL: the source's recorded build epoch, if any.
    pub fn copy_source_evidence(
        self,
        last_modified_epoch: Option<i64>,
    ) -> Result<(), &'static str> {
        match self {
            Self::DuckDb => duckdb::copy_source_evidence(last_modified_epoch),
            _ => Ok(()),
        }
    }

    /// When a recorded build happened on the server's wall clock (Unix ms),
    /// for measuring its age against Lag tolerance (ADR 0007). Epochs are
    /// wall-clock milliseconds on every warehouse except DuckDB, whose
    /// epochs are evidence ordinals: there, the server's receipt of the
    /// build's confirmation stands in.
    pub fn build_time_ms(self, build: &ExecutionRow) -> Option<i64> {
        match self {
            Self::DuckDb => build.confirmed_at_ms,
            Self::Databricks
                if build.clone_source_table.is_some() && build.node_sql_hash.is_some() =>
            {
                build
                    .submit_copy_provenance
                    .as_ref()
                    .filter(|p| p.version == 2 && p.published && p.source_epoch > 0)
                    .map(|p| p.source_epoch)
            }
            Self::Databricks if build.clone_source_table.is_some() => build
                .direct_copy_provenance
                .as_ref()
                .filter(|p| p.version == 2 && p.published)
                .and_then(|p| p.source_epoch.filter(|e| *e > 0)),
            _ => build.last_modified_epoch,
        }
    }

    /// Whether the submit fallback skips its volatile-SQL gate for a
    /// submission that has a copy logic identity. DuckDB computes that
    /// identity only for its checked copy subset. Like every rule, this
    /// follows [`Warehouse::from_dialect`], so the dialect's spelling (case,
    /// surrounding whitespace) does not matter.
    pub fn copy_identity_skips_volatility_gate(self) -> bool {
        matches!(self, Self::DuckDb)
    }

    /// Whether `values` (submission semantic extras or copy request labels)
    /// declare this warehouse's pinned incremental contract.
    pub fn declares_incremental_contract(
        self,
        values: &std::collections::HashMap<String, String>,
    ) -> bool {
        match self {
            Self::DuckDb => duckdb::declares_incremental_contract(values),
            _ => false,
        }
    }

    /// What a client-requested copy must declare about its source before any
    /// history is read.
    pub fn incremental_copy_contract(
        self,
        req: &crate::query_cache::CloneRequest,
    ) -> IncrementalCopyContract<'_> {
        match self {
            Self::DuckDb => duckdb::incremental_copy_contract(req),
            _ => IncrementalCopyContract::NotRequired,
        }
    }

    /// Whether `sql` reads a relation ([`crate::sql_identity::reads_relation`]),
    /// parsed in this warehouse's dialect. It chooses a view's Explain
    /// freshness wording (#117) and limits the existing-target-without-history
    /// reason to captured source-less SQL. `None` when the SQL does not parse
    /// as one query.
    pub fn sql_reads_relation(self, sql: &str) -> Option<bool> {
        use sqlparser::dialect::{
            BigQueryDialect, DatabricksDialect, DuckDbDialect, GenericDialect, RedshiftSqlDialect,
            SnowflakeDialect,
        };
        let dialect: &dyn sqlparser::dialect::Dialect = match self {
            Self::Snowflake => &SnowflakeDialect {},
            Self::DuckDb => &DuckDbDialect {},
            Self::BigQuery => &BigQueryDialect {},
            Self::Databricks => &DatabricksDialect {},
            Self::Redshift => &RedshiftSqlDialect {},
            Self::Other => &GenericDialect {},
        };
        crate::sql_identity::reads_relation(sql, dialect)
    }

    /// Whether the reported relation `name` is the relation
    /// `database`.`schema`.`identifier`, each part as dbt spells it in the
    /// request. Only Snowflake's identifier rules are characterized; on other
    /// warehouses nothing matches, so the relation stays distinct.
    pub fn names_relation(
        self,
        name: &str,
        database: &str,
        schema: &str,
        identifier: &str,
    ) -> bool {
        match self {
            Self::Snowflake => snowflake::names_relation(name, [database, schema, identifier]),
            _ => false,
        }
    }

    /// Bounded affirmative non-read proof for the Databricks audit-output slice.
    pub(crate) fn unread_test_audit(self, req: &crate::query_cache::SqlExecution) -> Option<&str> {
        match self {
            Self::Databricks => databricks::unread_test_audit(req),
            _ => None,
        }
    }

    /// The identifier of the reported relation `name` when it is a relation
    /// in `database`.`schema`, each part as dbt spells it in the request, as
    /// the warehouse resolves it (an unquoted identifier folded, a quoted one
    /// exact). Only Snowflake's identifier rules are characterized; on other
    /// warehouses nothing is in the schema.
    pub fn relation_in_schema(self, name: &str, database: &str, schema: &str) -> Option<String> {
        match self {
            Self::Snowflake => snowflake::relation_in_schema(name, [database, schema]),
            _ => None,
        }
    }
}

/// A submission's copy logic identity ([`Warehouse::copy_identity`]), which
/// `analyzed_workload::identity::copy_logic_hash` frames.
pub(crate) enum CopyIdentity {
    /// The namespace logic identity ([`Warehouse::namespace_identity`]).
    NamespaceLogic,
    /// Exact bounded Databricks submit identity, separately versioned.
    DatabricksSubmit(String),
    /// A checked copy graph, framed under `version`.
    CopyGraph {
        version: &'static [u8],
        graph: crate::analyzed_workload::binding::DuckDbCopyGraph,
    },
}

/// A client-requested copy's incremental contract
/// ([`Warehouse::incremental_copy_contract`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrementalCopyContract<'a> {
    /// This warehouse copies without one.
    NotRequired,
    /// Required but not declared: the copy is refused.
    Missing,
    /// Declared. The source must currently be the confirmed incremental build
    /// with this initialization identity.
    Declared { identity: &'a str },
}

#[cfg(test)]
mod tests {
    use super::{CopyIdentity, IncrementalCopyContract, NamespaceIdentity, Warehouse};
    use crate::store::ExecutionRow;

    /// ADR 0007: build age is measured on the server clock. Millisecond
    /// warehouses use the build's ConfirmExecution epoch; DuckDB's epochs are
    /// evidence ordinals, so it uses the server's receipt of the confirmation.
    #[test]
    fn build_time_is_the_confirm_epoch_except_on_duckdb() {
        let build = ExecutionRow {
            last_modified_epoch: Some(7),
            confirmed_at_ms: Some(1_794_000_000_000),
            ..ExecutionRow::default()
        };
        assert_eq!(
            Warehouse::from_dialect("snowflake").build_time_ms(&build),
            Some(7)
        );
        assert_eq!(
            Warehouse::from_dialect("postgres").build_time_ms(&build),
            Some(7)
        );
        assert_eq!(
            Warehouse::from_dialect("duckdb").build_time_ms(&build),
            Some(1_794_000_000_000)
        );
    }

    #[test]
    fn redshift_options_keep_one_physical_family() {
        let configured = "redshift, normalization_strategy = lowercase";
        assert_eq!(Warehouse::from_dialect(configured), Warehouse::Redshift);
        assert_eq!(
            Warehouse::from_dialect(configured).family_key(configured),
            "redshift"
        );
    }

    #[test]
    fn unverified_dialects_do_not_abstract_or_clone_sql() {
        for dialect in [
            "bigquery",
            "databricks",
            "redshift",
            "unknown",
            "snowflake, mode=x",
        ] {
            let warehouse = Warehouse::from_dialect(dialect);
            assert!(!warehouse.supports_namespace_reuse());
            assert_eq!(warehouse.normalized_sql("SELECT  a"), "SELECT  a");
            assert!(warehouse.has_volatile_sql("select rand()", std::iter::empty()));
        }
    }

    // ---- Fingerprint version tags -------------------------------------------

    const OTHERS: [Warehouse; 4] = [
        Warehouse::BigQuery,
        Warehouse::Databricks,
        Warehouse::Redshift,
        Warehouse::Other,
    ];

    #[test]
    fn strict_identities_keep_their_warehouse_version_contracts() {
        for warehouse in OTHERS
            .into_iter()
            .chain([Warehouse::Snowflake, Warehouse::DuckDb])
        {
            let expected: &[u8] = match warehouse {
                Warehouse::DuckDb => b"dbt-state-duckdb-sql-v1",
                Warehouse::Databricks => b"dbt-state-databricks-sql-v4",
                _ => b"dbt-state-sql-v4",
            };
            assert_eq!(warehouse.sql_identity_version(), expected, "{warehouse:?}");
        }
    }

    #[test]
    fn only_snowflake_abstracts_schemas_in_the_namespace_identity() {
        assert_eq!(
            Warehouse::Snowflake.namespace_identity(),
            NamespaceIdentity::SchemaAbstracted {
                version: b"dbt-state-ns-logic-v4"
            }
        );
        assert!(Warehouse::Snowflake.supports_namespace_reuse());
        for warehouse in OTHERS.into_iter().chain([Warehouse::DuckDb]) {
            assert_eq!(
                warehouse.namespace_identity(),
                NamespaceIdentity::Exact {
                    version: b"dbt-state-exact-ns-v1"
                },
                "{warehouse:?}"
            );
            assert!(!warehouse.supports_namespace_reuse(), "{warehouse:?}");
        }
    }

    // ---- Result copy rules --------------------------------------------------

    /// An eligible DuckDB model: a checked view chain over one physical base
    /// input with recorded evidence, built into a declared environment.
    fn duckdb_copy_submission() -> crate::query_cache::SqlExecution {
        duckdb_copy_submission_in("dev")
    }

    fn duckdb_copy_submission_in(env: &str) -> crate::query_cache::SqlExecution {
        use crate::query_cache as qc;
        let target = format!("\"local\".\"{env}\".\"m\"");
        qc::SqlExecution {
            target_table: Some(target.clone()),
            dialect: "duckdb".into(),
            default_catalog: "local".into(),
            default_schema: Some(env.into()),
            execution_type: qc::ModelExecutionType::Full as i32,
            sql: format!("select id from local.{env}.stg"),
            tables: vec![
                qc::TableModifiedInfo {
                    name: "local.raw.input".into(),
                    last_modified_epoch: Some(10),
                },
                // The target's own (unknown) evidence does not block a copy.
                qc::TableModifiedInfo {
                    name: target,
                    last_modified_epoch: None,
                },
            ],
            query_dependencies: vec![qc::QueryDependency {
                name: format!("\"local\".\"{env}\".\"stg\""),
                query: "select id from local.raw.input".into(),
                default_catalog: "local".into(),
                default_schema: env.into(),
            }],
            semantic_extras: [("duckdb_copy_contract".into(), "rows-schema-v1".into())].into(),
            ..Default::default()
        }
    }

    fn is_duckdb_copy_graph(identity: Option<CopyIdentity>) -> bool {
        matches!(
            identity,
            Some(CopyIdentity::CopyGraph {
                version: b"dbt-state-duckdb-copy-v2",
                ..
            })
        )
    }

    #[test]
    fn duckdb_copies_only_checked_table_models_with_complete_evidence() {
        use crate::query_cache::{ModelExecutionType, TableModifiedInfo};
        let duck = Warehouse::DuckDb;
        assert!(is_duckdb_copy_graph(
            duck.copy_identity(&duckdb_copy_submission())
        ));
        assert!(is_duckdb_copy_graph(
            duck.copy_identity(&duckdb_copy_submission_in("prod"))
        ));
        // Only the declared environments are copied between.
        assert!(duck
            .copy_identity(&duckdb_copy_submission_in("ci"))
            .is_none());
        let mut incremental = duckdb_copy_submission();
        incremental.execution_type = ModelExecutionType::DeleteInsert as i32;
        incremental.semantic_extras.insert(
            "duckdb_incremental_contract".into(),
            "delete-insert-fixed-v1".into(),
        );
        assert!(is_duckdb_copy_graph(duck.copy_identity(&incremental)));

        type Mutation = fn(&mut crate::query_cache::SqlExecution);
        let ineligible: Vec<(&str, Mutation)> = vec![
            ("view model", |r| {
                r.execution_type = ModelExecutionType::View as i32
            }),
            ("merge model", |r| {
                r.execution_type = ModelExecutionType::Merge as i32
            }),
            ("delete+insert without the pinned contract", |r| {
                r.execution_type = ModelExecutionType::DeleteInsert as i32
            }),
            ("no default schema", |r| r.default_schema = None),
            ("no copy contract", |r| {
                r.semantic_extras.remove("duckdb_copy_contract");
            }),
            ("other copy contract", |r| {
                r.semantic_extras
                    .insert("duckdb_copy_contract".into(), "rows-v0".into());
            }),
            ("unsupported SQL", |r| {
                r.sql = "select id from local.dev.stg order by id".into()
            }),
            ("unknown input epoch", |r| {
                r.tables[0].last_modified_epoch = None
            }),
            ("no input evidence", |r| {
                r.tables.remove(0);
            }),
            ("duplicate input evidence", |r| {
                let input = r.tables[0].clone();
                r.tables.push(input);
            }),
            ("unparseable evidence", |r| {
                r.tables.push(TableModifiedInfo {
                    name: "not a relation(".into(),
                    last_modified_epoch: Some(1),
                })
            }),
            ("no target", |r| r.target_table = None),
            ("target outside the default schema", |r| {
                r.target_table = Some(DUCK_PROD.into());
                r.tables[1].name = DUCK_PROD.into();
            }),
            ("target outside the default catalog", |r| {
                r.target_table = Some("\"other\".\"dev\".\"m\"".into());
                r.tables[1].name = "\"other\".\"dev\".\"m\"".into();
            }),
        ];
        for (why, mutate) in ineligible {
            let mut req = duckdb_copy_submission();
            mutate(&mut req);
            assert!(duck.copy_identity(&req).is_none(), "{why}");
        }
    }

    #[test]
    fn snowflake_copies_by_namespace_logic_and_other_warehouses_never_copy() {
        let mut req = duckdb_copy_submission();
        req.dialect = "snowflake".into();
        assert!(matches!(
            Warehouse::Snowflake.copy_identity(&req),
            Some(CopyIdentity::NamespaceLogic)
        ));
        for warehouse in OTHERS {
            assert!(warehouse.copy_identity(&req).is_none(), "{warehouse:?}");
        }
    }

    const DUCK_PROD: &str = "\"local\".\"prod\".\"m\"";
    const DUCK_DEV: &str = "\"local\".\"dev\".\"m\"";
    const CTAS: Option<&str> = Some("DUCKDB CTAS TABLE");

    fn ddl(
        dialect: &str,
        source: &str,
        target: &str,
        kind: Option<&str>,
    ) -> Result<Vec<String>, &'static str> {
        Warehouse::from_dialect(dialect).result_copy_ddl(source, target, kind, None)
    }

    /// Submit copies are FULL only, except a seed on Snowflake (#91); DuckDB
    /// keeps its own checked subset and reloads seeds.
    #[test]
    fn only_snowflake_admits_a_seed_copy_on_submit() {
        let none = std::collections::HashMap::new();
        for dialect in [
            "snowflake",
            "duckdb",
            "bigquery",
            "databricks",
            "redshift",
            "other",
        ] {
            let w = Warehouse::from_dialect(dialect);
            assert!(w.submit_copy_admits(1, &none), "{dialect} FULL");
            assert_eq!(
                w.submit_copy_admits(9, &none),
                dialect == "snowflake",
                "{dialect}"
            );
            for et in [2, 3, 4, 5, 6, 7, 8, 10, 11] {
                assert!(!w.submit_copy_admits(et, &none), "{dialect} {et}");
            }
        }
    }

    #[test]
    fn snowflake_transient_clone_matches_real_shape() {
        let sqls = ddl(
            "snowflake",
            "\"DB\".\"PROD\".\"T\"",
            "\"DB\".\"DEV\".\"T\"",
            Some("TRANSIENT TABLE"),
        )
        .unwrap();
        assert_eq!(sqls.len(), 1);
        let s = &sqls[0];
        assert!(s.contains("CREATE OR REPLACE TRANSIENT TABLE \"DB\".\"DEV\".\"T\""));
        assert!(s.contains("CLONE \"DB\".\"PROD\".\"T\""));
        assert!(s.contains("COPY GRANTS"));
    }

    #[test]
    fn snowflake_plain_table_clone() {
        let sqls = ddl("snowflake", "p.s.t", "d.s.t", Some("TABLE")).unwrap();
        assert!(sqls[0].contains("CREATE OR REPLACE TABLE"));
        assert!(!sqls[0].contains("TRANSIENT"));
    }

    #[test]
    fn documented_non_snowflake_shapes_and_capabilities() {
        assert_eq!(
            ddl("bigquery", "s", "t", Some("TABLE")).unwrap(),
            ["CREATE TABLE t CLONE s"]
        );
        assert_eq!(
            ddl("databricks", "s", "t", Some("DELTA TABLE")).unwrap(),
            ["\n        CREATE OR REPLACE TABLE t SHALLOW CLONE s\n        "]
        );
        assert!(ddl("databricks", "s", "t", None).is_ok());
        for kind in [Some("TABLE"), Some("ICEBERG TABLE")] {
            assert!(ddl("databricks", "s", "t", kind).is_err());
        }
    }

    #[test]
    fn unsupported_clone_sources_and_dialects_do_not_emit_guessed_sql() {
        assert!(ddl("snowflake", "s", "t", Some("VIEW")).is_err());
        assert!(ddl("unknown", "s", "t", Some("TABLE")).is_err());
        assert!(ddl("redshift", "s", "t", Some("TABLE")).is_err());
        assert!(Warehouse::from_dialect("bigquery")
            .result_copy_ddl(
                "s",
                "t",
                Some("TABLE"),
                Some(&crate::query_cache::TableProperties {
                    hours_to_expiration: Some(24),
                    partition_expiration_days: None,
                }),
            )
            .is_err());
    }

    #[test]
    fn duckdb_copies_ctas_sources_between_environments_of_one_file() {
        assert_eq!(
            ddl("duckdb", DUCK_PROD, DUCK_DEV, CTAS).unwrap(),
            [format!(
                "CREATE OR REPLACE TABLE {DUCK_DEV} AS SELECT * FROM {DUCK_PROD}"
            )]
        );
        let refused = [
            (
                DUCK_PROD,
                DUCK_DEV,
                Some("BASE TABLE"),
                "DuckDB copy requires inspected CTAS-compatible source",
            ),
            ("not a relation(", DUCK_DEV, CTAS, "invalid source"),
            (DUCK_PROD, "m", CTAS, "invalid target"),
            (
                DUCK_PROD,
                "\"other\".\"dev\".\"m\"",
                CTAS,
                "DuckDB copies require one file and supported environments",
            ),
            (
                DUCK_PROD,
                "\"local\".\"prod\".\"n\"",
                CTAS,
                "DuckDB copies require one file and supported environments",
            ),
            (
                "\"local\".\"ci\".\"m\"",
                DUCK_DEV,
                CTAS,
                "DuckDB copies require one file and supported environments",
            ),
        ];
        for (source, target, kind, why) in refused {
            assert_eq!(
                ddl("duckdb", source, target, kind),
                Err(why),
                "{source} -> {target}"
            );
        }
    }

    #[test]
    fn duckdb_copy_ends_must_be_two_schemas_of_one_file() {
        let duck = Warehouse::DuckDb;
        assert!(duck.copy_relations_compatible(DUCK_PROD, DUCK_DEV));
        // Environment support is a DDL requirement, not a relation identity one.
        assert!(duck.copy_relations_compatible("\"local\".\"ci\".\"m\"", DUCK_DEV));
        assert!(!duck.copy_relations_compatible(DUCK_PROD, "\"other\".\"dev\".\"m\""));
        assert!(!duck.copy_relations_compatible(DUCK_PROD, "\"local\".\"prod\".\"n\""));
        assert!(!duck.copy_relations_compatible("not a relation(", DUCK_DEV));
        assert!(!duck.copy_relations_compatible(DUCK_PROD, "m"));

        for warehouse in [Warehouse::Snowflake, Warehouse::Redshift] {
            assert!(
                warehouse.copy_relations_compatible("a.b.c", "a.b.c"),
                "{warehouse:?}"
            );
        }
    }

    #[test]
    fn duckdb_copy_chains_stop_at_two_ancestors() {
        let duck = Warehouse::DuckDb;
        // (requested limit, effective limit)
        let cases = [
            (None, Some(2)),
            (Some(0), Some(0)),
            (Some(1), Some(1)),
            (Some(2), Some(2)),
            (Some(5), Some(2)),
            // A negative request wraps to an unreachable limit, as it always has.
            (Some(-1), Some(usize::MAX)),
        ];
        for (requested, limit) in cases {
            assert_eq!(
                duck.copy_chain_depth_limit(requested),
                limit,
                "{requested:?}"
            );
        }
        assert_eq!(duck.copy_provenance_depth(), Some(2));

        for warehouse in [Warehouse::Snowflake, Warehouse::BigQuery] {
            for requested in [None, Some(1), Some(5)] {
                assert_eq!(
                    warehouse.copy_chain_depth_limit(requested),
                    None,
                    "{warehouse:?}"
                );
            }
            assert_eq!(warehouse.copy_provenance_depth(), None, "{warehouse:?}");
        }
    }

    #[test]
    fn duckdb_copy_sources_need_a_recorded_epoch() {
        assert!(Warehouse::DuckDb.copy_source_evidence(Some(5)).is_ok());
        assert_eq!(
            Warehouse::DuckDb.copy_source_evidence(None),
            Err("DuckDB source requires recorded physical evidence")
        );
        for warehouse in [Warehouse::Snowflake, Warehouse::BigQuery] {
            assert!(
                warehouse.copy_source_evidence(None).is_ok(),
                "{warehouse:?}"
            );
        }
    }

    #[test]
    fn only_duckdb_in_any_spelling_skips_the_volatility_gate() {
        for dialect in ["duckdb", "DuckDB", "DUCKDB", " duckdb "] {
            assert!(
                Warehouse::from_dialect(dialect).copy_identity_skips_volatility_gate(),
                "{dialect:?}"
            );
        }
        for dialect in [
            "snowflake",
            "Snowflake",
            "bigquery",
            "unknown",
            "duckdb, x = y",
        ] {
            assert!(
                !Warehouse::from_dialect(dialect).copy_identity_skips_volatility_gate(),
                "{dialect:?}"
            );
        }
    }

    fn incremental_copy() -> crate::query_cache::CloneRequest {
        let mut req = crate::query_cache::CloneRequest {
            execution_type: crate::query_cache::ModelExecutionType::DeleteInsert as i32,
            clone_source_table_type: CTAS.map(Into::into),
            ..Default::default()
        };
        req.labels.insert(
            "duckdb_incremental_contract".into(),
            "delete-insert-fixed-v1".into(),
        );
        req.labels.insert(
            "duckdb_incremental_identity".into(),
            "duckdb-incremental-init-v2:abc".into(),
        );
        req
    }

    #[test]
    fn duckdb_client_requested_copies_must_declare_the_pinned_incremental_contract() {
        let duck = Warehouse::DuckDb;
        assert_eq!(
            duck.incremental_copy_contract(&incremental_copy()),
            IncrementalCopyContract::Declared {
                identity: "duckdb-incremental-init-v2:abc"
            }
        );

        type Mutation = fn(&mut crate::query_cache::CloneRequest);
        let cases: Vec<(&str, Mutation)> = vec![
            ("not incremental", |r| r.execution_type = 1),
            ("no contract", |r| {
                r.labels.remove("duckdb_incremental_contract");
            }),
            ("other contract", |r| {
                r.labels
                    .insert("duckdb_incremental_contract".into(), "merge".into());
            }),
            ("no identity", |r| {
                r.labels.remove("duckdb_incremental_identity");
            }),
            ("old identity version", |r| {
                r.labels.insert(
                    "duckdb_incremental_identity".into(),
                    "duckdb-incremental-init-v1:abc".into(),
                );
            }),
            ("not inspected as CTAS", |r| {
                r.clone_source_table_type = Some("BASE TABLE".into())
            }),
        ];
        for (why, mutate) in cases {
            let mut req = incremental_copy();
            mutate(&mut req);
            assert_eq!(
                duck.incremental_copy_contract(&req),
                IncrementalCopyContract::Missing,
                "{why}"
            );
        }

        for warehouse in [Warehouse::Snowflake, Warehouse::Redshift] {
            assert_eq!(
                warehouse.incremental_copy_contract(&incremental_copy()),
                IncrementalCopyContract::NotRequired,
                "{warehouse:?}"
            );
        }
    }

    #[test]
    fn the_incremental_contract_is_one_declared_value() {
        let declared = |value: &str| {
            std::collections::HashMap::from([(
                "duckdb_incremental_contract".to_owned(),
                value.to_owned(),
            )])
        };
        assert!(
            Warehouse::DuckDb.declares_incremental_contract(&declared("delete-insert-fixed-v1"))
        );
        assert!(!Warehouse::DuckDb.declares_incremental_contract(&declared("merge")));
        assert!(!Warehouse::DuckDb.declares_incremental_contract(&Default::default()));
        assert!(!Warehouse::Snowflake
            .declares_incremental_contract(&declared("delete-insert-fixed-v1")));
    }
}
