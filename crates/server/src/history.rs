//! Prepare trustworthy State history for same-target reuse across warehouses.
//!
//! Own identity preparation, lookup ordering, clone adoption and evidence checks.
//! The caller retains selector writes, the pure decision, copy selection and
//! outcome recording. Keep these reads sequential and outside a transaction:
//! changing their snapshot or error ordering would change submit behavior.

use crate::decision;
use crate::query_cache as qc;
use crate::store::{ExecutionRow, InputTable, Store};

const EXECUTION_TYPE_DBT_DATA_TEST: i32 = 8;

pub(crate) struct SqlHistory {
    pub confirmed: Option<ExecutionRow>,
    pub target_has_prior_build: bool,
    pub node_body_hash: Option<String>,
    pub node_sql_hash: Option<String>,
    pub node_template_hash: Option<String>,
    pub ns_logic_hash: Option<String>,
    pub input_tables: Vec<InputTable>,
}

pub(crate) struct ValuesHistory {
    pub confirmed: Option<ExecutionRow>,
    pub target_has_prior_build: bool,
    pub seed_hash: String,
    pub values_hash: Option<String>,
    pub input_tables: Vec<InputTable>,
}

/// Prepare model or data-test history. Select current scoped physical state
/// before matching identity; data tests instead use their scoped UID/results.
pub(crate) async fn prepare_sql(
    store: &Store,
    org_id: &str,
    req: &qc::SubmitEnrichedSqlRequest,
) -> sqlx::Result<SqlHistory> {
    let target_table = req.target_table.clone().unwrap_or_default();
    let execution_type = req.execution_type;
    let node_body_hash = decision::node_body_hash_of(req);
    // `node_sql_hash` ALWAYS carries the RENDERED logic identity. In
    // `compare_unrendered_code` mode we ADDITIONALLY compute the unrendered
    // TEMPLATE identity (`node_template_hash`): the hosted service rebuilds a
    // cuc node ONLY when BOTH the rendered SQL and the template change, i.e.
    // it reuses when EITHER is unchanged (documented + verified live — see
    // crates/harness/tests/template_comparison.rs). Both hashes fold in the
    // same dependency/context/extras components, so a genuine dependency or
    // context change invalidates BOTH and rebuilds. When cuc is off,
    // node_template_hash is None and only the rendered hash is matched.
    let node_sql_hash = Some(crate::fingerprint::sql_logic_hash(
        &sql_execution(req),
        None,
    ));
    let node_template_hash = req.compare_unrendered_code.then(|| {
        crate::fingerprint::sql_logic_hash(
            &sql_execution(req),
            Some(node_body_hash.as_deref().unwrap_or("")),
        )
    });
    // Schema-abstracted cross-environment logic identity (clone adoption).
    // Only meaningful for rendered (non-template) comparison of real SQL.
    let ns_logic_hash = (!req.compare_unrendered_code && !req.sql.trim().is_empty())
        .then(|| crate::fingerprint::copy_logic_hash(&sql_execution(req)))
        .flatten();
    let input_tables = decision::input_tables_of(req);
    let unique_id = node_unique_id(req.dbt_node_state.as_ref(), &req.labels);
    let project_id = req
        .dbt_node_state
        .as_ref()
        .and_then(|s| s.project_id.clone());
    let table_namespace = namespace(req.table_namespace.as_deref());

    let candidate = if execution_type == EXECUTION_TYPE_DBT_DATA_TEST {
        match unique_id.as_deref() {
            Some(uid) => {
                store
                    .find_confirmed_test(
                        org_id,
                        uid,
                        project_id.as_deref(),
                        table_namespace,
                        node_sql_hash.as_deref(),
                        node_template_hash.as_deref(),
                        &req.dialect,
                    )
                    .await?
            }
            None => None,
        }
    } else {
        let strict = store
            .find_confirmed(
                org_id,
                &target_table,
                execution_type,
                node_sql_hash.as_deref(),
                table_namespace,
                &req.dialect,
                node_template_hash.as_deref(),
                ns_logic_hash.as_deref(),
            )
            .await?;
        // CROSS-ENVIRONMENT CLONE ADOPTION. When the strict (exact rendered
        // SQL) match misses, the physical target may still be reusable if it
        // was CLONED from (or is otherwise) the SAME model in the namespace:
        // its schema-abstracted logic identity equals this submit's. This
        // reproduces the hosted post-clone SKIP, which is logic-sensitive
        // (a genuine SQL change still executes). Verified live; see
        // experiments/clone_logic_sensitivity.sh. Freshness still governs
        // via decide(); we only supply the candidate here.
        match strict {
            Some(row) => Some(row),
            None => {
                // The CURRENT physical state of this target (latest confirmed
                // row, any provenance). Selecting the latest first guarantees
                // a stale clone-origin row cannot be resurrected for adoption
                // after an ordinary rebuild (review issue #1 P0#2): the row
                // is adopted (→ possible SKIP) only if it is ITSELF the
                // current state AND clone-origin AND logic-compatible.
                let latest_for_target = store
                    .find_latest_confirmed_target(
                        org_id,
                        &target_table,
                        execution_type,
                        table_namespace,
                        &req.dialect,
                    )
                    .await?;
                match ns_logic_hash.as_deref() {
                    Some(ns_hash) => latest_for_target
                        .filter(|prev| {
                            // Only CLONE-ORIGIN rows are adopted cross-environment.
                            // A clone establishes a physical object with the
                            // source's logic; a later submit skips when its
                            // schema-abstracted logic matches. Ordinary (non-clone)
                            // rows are NOT adopted here: that would let a genuine
                            // resolution-context change (e.g. default_schema) skip,
                            // which the hosted service rebuilds. Broader
                            // cross-environment reuse without a clone is a separate
                            // provenance feature (see correctness-handoff.md §A).
                            prev.clone_source_table.is_some()
                                && prev.node_namespace_logic_hash.as_deref() == Some(ns_hash)
                        })
                        .map(|mut prev| {
                            // A clone-origin row records NO upstream inputs (the
                            // clone had none). Adopt the submit's CURRENT inputs
                            // as the recorded set so the freshness check compares
                            // each upstream against the clone's own build epoch
                            // (prev.last_modified_epoch) rather than treating the
                            // empty recorded set as a dependency-set change. The
                            // own-build anchor stays the authoritative clone epoch.
                            if prev.input_tables.is_empty() {
                                prev.input_tables = input_tables.clone();
                            }
                            prev
                        }),
                    None => None,
                }
            }
        }
    };
    // Runtime volatile-function evidence is not implemented. Build instead
    // of claiming reuse when evaluation is requested. Empty template/SQL is
    // likewise insufficient.
    let confirmed = candidate.filter(|prev| {
        req.tolerate_nondeterminism
            && complete_dependencies(req)
            && !req.sql.trim().is_empty()
            && (!req.compare_unrendered_code
                || node_body_hash.as_ref().is_some_and(|s| !s.is_empty()))
            && if execution_type == EXECUTION_TYPE_DBT_DATA_TEST {
                reusable_test_results(prev.execution_results.as_ref())
            } else {
                current_target_matches(req, prev)
            }
    });

    // PHYSICAL-EXISTENCE signal (fingerprint-agnostic): did this target ever
    // have a confirmed build? When no fingerprint matched (confirmed is
    // None) but the target existed, this is a REBUILD and hosted reports the
    // rebuild rationale (not "did not exist"). Review #2 5th follow-up P1 #4.
    let target_has_prior_build = store
        .target_has_confirmed_history(org_id, &target_table, table_namespace, &req.dialect)
        .await?;
    Ok(SqlHistory {
        confirmed,
        target_has_prior_build,
        node_body_hash,
        node_sql_hash,
        node_template_hash,
        ns_logic_hash,
        input_tables,
    })
}

/// Seeds compare values/config identity and current target evidence separately
/// from the SQL/model path; their inputs remain empty for the pure decision.
pub(crate) async fn prepare_values(
    store: &Store,
    org_id: &str,
    req: &qc::SubmitValuesRequest,
) -> sqlx::Result<ValuesHistory> {
    let target_table = req.target_table.clone();
    let values_hash = if req.values_hash.is_empty() {
        None
    } else {
        Some(req.values_hash.clone())
    };

    let seed_hash = crate::fingerprint::seed_logic_hash(
        &req.dialect,
        &req.default_catalog,
        &req.semantic_extras,
    );
    let confirmed = store.find_confirmed_values(
        org_id, &target_table, &req.values_hash, &seed_hash, namespace(req.table_namespace.as_deref()), &req.dialect,
    ).await?.filter(|prev| {
        !req.values_hash.is_empty() && req.last_modified_epoch.is_some()
            && matches!((req.last_modified_epoch, prev.last_modified_epoch), (Some(current), Some(recorded)) if current <= recorded)
    });
    let input_tables: Vec<InputTable> = Vec::new();
    let target_has_prior_build = store
        .target_has_confirmed_history(
            org_id,
            &target_table,
            namespace(req.table_namespace.as_deref()),
            &req.dialect,
        )
        .await?;
    Ok(ValuesHistory {
        confirmed,
        target_has_prior_build,
        seed_hash,
        values_hash,
        input_tables,
    })
}

fn reusable_test_results(results: Option<&qc::Struct>) -> bool {
    use qc::value::Kind;
    let Some(results) = results else {
        return false;
    };
    let failures = matches!(
        results.fields.get("failures").and_then(|v| v.kind.as_ref()),
        Some(Kind::IntValue(_))
    ) || matches!(results.fields.get("failures").and_then(|v| v.kind.as_ref()),
        Some(Kind::DoubleValue(n)) if n.is_finite());
    failures
        && ["should_error", "should_warn"].iter().all(|key| {
            matches!(
                results.fields.get(*key).and_then(|v| v.kind.as_ref()),
                Some(Kind::BoolValue(_))
            )
        })
}

/// Older clients publish identity only in labels.
pub(crate) fn node_unique_id(
    state: Option<&qc::DbtNodeState>,
    labels: &std::collections::HashMap<String, String>,
) -> Option<String> {
    state
        .map(|s| s.node_unique_id.as_str())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            labels
                .get("dbt_node_unique_id")
                .map(String::as_str)
                .filter(|s| !s.is_empty())
        })
        .map(str::to_owned)
}

pub(crate) fn namespace(value: Option<&str>) -> Option<&str> {
    value.filter(|s| !s.is_empty())
}

fn complete_dependencies(req: &qc::SubmitEnrichedSqlRequest) -> bool {
    let mut names = std::collections::HashSet::new();
    req.query_dependencies
        .iter()
        .all(|d| !d.name.trim().is_empty() && !d.query.trim().is_empty() && names.insert(&d.name))
}

fn current_target_matches(
    req: &qc::SubmitEnrichedSqlRequest,
    prev: &crate::store::ExecutionRow,
) -> bool {
    let Some(target) = req.target_table.as_deref().filter(|s| !s.is_empty()) else {
        return false;
    };
    if prev.target_table != target {
        return false;
    }
    let own: Vec<_> = req.tables.iter().filter(|t| t.name == target).collect();
    // Missing/ambiguous target evidence cannot establish that an object exists.
    own.len() == 1
        && own[0].last_modified_epoch.is_some()
        && (req.ignore_external_modifications
            || matches!((own[0].last_modified_epoch, prev.last_modified_epoch), (Some(current), Some(recorded)) if current <= recorded))
}

/// Convert a `SubmitEnrichedSqlRequest` into the `SqlExecution` the fingerprint
/// functions consume. Exposed so an upgrade regression can compute a frozen
/// prior-version fingerprint for the exact request (review #2 9th follow-up P2).
pub fn sql_execution(req: &qc::SubmitEnrichedSqlRequest) -> qc::SqlExecution {
    qc::SqlExecution {
        target_table: req.target_table.clone(),
        dialect: req.dialect.clone(),
        default_catalog: req.default_catalog.clone(),
        default_schema: req.default_schema.clone(),
        execution_type: req.execution_type,
        sql: req.sql.clone(),
        tables: req.tables.clone(),
        query_dependencies: req.query_dependencies.clone(),
        semantic_extras: req.semantic_extras.clone(),
        labels: req.labels.clone(),
        dbt_node_state: req.dbt_node_state.clone(),
        table_namespace: req.table_namespace.clone(),
        from_speculative_submit: false,
    }
}
