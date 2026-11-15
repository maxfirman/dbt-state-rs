//! Request policy for bounded ordinary-SQL Result copy. Identity analysis and
//! provenance/adoption belong to the shared workload and copy lifecycle modules.
use crate::execution::ExecutionEvidence;
use crate::query_cache as qc;
#[cfg(test)]
use crate::store::{ExecutionRow, Producer};
#[cfg(test)]
use crate::warehouse::Warehouse;

#[cfg(test)]
use crate::copy_lifecycle::{ordinary, reusable, source_admits, Provenance};
#[cfg(test)]
fn physical(name: &str) -> String {
    Warehouse::physical_scope_key("databricks", name)
}

pub(crate) fn identity(req: &qc::SqlExecution) -> Option<String> {
    identity_from_proof(req, crate::analyzed_workload::query::proof(req).as_ref())
}
pub(crate) fn identity_from_proof(
    req: &qc::SqlExecution,
    proof: Option<&crate::analyzed_workload::query::CopyProof>,
) -> Option<String> {
    if req.execution_type != 1
        || req.semantic_extras.len() != 2
        || req.semantic_extras.get("lookback").map(String::as_str) != Some("1")
        || req
            .semantic_extras
            .get("on_schema_change")
            .map(String::as_str)
            != Some("\"ignore\"")
    {
        return None;
    }
    proof.map(auto_identity)
}
pub(crate) fn auto_identity(p: &crate::analyzed_workload::query::CopyProof) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(format!(
        "databricks-submit-copy-v2:{}",
        p.identity
    )))
}

pub(crate) fn request_admits(
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
) -> bool {
    req.execution_type == qc::ModelExecutionType::Full as i32
        && !req.compare_unrendered_code
        && req.tolerate_nondeterminism
        && !req.ignore_external_modifications
        && req.allow_clones == Some(true)
        && req.clone_chain_depth_limit == Some(1)
        && req.clone_table_properties.is_none()
        && req.clone_time_travel_limit.is_none()
        && req.freshness_tolerance_seconds == 0
        && req.stale_upstream_policy == 0
        && req.lenient_dependencies.is_empty()
        && req
            .tables
            .iter()
            .all(|t| t.last_modified_epoch.is_some_and(|e| e > 0))
        && evidence.node_namespace_logic_hash.is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request() -> qc::SubmitEnrichedSqlRequest {
        qc::SubmitEnrichedSqlRequest {
            target_table: Some("`cdo`.`dev`.`out`".into()),
            dialect: "databricks".into(),
            default_catalog: "cdo".into(),
            default_schema: Some("dev".into()),
            execution_type: 1,
            sql: "select * from cdo.raw.input".into(),
            table_namespace: Some("warehouse".into()),
            tables: vec![qc::TableModifiedInfo {
                name: "`cdo`.`raw`.`input`".into(),
                last_modified_epoch: Some(100),
            }],
            semantic_extras: [
                ("lookback".into(), "1".into()),
                ("on_schema_change".into(), "\"ignore\"".into()),
            ]
            .into(),
            dbt_node_state: Some(qc::DbtNodeState {
                node_unique_id: "model.project.out".into(),
                resource_type: "model".into(),
                node_body_hash: Some("a".repeat(32)),
                node_configs_hash: Some("b".repeat(32)),
                ..Default::default()
            }),
            tolerate_nondeterminism: true,
            allow_clones: Some(true),
            clone_chain_depth_limit: Some(1),
            ..Default::default()
        }
    }
    fn evidence(req: &qc::SubmitEnrichedSqlRequest) -> ExecutionEvidence {
        let mut e = ExecutionEvidence::sql_submit("org", req);
        for t in &mut e.input_tables {
            t.producer = Producer::NoBuild;
        }
        e
    }
    fn source(e: &ExecutionEvidence) -> ExecutionRow {
        ExecutionRow {
            id: 4,
            org_id: e.org_id.clone(),
            target_table: "`cdo`.`prod`.`out`".into(),
            execution_type: 1,
            dialect: e.dialect.clone(),
            table_namespace: e.table_namespace.clone(),
            node_unique_id: e.node_unique_id.clone(),
            node_namespace_logic_hash: e.node_namespace_logic_hash.clone(),
            analyzed_copy_proof: e.analyzed_copy_proof.clone(),
            last_modified_epoch: Some(200),
            input_tables: e
                .input_tables
                .iter()
                .filter(|t| physical(&t.name) != physical(&e.target_table))
                .cloned()
                .collect(),
            status: "confirmed".into(),
            ..Default::default()
        }
    }
    #[test]
    fn identity_is_distinct_exact_and_bounded() {
        let req = request();
        let original =
            crate::fingerprint::copy_logic_hash(&crate::execution::sql_execution(&req)).unwrap();
        let mut other = req.clone();
        other.default_schema = Some("prod".into());
        other.target_table = Some("`cdo`.`prod`.`out`".into());
        assert_eq!(
            evidence(&other).node_namespace_logic_hash.as_deref(),
            Some(original.as_str())
        );
        for fault in [
            "sql",
            "uid",
            "body",
            "config",
            "kind",
            "target_alias",
            "target_catalog",
            "extras",
            "dependency",
            "self",
            "unqualified",
            "resource_type",
        ] {
            let mut q = req.clone();
            match fault {
                "sql" => q.sql = "select * from `cdo`.`raw`.`input`".into(),
                "uid" => q.dbt_node_state.as_mut().unwrap().node_unique_id.push('x'),
                "body" => q.dbt_node_state.as_mut().unwrap().node_body_hash = Some("c".repeat(32)),
                "config" => {
                    q.dbt_node_state.as_mut().unwrap().node_configs_hash = Some("c".repeat(32))
                }
                "kind" => q.execution_type = 3,
                "target_alias" => q.target_table = Some("`cdo`.`dev`.`other`".into()),
                "target_catalog" => q.target_table = Some("`other`.`dev`.`out`".into()),
                "extras" => {
                    q.semantic_extras
                        .insert("file_format".into(), "\"parquet\"".into());
                }
                "dependency" => q.query_dependencies.push(qc::QueryDependency::default()),
                "self" => q.target_table = Some("`cdo`.`raw`.`input`".into()),
                "unqualified" => q.sql = "select * from input".into(),
                "resource_type" => {
                    q.dbt_node_state.as_mut().unwrap().resource_type = "snapshot".into()
                }
                _ => unreachable!(),
            }
            assert_ne!(
                evidence(&q).node_namespace_logic_hash.as_deref(),
                Some(original.as_str()),
                "{fault}"
            );
        }
    }
    #[test]
    fn source_needs_exact_current_positive_input_and_identity() {
        let e = evidence(&request());
        let s = source(&e);
        assert!(source_admits(&e, &s));
        for fault in [
            "unknown_epoch",
            "zero_epoch",
            "drift",
            "producer",
            "unknown_producer",
            "org",
            "namespace",
            "family",
            "type",
            "identity",
            "uid",
            "pending",
            "clone",
            "source_epoch",
            "other_input",
            "own_only",
            "partial_origin",
        ] {
            let mut bad = s.clone();
            match fault {
                "unknown_epoch" => bad.input_tables[0].last_modified_epoch = None,
                "zero_epoch" => bad.input_tables[0].last_modified_epoch = Some(0),
                "drift" => bad.input_tables[0].last_modified_epoch = Some(99),
                "producer" => bad.input_tables[0].producer = Producer::Build("changed".into()),
                "unknown_producer" => bad.input_tables[0].producer = Producer::Unrecorded,
                "org" => bad.org_id.push('x'),
                "namespace" => bad.table_namespace = Some("other".into()),
                "family" => bad.dialect = "snowflake".into(),
                "type" => bad.execution_type = 7,
                "identity" => bad.node_namespace_logic_hash = Some("other".into()),
                "uid" => bad.node_unique_id = None,
                "pending" => bad.status = "pending".into(),
                "clone" => bad.clone_source_table = Some("old".into()),
                "source_epoch" => bad.last_modified_epoch = None,
                "other_input" => bad.input_tables[0].name = "cdo.other.input".into(),
                "own_only" => bad.input_tables[0].name = bad.target_table.clone(),
                "partial_origin" => {
                    bad.submit_copy_provenance = Some(Provenance {
                        version: 1,
                        source_execution_id: bad.id,
                        source_epoch: 200,
                        source_physical_key: physical(&bad.target_table),
                        published: false,
                    })
                }
                _ => unreachable!(),
            }
            assert!(!source_admits(&e, &bad), "{fault}");
        }
        let mut unknown = e.clone();
        unknown.input_tables[0].last_modified_epoch = None;
        assert!(!source_admits(&unknown, &s));
        unknown = e.clone();
        unknown.input_tables[0].producer = Producer::Unrecorded;
        assert!(!source_admits(&unknown, &s));
    }
    #[test]
    fn request_flags_never_widen_the_checked_scope() {
        let req = request();
        assert!(request_admits(&req, &evidence(&req)));
        for fault in [
            "template",
            "volatile",
            "ignore_external",
            "disabled",
            "chain",
            "lag",
            "policy",
            "property",
            "unknown_own",
        ] {
            let mut q = req.clone();
            match fault {
                "template" => q.compare_unrendered_code = true,
                "volatile" => q.tolerate_nondeterminism = false,
                "ignore_external" => q.ignore_external_modifications = true,
                "disabled" => q.allow_clones = Some(false),
                "chain" => q.clone_chain_depth_limit = Some(2),
                "lag" => q.freshness_tolerance_seconds = 1,
                "policy" => q.stale_upstream_policy = 1,
                "property" => q.clone_table_properties = Some(qc::TableProperties::default()),
                "unknown_own" => q.tables.push(qc::TableModifiedInfo {
                    name: q.target_table.clone().unwrap(),
                    last_modified_epoch: None,
                }),
                _ => unreachable!(),
            }
            assert!(!request_admits(&q, &evidence(&q)), "{fault}");
        }
    }
    #[test]
    fn invalid_direct_style_origin_cannot_adopt_a_valid_auto_namespace_hash() {
        let mut req = request();
        req.tables.push(qc::TableModifiedInfo {
            name: req.target_table.clone().unwrap(),
            last_modified_epoch: Some(250),
        });
        let e = evidence(&req);
        let mut row = source(&e);
        row.clone_source_table = Some(row.target_table.clone());
        row.target_table = e.target_table.clone();
        row.last_modified_epoch = Some(250);
        row.node_sql_hash = None;
        row.direct_copy_provenance = None;
        row.submit_copy_provenance = None;
        assert!(e.node_namespace_logic_hash.is_some());
        assert_eq!(row.node_namespace_logic_hash, e.node_namespace_logic_hash);
        // Deliberate storage fault; native RegisterClone v1 does not grant FULL.
        assert!(crate::reuse_candidate::for_model(&req, &e, Some(row))
            .unwrap()
            .candidate
            .is_none());
    }

    #[test]
    fn invalid_publication_blocks_strict_sql_and_namespace_adoption() {
        let mut req = request();
        req.tables.push(qc::TableModifiedInfo {
            name: req.target_table.clone().unwrap(),
            last_modified_epoch: Some(250),
        });
        let e = evidence(&req);
        let s = source(&e);
        let copy = e.submit_copied_from(&s);
        let mut row = source(&e);
        row.target_table = e.target_table.clone();
        row.last_modified_epoch = Some(250);
        row.node_sql_hash = e.node_sql_hash.clone();
        assert!(e.node_namespace_logic_hash.is_some());
        assert_eq!(row.node_namespace_logic_hash, e.node_namespace_logic_hash);
        assert!(e.node_sql_hash.is_some());
        assert_eq!(row.node_sql_hash, e.node_sql_hash);
        row.clone_source_table = copy.clone_source_table.clone();
        row.input_tables = copy.input_tables.clone();
        row.submit_copy_provenance = copy.submit_copy_provenance;
        assert!(!reusable(&e, &row));
        assert!(
            crate::reuse_candidate::for_model(&req, &e, Some(row.clone()))
                .unwrap()
                .candidate
                .is_none()
        );
        row.submit_copy_provenance.as_mut().unwrap().published = true;
        assert!(reusable(&e, &row));
        assert!(
            crate::reuse_candidate::for_model(&req, &e, Some(row.clone()))
                .unwrap()
                .candidate
                .is_some()
        );
        // Neither legacy nor unknown origin domains can adopt a current
        // namespace hash, even when strict SQL also matches.
        for version in [1, 255] {
            let mut legacy = row.clone();
            legacy.submit_copy_provenance.as_mut().unwrap().version = version;
            assert!(crate::reuse_candidate::for_model(&req, &e, Some(legacy))
                .unwrap()
                .candidate
                .is_none());
        }
        let mut missing_analysis = row.clone();
        missing_analysis.analyzed_copy_proof = None;
        assert!(
            crate::reuse_candidate::for_model(&req, &e, Some(missing_analysis))
                .unwrap()
                .candidate
                .is_none()
        );
        // Injected storage fault: a direct-style row carrying the valid FULL
        // auto hash. Native direct v1 remains MERGE/snapshot only.
        let mut partial_origin = row.clone();
        partial_origin.clone_source_table = None;
        assert!(!ordinary(&partial_origin));
        assert!(!reusable(&e, &partial_origin));
        assert!(
            crate::reuse_candidate::for_model(&req, &e, Some(partial_origin))
                .unwrap()
                .candidate
                .is_none()
        );
        let mut direct_style = row.clone();
        direct_style.node_sql_hash = None;
        direct_style.submit_copy_provenance = None;
        direct_style.direct_copy_provenance = None;
        assert!(
            crate::reuse_candidate::for_model(&req, &e, Some(direct_style))
                .unwrap()
                .candidate
                .is_none(),
            "a direct-style origin with a valid auto namespace hash cannot bypass absent v1 proof"
        );
        row.submit_copy_provenance = None;
        assert!(crate::reuse_candidate::for_model(&req, &e, Some(row))
            .unwrap()
            .candidate
            .is_none());
        assert_eq!(
            copy.input_tables.len(),
            1,
            "only genuine frozen source inputs"
        );
        assert_eq!(copy.input_tables[0].last_modified_epoch, Some(100));
    }
}
