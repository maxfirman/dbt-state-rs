//! Bounded ordinary-SQL Result copy, separate from RegisterClone provenance.
//! Only exact fully-qualified SELECT-star over one physical input is eligible.
use crate::execution::ExecutionEvidence;
use crate::query_cache as qc;
use crate::store::{ExecutionRow, InputTable, Producer};
use crate::warehouse::Warehouse;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Provenance {
    pub version: u8,
    pub source_execution_id: i64,
    pub source_epoch: i64,
    pub source_physical_key: String,
    /// Set atomically at Confirm, only while the recorded source stays current.
    pub published: bool,
}

fn physical(name: &str) -> String {
    Warehouse::physical_scope_key("databricks", name)
}

/// No schema abstraction or SQL normalization. The proven fully-qualified
/// input makes the output schema irrelevant; all other logic remains exact.
pub(crate) fn identity(req: &qc::SqlExecution) -> Option<String> {
    use sqlparser::tokenizer::{Token, Tokenizer};
    if Warehouse::from_dialect(&req.dialect) != Warehouse::Databricks
        || req.execution_type != qc::ModelExecutionType::Full as i32
        || !req.query_dependencies.is_empty()
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
    let tokens = Tokenizer::new(&sqlparser::dialect::DatabricksDialect {}, &req.sql)
        .tokenize()
        .ok()?;
    let tokens: Vec<_> = tokens
        .into_iter()
        .filter(|t| !matches!(t, Token::Whitespace(_)))
        .collect();
    let word = |i: usize, text: &str| matches!(&tokens[i], Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(text));
    let identifier = |i: usize| {
        matches!(&tokens[i], Token::Word(w)
        if matches!(w.quote_style, None | Some('`'))
        && w.value.bytes().next().is_some_and(|c| c.is_ascii_alphabetic() || c == b'_')
        && w.value.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_'))
    };
    if tokens.len() != 8
        || !word(0, "select")
        || tokens[1] != Token::Mul
        || !word(2, "from")
        || tokens[4] != Token::Period
        || tokens[6] != Token::Period
        || ![3, 5, 7].into_iter().all(identifier)
    {
        return None;
    }
    let input = physical(&format!("{}.{}.{}", tokens[3], tokens[5], tokens[7]));
    let target = req.target_table.as_deref()?;
    let target_key = physical(target);
    let target_parts: Vec<_> = target_key.split('.').collect();
    if target_parts.len() != 3
        || !target_parts.iter().all(|p| {
            p.bytes()
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == b'_')
                && p.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
        })
    {
        return None;
    }
    if input == physical(target) {
        return None;
    }
    let inputs: Vec<_> = req
        .tables
        .iter()
        .filter(|t| physical(&t.name) != physical(target))
        .collect();
    if inputs.len() != 1 || physical(&inputs[0].name) != input {
        return None;
    }
    let state = req.dbt_node_state.as_ref()?;
    let signature = |value: &Option<String>| {
        value
            .as_ref()
            .is_some_and(|s| s.len() == 32 && s.bytes().all(|c| c.is_ascii_hexdigit()))
    };
    if state.resource_type != "model"
        || state.node_unique_id.is_empty()
        || !signature(&state.node_body_hash)
        || !signature(&state.node_configs_hash)
    {
        return None;
    }
    let extras: std::collections::BTreeMap<_, _> = req.semantic_extras.iter().collect();
    let framed = serde_json::to_vec(&(
        "databricks-submit-copy-v1",
        &req.dialect,
        &req.sql,
        req.execution_type,
        &req.default_catalog,
        (target_parts[0], target_parts[2]),
        extras,
        &state.node_unique_id,
        &state.node_body_hash,
        &state.node_configs_hash,
        &state.node_macros_hash,
        &state.node_contract_hash,
        input,
    ))
    .ok()?;
    Some(hex::encode(Sha256::digest(framed)))
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

pub(crate) fn inputs(row: &ExecutionRow) -> Vec<InputTable> {
    row.input_tables
        .iter()
        .filter(|t| physical(&t.name) != physical(&row.target_table))
        .cloned()
        .collect()
}

/// This is exact freshness, not a copy-age anchor. Unknown epochs/producers,
/// a different input binding and every copy source are conservatively refused.
pub(crate) fn source_admits(evidence: &ExecutionEvidence, source: &ExecutionRow) -> bool {
    let current: Vec<_> = evidence
        .input_tables
        .iter()
        .filter(|t| physical(&t.name) != physical(&evidence.target_table))
        .collect();
    let recorded = inputs(source);
    source.org_id == evidence.org_id
        && source.table_namespace == evidence.table_namespace
        && Warehouse::from_dialect(&source.dialect).family_key(&source.dialect)
            == Warehouse::from_dialect(&evidence.dialect).family_key(&evidence.dialect)
        && source.execution_type == evidence.execution_type
        && source.status == "confirmed"
        && source.clone_source_table.is_none()
        && source.last_modified_epoch.is_some_and(|e| e > 0)
        && source.node_namespace_logic_hash == evidence.node_namespace_logic_hash
        && source.node_namespace_logic_hash.is_some()
        && source.node_unique_id == evidence.node_unique_id
        && physical(&source.target_table) != physical(&evidence.target_table)
        && current.len() == 1
        && recorded.len() == 1
        && physical(&current[0].name) == physical(&recorded[0].name)
        && current[0].last_modified_epoch.is_some_and(|e| e > 0)
        && current[0].last_modified_epoch == recorded[0].last_modified_epoch
        && current[0].producer == Producer::NoBuild
        && recorded[0].producer == Producer::NoBuild
}

/// An invalid publication cannot regain reuse via its strict SQL hash or the
/// generic namespace-adoption path. Legacy direct-copy rows use v1 separately.
pub(crate) fn reusable(evidence: &ExecutionEvidence, row: &ExecutionRow) -> bool {
    if Warehouse::from_dialect(&row.dialect) != Warehouse::Databricks
        || row.clone_source_table.is_none()
        || row.node_sql_hash.is_none()
    {
        return true;
    }
    row.submit_copy_provenance.as_ref().is_some_and(|p| {
        p.version == 1
            && p.published
            && p.source_epoch > 0
            && p.source_execution_id > 0
            && row
                .clone_source_table
                .as_ref()
                .is_some_and(|s| physical(s) == p.source_physical_key)
    }) && evidence.node_namespace_logic_hash.is_some()
        && evidence.node_namespace_logic_hash == row.node_namespace_logic_hash
        && evidence.node_unique_id == row.node_unique_id
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
        // Injected storage fault: a direct-style row carrying the valid FULL
        // auto hash. Native direct v1 remains MERGE/snapshot only.
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
