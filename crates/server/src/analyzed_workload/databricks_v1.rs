//! Frozen v1 adapter: these exact token rules remain until separately versioned.
use crate::copy_lifecycle::DirectCopyProvenance;
use crate::query_cache as qc;
use crate::warehouse::Warehouse;
/// Context-independent identity only for the observed single fully-qualified
/// SELECT-star input. No general SQL rewriting or cross-target candidate search.
pub(crate) fn direct_copy_provenance(req: &qc::SqlExecution) -> Option<DirectCopyProvenance> {
    use sha2::{Digest, Sha256};
    use sqlparser::tokenizer::{Token, Tokenizer};
    if Warehouse::from_dialect(&req.dialect) != Warehouse::Databricks
        || !matches!(req.execution_type, 3 | 7)
        || !req.query_dependencies.is_empty()
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
    let direct_name = tokens.len() == 8
        && ([3, 5, 7]
            .iter()
            .all(|i| matches!(&tokens[*i], Token::Word(w) if w.quote_style == Some('`')))
            || [3, 5, 7].iter().all(|i| {
                matches!(&tokens[*i], Token::Word(w)
            if w.quote_style.is_none()
                && w.value.bytes().next().is_some_and(|c| c.is_ascii_alphabetic() || c == b'_')
                && w.value.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_'))
            }));
    if tokens.len() != 8
        || !matches!(&tokens[0], Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case("select"))
        || tokens[1] != Token::Mul
        || !matches!(&tokens[2], Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case("from"))
        || tokens[4] != Token::Period
        || tokens[6] != Token::Period
        || !direct_name
    {
        return None;
    }
    let input = format!("{}.{}.{}", tokens[3], tokens[5], tokens[7]);
    let physical = |name: &str| Warehouse::physical_scope_key(&req.dialect, name);
    let own = req.target_table.as_deref().map(physical);
    let inputs: Vec<_> = req
        .tables
        .iter()
        .filter(|t| Some(physical(&t.name)) != own)
        .collect();
    if inputs.len() != 1 || physical(&inputs[0].name) != physical(&input) {
        return None;
    }
    let state = req.dbt_node_state.as_ref()?;
    if state.node_unique_id.is_empty() {
        return None;
    }
    let mut neutral = req.clone();
    neutral.default_catalog.clear();
    neutral.default_schema = None;
    neutral.target_table = None;
    let identity = crate::fingerprint::sql_logic_hash(&neutral, None);
    let framed = serde_json::to_vec(&(
        "databricks-direct-copy-v1",
        identity,
        &state.node_unique_id,
        &state.node_body_hash,
        &state.node_configs_hash,
        &state.node_macros_hash,
        &state.node_contract_hash,
        physical(&input),
    ))
    .ok()?;
    Some(DirectCopyProvenance {
        version: 1,
        identity: hex::encode(Sha256::digest(framed)),
        default_catalog: req.default_catalog.clone(),
        default_schema: req.default_schema.clone(),
        source_execution_id: None,
        source_epoch: None,
        source_physical_key: None,
    })
}
