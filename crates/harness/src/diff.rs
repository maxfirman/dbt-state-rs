//! Differential test support: spin up our server against an isolated Postgres
//! schema, replay golden requests, and compare responses to the recorded ones.

use std::collections::HashMap;

use serde::Deserialize;

/// A recorded golden entry (as written by the record-proxy).
#[derive(Debug, Clone, Deserialize)]
pub struct GoldenEntry {
    pub service: String,
    pub method: String,
    #[serde(default)]
    pub metadata: Vec<(String, String)>,
    pub request: serde_json::Value,
    pub response: serde_json::Value,
}

/// Load and parse a golden `.jsonl` file.
pub fn load_golden(path: &str) -> std::io::Result<Vec<GoldenEntry>> {
    let text = std::fs::read_to_string(path)?;
    parse_golden(&text)
}

fn parse_golden(text: &str) -> std::io::Result<Vec<GoldenEntry>> {
    let mut out = Vec::new();
    for (index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<GoldenEntry>(line) {
            Ok(e) => out.push(e),
            Err(e) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("golden line {}: {e}", index + 1),
                ))
            }
        }
    }
    Ok(out)
}

/// The decision variant name of a SubmitSQLResponse golden `response` value,
/// e.g. "skip_execution" | "ready_to_execute" | "ready_to_clone".
pub fn decision_variant(response: &serde_json::Value) -> Option<String> {
    response
        .get("response")?
        .as_object()?
        .keys()
        .next()
        .cloned()
}

/// Extract the integer `decision` from an ExplainedDecision inside a response.
pub fn explained_decision_code(response: &serde_json::Value) -> Option<i64> {
    let r = response.get("response")?.as_object()?;
    let (_variant, body) = r.iter().next()?;
    body.get("explained_decision")?.get("decision")?.as_i64()
}

/// Group golden entries by node unique id (from request labels) for scenario
/// reconstruction.
pub fn by_node(entries: &[GoldenEntry]) -> HashMap<String, Vec<&GoldenEntry>> {
    let mut m: HashMap<String, Vec<&GoldenEntry>> = HashMap::new();
    for e in entries {
        if let Some(uid) = e
            .request
            .get("labels")
            .and_then(|l| l.get("dbt_node_unique_id"))
            .and_then(|v| v.as_str())
        {
            m.entry(uid.to_string()).or_default().push(e);
        }
    }
    m
}

#[cfg(test)]
mod tests {
    #[test]
    fn malformed_trace_is_an_error_not_partial_success() {
        let valid = r#"{"service":"SQL","method":"SubmitEnrichedSQL","request":{},"response":{}}"#;
        let error = super::parse_golden(&format!("{valid}\nnot json\n{valid}")).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("line 2"));
    }
}
