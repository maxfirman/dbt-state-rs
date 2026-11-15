//! Native UUIDs are minted after the handler samples its clock. Preserve the
//! actual audit clock independently of the immutable proxy capture.
use std::{collections::BTreeMap, path::Path};

use anyhow::{ensure, Context, Result};
use dbt_state_harness::diff::{self, GoldenEntry};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditClock {
    pub org_id: String,
    pub execution_decision_id: String,
    pub display_submitted_at_ms: i64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
pub enum Provenance {
    PersistedAudit {
        schema: String,
    },
    WalRecovery {
        wal_sha256: String,
        relation: String,
        decoder_sha256: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeClocks {
    pub capture_sha256: String,
    pub provenance: Provenance,
    pub audit_clocks: Vec<AuditClock>,
}

fn decision_id(entry: &GoldenEntry) -> Option<&str> {
    if !matches!(entry.method.as_str(), "SubmitEnrichedSQL" | "SubmitValues") {
        return None;
    }
    entry.response["response"].as_object()?.values().next()?["execution_decision_id"].as_str()
}

impl NativeClocks {
    pub fn load(capture: &Path, evidence: &Path) -> Result<Self> {
        let clocks: Self = serde_json::from_slice(&std::fs::read(evidence)?)?;
        let bytes = std::fs::read(capture)?;
        let entries = diff::load_golden(capture.to_str().context("capture path is not UTF-8")?)?;
        clocks.validate(&bytes, &entries)?;
        Ok(clocks)
    }

    pub fn validate(&self, bytes: &[u8], entries: &[GoldenEntry]) -> Result<()> {
        ensure!(
            self.capture_sha256 == format!("{:x}", Sha256::digest(bytes)),
            "native clock capture hash mismatch"
        );
        match &self.provenance {
            Provenance::PersistedAudit { schema } => {
                ensure!(!schema.is_empty(), "native audit schema missing")
            }
            Provenance::WalRecovery {
                wal_sha256,
                relation,
                decoder_sha256,
            } => {
                for digest in [wal_sha256, decoder_sha256] {
                    ensure!(
                        digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit()),
                        "invalid native audit provenance hash"
                    );
                }
                ensure!(
                    relation.split('/').count() == 3
                        && relation.split('/').all(
                            |part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit())
                        ),
                    "invalid native WAL relation"
                );
            }
        }
        let mut expected = BTreeMap::new();
        for entry in entries {
            if let Some(id) = decision_id(entry) {
                ensure!(
                    expected
                        .insert((super::captured_org(entry), id), entry)
                        .is_none(),
                    "duplicate native submit decision ID"
                );
            }
        }
        ensure!(
            !expected.is_empty(),
            "native capture contains no submit decisions"
        );
        ensure!(
            self.audit_clocks.len() == expected.len(),
            "native clock evidence must cover every submit decision exactly"
        );
        for row in &self.audit_clocks {
            let entry = expected
                .remove(&(row.org_id.as_str(), row.execution_decision_id.as_str()))
                .context("unknown, foreign-org or duplicate native audit clock")?;
            let sent: i64 = entry
                .metadata
                .iter()
                .find(|(name, _)| name == "x-submitted-at-epoch")
                .context("missing native submit instant")?
                .1
                .parse()?;
            let recorded = entry
                .recorded_at_ms()
                .context("missing native response instant")?;
            ensure!(
                (sent..=recorded).contains(&row.display_submitted_at_ms),
                "native audit clock outside captured request interval"
            );
        }
        ensure!(expected.is_empty(), "native clock evidence incomplete");
        Ok(())
    }

    pub fn submit_clock(&self, entry: &GoldenEntry) -> i64 {
        let id = decision_id(entry).expect("ordinary native submit decision");
        self.audit_clocks
            .iter()
            .find(|row| row.org_id == super::captured_org(entry) && row.execution_decision_id == id)
            .expect("validated native submit clock")
            .display_submitted_at_ms
    }
}

/// Call after the native proxy is drained and before TestServer is dropped.
/// Reads exact org-scoped audit evidence; never sets the live request clock.
pub async fn retain(server: &super::TestServer, capture: &Path, evidence: &Path) -> Result<()> {
    use dbt_state_proto::query_cache::{self as qc, explain_client::ExplainClient};
    let bytes = std::fs::read(capture)?;
    let entries = diff::load_golden(capture.to_str().context("capture path is not UTF-8")?)?;
    let mut ids_by_org: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for entry in &entries {
        if let Some(id) = decision_id(entry) {
            ids_by_org
                .entry(super::captured_org(entry))
                .or_default()
                .push(id.to_owned());
        }
    }
    let mut audit_clocks = Vec::new();
    let mut explain = ExplainClient::new(super::channel(server.addr).await);
    for (org, ids) in ids_by_org {
        // Public Explain waits for the exact audits before the evidence read.
        explain
            .get_explain_messages(super::request_with_org(
                qc::GetExplainMessagesRequest {
                    execution_decision_ids: ids.clone(),
                },
                org,
            ))
            .await?;
        for row in server
            .state
            .store
            .fetch_explain_decisions(org, &ids)
            .await?
        {
            audit_clocks.push(AuditClock {
                org_id: org.to_owned(),
                execution_decision_id: row
                    .execution_decision_id
                    .context("audit missing decision ID")?,
                display_submitted_at_ms: row
                    .display_submitted_at_ms
                    .context("audit missing sampled submit clock")?,
            });
        }
    }
    let clocks = NativeClocks {
        capture_sha256: format!("{:x}", Sha256::digest(&bytes)),
        provenance: Provenance::PersistedAudit {
            schema: server.schema.clone(),
        },
        audit_clocks,
    };
    clocks.validate(&bytes, &entries)?;
    std::fs::write(evidence, serde_json::to_vec_pretty(&clocks)?)?;
    Ok(())
}
