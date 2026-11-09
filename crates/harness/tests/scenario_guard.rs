//! Guard for the harness scenario module (no DB; runs in CI).
//!
//! Migrated suites learn the protocol in one place: they build submissions,
//! confirms, records and copy requests through `support::scenario`, read rows
//! back through `support::rows`, and leave raw State writes to the named helpers
//! in `support/legacy.rs` and `support/fault.rs`. This test fails when a
//! migrated file grows its own request builder, ready-id extractor or SQL, or
//! when raw executions/node-states writes or fault DDL appear outside the named
//! helpers. Add a file to `MIGRATED` when it moves to the scenario module.

use std::path::{Path, PathBuf};

/// Test files that use the scenario module. Tickets #52 and #53 extend this.
const MIGRATED: &[&str] = &[
    "auth.rs",
    "clone_submit_fallback.rs",
    "concurrency.rs",
    "correctness.rs",
    "hardening.rs",
    "lag_freshness.rs",
    "lag_policy.rs",
    "offline_regressions.rs",
    "robustness.rs",
    "selector_conformance.rs",
    "selector_offline.rs",
    "sql_normalization.rs",
    "warehouse_portability.rs",
];

/// The only support files allowed to write State with raw SQL.
const RAW_WRITERS: &[&str] = &["support/legacy.rs", "support/fault.rs"];

/// Hand-built protocol requests: the scenario builders own these.
const REQUEST_LITERALS: &[&str] = &[
    "SubmitEnrichedSqlRequest {",
    "SubmitValuesRequest {",
    "ConfirmExecutionRequest {",
    "ExecutionRecord {",
    "ExecutionOutcome {",
    "SqlExecution {",
    "ValuesExecution {",
    "CloneRequest {",
];

/// Per-file builders return one of these request types (or a scenario builder
/// pre-filled with per-file identity defaults).
const BUILDER_RETURNS: &[&str] = &[
    "-> qc::SubmitEnrichedSqlRequest",
    "-> qc::SubmitValuesRequest",
    "-> qc::ConfirmExecutionRequest",
    "-> qc::ExecutionRecord",
    "-> qc::CloneRequest",
    "-> Submit",
    "-> Seed",
    "-> CopyRequest",
    "-> Outcome",
    "-> support::scenario::",
];

/// Destructuring a decision by hand is what the ready-id extractors did; the
/// scenario `expect_*` helpers own it.
const DECISION_PATTERNS: &[&str] = &[
    "Response::ReadyToExecute(",
    "Response::SkipExecution(",
    "Response::ReadyToClone(",
];

/// Raw State writes and fault-injection DDL (matched case-insensitively).
const RAW_WRITES: &[&str] = &[
    "insert into executions",
    "insert into node_states",
    "insert into node_decisions",
    "update executions",
    "update node_states",
    "delete from executions",
    "delete from node_states",
    "alter table",
    "drop table",
];

fn tests_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests")
}

/// The code lines of `path` (comments skipped), numbered from 1.
fn code_lines(path: &Path) -> Vec<(usize, String)> {
    let text =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    text.lines()
        .enumerate()
        .filter(|(_, line)| !line.trim_start().starts_with("//"))
        .map(|(i, line)| (i + 1, line.to_string()))
        .collect()
}

fn offences(path: &Path, patterns: &[&str], case_insensitive: bool) -> Vec<String> {
    code_lines(path)
        .into_iter()
        .filter_map(|(n, line)| {
            let haystack = if case_insensitive {
                line.to_ascii_lowercase()
            } else {
                line.clone()
            };
            patterns
                .iter()
                .find(|p| haystack.contains(*p))
                .map(|p| format!("{}:{n}: `{p}` in `{}`", path.display(), line.trim()))
        })
        .collect()
}

#[test]
fn migrated_files_build_requests_through_the_scenario_module() {
    let mut found = Vec::new();
    for file in MIGRATED {
        let path = tests_dir().join(file);
        assert!(path.exists(), "MIGRATED lists a missing file: {file}");
        found.extend(offences(&path, REQUEST_LITERALS, false));
        found.extend(offences(&path, BUILDER_RETURNS, false));
        found.extend(offences(&path, DECISION_PATTERNS, false));
        found.extend(offences(&path, &["sqlx::"], false));
    }
    assert!(
        found.is_empty(),
        "migrated files must use support::scenario and support::rows:\n{}",
        found.join("\n")
    );
}

#[test]
fn raw_state_writes_live_only_in_the_named_helpers() {
    let mut files: Vec<PathBuf> = MIGRATED.iter().map(|f| tests_dir().join(f)).collect();
    files.push(tests_dir().join("support.rs"));
    for entry in std::fs::read_dir(tests_dir().join("support")).expect("support dir") {
        let path = entry.expect("support entry").path();
        let relative = path.strip_prefix(tests_dir()).unwrap().to_string_lossy();
        if !RAW_WRITERS.iter().any(|w| relative == *w) {
            files.push(path);
        }
    }
    let found: Vec<String> = files
        .iter()
        .flat_map(|path| offences(path, RAW_WRITES, true))
        .collect();
    assert!(
        found.is_empty(),
        "raw executions/node-states writes and fault DDL belong in {RAW_WRITERS:?}:\n{}",
        found.join("\n")
    );
    for writer in RAW_WRITERS {
        assert!(
            tests_dir().join(writer).exists(),
            "named raw-state helper module {writer} is missing"
        );
    }
}
