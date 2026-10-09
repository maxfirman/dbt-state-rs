//! F4 — Golden-fixture epoch-unit guard (no DB; runs in CI).
//!
//! Every `last_modified_epoch` the real client sends — in SubmitEnrichedSQL
//! `tables[]`, in ConfirmExecution, and in RecordExecutions outcomes — is a
//! Unix epoch in MILLISECONDS (13 digits, ~1.79e12 as of 2026). The decision
//! engine relies on this: it bridges `freshness_tolerance_seconds` (seconds) to
//! the epochs with a single `* 1000`. If a future capture were taken against a
//! client that emitted SECONDS (10 digits) the entire freshness comparison
//! would silently invert, so this test fails loudly if any fixture epoch is not
//! in the millisecond range.
//!
//! The check is intentionally structural: it recursively scans every JSON value
//! in every fixture for the key `last_modified_epoch` (and the clone variant
//! `clone_source_last_modified_epoch` / `clone_required_last_modified_epoch`)
//! and asserts the magnitude. Zero and null are allowed (absent/unknown).

use std::path::PathBuf;

/// Lower bound for a millisecond epoch: 2001-09-09 (1_000_000_000_000). Any
/// positive epoch below this is almost certainly seconds, not milliseconds.
const MS_LOWER_BOUND: i64 = 1_000_000_000_000;
/// Upper bound: year ~5138 in ms. Guards against accidental microseconds.
const MS_UPPER_BOUND: i64 = 100_000_000_000_000;

const EPOCH_KEYS: &[&str] = &[
    "last_modified_epoch",
    "clone_source_last_modified_epoch",
    "clone_required_last_modified_epoch",
];

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../golden/fixtures")
}

/// Recursively collect all (json-path, value) pairs for epoch-bearing keys.
fn collect_epochs(value: &serde_json::Value, path: &str, out: &mut Vec<(String, i64)>) {
    match value {
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                let child_path = format!("{path}.{k}");
                if EPOCH_KEYS.contains(&k.as_str()) {
                    if let Some(n) = v.as_i64() {
                        out.push((child_path.clone(), n));
                    }
                }
                collect_epochs(v, &child_path, out);
            }
        }
        serde_json::Value::Array(arr) => {
            for (i, v) in arr.iter().enumerate() {
                collect_epochs(v, &format!("{path}[{i}]"), out);
            }
        }
        _ => {}
    }
}

#[test]
fn all_fixture_epochs_are_millisecond_scale() {
    let dir = fixtures_dir();
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read fixtures dir {}: {e}", dir.display()))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("jsonl"))
        .collect();
    files.sort();
    assert!(
        !files.is_empty(),
        "no .jsonl fixtures found in {}",
        dir.display()
    );

    let mut checked = 0usize;
    for path in &files {
        let text = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        for (lineno, line) in text.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let v: serde_json::Value = match serde_json::from_str(line) {
                Ok(v) => v,
                Err(_) => continue, // malformed lines are the loader's concern
            };
            let mut epochs = Vec::new();
            collect_epochs(&v, "", &mut epochs);
            for (jpath, epoch) in epochs {
                if epoch == 0 {
                    continue; // sentinel / absent
                }
                assert!(
                    (MS_LOWER_BOUND..MS_UPPER_BOUND).contains(&epoch),
                    "fixture {} line {} path {}: epoch {} is not millisecond-scale \
                     (expected 13-digit ms in [{}, {})). A seconds-based epoch would \
                     silently invert every freshness comparison.",
                    path.display(),
                    lineno + 1,
                    jpath,
                    epoch,
                    MS_LOWER_BOUND,
                    MS_UPPER_BOUND,
                );
                checked += 1;
            }
        }
    }
    assert!(
        checked > 0,
        "guard asserted nothing: no non-zero epochs found across {} fixtures",
        files.len()
    );
    eprintln!(
        "fixture epoch guard: validated {checked} millisecond epochs across {} files",
        files.len()
    );
}
