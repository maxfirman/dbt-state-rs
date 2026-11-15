//! The opt-in live survey's guard and physical comparison contract stays offline.
#[test]
fn databricks_survey_refuses_unsafe_or_incomplete_evidence() {
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../experiments/databricks_survey_test.py");
    let output = std::process::Command::new("python3")
        .arg(script)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
