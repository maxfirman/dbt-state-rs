//! Credential-free probes of the #198 Volatile evaluation capture boundary.
//! The live capture (`experiments/databricks_volatile.py`) stays opt-in.
#[test]
fn volatile_capture_refuses_foreign_objects_and_incomplete_results() {
    let output = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_volatile_test.py"
        ))
        .output()
        .expect("run credential-free Volatile evaluation capture probes");
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
