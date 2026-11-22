//! The test-only TLS certificate (`crates/auth/testdata/tls-*.pem`): a
//! server certificate for `localhost`, `127.0.0.1` and `::1`, issued by a
//! test CA that clients trust.

use std::path::PathBuf;

fn testdata(file: &str) -> PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../auth/testdata")
        .join(file)
}

/// The CA clients trust, PEM.
pub fn ca_file() -> PathBuf {
    testdata("tls-ca.pem")
}

/// The listeners' certificate and key, loaded as the binaries load mounted
/// files.
pub fn identity() -> dbt_state_broker::TlsIdentity {
    dbt_state_broker::TlsIdentity::load(
        &testdata("tls-server.pem"),
        &testdata("tls-server-key.pem"),
    )
    .expect("test TLS identity")
}
