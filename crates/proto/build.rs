use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // proto/ lives at the workspace root.
    let proto_root = PathBuf::from("../../proto");
    let qc = proto_root.join("query_cache_protobuf/query_cache");
    let svc = qc.join("services");

    let proto_files = [
        qc.join("shared.proto"),
        qc.join("struct.proto"),
        svc.join("clone_service.proto"),
        svc.join("sql_service.proto"),
        svc.join("execution_service.proto"),
        svc.join("client_validation_service.proto"),
        svc.join("client_telemetry_service.proto"),
        svc.join("explain_service.proto"),
        svc.join("health_service.proto"),
        svc.join("selector_service.proto"),
    ];

    println!("cargo:rerun-if-changed=build.rs");
    for f in &proto_files {
        println!("cargo:rerun-if-changed={}", f.display());
    }

    // Build BOTH client and server stubs: server stubs power our implementation,
    // client stubs power the recording proxy that forwards to the real service.
    // google.protobuf well-known types are mapped to ::prost_types automatically.
    //
    // All message types also derive serde Serialize/Deserialize so the recording
    // proxy can emit JSON golden files that are both human-diffable and replayable
    // back into our server during differential tests.
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute(".", "#[serde(rename_all = \"snake_case\")]")
        // SessionEndRequest.session_duration is a google.protobuf.Duration
        // (prost_types) that doesn't implement serde. It belongs to the
        // ClientTelemetry service, which we treat as a no-op and never serialize,
        // so skip it (defaulting on deserialize).
        .field_attribute(
            ".com.fivetran.query_cache.SessionEndRequest.session_duration",
            "#[serde(skip, default)]",
        )
        .compile_protos(&proto_files, &[proto_root])?;

    // The proto defines a service literally named `Clone`. tonic generates a
    // service trait `clone_server::Clone`, which shadows `std::clone::Clone`
    // inside that module and makes the std-derive impl
    // `impl<T> Clone for CloneServer<T>` resolve to the service trait, failing
    // to compile. Qualify that one std-derive impl to `std::clone::Clone`.
    // This preserves the on-wire service name (`com.fivetran.query_cache.Clone`).
    patch_clone_collision()?;

    Ok(())
}

fn patch_clone_collision() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::env::var("OUT_DIR")?;
    let generated = PathBuf::from(&out_dir).join("com.fivetran.query_cache.rs");
    let contents = std::fs::read_to_string(&generated)?;
    let patched = contents.replace(
        "impl<T> Clone for CloneServer<T> {",
        "impl<T> std::clone::Clone for CloneServer<T> {",
    );
    if patched != contents {
        std::fs::write(&generated, patched)?;
    }
    Ok(())
}
