//! Generated gRPC stubs for the dbt State ("query cache") protocol.
//!
//! The `.proto` files are the Apache-2.0 definitions published by dbt Labs
//! (package `com.fivetran.query_cache` plus `grpc.health.v1`).

pub mod query_cache {
    #![allow(clippy::all)]
    tonic::include_proto!("com.fivetran.query_cache");
}

pub mod grpc_health {
    #![allow(clippy::all)]
    tonic::include_proto!("grpc.health.v1");
}
