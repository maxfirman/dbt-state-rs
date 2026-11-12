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

mod duration_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize, Deserialize)]
    struct Duration {
        seconds: i64,
        nanos: i32,
    }

    pub fn serialize<S: Serializer>(
        value: &Option<prost_types::Duration>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value
            .as_ref()
            .map(|value| Duration {
                seconds: value.seconds,
                nanos: value.nanos,
            })
            .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<prost_types::Duration>, D::Error> {
        Ok(
            Option::<Duration>::deserialize(deserializer)?.map(|value| prost_types::Duration {
                seconds: value.seconds,
                nanos: value.nanos,
            }),
        )
    }
}
