//! Persisted history stays valid only while these bytes are unchanged.
//! The expected values were produced by the implementation before the
//! Warehouse rules took over copy eligibility and version tags (#49).
use super::*;
use crate::query_cache as qc;

fn duckdb_copy() -> qc::SqlExecution {
    qc::SqlExecution {
        target_table: Some("\"db\".\"dev\".\"result\"".into()),
        dialect: "duckdb".into(),
        default_catalog: "db".into(),
        default_schema: Some("dev".into()),
        execution_type: 1,
        sql: "select id, amount + 1 as amount from db.dev.stg".into(),
        tables: vec![qc::TableModifiedInfo {
            name: "db.raw.input".into(),
            last_modified_epoch: Some(10),
        }],
        query_dependencies: vec![qc::QueryDependency {
            name: "\"db\".\"dev\".\"stg\"".into(),
            query: "select id, amount from db.raw.input".into(),
            default_catalog: "db".into(),
            default_schema: "dev".into(),
        }],
        semantic_extras: [("duckdb_copy_contract".into(), "rows-schema-v1".into())].into(),
        table_namespace: Some("ns".into()),
        ..Default::default()
    }
}

fn snowflake() -> qc::SqlExecution {
    qc::SqlExecution {
        target_table: Some("\"CDO\".\"DEV\".\"ORDERS\"".into()),
        dialect: "snowflake".into(),
        default_catalog: "CDO".into(),
        default_schema: Some("dev".into()),
        execution_type: 1,
        sql: "select order_id from CDO.dev.stg_orders".into(),
        query_dependencies: vec![qc::QueryDependency {
            name: "\"CDO\".\"DEV\".\"STG_ORDERS\"".into(),
            query: "select * from CDO.dev.raw_orders".into(),
            default_catalog: "CDO".into(),
            default_schema: "dev".into(),
        }],
        table_namespace: Some("ns".into()),
        ..Default::default()
    }
}

#[test]
fn fingerprint_values_are_unchanged() {
    let duck = duckdb_copy();
    let snow = snowflake();
    let bigquery = qc::SqlExecution {
        dialect: "bigquery".into(),
        ..snowflake()
    };
    let incremental = qc::SqlExecution {
        execution_type: qc::ModelExecutionType::DeleteInsert as i32,
        semantic_extras: [
            ("duckdb_copy_contract".into(), "rows-schema-v1".into()),
            (
                "duckdb_incremental_contract".into(),
                "delete-insert-fixed-v1".into(),
            ),
        ]
        .into(),
        ..duckdb_copy()
    };
    let values = [
        sql_logic_hash(&duck, None),
        sql_logic_hash(&duck, Some("body")),
        namespace_logic_hash(&duck),
        copy_logic_hash(&duck).unwrap(),
        copy_logic_hash(&incremental).unwrap(),
        sql_logic_hash(&snow, None),
        sql_logic_hash(&snow, Some("body")),
        namespace_logic_hash(&snow),
        copy_logic_hash(&snow).unwrap(),
        sql_logic_hash(&bigquery, None),
        namespace_logic_hash(&bigquery),
        seed_logic_hash("duckdb", "db", &duck.semantic_extras),
    ];
    assert_eq!(
        values,
        [
            // DuckDB strict, template, namespace, copy, incremental copy.
            "7bde339e739600bac9cafa38413233a4e4b23a81a770359de9e863dc23f4e56c",
            "e36181b1e5b75cc008807592d7847670e86a767ba88c30e6f8d1106cf6ae7a4e",
            "5faea4ddd8bdb097d7d920670ace91be89638d7ed467dc13c5c4e13e157eec9d",
            "77f2bf0a8d306e1c5ddde3d49e30f07e25e0976f31d2fd9af049c86867fca970",
            "90431d289fe50934e1edbf40ef8cdc5dd041a6bd1a1737559c884a26675df58e",
            // Snowflake strict, template, namespace, copy (the namespace logic).
            "d4f5ca8d45dcaa64ea35899e7afd5c47b05290450af6bb4f7911200c637d2fcc",
            "b1e089ed79d71846563ee7e1806fbd2e68ff208c0712000f42beac12792659d6",
            "ceffd1ab5732bb1f5d68c103e19e7912ce36ba1ae714541a37f1a0bc06ac927e",
            "ceffd1ab5732bb1f5d68c103e19e7912ce36ba1ae714541a37f1a0bc06ac927e",
            // BigQuery strict, exact namespace.
            "1158a6f0b167d472afa6027fb20f71073fb6c44642fd14ce815868f678b3bf84",
            "dae8e52a36ade6fc17d57ab28a5a4349bdcf0a06c12a21d6d645e544aa3e9378",
            // Seed.
            "dc8d7a80a4804272c0ba6917819f177b28f93010878892b713ba5a8886a3a758",
        ]
    );
    assert_eq!(copy_logic_hash(&bigquery), None);
}

/// The data-test history key (#200, `dbt-state-data-test-key-v1`).
#[test]
fn data_test_history_key_is_unchanged() {
    let test = qc::SqlExecution {
        dialect: "snowflake".into(),
        default_catalog: "CDO".into(),
        default_schema: Some("prod".into()),
        execution_type: qc::ModelExecutionType::DbtDataTest as i32,
        sql: "select id, v from CDO.prod.u where id < 0".into(),
        semantic_extras: [("severity".into(), "\"ERROR\"".into())].into(),
        ..Default::default()
    };
    assert_eq!(
        data_test_history_key(&test, &[]).as_deref(),
        Some("b1cc43e433e1f8a8eedd6b5dd0036c9b99254ef26b04b607913e11bcb240a83f")
    );
}
