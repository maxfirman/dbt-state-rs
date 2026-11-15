//! Scenario helpers: request builders that fill in the reuse identity, and a
//! protocol client that submits, confirms and expects decisions.
//!
//! Builders start from a submission that is complete enough to be reused once
//! built and confirmed, then each setter changes one input. The reuse identity
//! follows the inputs that dbt State fingerprints: rendered SQL, dialect,
//! default catalog/schema, execution type, allowlisted `semantic_extras`, query
//! dependencies, the physical target and (for data tests) the scoped unique id.
//! `body(..)` changes the node body hash AND the rendered SQL (unless `sql(..)`
//! pinned it), because the hosted service ignores `node_body_hash` for reuse:
//! a changed body must render to changed SQL to drive a rebuild.
//!
//! State history is seeded through the protocol: [`Scenario::build`] (submit,
//! expect a build, confirm) or [`Scenario::record`] with [`Submit::record`] /
//! [`Seed::record`]. Raw SQL writes live only in `legacy` and `fault`.

use std::collections::{BTreeSet, HashMap};

use dbt_state_proto::query_cache as qc;
use dbt_state_server::store::Store;
use qc::submit_sql_response::Response;
use tonic::transport::Channel;
use tonic::Status;

pub use qc::ModelExecutionType as ExecutionType;
pub use qc::StaleUpstreamPolicy as Policy;

use super::TestServer;

/// The current epoch [`model`] reports for its own target before this
/// scenario has built it. Once a [`Scenario`] has confirmed a build of the
/// target, a submit reporting `OWN_EPOCH` reports that build's epoch instead,
/// as a warehouse would: the table was last modified by its build.
pub const OWN_EPOCH: i64 = 1;
/// The body hash a builder starts from.
pub const DEFAULT_BODY: &str = "body";
/// DuckDB's copy contract (`semantic_extras` key and value) that makes a table
/// model result-copy eligible.
pub const DUCKDB_COPY_CONTRACT: (&str, &str) = ("duckdb_copy_contract", "rows-schema-v1");
/// DuckDB's incremental contract (`semantic_extras` key and value).
pub const DUCKDB_INCREMENTAL_CONTRACT: (&str, &str) =
    ("duckdb_incremental_contract", "delete-insert-fixed-v1");

/// The SQL a node with body hash `body` renders to. Distinct bodies render to
/// distinct SQL, the way dbt compiles distinct logic to distinct SQL.
pub fn rendered_sql(body: &str) -> String {
    format!("select 1 as id, '{}' as body", body.replace('\'', "''"))
}

fn table(name: &str, epoch: Option<i64>) -> qc::TableModifiedInfo {
    qc::TableModifiedInfo {
        name: name.into(),
        last_modified_epoch: epoch,
    }
}

/// Insert or replace the `tables` entry for `name`, keeping its position.
fn upsert_table(tables: &mut Vec<qc::TableModifiedInfo>, name: &str, epoch: Option<i64>) {
    match tables.iter_mut().find(|t| t.name == name) {
        Some(existing) => existing.last_modified_epoch = epoch,
        None => tables.push(table(name, epoch)),
    }
}

// ---------------------------------------------------------------------------
// SubmitEnrichedSQL builders
// ---------------------------------------------------------------------------

/// A SubmitEnrichedSQL builder. Convert with [`Submit::build`] or `.into()`.
#[derive(Clone, Debug)]
pub struct Submit {
    req: qc::SubmitEnrichedSqlRequest,
    /// The SQL is re-rendered from the body until `sql(..)` pins it.
    sql_follows_body: bool,
}

/// A Snowflake table model (`FULL`) building `target`. Its own target is
/// present at [`OWN_EPOCH`], it has no upstreams, its unique id is
/// `model.p.<target>` and its SQL is [`rendered_sql`] of [`DEFAULT_BODY`].
pub fn model(target: &str) -> Submit {
    Submit {
        req: qc::SubmitEnrichedSqlRequest {
            target_table: Some(target.into()),
            dialect: "snowflake".into(),
            execution_type: ExecutionType::Full as i32,
            sql: rendered_sql(DEFAULT_BODY),
            tolerate_nondeterminism: true,
            tables: vec![table(target, Some(OWN_EPOCH))],
            dbt_node_state: Some(qc::DbtNodeState {
                node_unique_id: format!("model.p.{target}"),
                node_hash: DEFAULT_BODY.into(),
                node_body_hash: Some(DEFAULT_BODY.into()),
                ..Default::default()
            }),
            ..Default::default()
        },
        sql_follows_body: true,
    }
}

/// [`model`] materialized as a view (`VIEW`).
pub fn view(target: &str) -> Submit {
    model(target).execution_type(ExecutionType::View)
}

/// A Snowflake data test (`DBT_DATA_TEST`) identified by `unique_id`. Data
/// tests carry no physical target and no tables; reuse is keyed on the scoped
/// unique id and needs complete cached results ([`test_results`]).
pub fn data_test(unique_id: &str) -> Submit {
    Submit {
        req: qc::SubmitEnrichedSqlRequest {
            target_table: None,
            dialect: "snowflake".into(),
            execution_type: ExecutionType::DbtDataTest as i32,
            sql: rendered_sql(DEFAULT_BODY),
            tolerate_nondeterminism: true,
            dbt_node_state: Some(qc::DbtNodeState {
                node_unique_id: unique_id.into(),
                resource_type: "test".into(),
                node_hash: DEFAULT_BODY.into(),
                node_body_hash: Some(DEFAULT_BODY.into()),
                ..Default::default()
            }),
            ..Default::default()
        },
        sql_follows_body: true,
    }
}

/// A DuckDB table model in environment `schema` that is result-copy eligible:
/// `"db"."<schema>"."result"` built from `db.raw.input` (at epoch 10) under the
/// copy contract, namespace `copy-file-generation`, no node state and no own
/// target entry. Copy across environments needs confirmed table type
/// `DUCKDB CTAS TABLE`.
pub fn duckdb_model(schema: &str) -> Submit {
    Submit {
        req: qc::SubmitEnrichedSqlRequest {
            target_table: Some(format!("\"db\".\"{schema}\".\"result\"")),
            dialect: "duckdb".into(),
            default_catalog: "db".into(),
            default_schema: Some(schema.into()),
            table_namespace: Some("copy-file-generation".into()),
            sql: "select id, amount + 1 as amount from db.raw.input".into(),
            execution_type: ExecutionType::Full as i32,
            tolerate_nondeterminism: true,
            semantic_extras: [(
                DUCKDB_COPY_CONTRACT.0.to_string(),
                DUCKDB_COPY_CONTRACT.1.to_string(),
            )]
            .into(),
            tables: vec![table("db.raw.input", Some(10))],
            ..Default::default()
        },
        sql_follows_body: false,
    }
}

/// The namespace of [`environment_model`] and [`environment_seed`].
pub const ENVIRONMENT_NAMESPACE: &str = "ns-environments";
/// The epoch [`environment_model`] reports for its upstream.
pub const ENVIRONMENT_UPSTREAM_EPOCH: i64 = 500;

/// The physical target of [`environment_model`] in environment `schema`.
pub fn environment_target(schema: &str) -> String {
    format!("\"DB\".\"{}\".\"M\"", schema.to_uppercase())
}

/// The upstream [`environment_model`] reads in environment `schema`.
pub fn environment_upstream(schema: &str) -> String {
    format!("\"DB\".\"{}\".\"UP\"", schema.to_uppercase())
}

/// One Snowflake table model (`FULL`, unique id `model.p.m`, project
/// `project`) built in environment `schema`: it builds
/// [`environment_target`] from [`environment_upstream`] (at
/// [`ENVIRONMENT_UPSTREAM_EPOCH`]) with default schema and dbt target name
/// `schema`, under [`ENVIRONMENT_NAMESPACE`]. Its own target is absent. The
/// SQL follows the schema, so the model in two environments shares one
/// schema-abstracted logic identity (a Result copy equivalent) but not its
/// rendered SQL.
pub fn environment_model(schema: &str) -> Submit {
    Submit {
        req: qc::SubmitEnrichedSqlRequest {
            target_table: Some(environment_target(schema)),
            dialect: "snowflake".into(),
            default_catalog: "DB".into(),
            default_schema: Some(schema.into()),
            execution_type: ExecutionType::Full as i32,
            sql: format!("select id from DB.{schema}.up"),
            tolerate_nondeterminism: true,
            tables: vec![table(
                &environment_upstream(schema),
                Some(ENVIRONMENT_UPSTREAM_EPOCH),
            )],
            table_namespace: Some(ENVIRONMENT_NAMESPACE.into()),
            dbt_node_state: Some(qc::DbtNodeState {
                node_unique_id: "model.p.m".into(),
                node_body_hash: Some(DEFAULT_BODY.into()),
                project_id: Some("project".into()),
                target_name: schema.into(),
                ..Default::default()
            }),
            ..Default::default()
        },
        sql_follows_body: false,
    }
}

/// A submission carrying only `dialect`: no target, SQL, tables or node state.
pub fn empty_submission(dialect: &str) -> Submit {
    Submit {
        req: qc::SubmitEnrichedSqlRequest {
            dialect: dialect.into(),
            ..Default::default()
        },
        sql_follows_body: false,
    }
}

impl Submit {
    fn node_state_mut(&mut self) -> &mut qc::DbtNodeState {
        self.req.dbt_node_state.get_or_insert_with(Default::default)
    }

    /// The node body hash (`node_hash` and `node_body_hash`). Re-renders the SQL
    /// from it unless `sql(..)` pinned the SQL.
    pub fn body(mut self, hash: &str) -> Self {
        let state = self.node_state_mut();
        state.node_hash = hash.into();
        state.node_body_hash = Some(hash.into());
        if self.sql_follows_body {
            self.req.sql = rendered_sql(hash);
        }
        self
    }

    /// Pin the rendered SQL; later `body(..)` calls keep it.
    pub fn sql(mut self, sql: &str) -> Self {
        self.req.sql = sql.into();
        self.sql_follows_body = false;
        self
    }

    pub fn dialect(mut self, dialect: &str) -> Self {
        self.req.dialect = dialect.into();
        self
    }

    pub fn execution_type(mut self, execution_type: ExecutionType) -> Self {
        self.req.execution_type = execution_type as i32;
        self
    }

    /// The physical target. Moves the own-target `tables` entry with it.
    pub fn target(mut self, target: &str) -> Self {
        if let Some(old) = self.req.target_table.replace(target.into()) {
            if let Some(entry) = self.req.tables.iter_mut().find(|t| t.name == old) {
                entry.name = target.into();
            }
        }
        self
    }

    /// The adapter-level `table_namespace` (connection namespace).
    pub fn namespace(mut self, namespace: &str) -> Self {
        self.req.table_namespace = Some(namespace.into());
        self
    }

    pub fn catalog(mut self, catalog: &str) -> Self {
        self.req.default_catalog = catalog.into();
        self
    }

    pub fn schema(mut self, schema: &str) -> Self {
        self.req.default_schema = Some(schema.into());
        self
    }

    /// The own target's current epoch (`None`: the target is absent).
    pub fn own_epoch(mut self, epoch: Option<i64>) -> Self {
        let target = self.req.target_table.clone().unwrap_or_default();
        match epoch {
            Some(_) => upsert_table(&mut self.req.tables, &target, epoch),
            None => self.req.tables.retain(|t| t.name != target),
        }
        self
    }

    /// An upstream `name` last modified at `epoch` (replaces an existing entry).
    pub fn upstream(self, name: &str, epoch: i64) -> Self {
        self.table(name, Some(epoch))
    }

    /// A `tables` entry with any epoch, including unknown (`None`).
    pub fn table(mut self, name: &str, epoch: Option<i64>) -> Self {
        upsert_table(&mut self.req.tables, name, epoch);
        self
    }

    /// Report no tables at all (not even the own target).
    pub fn no_tables(mut self) -> Self {
        self.req.tables.clear();
        self
    }

    pub fn policy(mut self, policy: Policy) -> Self {
        self.req.stale_upstream_policy = policy as i32;
        self
    }

    pub fn tolerance_seconds(mut self, seconds: i64) -> Self {
        self.req.freshness_tolerance_seconds = seconds;
        self
    }

    pub fn tolerate_nondeterminism(mut self, tolerate: bool) -> Self {
        self.req.tolerate_nondeterminism = tolerate;
        self
    }

    pub fn allow_clones(mut self, allow: bool) -> Self {
        self.req.allow_clones = Some(allow);
        self
    }

    pub fn clone_chain_depth_limit(mut self, limit: i64) -> Self {
        self.req.clone_chain_depth_limit = Some(limit);
        self
    }

    pub fn ignore_external_modifications(mut self) -> Self {
        self.req.ignore_external_modifications = true;
        self
    }

    pub fn compare_unrendered_code(mut self) -> Self {
        self.req.compare_unrendered_code = true;
        self
    }

    pub fn extra(mut self, key: &str, value: &str) -> Self {
        self.req.semantic_extras.insert(key.into(), value.into());
        self
    }

    pub fn label(mut self, key: &str, value: &str) -> Self {
        self.req.labels.insert(key.into(), value.into());
        self
    }

    /// A query dependency `name` defined by `query`, bound in the submission's
    /// default catalog/schema.
    pub fn dependency(mut self, name: &str, query: &str) -> Self {
        self.req.query_dependencies.push(qc::QueryDependency {
            name: name.into(),
            query: query.into(),
            ..Default::default()
        });
        self
    }

    /// A query dependency with its own default catalog and schema.
    pub fn dependency_in(mut self, name: &str, query: &str, catalog: &str, schema: &str) -> Self {
        self.req.query_dependencies.push(qc::QueryDependency {
            name: name.into(),
            query: query.into(),
            default_catalog: catalog.into(),
            default_schema: schema.into(),
        });
        self
    }

    pub fn unique_id(mut self, unique_id: &str) -> Self {
        self.node_state_mut().node_unique_id = unique_id.into();
        self
    }

    /// The dbt `project_id` (scopes data tests, selector and deferral state).
    pub fn project(mut self, project_id: &str) -> Self {
        self.node_state_mut().project_id = Some(project_id.into());
        self
    }

    /// The dbt target environment name (scopes selector and deferral state).
    pub fn target_name(mut self, target_name: &str) -> Self {
        self.node_state_mut().target_name = target_name.into();
        self
    }

    /// Edit the node state (created empty when absent).
    pub fn node_state(mut self, edit: impl FnOnce(&mut qc::DbtNodeState)) -> Self {
        edit(self.node_state_mut());
        self
    }

    pub fn without_node_state(mut self) -> Self {
        self.req.dbt_node_state = None;
        self
    }

    /// Escape hatch for a field without a setter.
    pub fn with(mut self, edit: impl FnOnce(&mut qc::SubmitEnrichedSqlRequest)) -> Self {
        edit(&mut self.req);
        self
    }

    pub fn build(self) -> qc::SubmitEnrichedSqlRequest {
        self.req
    }

    /// This submission's inputs as a confirmed RecordExecutions record, so a
    /// recorded row and a submitted row share one fingerprint.
    pub fn record(&self, outcome: Outcome) -> qc::ExecutionRecord {
        let req = &self.req;
        qc::ExecutionRecord {
            input: Some(qc::execution_record::Input::EnrichedSql(qc::SqlExecution {
                target_table: req.target_table.clone(),
                dialect: req.dialect.clone(),
                default_catalog: req.default_catalog.clone(),
                default_schema: req.default_schema.clone(),
                execution_type: req.execution_type,
                sql: req.sql.clone(),
                tables: req.tables.clone(),
                query_dependencies: req.query_dependencies.clone(),
                semantic_extras: req.semantic_extras.clone(),
                labels: req.labels.clone(),
                dbt_node_state: req.dbt_node_state.clone(),
                table_namespace: req.table_namespace.clone(),
                from_speculative_submit: false,
            })),
            outcome: Some(outcome.into()),
        }
    }
}

impl From<Submit> for qc::SubmitEnrichedSqlRequest {
    fn from(submit: Submit) -> Self {
        submit.build()
    }
}

// ---------------------------------------------------------------------------
// SubmitValues (seed) builder
// ---------------------------------------------------------------------------

/// A SubmitValues (seed) builder. Convert with [`Seed::build`] or `.into()`.
#[derive(Clone, Debug)]
pub struct Seed(qc::SubmitValuesRequest);

/// A Snowflake seed loading `target` with values hash `values`, its own target
/// present at [`OWN_EPOCH`], without node state.
pub fn seed(target: &str) -> Seed {
    Seed(qc::SubmitValuesRequest {
        target_table: target.into(),
        dialect: "snowflake".into(),
        values_hash: "values".into(),
        last_modified_epoch: Some(OWN_EPOCH),
        ..Default::default()
    })
}

impl Seed {
    fn node_state_mut(&mut self) -> &mut qc::DbtNodeState {
        self.0.dbt_node_state.get_or_insert_with(Default::default)
    }

    /// The hash of the seed's file contents.
    pub fn values(mut self, values_hash: &str) -> Self {
        self.0.values_hash = values_hash.into();
        self
    }

    /// The own target's current epoch (`None`: the target is absent).
    pub fn own_epoch(mut self, epoch: Option<i64>) -> Self {
        self.0.last_modified_epoch = epoch;
        self
    }

    pub fn dialect(mut self, dialect: &str) -> Self {
        self.0.dialect = dialect.into();
        self
    }

    pub fn catalog(mut self, catalog: &str) -> Self {
        self.0.default_catalog = catalog.into();
        self
    }

    pub fn namespace(mut self, namespace: &str) -> Self {
        self.0.table_namespace = Some(namespace.into());
        self
    }

    pub fn extra(mut self, key: &str, value: &str) -> Self {
        self.0.semantic_extras.insert(key.into(), value.into());
        self
    }

    pub fn label(mut self, key: &str, value: &str) -> Self {
        self.0.labels.insert(key.into(), value.into());
        self
    }

    pub fn unique_id(mut self, unique_id: &str) -> Self {
        self.node_state_mut().node_unique_id = unique_id.into();
        self
    }

    pub fn project(mut self, project_id: &str) -> Self {
        self.node_state_mut().project_id = Some(project_id.into());
        self
    }

    pub fn target_name(mut self, target_name: &str) -> Self {
        self.node_state_mut().target_name = target_name.into();
        self
    }

    pub fn node_state(mut self, edit: impl FnOnce(&mut qc::DbtNodeState)) -> Self {
        edit(self.node_state_mut());
        self
    }

    pub fn with(mut self, edit: impl FnOnce(&mut qc::SubmitValuesRequest)) -> Self {
        edit(&mut self.0);
        self
    }

    pub fn build(self) -> qc::SubmitValuesRequest {
        self.0
    }

    /// This seed's inputs as a confirmed RecordExecutions record.
    pub fn record(&self, outcome: Outcome) -> qc::ExecutionRecord {
        let req = &self.0;
        qc::ExecutionRecord {
            input: Some(qc::execution_record::Input::Values(qc::ValuesExecution {
                target_table: req.target_table.clone(),
                dialect: req.dialect.clone(),
                default_catalog: req.default_catalog.clone(),
                values_hash: req.values_hash.clone(),
                semantic_extras: req.semantic_extras.clone(),
                labels: req.labels.clone(),
                dbt_node_state: req.dbt_node_state.clone(),
                table_namespace: req.table_namespace.clone(),
            })),
            outcome: Some(outcome.into()),
        }
    }
}

/// The seed of [`environment_model`]'s project in environment `schema`:
/// `"DB"."<SCHEMA>"."SEED"` (unique id `seed.p.s`, project `project`, dbt
/// target name `schema`) under [`ENVIRONMENT_NAMESPACE`], its own target
/// absent.
pub fn environment_seed(schema: &str) -> Seed {
    seed(&format!("\"DB\".\"{}\".\"SEED\"", schema.to_uppercase()))
        .catalog("DB")
        .namespace(ENVIRONMENT_NAMESPACE)
        .own_epoch(None)
        .unique_id("seed.p.s")
        .project("project")
        .target_name(schema)
}

impl From<Seed> for qc::SubmitValuesRequest {
    fn from(seed: Seed) -> Self {
        seed.build()
    }
}

/// An invalid RecordExecutions record: an outcome with no `input` oneof. The
/// server must reject any batch containing one.
pub fn inputless_record(outcome: Outcome) -> qc::ExecutionRecord {
    qc::ExecutionRecord {
        input: None,
        outcome: Some(outcome.into()),
    }
}

// ---------------------------------------------------------------------------
// RegisterClone builder
// ---------------------------------------------------------------------------

/// A RegisterClone (client-requested copy) builder.
#[derive(Clone, Debug)]
pub struct CopyRequest(qc::CloneRequest);

/// A Snowflake `FULL` copy of `source` into `target`.
pub fn clone_request(source: &str, target: &str) -> CopyRequest {
    CopyRequest(qc::CloneRequest {
        target_table: target.into(),
        clone_source_table: source.into(),
        dialect: "snowflake".into(),
        execution_type: ExecutionType::Full as i32,
        ..Default::default()
    })
}

impl CopyRequest {
    pub fn dialect(mut self, dialect: &str) -> Self {
        self.0.dialect = dialect.into();
        self
    }

    pub fn catalog(mut self, catalog: &str) -> Self {
        self.0.default_catalog = catalog.into();
        self
    }

    pub fn execution_type(mut self, execution_type: ExecutionType) -> Self {
        self.0.execution_type = execution_type as i32;
        self
    }

    pub fn namespace(mut self, namespace: &str) -> Self {
        self.0.table_namespace = Some(namespace.into());
        self
    }

    pub fn source_epoch(mut self, epoch: i64) -> Self {
        self.0.clone_source_last_modified_epoch = Some(epoch);
        self
    }

    pub fn source_table_type(mut self, table_type: &str) -> Self {
        self.0.clone_source_table_type = Some(table_type.into());
        self
    }

    pub fn chain_depth_limit(mut self, limit: i64) -> Self {
        self.0.clone_chain_depth_limit = Some(limit);
        self
    }

    pub fn label(mut self, key: &str, value: &str) -> Self {
        self.0.labels.insert(key.into(), value.into());
        self
    }

    pub fn with(mut self, edit: impl FnOnce(&mut qc::CloneRequest)) -> Self {
        edit(&mut self.0);
        self
    }

    pub fn build(self) -> qc::CloneRequest {
        self.0
    }
}

impl From<CopyRequest> for qc::CloneRequest {
    fn from(request: CopyRequest) -> Self {
        request.build()
    }
}

// ---------------------------------------------------------------------------
// Outcomes and confirms
// ---------------------------------------------------------------------------

/// An execution outcome, reported by ConfirmExecution or RecordExecutions.
/// Every field starts unset, as on the wire.
#[derive(Clone, Debug, Default)]
pub struct Outcome {
    outcome: qc::ExecutionOutcome,
    failed_to_clone: bool,
    labels: HashMap<String, String>,
}

pub fn outcome() -> Outcome {
    Outcome::default()
}

impl Outcome {
    /// The built object's last-modified epoch.
    pub fn epoch(mut self, epoch: i64) -> Self {
        self.outcome.last_modified_epoch = Some(epoch);
        self
    }

    pub fn table_type(mut self, table_type: &str) -> Self {
        self.outcome.table_type = Some(table_type.into());
        self
    }

    /// Table type `TABLE`.
    pub fn table(self) -> Self {
        self.table_type("TABLE")
    }

    pub fn runtime(mut self, runtime_ms: i64) -> Self {
        self.outcome.execution_runtime_ms = Some(runtime_ms);
        self
    }

    /// Cached data test results.
    pub fn results(mut self, results: qc::Struct) -> Self {
        self.outcome.execution_results = Some(results);
        self
    }

    /// Confirm-only: the client's copy failed and it built instead.
    pub fn failed_to_clone(mut self) -> Self {
        self.failed_to_clone = true;
        self
    }

    pub fn label(mut self, key: &str, value: &str) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }

    /// The confirm builder: ConfirmExecution of `request_id` with this outcome.
    pub fn confirm(self, request_id: &str) -> qc::ConfirmExecutionRequest {
        qc::ConfirmExecutionRequest {
            request_id: request_id.into(),
            last_modified_epoch: self.outcome.last_modified_epoch,
            failed_to_clone: self.failed_to_clone,
            table_type: self.outcome.table_type,
            execution_results: self.outcome.execution_results,
            execution_runtime_ms: self.outcome.execution_runtime_ms,
            labels: self.labels,
        }
    }
}

impl From<Outcome> for qc::ExecutionOutcome {
    fn from(outcome: Outcome) -> Self {
        outcome.outcome
    }
}

/// Apply a ConfirmExecution straight to the store, as the Execution service
/// does, for module tests that run without gRPC. Returns `success`.
pub async fn confirm_in(store: &Store, org: &str, confirm: qc::ConfirmExecutionRequest) -> bool {
    let publication = store
        .confirm(
            org,
            &confirm.request_id,
            confirm.last_modified_epoch,
            confirm.table_type.as_deref(),
            confirm.execution_runtime_ms,
            confirm.execution_results.as_ref(),
            confirm.failed_to_clone,
            dbt_state_server::Clock::System.now_ms(&Default::default()),
        )
        .await
        .expect("confirm");
    publication == dbt_state_server::store::Confirmation::Published
}

/// The outcome [`Scenario::build`] confirms: `epoch`, a `TABLE`, 10 ms.
pub fn built_at(epoch: i64) -> Outcome {
    outcome().epoch(epoch).table().runtime(10)
}

/// The table type a DuckDB CTAS build confirms; result copy and incremental
/// initialization only accept sources confirmed with it.
pub const DUCKDB_CTAS_TABLE: &str = "DUCKDB CTAS TABLE";

/// A DuckDB CTAS build outcome at `epoch` (no runtime).
pub fn duckdb_ctas_at(epoch: i64) -> Outcome {
    outcome().epoch(epoch).table_type(DUCKDB_CTAS_TABLE)
}

/// Complete cached data test results: `failures`, `should_error` (when there
/// are failures) and `should_warn` (false).
pub fn test_results(failures: i64) -> qc::Struct {
    use qc::value::Kind;
    let value = |kind| qc::Value { kind: Some(kind) };
    qc::Struct {
        fields: [
            ("failures".to_string(), value(Kind::IntValue(failures))),
            (
                "should_error".to_string(),
                value(Kind::BoolValue(failures > 0)),
            ),
            ("should_warn".to_string(), value(Kind::BoolValue(false))),
        ]
        .into(),
    }
}

// ---------------------------------------------------------------------------
// Decisions
// ---------------------------------------------------------------------------

/// The decision variant name of a SubmitSQLResponse.
pub fn variant(resp: &qc::SubmitSqlResponse) -> &'static str {
    super::response_variant(resp)
}

/// The BUILD decision, or a panic naming what was decided instead.
pub fn into_execute(resp: qc::SubmitSqlResponse) -> qc::ReadyToExecuteResponse {
    match resp.response {
        Some(Response::ReadyToExecute(ready)) => ready,
        other => panic!("expected ready_to_execute, got {other:?}"),
    }
}

/// The ready request id of a BUILD decision (panics on any other decision).
pub fn ready_id(resp: &qc::SubmitSqlResponse) -> String {
    into_execute(resp.clone()).request_id
}

/// The SKIP decision, or a panic naming what was decided instead.
pub fn into_skip(resp: qc::SubmitSqlResponse) -> qc::SkipExecutionResponse {
    match resp.response {
        Some(Response::SkipExecution(skip)) => skip,
        other => panic!("expected skip_execution, got {other:?}"),
    }
}

/// The CLONE decision, or a panic naming what was decided instead.
pub fn into_clone(resp: qc::SubmitSqlResponse) -> qc::ReadyToCloneResponse {
    match resp.response {
        Some(Response::ReadyToClone(clone)) => clone,
        other => panic!("expected ready_to_clone, got {other:?}"),
    }
}

/// The answer variant name of a SubmitSQLSpeculativeResponse.
pub fn speculative_variant(resp: &qc::SubmitSqlSpeculativeResponse) -> &'static str {
    use qc::submit_sql_speculative_response::Response as Speculative;
    match &resp.response {
        Some(Speculative::ReadyToExecuteUntracked(_)) => "ready_to_execute_untracked",
        Some(Speculative::SkipExecution(_)) => "skip_execution",
        Some(Speculative::ReadyToClone(_)) => "ready_to_clone",
        Some(Speculative::Undecided(_)) => "undecided",
        None => "none",
    }
}

/// The speculative SKIP, or a panic naming what was answered instead.
pub fn into_speculative_skip(resp: qc::SubmitSqlSpeculativeResponse) -> qc::SkipExecutionResponse {
    use qc::submit_sql_speculative_response::Response as Speculative;
    match resp.response {
        Some(Speculative::SkipExecution(skip)) => skip,
        other => panic!("expected a speculative skip_execution, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Protocol client
// ---------------------------------------------------------------------------

/// A protocol client for one test server, optionally scoped to an org with
/// the `x-organization-id` header (no header: the default `local` org).
/// Cheap to clone; every call opens a client on the shared channel. Clones
/// share the record of which targets were built at which epoch.
#[derive(Clone)]
pub struct Scenario {
    channel: Channel,
    org: Option<String>,
    builds: std::sync::Arc<std::sync::Mutex<Builds>>,
}

/// Build epochs this scenario confirmed, so a submit reporting [`OWN_EPOCH`]
/// for its own target reports the epoch the target was built at.
#[derive(Default)]
struct Builds {
    /// Ready request id → (org, target) awaiting confirmation.
    pending: HashMap<String, (Option<String>, String)>,
    /// (org, target) → epoch of its latest confirmed build.
    built: HashMap<(Option<String>, String), i64>,
}

impl Scenario {
    pub async fn connect(server: &TestServer) -> Self {
        Self {
            channel: super::channel(server.addr).await,
            org: None,
            builds: Default::default(),
        }
    }

    /// The same server, scoped to `org`.
    pub fn org(&self, org: &str) -> Self {
        Self {
            channel: self.channel.clone(),
            org: Some(org.into()),
            builds: self.builds.clone(),
        }
    }

    /// Replace an own-target [`OWN_EPOCH`] with the target's confirmed build
    /// epoch, when this scenario built it.
    fn report_built_epoch(&self, req: &mut qc::SubmitEnrichedSqlRequest) {
        let Some(target) = req.target_table.clone() else {
            return;
        };
        let builds = self.builds.lock().unwrap();
        let Some(epoch) = builds.built.get(&(self.org.clone(), target.clone())) else {
            return;
        };
        for t in req.tables.iter_mut() {
            if t.name == target && t.last_modified_epoch == Some(OWN_EPOCH) {
                t.last_modified_epoch = Some(*epoch);
            }
        }
    }

    fn note_ready(&self, request_id: &str, target: &str) {
        self.builds
            .lock()
            .unwrap()
            .pending
            .insert(request_id.into(), (self.org.clone(), target.into()));
    }

    fn note_confirmed(&self, request_id: &str, epoch: Option<i64>) {
        let mut builds = self.builds.lock().unwrap();
        if let (Some(key), Some(epoch)) = (builds.pending.remove(request_id), epoch) {
            builds.built.insert(key, epoch);
        }
    }

    pub fn channel(&self) -> Channel {
        self.channel.clone()
    }

    fn request<T>(&self, body: T) -> tonic::Request<T> {
        match &self.org {
            Some(org) => super::request_with_org(body, org),
            None => tonic::Request::new(body),
        }
    }

    fn sql(&self) -> qc::sql_client::SqlClient<Channel> {
        qc::sql_client::SqlClient::new(self.channel.clone())
    }

    fn execution(&self) -> qc::execution_client::ExecutionClient<Channel> {
        qc::execution_client::ExecutionClient::new(self.channel.clone())
    }

    pub async fn try_submit(
        &self,
        req: impl Into<qc::SubmitEnrichedSqlRequest>,
    ) -> Result<qc::SubmitSqlResponse, Status> {
        let mut req = req.into();
        self.report_built_epoch(&mut req);
        let target = req.target_table.clone();
        let resp = self
            .sql()
            .submit_enriched_sql(self.request(req))
            .await
            .map(tonic::Response::into_inner)?;
        if let Some(target) = target {
            match &resp.response {
                Some(Response::ReadyToExecute(r)) => self.note_ready(&r.request_id, &target),
                Some(Response::ReadyToClone(r)) => self.note_ready(&r.request_id, &target),
                _ => {}
            }
        }
        Ok(resp)
    }

    pub async fn submit(
        &self,
        req: impl Into<qc::SubmitEnrichedSqlRequest>,
    ) -> qc::SubmitSqlResponse {
        self.try_submit(req).await.expect("SubmitEnrichedSQL")
    }

    /// The decision variant name for `req`.
    pub async fn verdict(&self, req: impl Into<qc::SubmitEnrichedSqlRequest>) -> &'static str {
        variant(&self.submit(req).await)
    }

    /// SubmitEnrichedSQLSpeculative of `req`.
    pub async fn speculative(
        &self,
        req: impl Into<qc::SubmitEnrichedSqlRequest>,
    ) -> qc::SubmitSqlSpeculativeResponse {
        let mut req = req.into();
        self.report_built_epoch(&mut req);
        self.sql()
            .submit_enriched_sql_speculative(self.request(req))
            .await
            .expect("SubmitEnrichedSQLSpeculative")
            .into_inner()
    }

    pub async fn expect_execute(
        &self,
        req: impl Into<qc::SubmitEnrichedSqlRequest>,
    ) -> qc::ReadyToExecuteResponse {
        into_execute(self.submit(req).await)
    }

    /// Submit, expect a BUILD and return its ready request id.
    pub async fn execute_id(&self, req: impl Into<qc::SubmitEnrichedSqlRequest>) -> String {
        self.expect_execute(req).await.request_id
    }

    pub async fn expect_skip(
        &self,
        req: impl Into<qc::SubmitEnrichedSqlRequest>,
    ) -> qc::SkipExecutionResponse {
        into_skip(self.submit(req).await)
    }

    pub async fn expect_clone(
        &self,
        req: impl Into<qc::SubmitEnrichedSqlRequest>,
    ) -> qc::ReadyToCloneResponse {
        into_clone(self.submit(req).await)
    }

    pub async fn try_submit_values(
        &self,
        req: impl Into<qc::SubmitValuesRequest>,
    ) -> Result<qc::SubmitSqlResponse, Status> {
        self.sql()
            .submit_values(self.request(req.into()))
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn submit_values(
        &self,
        req: impl Into<qc::SubmitValuesRequest>,
    ) -> qc::SubmitSqlResponse {
        self.try_submit_values(req).await.expect("SubmitValues")
    }

    pub async fn values_verdict(&self, req: impl Into<qc::SubmitValuesRequest>) -> &'static str {
        variant(&self.submit_values(req).await)
    }

    pub async fn try_confirm(
        &self,
        req: qc::ConfirmExecutionRequest,
    ) -> Result<qc::ConfirmExecutionResponse, Status> {
        let (request_id, epoch) = (req.request_id.clone(), req.last_modified_epoch);
        let resp = self
            .execution()
            .confirm_execution(self.request(req))
            .await
            .map(tonic::Response::into_inner)?;
        if resp.success {
            self.note_confirmed(&request_id, epoch);
        }
        Ok(resp)
    }

    /// Confirm `request_id` with `outcome`; returns the server's `success`.
    pub async fn confirm(&self, request_id: &str, outcome: Outcome) -> bool {
        self.try_confirm(outcome.confirm(request_id))
            .await
            .expect("ConfirmExecution")
            .success
    }

    /// Seed State history through submit/confirm: submit `req`, expect a
    /// BUILD, confirm it [`built_at`] `epoch`. Returns the request id.
    pub async fn build(&self, req: impl Into<qc::SubmitEnrichedSqlRequest>, epoch: i64) -> String {
        self.build_with(req, built_at(epoch)).await
    }

    /// [`Scenario::build`] confirming a chosen `outcome`.
    pub async fn build_with(
        &self,
        req: impl Into<qc::SubmitEnrichedSqlRequest>,
        outcome: Outcome,
    ) -> String {
        let request_id = self.execute_id(req).await;
        assert!(
            self.confirm(&request_id, outcome).await,
            "confirming the build must succeed"
        );
        request_id
    }

    /// [`Scenario::build_with`] for a seed.
    pub async fn build_values(
        &self,
        req: impl Into<qc::SubmitValuesRequest>,
        outcome: Outcome,
    ) -> String {
        let request_id = into_execute(self.submit_values(req).await).request_id;
        assert!(
            self.confirm(&request_id, outcome).await,
            "confirming the seed build must succeed"
        );
        request_id
    }

    /// Seed State history through RecordExecutions; returns `records_stored`.
    pub async fn record(
        &self,
        records: impl IntoIterator<Item = qc::ExecutionRecord>,
    ) -> Result<u32, Status> {
        let records: Vec<_> = records.into_iter().collect();
        let built: Vec<(String, i64)> = records
            .iter()
            .filter_map(|r| {
                let epoch = r.outcome.as_ref()?.last_modified_epoch?;
                match r.input.as_ref()? {
                    qc::execution_record::Input::EnrichedSql(s) => {
                        Some((s.target_table.clone()?, epoch))
                    }
                    qc::execution_record::Input::Values(_) => None,
                }
            })
            .collect();
        let stored = self
            .execution()
            .record_executions(self.request(qc::RecordExecutionsRequest { records }))
            .await
            .map(|r| r.into_inner().records_stored)?;
        let mut builds = self.builds.lock().unwrap();
        for (target, epoch) in built {
            builds.built.insert((self.org.clone(), target), epoch);
        }
        Ok(stored)
    }

    pub async fn register_clone(
        &self,
        req: impl Into<qc::CloneRequest>,
    ) -> Result<qc::CloneResponse, Status> {
        let req = req.into();
        let target = req.target_table.clone();
        let resp = qc::clone_client::CloneClient::new(self.channel.clone())
            .register_clone(self.request(req))
            .await
            .map(tonic::Response::into_inner)?;
        if let Some(ready) = &resp.ready_to_clone {
            self.note_ready(&ready.request_id, &target);
        }
        Ok(resp)
    }

    pub async fn try_select(&self, req: qc::SelectorRequest) -> Result<BTreeSet<String>, Status> {
        qc::selector_service_client::SelectorServiceClient::new(self.channel.clone())
            .get_state_selection(self.request(req))
            .await
            .map(|r| r.into_inner().node_unique_ids.into_iter().collect())
    }

    /// GetStateSelection: the selected unique ids.
    pub async fn select(&self, req: qc::SelectorRequest) -> BTreeSet<String> {
        self.try_select(req).await.expect("GetStateSelection")
    }

    /// ResolveDeferredRelations: the physical FQN of each resolvable node.
    pub async fn resolve_deferred(
        &self,
        req: qc::ResolveDeferredRelationsRequest,
    ) -> HashMap<String, String> {
        self.execution()
            .resolve_deferred_relations(self.request(req))
            .await
            .expect("ResolveDeferredRelations")
            .into_inner()
            .fqn_by_unique_id
    }
}
