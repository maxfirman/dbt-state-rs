//! Row-assertion helpers: read `executions` and `node_states` back for
//! assertions, so tests do not each hand-write SQL. Read-only; raw writes live
//! in `legacy` and `fault`.

use dbt_state_server::store::Store;
use dbt_state_server::AppState;
use sqlx::PgPool;

use super::TestServer;

/// Anything that reaches the test schema's pool.
pub trait Db {
    fn pool(&self) -> &PgPool;
}

impl Db for Store {
    fn pool(&self) -> &PgPool {
        Store::pool(self)
    }
}

impl Db for AppState {
    fn pool(&self) -> &PgPool {
        self.store.pool()
    }
}

impl Db for TestServer {
    fn pool(&self) -> &PgPool {
        self.state.store.pool()
    }
}

impl<T: Db> Db for std::sync::Arc<T> {
    fn pool(&self) -> &PgPool {
        T::pool(self)
    }
}

/// One `executions` row, as stored.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct StoredExecution {
    pub id: i64,
    pub org_id: String,
    pub request_id: String,
    pub execution_decision_id: Option<String>,
    pub status: String,
    pub target_table: String,
    pub execution_type: i32,
    pub dialect: String,
    pub table_namespace: Option<String>,
    pub project_id: Option<String>,
    pub node_unique_id: Option<String>,
    pub node_body_hash: Option<String>,
    pub node_sql_hash: Option<String>,
    pub node_template_hash: Option<String>,
    pub node_namespace_logic_hash: Option<String>,
    pub clone_source_table: Option<String>,
    pub last_modified_epoch: Option<i64>,
    pub table_type: Option<String>,
    pub execution_runtime_ms: Option<i64>,
    /// The Principal whose request wrote the row (`oidc` mode).
    pub principal_id: Option<String>,
}

impl StoredExecution {
    pub fn is_pending(&self) -> bool {
        self.status == "pending"
    }

    pub fn is_confirmed(&self) -> bool {
        self.status == "confirmed"
    }
}

const EXECUTION_COLUMNS: &str = "id, org_id, request_id, execution_decision_id, status, \
     target_table, execution_type, dialect, table_namespace, project_id, node_unique_id, \
     node_body_hash, node_sql_hash, node_template_hash, node_namespace_logic_hash, \
     clone_source_table, last_modified_epoch, table_type, execution_runtime_ms, principal_id";

/// Every `executions` row across orgs, oldest first.
pub async fn executions(db: &impl Db) -> Vec<StoredExecution> {
    sqlx::query_as(&format!(
        "SELECT {EXECUTION_COLUMNS} FROM executions ORDER BY id"
    ))
    .fetch_all(db.pool())
    .await
    .expect("read executions")
}

/// The `executions` row written for `request_id`, if any.
pub async fn execution(db: &impl Db, request_id: &str) -> Option<StoredExecution> {
    sqlx::query_as(&format!(
        "SELECT {EXECUTION_COLUMNS} FROM executions WHERE request_id = $1"
    ))
    .bind(request_id)
    .fetch_optional(db.pool())
    .await
    .expect("read execution")
}

/// The `executions` row written for `request_id` under `org_id`, if any.
pub async fn execution_in(db: &impl Db, org_id: &str, request_id: &str) -> Option<StoredExecution> {
    execution(db, request_id)
        .await
        .filter(|row| row.org_id == org_id)
}

/// Every `executions` row of `org_id`, oldest first.
pub async fn executions_in(db: &impl Db, org_id: &str) -> Vec<StoredExecution> {
    let mut rows = executions(db).await;
    rows.retain(|row| row.org_id == org_id);
    rows
}

/// The number of `executions` rows across orgs.
pub async fn execution_count(db: &impl Db) -> usize {
    executions(db).await.len()
}

/// One `node_states` row (selector and deferral comparison state).
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct StoredNodeState {
    pub org_id: String,
    pub project_id: String,
    pub target_name: String,
    pub node_unique_id: String,
    pub node_body_hash: Option<String>,
    pub node_configs_hash: Option<String>,
    pub node_persisted_descriptions_hash: Option<String>,
    pub node_macros_hash: Option<String>,
    pub node_contract_hash: Option<String>,
    pub node_database_representation: Option<String>,
    pub target_table: Option<String>,
}

/// Every `node_states` row across orgs.
pub async fn node_states(db: &impl Db) -> Vec<StoredNodeState> {
    sqlx::query_as(
        "SELECT org_id, project_id, target_name, node_unique_id, node_body_hash, \
                node_configs_hash, node_persisted_descriptions_hash, node_macros_hash, \
                node_contract_hash, node_database_representation, target_table \
         FROM node_states ORDER BY org_id, project_id, target_name, node_unique_id",
    )
    .fetch_all(db.pool())
    .await
    .expect("read node_states")
}

/// One `invocations` row: the dbt invocation a decision was recorded under,
/// with its decision counts.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct StoredInvocation {
    pub org_id: String,
    pub external_invocation_id: Option<String>,
    pub session_id: Option<String>,
    pub built_count: i32,
    pub reused_count: i32,
    pub cloned_count: i32,
}

/// Every `invocations` row across orgs.
pub async fn invocations(db: &impl Db) -> Vec<StoredInvocation> {
    sqlx::query_as(
        "SELECT org_id, external_invocation_id, session_id, built_count, reused_count, \
                cloned_count \
         FROM invocations ORDER BY org_id, external_invocation_id",
    )
    .fetch_all(db.pool())
    .await
    .expect("read invocations")
}

/// One `node_decisions` row: a recorded decision, and who made it.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct StoredDecision {
    pub org_id: String,
    pub execution_decision_id: Option<String>,
    pub decision: String,
    /// The Principal whose request made the decision (`oidc` mode).
    pub principal_id: Option<String>,
}

/// Every `node_decisions` row across orgs, oldest first, once at least
/// `count` are recorded (decision capture is asynchronous).
pub async fn decisions(db: &impl Db, count: usize) -> Vec<StoredDecision> {
    for _ in 0..100 {
        let rows: Vec<StoredDecision> = sqlx::query_as(
            "SELECT org_id, execution_decision_id, decision, principal_id \
             FROM node_decisions ORDER BY id",
        )
        .fetch_all(db.pool())
        .await
        .expect("read node_decisions");
        if rows.len() >= count {
            return rows;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("fewer than {count} decisions were recorded");
}

/// One `principals` row: a Principal the Token broker issued a token to.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct StoredPrincipal {
    /// The opaque Principal id: the token's `sub`.
    pub id: String,
    pub issuer: String,
    pub subject: String,
    pub kind: String,
    pub email: Option<String>,
}

/// Every `principals` row, by issuer and subject.
pub async fn principals(db: &impl Db) -> Vec<StoredPrincipal> {
    sqlx::query_as(
        "SELECT id, issuer, subject, kind, email FROM principals ORDER BY issuer, subject",
    )
    .fetch_all(db.pool())
    .await
    .expect("read principals")
}

/// The names of the tables in the server's schema, sorted.
pub async fn tables(db: &impl Db) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT table_name::text FROM information_schema.tables \
         WHERE table_schema = current_schema() AND table_type = 'BASE TABLE' \
         ORDER BY table_name",
    )
    .fetch_all(db.pool())
    .await
    .expect("list tables")
}

/// Establish that an exact decision's audit passed a selective capture fault.
pub async fn wait_for_decision_capture(db: &impl Db, org: &str, id: &str) {
    for _ in 0..100 {
        let captured: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM node_decisions WHERE org_id = $1 AND execution_decision_id = $2)",
        ).bind(org).bind(id).fetch_one(db.pool()).await.expect("read exact captured decision");
        if captured {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("decision audit did not pass selective capture hold");
}

/// Complete persisted execution evidence for upgrade/publication regressions.
pub async fn complete_execution(db: &impl Db, request_id: &str) -> serde_json::Value {
    sqlx::query_scalar("SELECT to_jsonb(e) FROM executions e WHERE request_id = $1")
        .bind(request_id)
        .fetch_one(db.pool())
        .await
        .expect("complete execution evidence")
}

/// Every row of every table in the server's schema, as text: for asserting
/// that a value (such as a client secret) was never stored.
pub async fn every_row_as_text(server: &TestServer) -> String {
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT table_name::text FROM information_schema.tables \
         WHERE table_schema = $1 AND table_type = 'BASE TABLE'",
    )
    .bind(&server.schema)
    .fetch_all(server.pool())
    .await
    .expect("list tables");
    let mut text = String::new();
    for table in tables {
        let rows: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT t::text FROM \"{}\".\"{}\" t",
            server.schema,
            table.replace('"', "\"\"")
        ))
        .fetch_all(server.pool())
        .await
        .expect("read table");
        for row in rows {
            text.push_str(&row);
            text.push('\n');
        }
    }
    text
}
