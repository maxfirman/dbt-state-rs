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
     clone_source_table, last_modified_epoch, table_type, execution_runtime_ms";

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
