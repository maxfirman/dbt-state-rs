//! Named fault-injection helpers: schema changes that make a State write fail
//! on purpose, scoped to the per-test schema. With `legacy.rs`, this is the
//! only code that changes `executions` or `node_states` with raw SQL.

use super::rows::Db;

/// Hold the first UI capture write while leaving State execution writes free.
pub struct CaptureHold(sqlx::PgConnection);

impl CaptureHold {
    pub async fn rollback(mut self) -> sqlx::Result<()> {
        use sqlx::Connection;
        sqlx::query("ROLLBACK").execute(&mut self.0).await?;
        self.0.close().await
    }
}

pub async fn hold_decision_capture(db: &impl Db) -> CaptureHold {
    use sqlx::Connection;
    let schema: String = sqlx::query_scalar("SELECT current_schema()")
        .fetch_one(db.pool())
        .await
        .expect("read fault scope");
    // Do not consume a server-pool slot: the intended fault is a delayed
    // capture write, not starvation of unrelated gRPC reads.
    let mut connection = sqlx::PgConnection::connect_with(&db.pool().connect_options())
        .await
        .expect("connect scoped fault injector");
    sqlx::query("SELECT set_config('search_path', $1, false)")
        .bind(schema)
        .execute(&mut connection)
        .await
        .expect("set fault scope");
    sqlx::query("BEGIN")
        .execute(&mut connection)
        .await
        .expect("begin capture hold");
    sqlx::query("LOCK TABLE organizations IN ACCESS EXCLUSIVE MODE")
        .execute(&mut connection)
        .await
        .expect("hold capture writes");
    CaptureHold(connection)
}

/// Make every `node_states` write fail (a `CHECK (false)` constraint), so a
/// transaction that writes node state rolls back.
pub async fn reject_node_state_writes(db: &impl Db) {
    sqlx::query("ALTER TABLE node_states ADD CONSTRAINT fault_injection CHECK (false)")
        .execute(db.pool())
        .await
        .expect("add fault constraint");
}

/// Undo [`reject_node_state_writes`].
pub async fn accept_node_state_writes(db: &impl Db) {
    sqlx::query("ALTER TABLE node_states DROP CONSTRAINT IF EXISTS fault_injection")
        .execute(db.pool())
        .await
        .expect("drop fault constraint");
}

/// Remove the `node_states` table, so the best-effort selector-state write
/// fails while the executions ledger keeps working.
pub async fn drop_node_states(db: &impl Db) {
    sqlx::query("DROP TABLE node_states")
        .execute(db.pool())
        .await
        .expect("drop node_states");
}
