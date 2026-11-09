//! Named fault-injection helpers: schema changes that make a State write fail
//! on purpose, scoped to the per-test schema. With `legacy.rs`, this is the
//! only code that changes `executions` or `node_states` with raw SQL.

use super::rows::Db;

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
