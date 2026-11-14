//! Named fault-injection helpers: schema changes that make a State write fail
//! on purpose, scoped to the per-test schema. With `legacy.rs`, this is the
//! only code that changes `executions` or `node_states` with raw SQL.

use super::rows::Db;

/// Corrupt current-version physical completion evidence without changing its
/// identity. This is a storage fault, not a simulated historical fingerprint.
pub async fn forget_completion_epoch(db: &impl Db, target: &str) {
    let affected = sqlx::query(
        "UPDATE executions SET last_modified_epoch = NULL WHERE target_table = $1 AND status = 'confirmed'",
    )
    .bind(target)
    .execute(db.pool())
    .await
    .expect("inject missing completion epoch")
    .rows_affected();
    assert!(
        affected > 0,
        "completion fault must touch confirmed evidence"
    );
}

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

/// Remove or corrupt only auxiliary Explain audit data. Execution fingerprints,
/// physical epochs, confirmations and outcomes remain genuine current rows.
pub enum ComparisonAuditFault {
    LegacyJson,
    MalformedRendered,
    MissingComparison,
    MissingAudit,
}

pub async fn comparison_audit_fault(
    db: &impl Db,
    org: &str,
    decision_id: &str,
    fault: ComparisonAuditFault,
) {
    let before: serde_json::Value = sqlx::query_scalar(
        "SELECT template_comparison FROM node_decisions WHERE org_id = $1 AND execution_decision_id = $2",
    ).bind(org).bind(decision_id).fetch_one(db.pool()).await.expect("read genuine comparison audit");
    assert!(
        before["rendered_query"].is_object(),
        "fault must start from genuine new comparison evidence"
    );
    if matches!(fault, ComparisonAuditFault::MissingAudit) {
        let affected = sqlx::query(
            "DELETE FROM node_decisions WHERE org_id = $1 AND execution_decision_id = $2",
        )
        .bind(org)
        .bind(decision_id)
        .execute(db.pool())
        .await
        .expect("remove exact auxiliary audit")
        .rows_affected();
        assert_eq!(affected, 1);
        return;
    }
    let after = match fault {
        ComparisonAuditFault::LegacyJson => {
            let mut old = before.clone();
            old.as_object_mut().unwrap().remove("rendered_query");
            assert_eq!(
                old.as_object().unwrap().len() + 1,
                before.as_object().unwrap().len()
            );
            Some(old)
        }
        ComparisonAuditFault::MalformedRendered => {
            let mut malformed = before;
            malformed["rendered_query"] = serde_json::json!(17);
            Some(malformed)
        }
        ComparisonAuditFault::MissingComparison => None,
        ComparisonAuditFault::MissingAudit => unreachable!(),
    };
    let affected = sqlx::query("UPDATE node_decisions SET template_comparison = $1 WHERE org_id = $2 AND execution_decision_id = $3")
        .bind(after).bind(org).bind(decision_id).execute(db.pool()).await.expect("alter exact auxiliary comparison only").rows_affected();
    assert_eq!(affected, 1);
}

/// Deliberately collide a genuine foreign audit's request UUID with a local
/// execution. The new Explain lookup must retain its organization boundary.
pub async fn collide_foreign_audit_request(
    db: &impl Db,
    foreign_org: &str,
    foreign_decision_id: &str,
    local_request_id: &str,
) {
    assert_ne!(foreign_org, "local");
    let affected = sqlx::query("UPDATE node_decisions SET request_id = $1 WHERE org_id = $2 AND execution_decision_id = $3")
        .bind(local_request_id).bind(foreign_org).bind(foreign_decision_id).execute(db.pool()).await.expect("collide foreign auxiliary request UUID").rows_affected();
    assert_eq!(affected, 1);
}

/// Selective capture ordering fault: delay a physical-source baseline audit,
/// without delaying the later literal-view decision audit or execution writes.
pub async fn hold_physical_source_audit(db: &impl Db, target: &str) -> CaptureHold {
    use sqlx::Connection;
    let schema: String = sqlx::query_scalar("SELECT current_schema()")
        .fetch_one(db.pool())
        .await
        .expect("read capture fault scope");
    let target_literal = target.replace('\'', "''");
    sqlx::query(&format!(
        r#"
        CREATE FUNCTION hold_physical_source_capture() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.org_id = 'local' AND NEW.target_table = '{target_literal}'
               AND NEW.query_dependencies = '[]'::jsonb THEN
                PERFORM pg_advisory_xact_lock(hashtext(TG_TABLE_SCHEMA), 141882);
            END IF;
            RETURN NEW;
        END
        $$
    "#
    ))
    .execute(db.pool())
    .await
    .expect("create selective capture hold");
    sqlx::query("CREATE TRIGGER hold_physical_source_capture BEFORE INSERT ON node_decisions FOR EACH ROW EXECUTE FUNCTION hold_physical_source_capture()")
        .execute(db.pool()).await.expect("install selective capture hold");
    let mut connection = sqlx::PgConnection::connect_with(&db.pool().connect_options())
        .await
        .expect("connect selective capture hold");
    sqlx::query("SELECT set_config('search_path', $1, false)")
        .bind(schema)
        .execute(&mut connection)
        .await
        .expect("scope selective capture hold");
    sqlx::query("BEGIN")
        .execute(&mut connection)
        .await
        .expect("begin selective capture hold");
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext(current_schema()), 141882)")
        .execute(&mut connection)
        .await
        .expect("hold only baseline audit lock");
    CaptureHold(connection)
}

pub async fn wait_for_physical_source_audit_hold(db: &impl Db) {
    for _ in 0..100 {
        let waiting: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND classid = hashtext(current_schema())::oid AND objid = 141882 AND NOT granted)")
            .fetch_one(db.pool()).await.expect("observe selective audit waiter");
        if waiting {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("prior audit did not reach the selective trigger");
}
