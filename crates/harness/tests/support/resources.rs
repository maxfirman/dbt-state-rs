//! Own database and server lifetimes independently of each test's async runtime.

use std::net::SocketAddr;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use dbt_state_server::AppState;
use sqlx::postgres::PgPoolOptions;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

/// An isolated schema, removed on success, panic, or failed server startup.
pub struct TestDatabase {
    pub schema: String,
    dsn: String,
}

impl TestDatabase {
    pub async fn new(prefix: &str) -> Self {
        let database = Self {
            schema: format!("{prefix}_{}", uuid::Uuid::new_v4().simple()),
            dsn: super::dsn(),
        };
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database.dsn)
            .await
            .expect("connect admin");
        sqlx::query(&format!("CREATE SCHEMA \"{}\"", database.schema))
            .execute(&admin)
            .await
            .expect("create schema");
        admin.close().await;
        database
    }

    pub fn scoped_dsn(&self) -> String {
        let mut dsn = reqwest::Url::parse(&self.dsn).expect("database URL");
        dsn.query_pairs_mut()
            .append_pair("options[search_path]", &self.schema);
        dsn.into()
    }

    async fn state(&self, require_org_header: bool) -> AppState {
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .after_connect({
                let schema = self.schema.clone();
                move |conn, _meta| {
                    let schema = schema.clone();
                    Box::pin(async move {
                        use sqlx::Executor;
                        conn.execute(format!("SET search_path TO \"{schema}\"").as_str())
                            .await?;
                        Ok(())
                    })
                }
            })
            .connect(&self.dsn)
            .await
            .expect("connect server pool");
        sqlx::migrate!("../server/migrations")
            .run(&pool)
            .await
            .expect("migrate");
        AppState::from_pool(pool).with_require_org_header(require_org_header)
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        let dsn = self.dsn.clone();
        let schema = self.schema.clone();
        // A separate runtime lets cleanup complete while a current-thread test
        // unwinds or after the server runtime has stopped. Join before returning.
        let cleanup = thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("cleanup runtime")
                .block_on(async move {
                    let admin = PgPoolOptions::new()
                        .max_connections(1)
                        .acquire_timeout(Duration::from_secs(5))
                        .connect(&dsn)
                        .await?;
                    sqlx::query("SET statement_timeout = 5000")
                        .execute(&admin)
                        .await?;
                    let result =
                        sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
                            .execute(&admin)
                            .await;
                    admin.close().await;
                    result.map(|_| ())
                })
        })
        .join();
        match cleanup {
            Ok(Ok(())) => {}
            error if thread::panicking() => eprintln!("schema cleanup failed: {error:?}"),
            error => panic!("schema cleanup failed: {error:?}"),
        }
    }
}

/// Keep this owner alive for as long as clients or row assertions need the server.
/// Dropping it stops the runtime (including detached captures), then removes the
/// schema. Tests never need to coordinate shutdown, pool disposal or cleanup.
#[must_use = "keep the server owner alive while clients and row assertions need it"]
pub struct TestServer {
    pub addr: SocketAddr,
    pub schema: String,
    pub state: AppState,
    _lifetime: ServerLifetime,
}

struct ServerLifetime {
    stop: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl Drop for ServerLifetime {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(worker) = self.thread.take() {
            if let Err(error) = worker.join() {
                if thread::panicking() {
                    eprintln!("test server failed during cleanup: {error:?}");
                } else {
                    resume_unwind(error);
                }
            }
        }
    }
}

pub async fn start_server() -> TestServer {
    start_server_configured(false).await
}

pub async fn start_server_require_org() -> TestServer {
    start_server_configured(true).await
}

async fn start_server_configured(require_org_header: bool) -> TestServer {
    let (ready, startup) = oneshot::channel();
    let (stop, shutdown) = oneshot::channel();
    let worker = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("server runtime");
        let database = runtime.block_on(TestDatabase::new("difftest"));
        // Always dispose of the runtime before schema cleanup, including on a
        // migration/startup panic. This drops any in-flight capture transactions.
        let result = catch_unwind(AssertUnwindSafe(|| {
            runtime.block_on(async {
                let state = database.state(require_org_header).await;
                let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
                let addr = listener.local_addr().expect("server address");
                let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
                let router = dbt_state_server::build_router(state.clone());
                if ready.send((addr, database.schema.clone(), state)).is_err() {
                    return;
                }
                tokio::select! {
                    result = router.serve_with_incoming(incoming) => result.expect("serve"),
                    _ = shutdown => {}
                }
            });
        }));
        drop(runtime);
        drop(database);
        if let Err(error) = result {
            resume_unwind(error);
        }
    });
    // Construct the owner before awaiting startup, so cancellation or failed
    // startup also stops and joins the worker.
    let lifetime = ServerLifetime {
        stop: Some(stop),
        thread: Some(worker),
    };
    let (addr, schema, state) = startup.await.expect("test server startup");
    TestServer {
        addr,
        schema,
        state,
        _lifetime: lifetime,
    }
}
