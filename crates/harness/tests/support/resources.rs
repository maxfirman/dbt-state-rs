//! Own database and server lifetimes independently of each test's async runtime.

use std::net::SocketAddr;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime};

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

    /// Server state over this schema, migrated. Tests that call server modules
    /// directly (without gRPC) use it; the owner must outlive the state.
    pub async fn state(&self, require_org_header: bool) -> AppState {
        let pool = self
            .pool_options()
            .connect(&self.dsn)
            .await
            .expect("connect server pool");
        sqlx::migrate!("../server/migrations")
            .run(&pool)
            .await
            .expect("migrate");
        AppState::from_pool(pool)
            .with_require_org_header(require_org_header)
            .with_clock(dbt_state_server::Clock::Replay)
    }

    /// Migrate this schema, as a standalone Token broker does at startup.
    pub async fn migrate(&self) {
        let pool = self
            .pool_options()
            .max_connections(1)
            .connect(&self.dsn)
            .await
            .expect("connect migration pool");
        dbt_state_broker::MIGRATOR
            .run(&pool)
            .await
            .expect("migrate");
        pool.close().await;
    }

    /// Makes a pool over this schema that connects on first use. Call it on
    /// the runtime that will use the pool.
    pub fn lazy_pool_factory(&self) -> impl FnOnce() -> sqlx::PgPool + Send + 'static {
        let (options, dsn) = (self.pool_options(), self.dsn.clone());
        move || options.connect_lazy(&dsn).expect("lazy pool")
    }

    fn pool_options(&self) -> PgPoolOptions {
        PgPoolOptions::new().max_connections(2).after_connect({
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
    }
}

/// A Token broker clock a test moves forward. Clones share it.
#[derive(Clone, Default)]
pub struct TestClock(Arc<AtomicU64>);

impl TestClock {
    /// Move the clock `by` forward.
    pub fn advance(&self, by: Duration) {
        self.0.fetch_add(by.as_secs(), Ordering::SeqCst);
    }

    /// The Token broker clock: the host's, plus every advance so far.
    pub fn broker_clock(&self) -> dbt_state_broker::Clock {
        let offset = Arc::clone(&self.0);
        dbt_state_broker::Clock::new(move || {
            SystemTime::now() + Duration::from_secs(offset.load(Ordering::SeqCst))
        })
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
    /// The in-process Token broker's listener, when it runs one.
    pub broker: Option<SocketAddr>,
    /// The in-process Token broker's clock.
    pub broker_clock: TestClock,
    /// Both listeners serve native TLS with the test certificate
    /// ([`super::tls`]).
    pub tls: bool,
    _lifetime: ServerLifetime,
}

impl TestServer {
    /// The in-process Token broker's base URL.
    pub fn broker_url(&self) -> String {
        let scheme = if self.tls { "https" } else { "http" };
        format!(
            "{scheme}://{}",
            self.broker.expect("a Token broker listener")
        )
    }
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
    start_server_with_database(require_org_header, None).await
}

/// Restart production routers while an external owner retains the same schema.
pub async fn start_server_on_database(database: std::sync::Arc<TestDatabase>) -> TestServer {
    start_server_with_database(false, Some(database)).await
}

/// Native acceptance uses the production clock and never accepts replay metadata.
pub async fn start_server_native() -> TestServer {
    start_server_with_database_clock(false, None, dbt_state_server::Clock::System, None).await
}

/// A server in `oidc` mode: every RPC but health needs a token `auth`
/// accepts, for an active Organization of `auth`'s config.
pub async fn start_server_oidc(auth: &super::oidc::TestAuth) -> TestServer {
    start_server_with_database_clock(
        false,
        None,
        dbt_state_server::Clock::Replay,
        Some(Oidc {
            config: auth.config().clone(),
            broker: None,
            tls: false,
        }),
    )
    .await
}

/// A server in `oidc` mode with the Token broker in the same process, on its
/// own listener, sharing its key set: one deployment, as the server binary
/// runs with `DBT_STATE_BROKER_LISTEN`.
pub async fn start_server_with_broker(auth: &super::oidc::TestAuth) -> TestServer {
    start_server_with_database_clock(
        false,
        None,
        dbt_state_server::Clock::Replay,
        Some(Oidc {
            config: auth.config().clone(),
            broker: Some(super::http::HttpServer::bind()),
            tls: false,
        }),
    )
    .await
}

/// A deployment as the server binary runs it with native TLS on both
/// listeners (`DBT_STATE_TLS_*`, `DBT_STATE_BROKER_TLS_*`) and the Token
/// broker in process, with the test certificate ([`super::tls`]). The
/// broker's issuer is its own `https://127.0.0.1:<port>` URL.
pub async fn start_server_tls(auth: super::oidc::TestAuthBuilder) -> TestServer {
    let broker = super::http::HttpServer::bind();
    let issuer = format!("https://{}", broker.local_addr().expect("broker address"));
    let auth = auth.issuer(&issuer).build();
    start_server_with_database_clock(
        false,
        None,
        dbt_state_server::Clock::Replay,
        Some(Oidc {
            config: auth.config().clone(),
            broker: Some(broker),
            tls: true,
        }),
    )
    .await
}

/// A standalone Token broker, as the `dbt-state-broker` binary runs, on its
/// own migrated schema.
#[must_use = "keep the broker owner alive while clients need it"]
pub struct TestBroker {
    pub auth: super::oidc::TestAuth,
    pub clock: TestClock,
    server: super::http::HttpServer,
    _database: TestDatabase,
}

impl TestBroker {
    /// Its base URL, which is also its issuer.
    pub fn url(&self) -> String {
        self.server.url()
    }
}

/// Start a standalone Token broker whose issuer is its own URL, with the
/// config `auth` builds.
pub async fn start_broker(auth: super::oidc::TestAuthBuilder) -> TestBroker {
    let database = TestDatabase::new("broker").await;
    database.migrate().await;
    let listener = super::http::HttpServer::bind();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let auth = auth.issuer(&url).build();
    let clock = TestClock::default();
    let (config, broker_clock) = (auth.config().clone(), clock.broker_clock());
    let pool = database.lazy_pool_factory();
    let server = super::http::HttpServer::start(listener, move || {
        dbt_state_broker::Broker::with_clock(&config, pool(), broker_clock)
            .expect("broker config")
            .router()
    });
    TestBroker {
        auth,
        clock,
        server,
        _database: database,
    }
}

/// How a test server runs `oidc` mode.
struct Oidc {
    config: dbt_state_auth::AuthConfig,
    /// Also run the Token broker, on this listener.
    broker: Option<std::net::TcpListener>,
    /// Serve both listeners over TLS with the test certificate.
    tls: bool,
}

async fn start_server_with_database(
    require_org_header: bool,
    database: Option<std::sync::Arc<TestDatabase>>,
) -> TestServer {
    start_server_with_database_clock(
        require_org_header,
        database,
        dbt_state_server::Clock::Replay,
        None,
    )
    .await
}

async fn start_server_with_database_clock(
    require_org_header: bool,
    database: Option<std::sync::Arc<TestDatabase>>,
    clock: dbt_state_server::Clock,
    oidc: Option<Oidc>,
) -> TestServer {
    let (ready, startup) = oneshot::channel();
    let (stop, shutdown) = oneshot::channel();
    let broker_clock = TestClock::default();
    let server_clock = broker_clock.clone();
    let worker = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("server runtime");
        let database = database.unwrap_or_else(|| {
            std::sync::Arc::new(runtime.block_on(TestDatabase::new("difftest")))
        });
        // Always dispose of the runtime before schema cleanup, including on a
        // migration/startup panic. This drops any in-flight capture transactions.
        let result = catch_unwind(AssertUnwindSafe(|| {
            runtime.block_on(async {
                let mut state = database.state(require_org_header).await.with_clock(clock);
                let mut broker = None;
                let mut tls = None;
                if let Some(oidc) = oidc {
                    state = state.with_oidc(&oidc.config).expect("oidc mode");
                    tls = oidc.tls.then(super::tls::identity);
                    if let Some(listener) = oidc.broker {
                        let listener = TcpListener::from_std(listener).expect("broker listener");
                        let instance = dbt_state_broker::Broker::with_clock(
                            &oidc.config,
                            state.store.pool().clone(),
                            broker_clock.broker_clock(),
                        )
                        .expect("broker config");
                        broker = Some(listener.local_addr().expect("broker address"));
                        match tls.clone() {
                            Some(identity) => tokio::spawn(async move {
                                dbt_state_broker::serve_tls(listener, instance, &identity).await
                            }),
                            None => tokio::spawn(dbt_state_broker::serve(listener, instance)),
                        };
                    }
                }
                let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
                let addr = listener.local_addr().expect("server address");
                let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
                let router = match &tls {
                    Some(identity) => dbt_state_server::build_tls_router(state.clone(), identity)
                        .expect("gRPC TLS"),
                    None => dbt_state_server::build_router(state.clone()),
                };
                let tls = tls.is_some();
                if ready
                    .send((addr, database.schema.clone(), state, broker, tls))
                    .is_err()
                {
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
    let (addr, schema, state, broker, tls) = startup.await.expect("test server startup");
    TestServer {
        addr,
        schema,
        state,
        broker,
        broker_clock: server_clock,
        tls,
        _lifetime: lifetime,
    }
}
