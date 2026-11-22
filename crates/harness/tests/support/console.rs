//! The UI console (`dbt-state-ui`) as a deployment runs it, and a browser
//! for it.
//!
//! [`start_ui`] serves the console's router on its own thread and runtime,
//! configured from the same environment variables an operator sets
//! (`dbt_state_ui::Settings::from_vars`), over the test server's schema and
//! on the test server's broker clock, so moving that clock ages the
//! console's tokens and cached session statuses too.
//!
//! [`ConsoleBrowser`] follows redirects as a browser does, from the console
//! through the Token broker and the fake Upstream IdP and back. The
//! console's public URL ([`CONSOLE`], the origin of the UI's registered
//! callback) and the broker's ([`ISSUER`]) are routed to the test
//! listeners, as a TLS-terminating proxy would. It keeps the console's
//! cookies as a browser keeps them for that https origin, and records every
//! `Set-Cookie` the console sends so tests can check their attributes.
//! It needs no cookies elsewhere: the fake IdP signs the browser in without
//! one, and the broker keeps its pending logins in Postgres.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::panic::resume_unwind;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::thread::{self, JoinHandle};

use tokio::sync::oneshot;

use crate::support::http::HttpServer;
use crate::support::oidc::{TestAuth, ISSUER};
use crate::support::TestServer;

/// The console's public origin: where its registered callback
/// (`support::ui_client::UI_REDIRECT_URI`) lives.
pub const CONSOLE: &str = "https://console.dbt-state.test";

/// The test cookie key (`openssl rand -base64 32`), shared by every console
/// a test starts, as replicas of one deployment share theirs.
const COOKIE_KEY: &str = "q7F0c2m3Vb9yX1kLr8tA5nE6pW4sZ0dGh2jK3lM9oPc=";

fn testdata(file: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../auth/testdata")
        .join(file)
}

/// A running console. Dropping it stops the console and joins its thread.
#[must_use = "keep the console owner alive while browsers need it"]
pub struct TestUi {
    pub addr: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
    _cookie_key: tempfile::TempDir,
}

impl TestUi {
    /// `http://<addr>`.
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }
}

impl Drop for TestUi {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            if let Err(error) = thread.join() {
                if thread::panicking() {
                    eprintln!("test console failed during cleanup: {error:?}");
                } else {
                    resume_unwind(error);
                }
            }
        }
    }
}

/// The `oidc` mode environment of a console signing in through `server`'s
/// Token broker with the config `auth` (which must register the UI), its
/// test private key and the shared test cookie key at `cookie_key`.
pub fn oidc_vars(server: &TestServer, auth: &TestAuth, cookie_key: &Path) -> Vec<(String, String)> {
    [
        ("DBT_STATE_AUTH", "oidc".to_owned()),
        (
            "DBT_STATE_AUTH_CONFIG",
            auth.config_path().display().to_string(),
        ),
        (
            "DBT_STATE_UI_PRIVATE_KEY_FILE",
            testdata("ui-client-key.pem").display().to_string(),
        ),
        (
            "DBT_STATE_UI_COOKIE_KEY_FILE",
            cookie_key.display().to_string(),
        ),
        ("DBT_STATE_UI_BROKER_URL", server.broker_url()),
    ]
    .into_iter()
    .map(|(name, value)| (name.to_owned(), value))
    .collect()
}

/// A console in `oidc` mode for `server`'s deployment (see [`oidc_vars`]).
pub fn start_ui(server: &TestServer, auth: &TestAuth) -> TestUi {
    start_ui_with(server, |cookie_key| oidc_vars(server, auth, cookie_key))
}

/// A console over `server`'s schema and clock with the environment `vars`
/// builds from the test cookie key file's path.
pub fn start_ui_with(
    server: &TestServer,
    vars: impl FnOnce(&Path) -> Vec<(String, String)>,
) -> TestUi {
    let cookie_key = tempfile::tempdir().expect("cookie key dir");
    let key_file = cookie_key.path().join("ui-cookie.key");
    std::fs::write(&key_file, COOKIE_KEY).expect("write cookie key");
    let vars: BTreeMap<String, String> = vars(&key_file).into_iter().collect();
    let settings = dbt_state_ui::Settings::from_vars(|name| vars.get(name).cloned())
        .expect("console settings");
    let clock = server.broker_clock.broker_clock();
    let dsn = scoped_dsn(&server.schema);
    let listener = HttpServer::bind();
    let addr = listener.local_addr().expect("console address");
    let (stop, shutdown) = oneshot::channel::<()>();
    let thread = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("console runtime");
        runtime.block_on(async move {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(2)
                .connect_lazy(&dsn)
                .expect("console pool");
            let router = dbt_state_ui::router(pool, settings, clock).expect("console router");
            let listener = tokio::net::TcpListener::from_std(listener).expect("tokio listener");
            topcoat::serve_until(listener, router, async {
                let _ = shutdown.await;
            })
            .await
            .expect("serve console");
        });
    });
    TestUi {
        addr,
        stop: Some(stop),
        thread: Some(thread),
        _cookie_key: cookie_key,
    }
}

/// The test database URL, scoped to `schema`.
fn scoped_dsn(schema: &str) -> String {
    let mut dsn = reqwest::Url::parse(&crate::support::dsn()).expect("database URL");
    dsn.query_pairs_mut()
        .append_pair("options[search_path]", schema);
    dsn.into()
}

/// A response the browser received from one request.
#[derive(Debug, Clone)]
pub struct Exchange {
    /// The public URL requested.
    pub url: String,
    pub status: u16,
    /// The `location` header, resolved against [`Self::url`].
    pub location: Option<String>,
    pub cache_control: Option<String>,
    pub body: String,
}

/// Where a visit ended: the last response, and every URL it passed
/// through.
#[derive(Debug, Clone)]
pub struct Page {
    pub url: String,
    pub status: u16,
    pub body: String,
    /// Every public URL requested, in order.
    pub visited: Vec<String>,
}

impl Page {
    /// The value of the hidden form field `name` (as the console renders
    /// it: `name="<name>" value="<value>"`).
    pub fn field(&self, name: &str) -> Option<String> {
        let needle = format!("name=\"{name}\" value=\"");
        let start = self.body.find(&needle)? + needle.len();
        let end = self.body[start..].find('"')?;
        Some(self.body[start..start + end].to_owned())
    }
}

/// See the module docs.
pub struct ConsoleBrowser {
    http: reqwest::Client,
    routes: Vec<(String, String)>,
    /// The console's cookies, by name.
    cookies: Mutex<BTreeMap<String, String>>,
    /// Every `Set-Cookie` the console sent, in order.
    set_cookies: Mutex<Vec<String>>,
}

impl ConsoleBrowser {
    /// A browser for `ui`, with the Token broker of `server` (if it runs
    /// one) at [`ISSUER`].
    pub fn new(ui: &TestUi, server: &TestServer) -> Self {
        let mut routes = vec![(CONSOLE.to_owned(), ui.url())];
        if server.broker.is_some() {
            routes.push((ISSUER.to_owned(), server.broker_url()));
        }
        Self {
            http: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("browser HTTP client"),
            routes,
            cookies: Mutex::default(),
            set_cookies: Mutex::default(),
        }
    }

    /// The same browser (its cookies) against another console: the
    /// deployment's console after a restart, or another replica.
    pub fn against(&self, ui: &TestUi) -> Self {
        let mut routes = self.routes.clone();
        routes[0].1 = ui.url();
        Self {
            http: self.http.clone(),
            routes,
            cookies: Mutex::new(self.cookies.lock().unwrap().clone()),
            set_cookies: Mutex::default(),
        }
    }

    fn routed(&self, url: &str) -> String {
        for (public, actual) in &self.routes {
            if let Some(rest) = url.strip_prefix(public.as_str()) {
                if rest.is_empty() || rest.starts_with(['/', '?']) {
                    return format!("{actual}{rest}");
                }
            }
        }
        url.to_owned()
    }

    fn is_console(url: &str) -> bool {
        url.strip_prefix(CONSOLE)
            .is_some_and(|rest| rest.is_empty() || rest.starts_with(['/', '?']))
    }

    /// The console's cookies the browser holds now, by name.
    pub fn cookies(&self) -> BTreeMap<String, String> {
        self.cookies.lock().unwrap().clone()
    }

    /// Every `Set-Cookie` the console sent so far.
    pub fn set_cookies(&self) -> Vec<String> {
        self.set_cookies.lock().unwrap().clone()
    }

    /// Forget every cookie: a fresh browser profile.
    pub fn clear_cookies(&self) {
        self.cookies.lock().unwrap().clear();
    }

    /// One request to the public `url` (a console path is relative to
    /// [`CONSOLE`]): a GET, or a form POST of `form` from a console page
    /// (`sec-fetch-site: same-origin`), with the console's cookies.
    pub async fn request(&self, url: &str, form: Option<&[(&str, &str)]>) -> Exchange {
        self.request_from(url, form, "same-origin").await
    }

    /// [`Self::request`], with the browser declaring the request's
    /// `sec-fetch-site` as `site` (for example `cross-site` for a form on
    /// another site).
    pub async fn request_from(
        &self,
        url: &str,
        form: Option<&[(&str, &str)]>,
        site: &str,
    ) -> Exchange {
        let url = if url.starts_with('/') {
            format!("{CONSOLE}{url}")
        } else {
            url.to_owned()
        };
        let mut request = match form {
            Some(fields) => self
                .http
                .post(self.routed(&url))
                .header("content-type", "application/x-www-form-urlencoded")
                .header("sec-fetch-site", site)
                .body(
                    form_urlencoded::Serializer::new(String::new())
                        .extend_pairs(fields)
                        .finish(),
                ),
            None => self.http.get(self.routed(&url)),
        };
        let console = Self::is_console(&url);
        if console {
            let cookies = self.cookies.lock().unwrap();
            if !cookies.is_empty() {
                let header = cookies
                    .iter()
                    .map(|(name, value)| format!("{name}={value}"))
                    .collect::<Vec<_>>()
                    .join("; ");
                request = request.header("cookie", header);
            }
        }
        let response = request.send().await.expect("browser request");
        if console {
            for value in response.headers().get_all("set-cookie") {
                let value = value.to_str().expect("ASCII Set-Cookie").to_owned();
                self.store(&value);
                self.set_cookies.lock().unwrap().push(value);
            }
        }
        let status = response.status().as_u16();
        let location = response
            .headers()
            .get("location")
            .map(|value| resolve(&url, value.to_str().expect("ASCII location")));
        let cache_control = response
            .headers()
            .get("cache-control")
            .map(|value| value.to_str().expect("ASCII cache-control").to_owned());
        Exchange {
            url,
            status,
            location,
            cache_control,
            body: response.text().await.unwrap_or_default(),
        }
    }

    /// Keep or drop a cookie as a browser does for `Set-Cookie: value`.
    fn store(&self, value: &str) {
        let mut parts = value.split(';');
        let Some((name, cookie)) = parts.next().and_then(|pair| pair.split_once('=')) else {
            return;
        };
        let removed = parts.any(|attribute| {
            let attribute = attribute.trim().to_ascii_lowercase();
            attribute == "max-age=0" || attribute.starts_with("max-age=-")
        });
        let mut cookies = self.cookies.lock().unwrap();
        if removed {
            cookies.remove(name.trim());
        } else {
            cookies.insert(name.trim().to_owned(), cookie.trim().to_owned());
        }
    }

    /// Open `url` and follow redirects until a page answers.
    pub async fn open(&self, url: &str) -> Page {
        self.follow(url, None).await
    }

    /// Submit a console form: POST `fields` to `action`, then follow
    /// redirects.
    pub async fn submit(&self, action: &str, fields: &[(&str, &str)]) -> Page {
        self.follow(action, Some(fields)).await
    }

    async fn follow(&self, url: &str, mut form: Option<&[(&str, &str)]>) -> Page {
        let mut url = url.to_owned();
        let mut visited = Vec::new();
        for _ in 0..12 {
            let exchange = self.request(&url, form.take()).await;
            visited.push(exchange.url.clone());
            match exchange.location {
                Some(location) if (300..400).contains(&exchange.status) => url = location,
                _ => {
                    return Page {
                        url: exchange.url,
                        status: exchange.status,
                        body: exchange.body,
                        visited,
                    }
                }
            }
        }
        panic!("too many redirects: {visited:?}");
    }
}

/// `location` resolved against the URL `base` it was received from.
fn resolve(base: &str, location: &str) -> String {
    if location.starts_with('/') && !location.starts_with("//") {
        let origin_end = base
            .find("://")
            .map(|scheme| {
                base[scheme + 3..]
                    .find('/')
                    .map_or(base.len(), |path| scheme + 3 + path)
            })
            .unwrap_or(base.len());
        format!("{}{location}", &base[..origin_end])
    } else {
        location.to_owned()
    }
}
