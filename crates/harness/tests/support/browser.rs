//! A browser for sign-in tests: it follows HTTP redirects the way a real
//! one does, without cookies or scripts (the broker and the fake Upstream
//! IdP need neither), until it reaches the dbt client's loopback handler.
//! It never connects there: the handler's query is what the dbt client
//! would read.
//!
//! The broker's public URL (its issuer, for example
//! `https://broker.dbt-state.test`) is routed to the test listener, as a
//! TLS-terminating proxy in front of a deployment would.

use std::collections::BTreeMap;

/// Where a browser visit ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Landing {
    /// Redirected to the dbt client's loopback handler with this query.
    Loopback(BTreeMap<String, String>),
    /// A page that redirects nowhere.
    Page { status: u16, body: String },
}

impl Landing {
    /// The loopback handler's query, or a panic naming the page.
    pub fn loopback(self) -> BTreeMap<String, String> {
        match self {
            Self::Loopback(query) => query,
            Self::Page { status, body } => panic!("no loopback redirect: HTTP {status}: {body}"),
        }
    }
}

/// See the module docs.
pub struct Browser {
    http: reqwest::Client,
    routes: Vec<(String, String)>,
}

impl Default for Browser {
    fn default() -> Self {
        Self::new()
    }
}

impl Browser {
    pub fn new() -> Self {
        Self {
            http: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("browser HTTP client"),
            routes: Vec::new(),
        }
    }

    /// Requests under the base URL `public` go to `actual` instead.
    pub fn route(mut self, public: &str, actual: &str) -> Self {
        self.routes.push((
            public.trim_end_matches('/').into(),
            actual.trim_end_matches('/').into(),
        ));
        self
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

    /// Open `url` and follow redirects until one points at `loopback` (the
    /// dbt client's redirect URI) or a page answers.
    pub async fn visit(&self, url: &str, loopback: &str) -> Landing {
        let mut url = url.to_owned();
        for _ in 0..10 {
            if let Some(query) = url.strip_prefix(loopback) {
                let query = query.strip_prefix('?').unwrap_or_default();
                return Landing::Loopback(
                    form_urlencoded::parse(query.as_bytes())
                        .into_owned()
                        .collect(),
                );
            }
            let response = self
                .http
                .get(self.routed(&url))
                .send()
                .await
                .expect("browser request");
            if response.status().is_redirection() {
                url = response.headers()["location"]
                    .to_str()
                    .expect("ASCII location")
                    .to_owned();
                continue;
            }
            return Landing::Page {
                status: response.status().as_u16(),
                body: response.text().await.unwrap_or_default(),
            };
        }
        panic!("too many redirects from {url}");
    }
}
