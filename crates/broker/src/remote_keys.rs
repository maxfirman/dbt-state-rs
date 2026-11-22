//! A JSON Web Key Set fetched over HTTP and cached: the Upstream IdP's keys
//! (for the Token broker), or a standalone Token broker's keys (for a gRPC
//! server without `[signing]`).
//!
//! The cached keys are refetched when they are older than [`MAX_AGE`], or
//! when a token names a key they lack (a rotation). Fetches are serialized
//! and at most one starts per [`MIN_INTERVAL`], so tokens with made-up key
//! ids cannot make the server hammer the key endpoint. A failed fetch keeps
//! the keys already loaded.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use dbt_state_auth::VerificationKeys;

use crate::discovery::Discovery;

/// Cached keys older than this are refetched before use.
pub const MAX_AGE: Duration = Duration::from_secs(5 * 60);
/// At most one fetch starts per this interval.
pub const MIN_INTERVAL: Duration = Duration::from_secs(30);

/// Where the key set is published.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyLocation {
    /// A JWKS URL.
    Jwks(String),
    /// The `jwks_uri` of the OIDC discovery document of this issuer.
    Discovery(String),
}

#[derive(Default)]
struct Fetched {
    attempted: Option<Instant>,
    loaded: Option<Instant>,
}

/// See the module docs.
pub struct RemoteKeys {
    location: KeyLocation,
    keys: VerificationKeys,
    http: reqwest::Client,
    fetched: Mutex<Fetched>,
    fetching: tokio::sync::Mutex<()>,
}

/// Why a key set fetch failed.
#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    #[error("{0}")]
    Discovery(#[from] crate::discovery::DiscoveryError),
    #[error("cannot fetch {url}: {error}")]
    Http { url: String, error: reqwest::Error },
    #[error("{url}: {error}")]
    Jwks {
        url: String,
        error: dbt_state_auth::JwksError,
    },
}

impl RemoteKeys {
    /// Keys published at `location`, none loaded yet.
    pub fn new(location: KeyLocation, http: reqwest::Client) -> Self {
        Self {
            location,
            keys: VerificationKeys::empty(),
            http,
            fetched: Mutex::default(),
            fetching: tokio::sync::Mutex::new(()),
        }
    }

    /// The cached keys. The handle sees every later refresh.
    pub fn keys(&self) -> VerificationKeys {
        self.keys.clone()
    }

    /// Refetch when nothing is loaded yet or the keys are older than
    /// [`MAX_AGE`].
    pub async fn refresh_if_stale(&self) {
        let stale = self
            .lock()
            .loaded
            .is_none_or(|loaded| loaded.elapsed() >= MAX_AGE);
        if stale {
            self.refresh().await;
        }
    }

    /// Refetch because a token's key was not among the cached keys. Waits
    /// for a fetch already under way; skips when one started within
    /// [`MIN_INTERVAL`].
    pub async fn refresh_after_miss(&self) {
        self.refresh().await;
    }

    /// Fetch now, unless a fetch started within [`MIN_INTERVAL`]. Returns
    /// whether keys were loaded by this call.
    pub(crate) async fn refresh(&self) -> bool {
        let _fetching = self.fetching.lock().await;
        {
            let mut fetched = self.lock();
            if fetched
                .attempted
                .is_some_and(|attempted| attempted.elapsed() < MIN_INTERVAL)
            {
                return false;
            }
            fetched.attempted = Some(Instant::now());
        }
        match self.fetch().await {
            Ok((url, count)) => {
                self.lock().loaded = Some(Instant::now());
                tracing::info!(jwks = url.as_str(), keys = count, "signing keys loaded");
                true
            }
            Err(error) => {
                tracing::warn!(%error, "signing keys not refreshed");
                false
            }
        }
    }

    async fn fetch(&self) -> Result<(String, usize), FetchError> {
        let url = match &self.location {
            KeyLocation::Jwks(url) => url.clone(),
            KeyLocation::Discovery(issuer) => Discovery::fetch(&self.http, issuer).await?.jwks_uri,
        };
        let body = async {
            self.http
                .get(&url)
                .send()
                .await?
                .error_for_status()?
                .bytes()
                .await
        }
        .await
        .map_err(|error| FetchError::Http {
            url: url.clone(),
            error,
        })?;
        let count = self
            .keys
            .replace_from_jwks(&body)
            .map_err(|error| FetchError::Jwks {
                url: url.clone(),
                error,
            })?;
        Ok((url, count))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Fetched> {
        self.fetched.lock().unwrap_or_else(|e| e.into_inner())
    }
}
