//! The Token broker's clock: when pending logins and authorization codes
//! expire, sessions reach their cap, and tokens are issued.

use std::fmt;
use std::sync::Arc;
use std::time::SystemTime;

/// The host's wall clock unless a test supplies another. Nothing a client
/// sends can set it.
#[derive(Clone)]
pub struct Clock(Arc<dyn Fn() -> SystemTime + Send + Sync>);

impl fmt::Debug for Clock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Clock")
    }
}

impl Default for Clock {
    fn default() -> Self {
        Self::system()
    }
}

impl Clock {
    /// The host's wall clock.
    pub fn system() -> Self {
        Self(Arc::new(SystemTime::now))
    }

    /// A clock reading `now`: tests use it to move past expiry.
    pub fn new(now: impl Fn() -> SystemTime + Send + Sync + 'static) -> Self {
        Self(Arc::new(now))
    }

    pub fn now(&self) -> SystemTime {
        (self.0)()
    }
}
