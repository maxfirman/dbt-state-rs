//! Server configuration from environment.

#[derive(Debug, Clone)]
pub struct Config {
    pub listen: String,
    pub database_url: String,
    /// When true, every request MUST carry a non-empty `x-organization-id`
    /// metadata header; requests without it are rejected rather than silently
    /// bucketed into the shared default tenant. Off by default to preserve the
    /// insecure local-dev flow (the dbt client omits the header when pointed at
    /// an insecure endpoint). A multi-tenant deployment should enable this.
    /// NOTE: this enforces org SCOPING presence, not identity AUTHENTICATION —
    /// verifying the claimed org against a trusted bearer token remains
    /// deployment work (see docs/correctness-handoff.md §D and docs/protocol.md).
    pub require_org_header: bool,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            listen: std::env::var("DBT_STATE_LISTEN")
                .unwrap_or_else(|_| "127.0.0.1:50051".to_string()),
            database_url: std::env::var("DATABASE_URL").unwrap_or_else(|_| {
                "postgres://dbtstate:dbtstate@localhost:55441/dbtstate".to_string()
            }),
            require_org_header: std::env::var("DBT_STATE_REQUIRE_ORG_HEADER")
                .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
                .unwrap_or(false),
        }
    }
}

#[cfg(test)]
mod tests {
    /// The env flag parses the common truthy spellings; everything else is off.
    #[test]
    fn require_org_header_truthy_parsing() {
        let truthy =
            |v: &str| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes");
        for v in ["1", "true", "TRUE", " yes ", "Yes"] {
            assert!(truthy(v), "{v:?} should be truthy");
        }
        for v in ["0", "false", "", "off", "no", "maybe"] {
            assert!(!truthy(v), "{v:?} should be falsy");
        }
    }
}
