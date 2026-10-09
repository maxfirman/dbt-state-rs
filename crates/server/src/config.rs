//! Server configuration from environment.

#[derive(Debug, Clone)]
pub struct Config {
    pub listen: String,
    pub database_url: String,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            listen: std::env::var("DBT_STATE_LISTEN")
                .unwrap_or_else(|_| "127.0.0.1:50051".to_string()),
            database_url: std::env::var("DATABASE_URL").unwrap_or_else(|_| {
                "postgres://dbtstate:dbtstate@localhost:55441/dbtstate".to_string()
            }),
        }
    }
}
