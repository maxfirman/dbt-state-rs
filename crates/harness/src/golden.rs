//! Golden-file capture for recorded gRPC traffic.
//!
//! Each intercepted call appends one JSON line to a `.jsonl` file under the
//! golden directory, recording the service, method, decoded request and
//! response, plus the request metadata headers the client sent. These files are
//! both human-diffable and replayable into our server during differential tests.

use std::fs::{create_dir_all, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;

use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct GoldenEntry<Req: Serialize, Resp: Serialize> {
    pub service: String,
    pub method: String,
    pub recorded_at: String,
    pub metadata: Vec<(String, String)>,
    pub request: Req,
    pub response: Resp,
}

/// Append-only JSONL writer, one file per proxy session.
#[derive(Debug)]
pub struct GoldenLog {
    path: PathBuf,
    file: Mutex<std::fs::File>,
}

impl GoldenLog {
    pub fn create(dir: impl Into<PathBuf>) -> std::io::Result<Self> {
        let dir = dir.into();
        create_dir_all(&dir)?;
        let ts = chrono::Utc::now().format("%Y%m%dT%H%M%S%.3fZ");
        let path = dir.join(format!("golden_{ts}.jsonl"));
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self {
            path,
            file: Mutex::new(file),
        })
    }

    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    pub fn append<Req: Serialize, Resp: Serialize>(&self, entry: &GoldenEntry<Req, Resp>) {
        match serde_json::to_string(entry) {
            Ok(line) => {
                if let Ok(mut f) = self.file.lock() {
                    let _ = writeln!(f, "{line}");
                    let _ = f.flush();
                }
            }
            Err(e) => {
                tracing::warn!("failed to serialize golden entry: {e}");
            }
        }
    }
}
