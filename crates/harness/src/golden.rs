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

use serde::{Deserialize, Serialize};
use tonic::{
    metadata::{KeyAndValueRef, MetadataMap},
    Response, Status,
};

#[derive(Debug, Serialize)]
pub struct GoldenEntry<Req: Serialize, Resp: Serialize> {
    pub service: String,
    pub method: String,
    pub recorded_at: String,
    pub metadata: Vec<(String, String)>,
    pub request: Req,
    pub response: Resp,
    pub status: RpcStatus,
    pub response_metadata: RecordedMetadata,
}

/// Status details are opaque bytes, not UTF-8. Old fixtures omit this field
/// and continue to mean a successful RPC.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RpcStatus {
    pub code: i32,
    pub message: String,
    pub details: Vec<u8>,
    pub metadata: RecordedMetadata,
}

/// Keep ASCII and binary metadata separate so binary evidence is lossless.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordedMetadata {
    pub ascii: Vec<(String, String)>,
    pub binary: Vec<(String, Vec<u8>)>,
}

impl RecordedMetadata {
    /// Hosted ingress adds these HTTP transport headers (observed in the
    /// success/error fixture). Retain them in captures, exclude them from RPC
    /// conformance against a server without that ingress. No application
    /// metadata or status evidence is normalized.
    pub fn protocol(&self) -> Self {
        const INGRESS: &[&str] = &[
            "server",
            "strict-transport-security",
            "x-content-type-options",
            "x-envoy-upstream-service-time",
            "x-frame-options",
        ];
        Self {
            ascii: self
                .ascii
                .iter()
                .filter(|(key, _)| !INGRESS.contains(&key.as_str()))
                .cloned()
                .collect(),
            binary: self.binary.clone(),
        }
    }

    pub fn capture(metadata: &MetadataMap) -> Self {
        let mut result = Self::default();
        for item in metadata.iter() {
            match item {
                KeyAndValueRef::Ascii(key, value) if record_header(key.as_str()) => {
                    if let Ok(value) = value.to_str() {
                        result.ascii.push((key.to_string(), value.to_owned()));
                    }
                }
                KeyAndValueRef::Binary(key, value) if record_header(key.as_str()) => {
                    if let Ok(value) = value.to_bytes() {
                        result.binary.push((key.to_string(), value.to_vec()));
                    }
                }
                _ => {}
            }
        }
        result.ascii.sort();
        result.binary.sort();
        result
    }
}

/// Transport headers are not protocol evidence. Credential headers must never
/// enter fixtures, including error trailers and binary metadata.
pub fn record_header(key: &str) -> bool {
    !matches!(
        key,
        "authorization"
            | "proxy-authorization"
            | "cookie"
            | "set-cookie"
            | "content-type"
            | "te"
            | "user-agent"
            | "host"
            | "date"
    ) && !key.starts_with("grpc-")
        && ![
            "authorization",
            "cookie",
            "token",
            "secret",
            "password",
            "credential",
            "api-key",
            "api_key",
        ]
        .iter()
        .any(|part| key.contains(part))
}

impl RpcStatus {
    pub fn equivalent(&self, other: &Self) -> bool {
        self.code == other.code
            && self.message == other.message
            && self.details == other.details
            && self.metadata.protocol() == other.metadata.protocol()
    }

    pub fn capture<T>(outcome: &Result<Response<T>, Status>) -> Self {
        match outcome {
            Ok(_) => Self {
                code: 0,
                message: String::new(),
                details: vec![],
                metadata: RecordedMetadata::default(),
            },
            Err(status) => Self {
                code: status.code() as i32,
                message: status.message().to_owned(),
                details: status.details().to_vec(),
                metadata: RecordedMetadata::capture(status.metadata()),
            },
        }
    }
}

/// Per-session accounting lives beside the JSONL, never inside promoted
/// fixtures. A killed proxy leaves `complete=false`; missing outcomes and
/// failed writes cannot produce a successful campaign.
#[derive(Debug, Default, Serialize)]
struct CaptureState {
    started: u64,
    recorded: u64,
    complete: bool,
    failed: bool,
}

#[derive(Debug)]
struct Writer {
    file: std::fs::File,
    state: CaptureState,
}

/// Append-only JSONL writer, one file per proxy session.
#[derive(Debug)]
pub struct GoldenLog {
    path: PathBuf,
    file: Mutex<Writer>,
}

impl GoldenLog {
    pub fn create(dir: impl Into<PathBuf>) -> std::io::Result<Self> {
        let dir = dir.into();
        create_dir_all(&dir)?;
        let ts = chrono::Utc::now().format("%Y%m%dT%H%M%S%.3fZ");
        let path = dir.join(format!(
            "golden_{ts}_{}.jsonl",
            uuid::Uuid::new_v4().simple()
        ));
        let file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&path)?;
        let log = Self {
            path,
            file: Mutex::new(Writer {
                file,
                state: CaptureState::default(),
            }),
        };
        log.persist(&CaptureState::default())?;
        Ok(log)
    }

    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    pub fn state_path(&self) -> PathBuf {
        PathBuf::from(format!("{}.capture.json", self.path.display()))
    }

    fn persist(&self, state: &CaptureState) -> std::io::Result<()> {
        let pending = self.state_path().with_extension("tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&pending)?;
        serde_json::to_writer(&mut file, state)?;
        file.sync_all()?;
        std::fs::rename(pending, self.state_path())
    }

    fn lock(&self) -> std::io::Result<std::sync::MutexGuard<'_, Writer>> {
        self.file
            .lock()
            .map_err(|_| std::io::Error::other("capture writer poisoned"))
    }

    /// Call before forwarding, so a request with no recorded outcome is visible.
    pub fn begin(&self) -> std::io::Result<()> {
        let mut writer = self.lock()?;
        if writer.state.failed || writer.state.complete {
            return Err(std::io::Error::other("capture session is not writable"));
        }
        writer.state.started += 1;
        if let Err(error) = self.persist(&writer.state) {
            writer.state.failed = true;
            return Err(error);
        }
        Ok(())
    }

    pub fn append<Req: Serialize, Resp: Serialize>(
        &self,
        entry: &GoldenEntry<Req, Resp>,
    ) -> std::io::Result<()> {
        let mut writer = self.lock()?;
        let result = (|| {
            if writer.state.failed
                || writer.state.complete
                || writer.state.recorded >= writer.state.started
            {
                return Err(std::io::Error::other("capture has no pending call"));
            }
            let line = serde_json::to_vec(entry)?;
            writer.file.write_all(&line)?;
            writer.file.write_all(b"\n")?;
            writer.file.sync_data()?;
            writer.state.recorded += 1;
            self.persist(&writer.state)
        })();
        if result.is_err() {
            writer.state.failed = true;
            let _ = self.persist(&writer.state);
        }
        result
    }

    /// Only graceful shutdown after all RPCs settle can certify completeness.
    pub fn finish(&self) -> std::io::Result<()> {
        let mut writer = self.lock()?;
        if writer.state.failed || writer.state.started != writer.state.recorded {
            return Err(std::io::Error::other(
                "incomplete capture: lost RPC outcomes",
            ));
        }
        writer.file.sync_all()?;
        writer.state.complete = true;
        self.persist(&writer.state)
    }
}
