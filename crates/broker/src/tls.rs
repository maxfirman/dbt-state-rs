//! Native TLS for the Token broker's and the gRPC server's listeners, from
//! mounted PEM files: a certificate chain (leaf first) and its private key
//! (PKCS#8, PKCS#1 or SEC1). Both binaries read them at startup, so a
//! renewed certificate applies on restart. A listener without them serves
//! plain HTTP, for a deployment behind a TLS-terminating proxy.
//!
//! TLS uses rustls with the `ring` provider, the workspace's only crypto
//! backend.

use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context};
use axum::extract::ConnectInfo;
use hyper_util::rt::TokioIo;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::TcpListener;
use tokio_rustls::rustls;
use tokio_rustls::TlsAcceptor;
use tower::ServiceExt;

use crate::Broker;

/// How long a client has to complete the TLS handshake.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// A listener's TLS certificate chain and private key, read from mounted
/// PEM files and checked to belong together.
#[derive(Clone)]
pub struct TlsIdentity {
    certificate_chain: Vec<u8>,
    private_key: Vec<u8>,
}

impl fmt::Debug for TlsIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsIdentity").finish_non_exhaustive()
    }
}

impl TlsIdentity {
    /// The identity named by the environment variables `cert_var` and
    /// `key_var` (file paths): `None` when neither is set (or both are
    /// empty), an error when only one is, or when the files do not hold a
    /// certificate chain and its private key.
    pub fn from_env(cert_var: &str, key_var: &str) -> anyhow::Result<Option<Self>> {
        let var = |key: &str| {
            std::env::var_os(key)
                .filter(|value| !value.is_empty())
                .map(PathBuf::from)
        };
        match (var(cert_var), var(key_var)) {
            (None, None) => Ok(None),
            (Some(cert), Some(key)) => Self::load(&cert, &key)
                .with_context(|| {
                    format!(
                        "{cert_var}={} and {key_var}={} are not a usable TLS certificate \
                         chain and private key",
                        cert.display(),
                        key.display()
                    )
                })
                .map(Some),
            (Some(_), None) => bail!(
                "{cert_var} is set without {key_var}: set both for native TLS, or neither \
                 to serve plain HTTP behind a TLS-terminating proxy"
            ),
            (None, Some(_)) => bail!(
                "{key_var} is set without {cert_var}: set both for native TLS, or neither \
                 to serve plain HTTP behind a TLS-terminating proxy"
            ),
        }
    }

    /// Read the certificate chain from `cert_file` and its private key from
    /// `key_file`.
    pub fn load(cert_file: &Path, key_file: &Path) -> anyhow::Result<Self> {
        let certificate_chain = std::fs::read(cert_file)
            .with_context(|| format!("cannot read the certificate {}", cert_file.display()))?;
        let private_key = std::fs::read(key_file)
            .with_context(|| format!("cannot read the private key {}", key_file.display()))?;
        let identity = Self {
            certificate_chain,
            private_key,
        };
        identity.server_config(Vec::new())?;
        Ok(identity)
    }

    /// The certificate chain, PEM.
    pub fn certificate_chain_pem(&self) -> &[u8] {
        &self.certificate_chain
    }

    /// The private key, PEM.
    pub fn private_key_pem(&self) -> &[u8] {
        &self.private_key
    }

    /// A rustls server config offering `alpn`.
    fn server_config(&self, alpn: Vec<Vec<u8>>) -> anyhow::Result<Arc<rustls::ServerConfig>> {
        let chain = CertificateDer::pem_slice_iter(&self.certificate_chain)
            .collect::<Result<Vec<_>, _>>()
            .context("the certificate file is not PEM")?;
        if chain.is_empty() {
            bail!("the certificate file holds no PEM certificate");
        }
        // The error never quotes the key.
        let key = PrivateKeyDer::from_pem_slice(&self.private_key)
            .map_err(|_| anyhow::anyhow!("the private key file holds no PEM private key"))?;
        let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .context("TLS protocol versions")?
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .context("the private key does not belong to the certificate")?;
        config.alpn_protocols = alpn;
        Ok(Arc::new(config))
    }
}

/// Serve `broker` over TLS with `identity` (HTTP/1.1) on `listener` until
/// the process stops. Each connection's handshake runs on its own task,
/// bounded by [`HANDSHAKE_TIMEOUT`], so a slow client delays no other.
pub async fn serve_tls(
    listener: TcpListener,
    broker: Broker,
    identity: &TlsIdentity,
) -> std::io::Result<()> {
    let config = identity
        .server_config(vec![b"http/1.1".to_vec()])
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    let acceptor = TlsAcceptor::from(config);
    let router = broker.router();
    loop {
        let (tcp, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(error) => {
                accept_error(error).await;
                continue;
            }
        };
        let (acceptor, router) = (acceptor.clone(), router.clone());
        tokio::spawn(async move {
            let stream = match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(tcp)).await {
                Ok(Ok(stream)) => stream,
                Ok(Err(error)) => {
                    tracing::debug!(%peer, %error, "Token broker TLS handshake failed");
                    return;
                }
                Err(_) => {
                    tracing::debug!(%peer, "Token broker TLS handshake timed out");
                    return;
                }
            };
            serve_connection(stream, peer, router).await;
        });
    }
}

/// Serve one TLS connection, giving handlers the peer address the way
/// `axum::serve` does (`ConnectInfo<SocketAddr>`).
async fn serve_connection(
    stream: tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
    peer: SocketAddr,
    router: axum::Router,
) {
    let service = hyper::service::service_fn(move |mut request: axum::http::Request<_>| {
        request.extensions_mut().insert(ConnectInfo(peer));
        router.clone().oneshot(request)
    });
    if let Err(error) = hyper::server::conn::http1::Builder::new()
        .serve_connection(TokioIo::new(stream), service)
        .await
    {
        tracing::debug!(%peer, %error, "Token broker connection ended with an error");
    }
}

/// As `axum::serve`: a failed connection is the client's problem; anything
/// else (for example, out of file descriptors) waits a second before the
/// next accept.
async fn accept_error(error: std::io::Error) {
    use std::io::ErrorKind::{ConnectionAborted, ConnectionRefused, ConnectionReset};
    if matches!(
        error.kind(),
        ConnectionRefused | ConnectionAborted | ConnectionReset
    ) {
        return;
    }
    tracing::error!(%error, "Token broker accept failed");
    tokio::time::sleep(Duration::from_secs(1)).await;
}
