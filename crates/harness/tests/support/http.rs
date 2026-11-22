//! HTTP test servers (the fake Upstream IdP, a standalone Token broker) on
//! their own thread and runtime, so they outlive any one test's runtime and
//! stop when their owner drops.

use std::net::SocketAddr;
use std::panic::resume_unwind;
use std::thread::{self, JoinHandle};

use tokio::sync::oneshot;

/// A running HTTP server. Dropping it stops the server and joins its thread.
#[must_use = "keep the HTTP server owner alive while clients need it"]
pub struct HttpServer {
    pub addr: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl HttpServer {
    /// Bind a loopback listener on an ephemeral port.
    pub fn bind() -> std::net::TcpListener {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind HTTP server");
        listener
            .set_nonblocking(true)
            .expect("non-blocking listener");
        listener
    }

    /// Serve `router` (built on the server's runtime) on `listener`.
    pub fn start(
        listener: std::net::TcpListener,
        router: impl FnOnce() -> axum::Router + Send + 'static,
    ) -> Self {
        let addr = listener.local_addr().expect("HTTP server address");
        let (stop, shutdown) = oneshot::channel::<()>();
        let thread = thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("HTTP server runtime");
            runtime.block_on(async move {
                let listener = tokio::net::TcpListener::from_std(listener).expect("tokio listener");
                // Stopping drops the runtime, and with it every connection.
                tokio::select! {
                    result = axum::serve(
                        listener,
                        router().into_make_service_with_connect_info::<SocketAddr>(),
                    ) => result.expect("serve HTTP"),
                    _ = shutdown => {}
                }
            });
        });
        Self {
            addr,
            stop: Some(stop),
            thread: Some(thread),
        }
    }

    /// `http://<addr>`.
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }
}

impl Drop for HttpServer {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            if let Err(error) = thread.join() {
                if thread::panicking() {
                    eprintln!("HTTP test server failed during cleanup: {error:?}");
                } else {
                    resume_unwind(error);
                }
            }
        }
    }
}
