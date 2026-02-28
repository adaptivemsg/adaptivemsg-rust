use std::future::Future;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::Error;
use crate::registry::Registry;
use crate::stream::{server as stream_server, Connection, ConnectionInner, Stream};
use tracing::warn;

pub struct Server {
    registry: Registry,
    on_connect: Option<Arc<dyn Fn(Connection) -> Result<(), Error> + Send + Sync>>,
    on_disconnect: Option<Arc<dyn Fn(Connection) -> Result<(), Error> + Send + Sync>>,
    on_new_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
    on_close_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
}

impl Server {
    pub fn new() -> Self {
        Self {
            registry: Registry::from_inventory(),
            on_connect: None,
            on_disconnect: None,
            on_new_stream: None,
            on_close_stream: None,
        }
    }

    pub fn on_connect<F>(mut self, f: F) -> Self
    where
        F: Fn(Connection) -> Result<(), Error> + Send + Sync + 'static,
    {
        self.on_connect = Some(Arc::new(f));
        self
    }

    pub fn on_disconnect<F>(mut self, f: F) -> Self
    where
        F: Fn(Connection) -> Result<(), Error> + Send + Sync + 'static,
    {
        self.on_disconnect = Some(Arc::new(f));
        self
    }

    pub fn on_new_stream<F>(mut self, f: F) -> Self
    where
        F: Fn(&Stream) + Send + Sync + 'static,
    {
        self.on_new_stream = Some(Arc::new(f));
        self
    }

    pub fn on_close_stream<F>(mut self, f: F) -> Self
    where
        F: Fn(&Stream) + Send + Sync + 'static,
    {
        self.on_close_stream = Some(Arc::new(f));
        self
    }

    pub async fn serve(self, addr: &str) -> Result<(), Error> {
        if let Some(stripped) = addr.strip_prefix("tcp://") {
            return self.serve_tcp(stripped).await;
        }
        #[cfg(feature = "uds")]
        if let Some(stripped) = addr.strip_prefix("uds://") {
            return self.serve_uds(stripped).await;
        }
        #[cfg(feature = "uds")]
        if let Some(stripped) = addr.strip_prefix("unix://") {
            return self.serve_uds(stripped).await;
        }
        self.serve_tcp(addr).await
    }

    pub async fn serve_tcp(self, addr: &str) -> Result<(), Error> {
        let listener = Arc::new(crate::transport::tcp::listen(addr).await?);
        self.serve_with_accept(move || {
            let listener = listener.clone();
            crate::transport::tcp::accept_stream(listener)
        })
        .await
    }

    #[cfg(feature = "uds")]
    pub async fn serve_uds(self, path: &str) -> Result<(), Error> {
        let listener = Arc::new(crate::transport::uds::listen(path).await?);
        self.serve_with_accept(move || {
            let listener = listener.clone();
            crate::transport::uds::accept_stream(listener)
        })
        .await
    }

    async fn serve_with_accept<R, A, F>(self, mut accept: A) -> Result<(), Error>
    where
        R: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        A: FnMut() -> F,
        F: Future<Output = Result<(R, Option<String>), Error>>,
    {
        let server = Arc::new(self);

        loop {
            let (socket, peer_addr) = accept().await?;
            let peer_label = peer_addr
                .clone()
                .unwrap_or_else(|| "client-unknown".to_string());
            let server = server.clone();

            tokio::spawn(async move {
                let pending = ConnectionInner::new_pending(
                    socket,
                    peer_addr,
                    stream_server::dispatch(Some(server.registry.clone())),
                    server.on_new_stream.clone(),
                    server.on_close_stream.clone(),
                );
                if let Some(ref f) = server.on_connect {
                    if let Err(err) = f(pending.connection()) {
                        warn!("on_connect failed for {peer_label}: {err}");
                        pending.connection().close();
                        return;
                    }
                }
                let conn = pending.start();
                conn.wait_closed().await;

                conn.close_all_streams();

                if let Some(ref f) = server.on_disconnect {
                    if let Err(err) = f(conn) {
                        warn!("on_disconnect failed for {peer_label}: {err}");
                    }
                }
            });
        }
    }
}
