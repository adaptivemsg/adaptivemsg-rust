use std::future::Future;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use tracing::warn;

use crate::codec::CodecID;
use crate::codec_msgpack::{CodecMsgpackCompact, CodecMsgpackMap};
use crate::connection::{ConnectionInner, Netconn};
use crate::context::Context;
use crate::error::Error;
use crate::protocol::DEFAULT_MAX_FRAME;
use crate::registry::Registry;

pub struct Server {
    registry: Registry,
    codecs: Vec<CodecID>,
    on_connect: Option<Arc<dyn Fn(Netconn) -> Result<(), Error> + Send + Sync>>,
    on_disconnect: Option<Arc<dyn Fn(Netconn) -> Result<(), Error> + Send + Sync>>,
    on_new_stream: Option<Arc<dyn Fn(Context) + Send + Sync>>,
    on_close_stream: Option<Arc<dyn Fn(Context) + Send + Sync>>,
}

impl Server {
    pub fn new() -> Self {
        Self {
            registry: Registry::from_inventory(),
            codecs: vec![CodecMsgpackMap, CodecMsgpackCompact],
            on_connect: None,
            on_disconnect: None,
            on_new_stream: None,
            on_close_stream: None,
        }
    }

    pub fn on_connect<F>(mut self, f: F) -> Self
    where
        F: Fn(Netconn) -> Result<(), Error> + Send + Sync + 'static,
    {
        self.on_connect = Some(Arc::new(f));
        self
    }

    pub fn on_disconnect<F>(mut self, f: F) -> Self
    where
        F: Fn(Netconn) -> Result<(), Error> + Send + Sync + 'static,
    {
        self.on_disconnect = Some(Arc::new(f));
        self
    }

    pub fn on_new_stream<F>(mut self, f: F) -> Self
    where
        F: Fn(Context) + Send + Sync + 'static,
    {
        self.on_new_stream = Some(Arc::new(f));
        self
    }

    pub fn on_close_stream<F>(mut self, f: F) -> Self
    where
        F: Fn(Context) + Send + Sync + 'static,
    {
        self.on_close_stream = Some(Arc::new(f));
        self
    }

    pub fn with_codecs(mut self, codecs: &[CodecID]) -> Self {
        self.codecs = codecs.to_vec();
        self
    }

    pub async fn serve(self, addr: &str) -> Result<(), Error> {
        if let Some(stripped) = addr.strip_prefix("tcp://") {
            return self.serve_tcp(stripped).await;
        }
        if let Some(stripped) = addr.strip_prefix("uds://") {
            return self.serve_uds(stripped).await;
        }
        if let Some(stripped) = addr.strip_prefix("unix://") {
            return self.serve_uds(stripped).await;
        }
        self.serve_tcp(addr).await
    }

    async fn serve_tcp(self, addr: &str) -> Result<(), Error> {
        let listener = Arc::new(crate::transport::tcp::listen(addr).await?);
        self.serve_with_accept(move || {
            let listener = listener.clone();
            crate::transport::tcp::accept_stream(listener)
        })
        .await
    }

    async fn serve_uds(self, path: &str) -> Result<(), Error> {
        #[cfg(feature = "uds")]
        {
            let listener = Arc::new(crate::transport::uds::listen(path).await?);
            return self
                .serve_with_accept(move || {
                    let listener = listener.clone();
                    crate::transport::uds::accept_stream(listener)
                })
                .await;
        }
        #[cfg(not(feature = "uds"))]
        {
            let _ = path;
            return Err(Error::UnsupportedTransport(
                "uds transport not enabled".to_string(),
            ));
        }
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
            let netconn = Netconn::new(peer_addr.clone());
            let server = server.clone();
            let codecs = server.codecs.clone();

            tokio::spawn(async move {
                let pending = ConnectionInner::new_pending(
                    socket,
                    server.registry.clone(),
                    server.on_new_stream.clone(),
                    server.on_close_stream.clone(),
                );
                let conn_handle = pending.connection();
                if let Some(ref f) = server.on_connect {
                    if let Err(err) = f(netconn.clone()) {
                        warn!("on_connect failed for {peer_label}: {err}");
                        conn_handle.close();
                        return;
                    }
                }
                let conn = match pending.start_server(&codecs, DEFAULT_MAX_FRAME).await {
                    Ok(conn) => conn,
                    Err(err) => {
                        warn!("handshake failed for {peer_label}: {err}");
                        conn_handle.close();
                        return;
                    }
                };
                conn.wait_closed().await;

                conn.close_all_streams();

                if let Some(ref f) = server.on_disconnect {
                    if let Err(err) = f(netconn) {
                        warn!("on_disconnect failed for {peer_label}: {err}");
                    }
                }
            });
        }
    }
}
