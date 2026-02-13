use std::future::Future;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::Error;
use crate::registry::Registry;
use crate::stream::{Conn, Connection as ConnectionInner, Stream};

pub struct Server {
    registry: Registry,
    on_connect: Option<Arc<dyn Fn(Conn) + Send + Sync>>,
    on_disconnect: Option<Arc<dyn Fn(Conn) + Send + Sync>>,
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

    pub fn with_registry(mut self, registry: Registry) -> Self {
        self.registry = registry;
        self
    }

    pub fn on_connect<F>(mut self, f: F) -> Self
    where
        F: Fn(Conn) + Send + Sync + 'static,
    {
        self.on_connect = Some(Arc::new(f));
        self
    }

    pub fn on_disconnect<F>(mut self, f: F) -> Self
    where
        F: Fn(Conn) + Send + Sync + 'static,
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
            let server = server.clone();

            tokio::spawn(async move {
                let conn = ConnectionInner::new(
                    socket,
                    peer_addr,
                    Some(server.registry.clone()),
                    server.on_new_stream.clone(),
                    server.on_close_stream.clone(),
                );
                if let Some(ref f) = server.on_connect {
                    f(conn.connection());
                }

                let conn = conn.start();
                conn.wait_closed().await;

                conn.close_all_streams();

                if let Some(ref f) = server.on_disconnect {
                    f(conn);
                }
            });
        }
    }
}
