use std::sync::Arc;

use tokio::net::TcpListener;

use crate::error::Error;
use crate::registry::Registry;
use crate::stream::{Connection, Stream};
use crate::worker::WorkerConfig;

pub struct Server {
    registry: Registry,
    worker_cfg: WorkerConfig,
    on_connect: Option<Arc<dyn Fn(&Connection) + Send + Sync>>,
    on_disconnect: Option<Arc<dyn Fn(&Connection) + Send + Sync>>,
    on_new_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
}

impl Server {
    pub fn new() -> Self {
        Self {
            registry: Registry::from_inventory(),
            worker_cfg: WorkerConfig::default(),
            on_connect: None,
            on_disconnect: None,
            on_new_stream: None,
        }
    }

    pub fn with_registry(mut self, registry: Registry) -> Self {
        self.registry = registry;
        self
    }

    pub fn with_worker_config(mut self, cfg: WorkerConfig) -> Self {
        self.worker_cfg = cfg;
        self
    }

    pub fn on_connect<F>(mut self, f: F) -> Self
    where
        F: Fn(&Connection) + Send + Sync + 'static,
    {
        self.on_connect = Some(Arc::new(f));
        self
    }

    pub fn on_disconnect<F>(mut self, f: F) -> Self
    where
        F: Fn(&Connection) + Send + Sync + 'static,
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
        let listener = TcpListener::bind(addr).await?;
        loop {
            let (socket, _) = listener.accept().await?;
            let peer_addr = socket.peer_addr().ok().map(|addr| addr.to_string());
            let reg = self.registry.clone();
            let cfg = self.worker_cfg.clone();
            let on_connect = self.on_connect.clone();
            let on_disconnect = self.on_disconnect.clone();
            let on_new_stream = self.on_new_stream.clone();

            tokio::spawn(async move {
                let conn = Connection::new_with_peer_addr(socket, peer_addr, Some(reg), Some(cfg));
                handle_connection(conn, on_connect, on_disconnect, on_new_stream).await;
            });
        }
    }

    #[cfg(feature = "uds")]
    pub async fn serve_uds(self, path: &str) -> Result<(), Error> {
        let listener = crate::transport::uds::listen(path).await?;
        loop {
            let reg = self.registry.clone();
            let cfg = self.worker_cfg.clone();
            let on_connect = self.on_connect.clone();
            let on_disconnect = self.on_disconnect.clone();
            let on_new_stream = self.on_new_stream.clone();

            let conn = crate::transport::uds::accept(&listener, Some(reg), Some(cfg)).await?;
            tokio::spawn(async move {
                handle_connection(conn, on_connect, on_disconnect, on_new_stream).await;
            });
        }
    }
}

async fn handle_connection(
    conn: Connection,
    on_connect: Option<Arc<dyn Fn(&Connection) + Send + Sync>>,
    on_disconnect: Option<Arc<dyn Fn(&Connection) + Send + Sync>>,
    on_new_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
) {
    if let Some(ref f) = on_connect {
        f(&conn);
    }

    let stream_loop = async move {
        while let Some(stream) = conn.accept_stream().await {
            if let Some(ref f) = on_new_stream {
                f(&stream);
            }
        }
    };

    tokio::select! {
        _ = stream_loop => {}
        _ = conn.wait_closed() => {}
    }

    conn.close_all_streams();

    if let Some(ref f) = on_disconnect {
        f(&conn);
    }
}
