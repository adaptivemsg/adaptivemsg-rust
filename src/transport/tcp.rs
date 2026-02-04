use tokio::net::{TcpListener, TcpStream};

use crate::error::Error;
use crate::registry::Registry;
use crate::stream::Connection;
use crate::worker::WorkerConfig;

pub async fn connect(addr: &str) -> Result<Connection, Error> {
    let stream = TcpStream::connect(addr).await?;
    Ok(Connection::new(stream, None, None))
}

pub async fn listen(addr: &str) -> Result<TcpListener, Error> {
    Ok(TcpListener::bind(addr).await?)
}

pub async fn accept(
    listener: &TcpListener,
    registry: Option<Registry>,
    worker_cfg: Option<WorkerConfig>,
) -> Result<Connection, Error> {
    let (stream, _) = listener.accept().await?;
    Ok(Connection::new(stream, registry, worker_cfg))
}
