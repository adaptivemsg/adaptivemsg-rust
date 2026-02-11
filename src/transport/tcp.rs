use tokio::net::{TcpListener, TcpStream};

use crate::error::Error;
use crate::registry::Registry;
use crate::stream::Connection;
use crate::worker::WorkerConfig;

pub async fn connect(addr: &str) -> Result<Connection, Error> {
    let stream = TcpStream::connect(addr).await?;
    let peer_addr = stream.peer_addr().ok().map(|addr| addr.to_string());
    Ok(Connection::new_with_peer_addr(stream, peer_addr, None, None))
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
    let peer_addr = stream.peer_addr().ok().map(|addr| addr.to_string());
    Ok(Connection::new_with_peer_addr(stream, peer_addr, registry, worker_cfg))
}
