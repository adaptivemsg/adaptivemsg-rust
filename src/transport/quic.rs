use std::io;

use quinn::Endpoint;

use crate::error::Error;
use crate::registry::Registry;
use crate::stream::Connection;
use crate::worker::WorkerConfig;

pub async fn connect(endpoint: &Endpoint, addr: &str, server_name: &str) -> Result<Connection, Error> {
    let addr = addr.parse().map_err(to_io_err)?;
    let conn = endpoint.connect(addr, server_name).map_err(to_io_err)?.await.map_err(to_io_err)?;
    let (send, recv) = conn.open_bi().await.map_err(to_io_err)?;
    Ok(Connection::from_split(recv, send, None, None))
}

pub async fn accept(
    endpoint: &Endpoint,
    registry: Option<Registry>,
    worker_cfg: Option<WorkerConfig>,
) -> Result<Connection, Error> {
    let incoming = endpoint.accept().await.ok_or_else(|| {
        io::Error::new(io::ErrorKind::UnexpectedEof, "no incoming connection")
    })?;
    let conn = incoming.await.map_err(to_io_err)?;
    let (send, recv) = conn.accept_bi().await.map_err(to_io_err)?;
    Ok(Connection::from_split(recv, send, registry, worker_cfg))
}

fn to_io_err<E: std::error::Error>(err: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, err.to_string())
}
