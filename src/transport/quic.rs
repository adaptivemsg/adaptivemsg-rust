use std::io;

use quinn::Endpoint;

use crate::error::Error;
use crate::registry::Registry;
use crate::stream::{Conn, Connection as ConnectionInner};

pub async fn connect(endpoint: &Endpoint, addr: &str, server_name: &str) -> Result<Conn, Error> {
    let addr = addr.parse().map_err(to_io_err)?;
    let conn = endpoint.connect(addr, server_name).map_err(to_io_err)?.await.map_err(to_io_err)?;
    let peer_addr = Some(conn.remote_address().to_string());
    let (send, recv) = conn.open_bi().await.map_err(to_io_err)?;
    Ok(ConnectionInner::from_split(recv, send, peer_addr, None, None, None).start())
}

pub async fn accept(
    endpoint: &Endpoint,
    registry: Option<Registry>,
) -> Result<Conn, Error> {
    let incoming = endpoint.accept().await.ok_or_else(|| {
        io::Error::new(io::ErrorKind::UnexpectedEof, "no incoming connection")
    })?;
    let conn = incoming.await.map_err(to_io_err)?;
    let peer_addr = Some(conn.remote_address().to_string());
    let (send, recv) = conn.accept_bi().await.map_err(to_io_err)?;
    Ok(ConnectionInner::from_split(recv, send, peer_addr, registry, None, None).start())
}

fn to_io_err<E: std::error::Error>(err: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, err.to_string())
}
