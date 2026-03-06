use std::io;

use quinn::Endpoint;

use crate::error::Error;
use crate::registry::Registry;
use crate::stream::{Codec, Connection, ConnectionInner};

pub async fn connect(
    endpoint: &Endpoint,
    addr: &str,
    server_name: &str,
    codec: Codec,
    max_frame: u32,
) -> Result<Connection, Error> {
    let addr = addr.parse().map_err(to_io_err)?;
    let conn = endpoint
        .connect(addr, server_name)
        .map_err(to_io_err)?
        .await
        .map_err(to_io_err)?;
    let (send, recv) = conn.open_bi().await.map_err(to_io_err)?;
    let pending = ConnectionInner::new_pending_from_split(
        recv,
        send,
        Registry::from_inventory(),
        None,
        None,
    );
    pending.start_client(codec, max_frame).await
}

pub async fn accept(
    endpoint: &Endpoint,
    registry: Option<Registry>,
) -> Result<Connection, Error> {
    let registry = registry.unwrap_or_else(Registry::from_inventory);
    let incoming = endpoint.accept().await.ok_or_else(|| {
        io::Error::new(io::ErrorKind::UnexpectedEof, "no incoming connection")
    })?;
    let conn = incoming.await.map_err(to_io_err)?;
    let (send, recv) = conn.accept_bi().await.map_err(to_io_err)?;
    let pending = ConnectionInner::new_pending_from_split(recv, send, registry, None, None);
    pending.start_server().await
}

fn to_io_err<E: std::error::Error>(err: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, err.to_string())
}
