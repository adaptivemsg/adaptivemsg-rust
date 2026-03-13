use std::io;

use quinn::Endpoint;

use crate::codec::CodecID;
use crate::connection::{Connection, ConnectionInner};
use crate::error::Error;
use crate::registry::Registry;

pub async fn connect(
    endpoint: &Endpoint,
    addr: &str,
    server_name: &str,
    registry: Registry,
    codecs: &[CodecID],
    max_frame: u32,
) -> Result<Connection, Error> {
    let addr = addr.parse().map_err(to_io_err)?;
    let conn = endpoint
        .connect(addr, server_name)
        .map_err(to_io_err)?
        .await
        .map_err(to_io_err)?;
    let (send, recv) = conn.open_bi().await.map_err(to_io_err)?;
    let pending = ConnectionInner::new_pending_from_split(recv, send, registry, None, None);
    pending.start_client(codecs, max_frame).await
}

pub async fn accept(
    endpoint: &Endpoint,
    registry: Registry,
    codecs: &[CodecID],
    max_frame: u32,
) -> Result<Connection, Error> {
    let incoming = endpoint.accept().await.ok_or_else(|| {
        io::Error::new(io::ErrorKind::UnexpectedEof, "no incoming connection")
    })?;
    let conn = incoming.await.map_err(to_io_err)?;
    let (send, recv) = conn.accept_bi().await.map_err(to_io_err)?;
    let pending = ConnectionInner::new_pending_from_split(recv, send, registry, None, None);
    pending.start_server(codecs, max_frame).await
}

fn to_io_err<E: std::error::Error>(err: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, err.to_string())
}
