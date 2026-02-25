use tokio::net::{TcpListener, TcpStream};

use crate::error::Error;
use crate::registry::Registry;
use crate::stream::{client as stream_client, server as stream_server, Connection};

pub async fn connect(addr: &str) -> Result<Connection, Error> {
    let stream = TcpStream::connect(addr).await?;
    let peer_addr = stream.peer_addr().ok().map(|addr| addr.to_string());
    Ok(stream_client::new(stream, peer_addr))
}

pub async fn listen(addr: &str) -> Result<TcpListener, Error> {
    Ok(TcpListener::bind(addr).await?)
}

pub(crate) async fn accept_stream<L>(
    listener: L,
) -> Result<(TcpStream, Option<String>), Error>
where
    L: std::ops::Deref<Target = TcpListener>,
{
    let (stream, peer_addr) = listener.accept().await?;
    Ok((stream, Some(peer_addr.to_string())))
}

pub async fn accept(
    listener: &TcpListener,
    registry: Option<Registry>,
) -> Result<Connection, Error> {
    let (stream, peer_addr) = accept_stream(listener).await?;
    Ok(stream_server::new_connection(
        stream,
        peer_addr,
        registry,
        None,
        None,
    )
    .start())
}
