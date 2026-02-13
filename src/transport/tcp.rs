use tokio::net::{TcpListener, TcpStream};

use crate::error::Error;
use crate::registry::Registry;
use crate::stream::{Conn, Connection as ConnectionInner};

pub async fn connect(addr: &str) -> Result<Conn, Error> {
    let stream = TcpStream::connect(addr).await?;
    let peer_addr = stream.peer_addr().ok().map(|addr| addr.to_string());
    Ok(ConnectionInner::new(stream, peer_addr, None, None, None).start())
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
) -> Result<Conn, Error> {
    let (stream, peer_addr) = accept_stream(listener).await?;
    Ok(ConnectionInner::new(stream, peer_addr, registry, None, None).start())
}
