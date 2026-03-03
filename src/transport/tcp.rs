use tokio::net::{TcpListener, TcpStream};

use crate::error::Error;
use crate::stream::{client as stream_client, Connection, ConnectionInner};

pub async fn connect(addr: &str) -> Result<Connection, Error> {
    let stream = TcpStream::connect(addr).await?;
    let peer_addr = stream.peer_addr().ok().map(|addr| addr.to_string());
    Ok(ConnectionInner::new_pending(
        stream,
        peer_addr,
        stream_client::dispatch(),
        None,
        None,
        None,
    )
    .start())
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
