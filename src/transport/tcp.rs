use tokio::net::{TcpListener, TcpStream};

use crate::error::Error;

pub(crate) async fn dial(addr: &str) -> Result<TcpStream, Error> {
    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    Ok(stream)
}

pub async fn listen(addr: &str) -> Result<TcpListener, Error> {
    Ok(TcpListener::bind(addr).await?)
}

pub(crate) async fn accept_stream<L>(listener: L) -> Result<(TcpStream, Option<String>), Error>
where
    L: std::ops::Deref<Target = TcpListener>,
{
    let (stream, peer_addr) = listener.accept().await?;
    stream.set_nodelay(true)?;
    Ok((stream, Some(peer_addr.to_string())))
}
