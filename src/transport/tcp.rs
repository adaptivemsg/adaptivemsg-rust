use tokio::net::{TcpListener, TcpStream};

use crate::codec::CodecID;
use crate::connection::{Connection, ConnectionInner};
use crate::error::Error;
use crate::registry::Registry;

pub async fn connect(
    addr: &str,
    registry: Registry,
    codecs: &[CodecID],
    max_frame: u32,
) -> Result<Connection, Error> {
    let stream = TcpStream::connect(addr).await?;
    let pending = ConnectionInner::new_pending(stream, registry, None, None);
    pending.start_client(codecs, max_frame).await
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
