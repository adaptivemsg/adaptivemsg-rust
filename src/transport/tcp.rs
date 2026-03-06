use tokio::net::{TcpListener, TcpStream};

use crate::error::Error;
use crate::registry::Registry;
use crate::stream::{Codec, Connection, ConnectionInner};

pub async fn connect(addr: &str, codec: Codec, max_frame: u32) -> Result<Connection, Error> {
    let stream = TcpStream::connect(addr).await?;
    let pending = ConnectionInner::new_pending(stream, Registry::from_inventory(), None, None);
    pending.start_client(codec, max_frame).await
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
