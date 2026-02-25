use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};

use crate::stream::core::{Connection, ConnectionInner, DispatchFn, Stream};
use crate::wire::Envelope;

pub(crate) fn new<RW>(
    io: RW,
    peer_addr: Option<String>,
) -> Connection
where
    RW: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (connection, reader, writer, outbound_receiver) =
        ConnectionInner::new_with_dispatch(io, peer_addr, client_dispatch(), None, None);
    connection.start(reader, writer, outbound_receiver)
}

pub(crate) fn from_split<R, W>(
    reader: R,
    writer: W,
    peer_addr: Option<String>,
) -> Connection
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (connection, reader, writer, outbound_receiver) =
        ConnectionInner::from_split_with_dispatch(reader, writer, peer_addr, client_dispatch(), None, None);
    connection.start(reader, writer, outbound_receiver)
}

fn client_dispatch() -> DispatchFn {
    Arc::new(|stream: Stream, env: Envelope| {
        Box::pin(async move {
            let _ = stream.push_env(env).await;
        })
    })
}
