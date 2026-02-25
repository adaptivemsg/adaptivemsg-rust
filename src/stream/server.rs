use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};
use tracing::warn;

use crate::registry::{ContextStream, Registry};
use crate::stream::core::{Connection, ConnectionInner, ConnectionParts, DispatchFn, Stream};
use crate::wire::{Envelope, Meta};

pub(crate) struct ServerConnection<R, W> {
    parts: ConnectionParts<R, W>,
}

impl<R, W> ServerConnection<R, W>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    pub(crate) fn connection(&self) -> Connection {
        let (connection, _, _, _) = &self.parts;
        connection.clone()
    }

    pub(crate) fn start(self) -> Connection {
        let (connection, reader, writer, outbound_receiver) = self.parts;
        connection.start(reader, writer, outbound_receiver)
    }
}

pub(crate) fn new_connection<RW>(
    io: RW,
    peer_addr: Option<String>,
    registry: Option<Registry>,
    on_new_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
    on_close_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
) -> ServerConnection<ReadHalf<RW>, WriteHalf<RW>>
where
    RW: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    ServerConnection {
        parts: ConnectionInner::new_with_dispatch(
            io,
            peer_addr,
            server_dispatch(registry),
            on_new_stream,
            on_close_stream,
        ),
    }
}

pub(crate) fn from_split_connection<R, W>(
    reader: R,
    writer: W,
    peer_addr: Option<String>,
    registry: Option<Registry>,
    on_new_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
    on_close_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
) -> ServerConnection<R, W>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    ServerConnection {
        parts: ConnectionInner::from_split_with_dispatch(
            reader,
            writer,
            peer_addr,
            server_dispatch(registry),
            on_new_stream,
            on_close_stream,
        ),
    }
}

fn server_dispatch(registry: Option<Registry>) -> DispatchFn {
    Arc::new(move |stream: Stream, env: Envelope| {
        let registry = registry.clone();
        Box::pin(async move {
            if let Some(registry) = registry.as_ref() {
                let type_name = env.msg().type_name();
                if let Some(handler) = registry.handler(type_name) {
                    let (meta, msg) = env.into_parts();
                    let ctx = ContextStream::new(stream.clone(), meta);
                    match handler.handle(msg, ctx).await {
                        Ok(Some(reply)) => {
                            let _ = stream.send_boxed(reply, Meta::default()).await;
                        }
                        Ok(None) => {
                            let _ = stream
                                .send_boxed(Box::new(crate::message::OkReply), Meta::default())
                                .await;
                        }
                        Err(err) => {
                            warn!("handler error: {err}");
                            let _ = stream
                                .send_boxed(
                                    Box::new(crate::message::ErrorReply::new(
                                        "handler_error",
                                        err.to_string(),
                                    )),
                                    Meta::default(),
                                )
                                .await;
                        }
                    }
                    return;
                }
            }

            let _ = stream.push_env(env).await;
        })
    })
}
