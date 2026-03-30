use std::future::Future;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncRead, AsyncWrite};
use tracing::warn;

use crate::codec::CodecID;
use crate::codec_msgpack::{CodecMsgpackCompact, CodecMsgpackMap};
use crate::codec_postcard::CodecPostcard;
use crate::connection::{ConnectionInner, Netconn};
use crate::context::Context;
use crate::error::Error;
use crate::protocol::{DEFAULT_MAX_FRAME, PROTOCOL_VERSION_V3};
use crate::recovery::{RecoveryRegistry, RecoveryState, ServerRecoveryOptions};
use crate::recovery_protocol::{
    new_recovery_token, read_attach_request, write_attach_response, AttachResponse,
    ATTACH_MODE_NEW, ATTACH_MODE_RESUME, ATTACH_STATUS_OK, ATTACH_STATUS_REJECTED,
};
use crate::registry::Registry;

/// Message server that accepts connections and dispatches handlers.
pub struct Server {
    registry: Registry,
    codecs: Vec<CodecID>,
    recovery: ServerRecoveryOptions,
    recovery_conns: RecoveryRegistry,
    on_connect: Option<Arc<dyn Fn(Netconn) -> Result<(), Error> + Send + Sync>>,
    on_disconnect: Option<Arc<dyn Fn(Netconn) -> Result<(), Error> + Send + Sync>>,
    on_new_stream: Option<Arc<dyn Fn(Context) + Send + Sync>>,
    on_close_stream: Option<Arc<dyn Fn(Context) + Send + Sync>>,
}

impl Server {
    /// Create a server with inventory handlers and built-in codecs.
    pub fn new() -> Self {
        Self {
            registry: Registry::from_inventory(),
            codecs: vec![CodecPostcard, CodecMsgpackCompact, CodecMsgpackMap],
            recovery: ServerRecoveryOptions::default(),
            recovery_conns: Arc::new(Mutex::new(std::collections::HashMap::new())),
            on_connect: None,
            on_disconnect: None,
            on_new_stream: None,
            on_close_stream: None,
        }
    }

    /// Run a callback when a new connection is accepted.
    pub fn on_connect<F>(mut self, f: F) -> Self
    where
        F: Fn(Netconn) -> Result<(), Error> + Send + Sync + 'static,
    {
        self.on_connect = Some(Arc::new(f));
        self
    }

    /// Run a callback when a connection is closed.
    pub fn on_disconnect<F>(mut self, f: F) -> Self
    where
        F: Fn(Netconn) -> Result<(), Error> + Send + Sync + 'static,
    {
        self.on_disconnect = Some(Arc::new(f));
        self
    }

    /// Run a callback when a new stream is opened.
    pub fn on_new_stream<F>(mut self, f: F) -> Self
    where
        F: Fn(Context) + Send + Sync + 'static,
    {
        self.on_new_stream = Some(Arc::new(f));
        self
    }

    /// Run a callback when a stream is closed.
    pub fn on_close_stream<F>(mut self, f: F) -> Self
    where
        F: Fn(Context) + Send + Sync + 'static,
    {
        self.on_close_stream = Some(Arc::new(f));
        self
    }

    /// Override the codec preference list.
    pub fn with_codecs(mut self, codecs: &[CodecID]) -> Self {
        self.codecs = codecs.to_vec();
        self
    }

    /// Configure server-side recovery behavior.
    pub fn with_recovery(mut self, recovery: ServerRecoveryOptions) -> Self {
        self.recovery = recovery.normalized();
        self
    }

    /// Serve on `addr`, using `tcp://`, `uds://`, or `unix://` prefixes.
    ///
    /// Without a scheme, TCP is used by default.
    pub async fn serve(self, addr: &str) -> Result<(), Error> {
        if let Some(stripped) = addr.strip_prefix("tcp://") {
            return self.serve_tcp(stripped).await;
        }
        if let Some(stripped) = addr.strip_prefix("uds://") {
            return self.serve_uds(stripped).await;
        }
        if let Some(stripped) = addr.strip_prefix("unix://") {
            return self.serve_uds(stripped).await;
        }
        self.serve_tcp(addr).await
    }

    async fn serve_tcp(self, addr: &str) -> Result<(), Error> {
        let listener = Arc::new(crate::transport::tcp::listen(addr).await?);
        self.serve_with_accept(move || {
            let listener = listener.clone();
            crate::transport::tcp::accept_stream(listener)
        })
        .await
    }

    async fn serve_uds(self, path: &str) -> Result<(), Error> {
        #[cfg(feature = "uds")]
        {
            let listener = Arc::new(crate::transport::uds::listen(path).await?);
            return self
                .serve_with_accept(move || {
                    let listener = listener.clone();
                    crate::transport::uds::accept_stream(listener)
                })
                .await;
        }
        #[cfg(not(feature = "uds"))]
        {
            let _ = path;
            return Err(Error::UnsupportedTransport(
                "uds transport not enabled".to_string(),
            ));
        }
    }

    async fn serve_with_accept<R, A, F>(self, mut accept: A) -> Result<(), Error>
    where
        R: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        A: FnMut() -> F,
        F: Future<Output = Result<(R, Option<String>), Error>>,
    {
        let server = Arc::new(self);

        loop {
            let (socket, peer_addr) = accept().await?;
            let peer_label = peer_addr
                .clone()
                .unwrap_or_else(|| "client-unknown".to_string());
            let netconn = Netconn::new(peer_addr.clone());
            let server = server.clone();
            let codecs = server.codecs.clone();

            tokio::spawn(async move {
                let mut pending = ConnectionInner::new_pending(
                    socket,
                    server.registry.clone(),
                    server.on_new_stream.clone(),
                    server.on_close_stream.clone(),
                );
                let conn_handle = pending.connection();
                let (reader, writer) = pending.io_mut();
                let config = match crate::protocol::handshake_server(
                    reader,
                    writer,
                    &codecs,
                    DEFAULT_MAX_FRAME,
                    server.recovery.enable,
                )
                .await {
                    Ok(config) => config,
                    Err(err) => {
                        warn!("handshake failed for {peer_label}: {err}");
                        conn_handle.close();
                        return;
                    }
                };

                let conn = if config.version != PROTOCOL_VERSION_V3 {
                    if let Some(ref f) = server.on_connect {
                        if let Err(err) = f(netconn.clone()) {
                            warn!("on_connect failed for {peer_label}: {err}");
                            conn_handle.close();
                            return;
                        }
                    }
                    match pending.start_with_config(config, None, 0) {
                        Ok(conn) => conn,
                        Err(err) => {
                            warn!("start failed for {peer_label}: {err}");
                            conn_handle.close();
                            return;
                        }
                    }
                } else {
                    let request = match read_attach_request(reader).await {
                        Ok(request) => request,
                        Err(err) => {
                            warn!("attach read failed for {peer_label}: {err}");
                            conn_handle.close();
                            return;
                        }
                    };
                    match request.mode {
                        ATTACH_MODE_NEW => {
                            let connection_id = match new_recovery_token() {
                                Ok(token) => token,
                                Err(err) => {
                                    warn!("token generation failed for {peer_label}: {err}");
                                    conn_handle.close();
                                    return;
                                }
                            };
                            let resume_secret = match new_recovery_token() {
                                Ok(token) => token,
                                Err(err) => {
                                    warn!("token generation failed for {peer_label}: {err}");
                                    conn_handle.close();
                                    return;
                                }
                            };
                            let recovery = RecoveryState::new_server(
                                server.recovery.clone(),
                                connection_id,
                                resume_secret,
                                server.recovery_conns.clone(),
                            );
                            {
                                server
                                    .recovery_conns
                                    .lock()
                                    .unwrap()
                                    .insert(connection_id, conn_handle.clone());
                            }
                            let response = AttachResponse {
                                status: ATTACH_STATUS_OK,
                                connection_id,
                                resume_secret,
                                last_recv_seq: 0,
                                negotiated: server.recovery.negotiated(),
                            };
                            if let Err(err) = write_attach_response(writer, &response).await {
                                warn!("attach response failed for {peer_label}: {err}");
                                server.recovery_conns.lock().unwrap().remove(&connection_id);
                                conn_handle.close();
                                return;
                            }
                            if let Some(ref f) = server.on_connect {
                                if let Err(err) = f(netconn.clone()) {
                                    warn!("on_connect failed for {peer_label}: {err}");
                                    server.recovery_conns.lock().unwrap().remove(&connection_id);
                                    conn_handle.close();
                                    return;
                                }
                            }
                            match pending.start_with_config(config, Some(recovery), 0) {
                                Ok(conn) => conn,
                                Err(err) => {
                                    warn!("recovery start failed for {peer_label}: {err}");
                                    server.recovery_conns.lock().unwrap().remove(&connection_id);
                                    conn_handle.close();
                                    return;
                                }
                            }
                        }
                        ATTACH_MODE_RESUME => {
                            let existing = {
                                server
                                    .recovery_conns
                                    .lock()
                                    .unwrap()
                                    .get(&request.connection_id)
                                    .cloned()
                            };
                            let Some(existing) = existing else {
                                let _ = write_attach_response(writer, &AttachResponse { status: ATTACH_STATUS_REJECTED, ..AttachResponse::default() }).await;
                                conn_handle.close();
                                return;
                            };
                            let Some(existing_recovery) = existing.recovery_state() else {
                                let _ = write_attach_response(writer, &AttachResponse { status: ATTACH_STATUS_REJECTED, ..AttachResponse::default() }).await;
                                conn_handle.close();
                                return;
                            };
                            if existing.is_closed() || existing.codec_id() != config.codec_id {
                                let _ = write_attach_response(writer, &AttachResponse { status: ATTACH_STATUS_REJECTED, ..AttachResponse::default() }).await;
                                conn_handle.close();
                                return;
                            }
                            if existing_recovery.resume_secret != request.resume_secret {
                                let _ = write_attach_response(writer, &AttachResponse { status: ATTACH_STATUS_REJECTED, ..AttachResponse::default() }).await;
                                conn_handle.close();
                                return;
                            }
                            let response = AttachResponse {
                                status: ATTACH_STATUS_OK,
                                connection_id: existing_recovery.connection_id,
                                resume_secret: existing_recovery.resume_secret,
                                last_recv_seq: existing_recovery.last_received(),
                                negotiated: existing_recovery.negotiated(),
                            };
                            if let Err(err) = write_attach_response(writer, &response).await {
                                warn!("resume response failed for {peer_label}: {err}");
                                conn_handle.close();
                                return;
                            }
                            existing.attach_transport_parts(pending.into_transport_parts(), request.last_recv_seq);
                            existing
                        }
                        _ => {
                            let _ = write_attach_response(writer, &AttachResponse { status: ATTACH_STATUS_REJECTED, ..AttachResponse::default() }).await;
                            conn_handle.close();
                            return;
                        }
                    }
                };
                conn.wait_closed().await;

                conn.close_all_streams();

                if let Some(ref f) = server.on_disconnect {
                    if let Err(err) = f(netconn) {
                        warn!("on_disconnect failed for {peer_label}: {err}");
                    }
                }
            });
        }
    }
}
