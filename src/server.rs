use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::JoinSet;
use tracing::warn;

use crate::codec::CodecID;
use crate::codec_msgpack::{CodecMsgpackCompact, CodecMsgpackMap};
use crate::codec_postcard::CodecPostcard;
use crate::connection::{
    ConnectionInner, ConnectionRef, Netconn, PendingConnection, TransportParts, TransportWriter,
};
use crate::context::Context;
use crate::error::Error;
use crate::protocol::{HandshakeConfig, DEFAULT_MAX_FRAME, PROTOCOL_VERSION_V3};
use crate::recovery::{RecoveryRegistry, RecoveryState, ServerRecoveryOptions};
use crate::recovery_protocol::{
    new_recovery_token, read_attach_request, write_attach_response, AttachRequest, AttachResponse,
    RecoveryToken, ATTACH_MODE_NEW, ATTACH_MODE_RESUME, ATTACH_STATUS_OK, ATTACH_STATUS_REJECTED,
};
use crate::registry::Registry;
use crate::transport::Endpoint;

/// Bounds the handshake and the v3 attach on an accepted connection, so a
/// peer that connects and sends nothing does not hold a task and a file
/// descriptor. It leaves room for lossy links and for a server that a burst
/// of reconnects keeps busy.
const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(120);

/// Bounds for the wait between attempts after accept fails, for example with
/// EMFILE when the process is out of file descriptors; such errors clear once
/// connections close.
const ACCEPT_RETRY_MIN_BACKOFF: Duration = Duration::from_millis(5);
const ACCEPT_RETRY_MAX_BACKOFF: Duration = Duration::from_secs(1);

/// An accept error that concerns one peer only, such as a reset before
/// accept returned; the next accept is unaffected.
fn is_peer_error(err: &Error) -> bool {
    use std::io::ErrorKind;
    matches!(
        err,
        Error::Io(io) if matches!(
            io.kind(),
            ErrorKind::ConnectionAborted | ErrorKind::ConnectionReset | ErrorKind::ConnectionRefused
        )
    )
}

/// The instant the handshake must finish by, or `None` for no limit.
fn handshake_deadline(timeout: Duration) -> Option<tokio::time::Instant> {
    if timeout.is_zero() {
        return None;
    }
    tokio::time::Instant::now().checked_add(timeout)
}

/// Runs one step of the handshake until it completes, `deadline` passes, or
/// the server shuts down. Returns `None`, after logging why, if the step did
/// not succeed.
async fn handshake_step<T, F>(
    deadline: Option<tokio::time::Instant>,
    lifecycle: &Lifecycle,
    what: &str,
    peer: &str,
    fut: F,
) -> Option<T>
where
    F: Future<Output = Result<T, Error>>,
{
    let fut = async {
        match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline, fut).await.ok(),
            None => Some(fut.await),
        }
    };
    tokio::select! {
        biased;
        _ = lifecycle.wait_stopped() => None,
        output = fut => match output {
            Some(Ok(value)) => Some(value),
            Some(Err(err)) => {
                warn!("{what} failed for {peer}: {err}");
                None
            }
            None => {
                warn!("{what} timed out for {peer}");
                None
            }
        },
    }
}

/// Tells the client that its attach request is refused.
async fn reject_attach(writer: &mut TransportWriter) {
    let response = AttachResponse {
        status: ATTACH_STATUS_REJECTED,
        ..AttachResponse::default()
    };
    let _ = write_attach_response(writer, &response).await;
}

/// How the handshake and attach of an accepted connection ended.
enum Attached {
    /// A new connection, served by the task that accepted it.
    New(ConnectionRef),
    /// The transport now belongs to an existing connection, which the task
    /// that accepted that connection serves.
    Resumed,
    /// The connection was refused or failed and must be closed.
    Refused,
}

/// The connections a serving server closes when it shuts down.
struct Lifecycle {
    /// `None` once shutdown has begun.
    conns: Mutex<Option<HashMap<usize, ConnectionRef>>>,
    stopped: Notify,
}

impl Lifecycle {
    fn new() -> Self {
        Self {
            conns: Mutex::new(Some(HashMap::new())),
            stopped: Notify::new(),
        }
    }

    /// Registers `conn` for shutdown to close. Returns false once shutdown
    /// has begun; the caller then closes the connection.
    fn track(&self, conn: &ConnectionRef) -> bool {
        match self.conns.lock().unwrap().as_mut() {
            Some(conns) => {
                conns.insert(Arc::as_ptr(conn) as usize, Arc::clone(conn));
                true
            }
            None => false,
        }
    }

    fn untrack(&self, conn: &ConnectionRef) {
        if let Some(conns) = self.conns.lock().unwrap().as_mut() {
            conns.remove(&(Arc::as_ptr(conn) as usize));
        }
    }

    /// Begins shutdown: stops the handshakes in progress and returns the
    /// established connections to close.
    fn stop(&self) -> Vec<ConnectionRef> {
        let conns = self.conns.lock().unwrap().take();
        self.stopped.notify_waiters();
        conns
            .map(|conns| conns.into_values().collect())
            .unwrap_or_default()
    }

    /// Resolves once shutdown has begun.
    async fn wait_stopped(&self) {
        // notify_waiters only wakes futures that already exist, so create the
        // future before checking.
        let notified = self.stopped.notified();
        if self.conns.lock().unwrap().is_none() {
            return;
        }
        notified.await;
    }
}

/// Compares in time that does not depend on where the tokens differ.
fn tokens_equal(a: &RecoveryToken, b: &RecoveryToken) -> bool {
    let diff = a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y));
    std::hint::black_box(diff) == 0
}

/// Message server that accepts connections and dispatches handlers.
///
/// Automatically discovers handlers registered via `#[message_handler]` through
/// the inventory system. Use lifecycle callbacks (`on_connect`, `on_disconnect`,
/// etc.) to hook into connection and stream events.
///
/// # Example
///
/// ```rust,ignore
/// use adaptivemsg::Server;
///
/// Server::new()
///     .on_connect(|netconn| { println!("connected"); Ok(()) })
///     .serve("tcp://0.0.0.0:9000")
///     .await?;
/// ```
pub struct Server {
    registry: Registry,
    codecs: Vec<CodecID>,
    max_frame: u32,
    handshake_timeout: Duration,
    stream_queue_size: usize,
    recovery: ServerRecoveryOptions,
    recovery_conns: RecoveryRegistry,
    on_connect: Option<Arc<dyn Fn(Netconn) -> Result<(), Error> + Send + Sync>>,
    on_disconnect: Option<Arc<dyn Fn(Netconn) -> Result<(), Error> + Send + Sync>>,
    on_new_stream: Option<Arc<dyn Fn(Context) + Send + Sync>>,
    on_close_stream: Option<Arc<dyn Fn(Context) + Send + Sync>>,
}

impl Server {
    /// Create a server with inventory handlers, built-in codecs, and a
    /// 2-minute handshake timeout.
    pub fn new() -> Self {
        Self {
            registry: Registry::from_inventory(),
            codecs: vec![CodecPostcard, CodecMsgpackCompact, CodecMsgpackMap],
            max_frame: DEFAULT_MAX_FRAME,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            stream_queue_size: 0,
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

    /// Set the maximum payload size per frame that the server accepts and
    /// advertises during the handshake.
    ///
    /// The effective limit for a connection is the smaller of the client's
    /// and the server's values. A larger frame closes the connection. The
    /// default is 16 MiB.
    pub fn with_max_frame(mut self, max_frame: u32) -> Self {
        self.max_frame = max_frame;
        self
    }

    /// Set how long an accepted connection may take to complete the handshake
    /// and, with recovery, the attach; the server closes a connection that
    /// takes longer.
    ///
    /// `Duration::ZERO` disables the limit. The default is 2 minutes. The limit
    /// no longer applies once `on_connect` runs.
    pub fn with_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    /// Set how many received messages each stream buffers: messages waiting
    /// for a handler or for `recv`, and frames waiting to be decoded.
    ///
    /// When a stream's buffer is full, the connection stops reading until
    /// the stream catches up, which holds up its other streams too. A smaller
    /// buffer saves memory on connections with many streams but reaches that
    /// point sooner. The default is 1024; 0 keeps the default.
    pub fn with_stream_queue_size(mut self, size: usize) -> Self {
        self.stream_queue_size = size;
        self
    }

    /// Configure server-side recovery behavior.
    pub fn with_recovery(mut self, recovery: ServerRecoveryOptions) -> Self {
        self.recovery = recovery.normalized();
        self
    }

    /// Serve on `addr`, using `tcp://`, `uds://`, or `unix://` prefixes.
    ///
    /// Without a scheme, TCP is used by default. `serve` returns an error if
    /// it cannot listen on `addr`. A failed accept, for example when the
    /// process runs out of file descriptors, does not end it: it waits briefly
    /// and accepts again. Use
    /// [`serve_with_shutdown`](Self::serve_with_shutdown) to stop serving.
    pub async fn serve(self, addr: &str) -> Result<(), Error> {
        self.serve_with_shutdown(addr, std::future::pending()).await
    }

    /// Serve on `addr` like [`serve`](Self::serve) until `signal` completes,
    /// then shut down.
    ///
    /// Shutting down stops accepting, drops the connections still in the
    /// handshake, and closes the established connections as
    /// `Connection::close` does: messages already queued are delivered, and
    /// recovery sessions end instead of waiting for the client to reconnect.
    /// It returns once every connection has closed and its `on_disconnect`
    /// callback has run.
    pub async fn serve_with_shutdown<S>(self, addr: &str, signal: S) -> Result<(), Error>
    where
        S: Future<Output = ()>,
    {
        match Endpoint::parse(addr) {
            #[cfg(feature = "tcp")]
            Endpoint::Tcp(addr) => self.serve_tcp(addr, signal).await,
            #[cfg(feature = "uds")]
            Endpoint::Uds(path) => self.serve_uds(path, signal).await,
            #[allow(unreachable_patterns)]
            endpoint => {
                drop(signal);
                Err(endpoint.not_enabled())
            }
        }
    }

    #[cfg(feature = "tcp")]
    async fn serve_tcp<S: Future<Output = ()>>(self, addr: &str, signal: S) -> Result<(), Error> {
        let listener = Arc::new(crate::transport::tcp::listen(addr).await?);
        self.serve_with_accept(
            move || {
                let listener = listener.clone();
                async move {
                    let (stream, peer_addr) =
                        crate::transport::tcp::accept_stream(listener).await?;
                    let (reader, writer) = stream.into_split();
                    Ok((crate::transport::parts(reader, writer), peer_addr))
                }
            },
            signal,
        )
        .await
    }

    #[cfg(feature = "uds")]
    async fn serve_uds<S: Future<Output = ()>>(self, path: &str, signal: S) -> Result<(), Error> {
        let listener = Arc::new(crate::transport::uds::listen(path).await?);
        self.serve_with_accept(
            move || {
                let listener = listener.clone();
                async move {
                    let (stream, peer_addr) =
                        crate::transport::uds::accept_stream(listener).await?;
                    let (reader, writer) = stream.into_split();
                    Ok((crate::transport::parts(reader, writer), peer_addr))
                }
            },
            signal,
        )
        .await
    }

    pub(crate) async fn serve_with_accept<A, F, S>(
        self,
        mut accept: A,
        signal: S,
    ) -> Result<(), Error>
    where
        A: FnMut() -> F,
        F: Future<Output = Result<(TransportParts, Option<String>), Error>>,
        S: Future<Output = ()>,
    {
        let server = Arc::new(self);
        let lifecycle = Arc::new(Lifecycle::new());
        let mut tasks = JoinSet::new();
        tokio::pin!(signal);
        let mut backoff = Duration::ZERO;

        loop {
            let accepted = tokio::select! {
                biased;
                _ = &mut signal => break,
                accepted = accept() => accepted,
            };
            while tasks.try_join_next().is_some() {}
            let (parts, peer_addr) = match accepted {
                Ok(accepted) => {
                    backoff = Duration::ZERO;
                    accepted
                }
                Err(err) if is_peer_error(&err) => continue,
                Err(err) => {
                    backoff =
                        (backoff * 2).clamp(ACCEPT_RETRY_MIN_BACKOFF, ACCEPT_RETRY_MAX_BACKOFF);
                    warn!("accept failed, retrying in {backoff:?}: {err}");
                    tokio::select! {
                        biased;
                        _ = &mut signal => break,
                        _ = tokio::time::sleep(backoff) => continue,
                    }
                }
            };
            tasks.spawn(Arc::clone(&server).handle_conn(Arc::clone(&lifecycle), parts, peer_addr));
        }

        // Dropping the accept function closes the listener.
        drop(accept);
        for conn in lifecycle.stop() {
            tasks.spawn(async move { conn.close().await });
        }
        while tasks.join_next().await.is_some() {}
        Ok(())
    }

    /// Serves one accepted connection: the handshake, with recovery the
    /// attach, and then the connection until it closes.
    async fn handle_conn(
        self: Arc<Self>,
        lifecycle: Arc<Lifecycle>,
        parts: TransportParts,
        peer_addr: Option<String>,
    ) {
        let peer = peer_addr
            .clone()
            .unwrap_or_else(|| "client-unknown".to_string());
        let netconn = Netconn::new(peer_addr);
        let mut pending = ConnectionInner::new_pending_from_parts(
            parts,
            self.registry.clone(),
            self.on_new_stream.clone(),
            self.on_close_stream.clone(),
        );
        let conn_handle = pending.connection();
        conn_handle.set_stream_queue_size(self.stream_queue_size);
        let deadline = handshake_deadline(self.handshake_timeout);

        let (reader, writer) = pending.io_mut();
        let handshake = crate::protocol::handshake_server(
            reader,
            writer,
            &self.codecs,
            self.max_frame,
            self.recovery.enable,
        );
        let attached =
            match handshake_step(deadline, &lifecycle, "handshake", &peer, handshake).await {
                None => Attached::Refused,
                Some(config) if config.version != PROTOCOL_VERSION_V3 => {
                    match self.start(pending, config, None, &netconn, &peer) {
                        Some(conn) => Attached::New(conn),
                        None => Attached::Refused,
                    }
                }
                Some(config) => {
                    self.attach(pending, config, deadline, &lifecycle, &netconn, &peer)
                        .await
                }
            };
        let conn = match attached {
            Attached::New(conn) => conn,
            Attached::Resumed => return,
            Attached::Refused => {
                conn_handle.close_now();
                return;
            }
        };

        if !lifecycle.track(&conn) {
            conn.close().await;
        }
        conn.wait_closed().await;
        lifecycle.untrack(&conn);
        if let Some(on_disconnect) = &self.on_disconnect {
            if let Err(err) = on_disconnect(netconn) {
                warn!("on_disconnect failed for {peer}: {err}");
            }
        }
    }

    /// Runs `on_connect`, then starts the connection.
    fn start(
        &self,
        pending: PendingConnection,
        config: HandshakeConfig,
        recovery: Option<Arc<RecoveryState>>,
        netconn: &Netconn,
        peer: &str,
    ) -> Option<ConnectionRef> {
        if let Some(on_connect) = &self.on_connect {
            if let Err(err) = on_connect(netconn.clone()) {
                warn!("on_connect failed for {peer}: {err}");
                return None;
            }
        }
        match pending.start_with_config(config, recovery, 0) {
            Ok(conn) => Some(conn),
            Err(err) => {
                warn!("start failed for {peer}: {err}");
                None
            }
        }
    }

    /// Reads the attach request of a recovery connection and acts on it.
    async fn attach(
        &self,
        mut pending: PendingConnection,
        config: HandshakeConfig,
        deadline: Option<tokio::time::Instant>,
        lifecycle: &Lifecycle,
        netconn: &Netconn,
        peer: &str,
    ) -> Attached {
        let (reader, _) = pending.io_mut();
        let Some(request) = handshake_step(
            deadline,
            lifecycle,
            "attach",
            peer,
            read_attach_request(reader),
        )
        .await
        else {
            return Attached::Refused;
        };
        match request.mode {
            ATTACH_MODE_NEW => self.attach_new(pending, config, netconn, peer).await,
            ATTACH_MODE_RESUME => self.attach_resume(pending, config, &request, peer).await,
            _ => {
                reject_attach(pending.io_mut().1).await;
                Attached::Refused
            }
        }
    }

    /// Starts a new recovery connection.
    async fn attach_new(
        &self,
        mut pending: PendingConnection,
        config: HandshakeConfig,
        netconn: &Netconn,
        peer: &str,
    ) -> Attached {
        let connection_id = new_recovery_token();
        let resume_secret = new_recovery_token();
        let recovery = RecoveryState::new_server(
            self.recovery.clone(),
            connection_id,
            resume_secret,
            self.recovery_conns.clone(),
        );
        self.recovery_conns
            .lock()
            .unwrap()
            .insert(connection_id, pending.connection());
        let response = AttachResponse {
            status: ATTACH_STATUS_OK,
            connection_id,
            resume_secret,
            last_recv_seq: 0,
            negotiated: self.recovery.negotiated(),
        };
        let started = match write_attach_response(pending.io_mut().1, &response).await {
            Ok(()) => self.start(pending, config, Some(recovery), netconn, peer),
            Err(err) => {
                warn!("attach response failed for {peer}: {err}");
                None
            }
        };
        match started {
            Some(conn) => Attached::New(conn),
            None => {
                self.recovery_conns.lock().unwrap().remove(&connection_id);
                Attached::Refused
            }
        }
    }

    /// Moves the transport to the connection that a resume request names.
    async fn attach_resume(
        &self,
        mut pending: PendingConnection,
        config: HandshakeConfig,
        request: &AttachRequest,
        peer: &str,
    ) -> Attached {
        let existing = self
            .recovery_conns
            .lock()
            .unwrap()
            .get(&request.connection_id)
            .cloned();
        let target = existing.and_then(|conn| {
            let recovery = conn.recovery_state()?;
            let accepted = !conn.is_closed()
                && conn.codec_id() == config.codec_id
                && tokens_equal(&recovery.resume_secret, &request.resume_secret);
            accepted.then_some((conn, recovery))
        });
        let Some((existing, recovery)) = target else {
            reject_attach(pending.io_mut().1).await;
            return Attached::Refused;
        };
        let response = AttachResponse {
            status: ATTACH_STATUS_OK,
            connection_id: recovery.connection_id,
            resume_secret: recovery.resume_secret,
            last_recv_seq: recovery.last_received(),
            negotiated: recovery.negotiated(),
        };
        if let Err(err) = write_attach_response(pending.io_mut().1, &response).await {
            warn!("resume response failed for {peer}: {err}");
            return Attached::Refused;
        }
        existing.attach_transport_parts(pending.into_transport_parts(), request.last_recv_seq);
        Attached::Resumed
    }
}
