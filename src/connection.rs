use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::sync::{mpsc, oneshot, Notify};
use tokio::task::{AbortHandle, JoinHandle};
use tracing::{debug, warn};

use crate::codec::{CodecID, CodecImpl, Envelope};
use crate::codec_registry::codec_by_id;
use crate::context::{Context, ContextInner, StreamContext, StreamContextInner};
use crate::debug::{ConnectionDebugCounters, ConnectionDebugState};
use crate::error::Error;
use crate::frame::{build_header, frame_header_len_for_version, parse_header, FRAME_HEADER_LEN_V3};
use crate::protocol::{HandshakeConfig, PROTOCOL_VERSION_V3};
use crate::raw_message::RawMessage;
use crate::recovery::RecoveryState;
use crate::registry::{Handler, Registry};
use crate::stream::{Stream, StreamInner};

#[path = "recovery_runtime.rs"]
mod recovery_runtime;

pub(crate) const STREAM_QUEUE_SIZE: usize = 1024;
const DEFAULT_STREAM_ID: u32 = 0;

pub(crate) type TransportReader = Box<dyn AsyncRead + Unpin + Send>;
pub(crate) type TransportWriter = Box<dyn AsyncWrite + Unpin + Send>;

pub(crate) struct TransportParts {
    pub reader: TransportReader,
    pub writer: TransportWriter,
}

/// Shared handle to a negotiated connection.
pub type Connection = Arc<ConnectionInner>;

#[derive(Clone, Debug)]
/// Connection metadata passed to server callbacks.
pub struct Netconn {
    peer_addr: Option<String>,
}

impl Netconn {
    pub(crate) fn new(peer_addr: Option<String>) -> Self {
        Self { peer_addr }
    }

    /// Peer address string when available.
    pub fn peer_addr(&self) -> Option<&str> {
        self.peer_addr.as_deref()
    }
}

#[derive(Clone)]
pub struct ConnConfig {
    pub version: u8,
    pub codec_id: CodecID,
    pub codec: Arc<dyn CodecImpl>,
    pub max_frame: u32,
}

type InboundFrame = (u32, u64, Vec<u8>);
pub(crate) enum OutboundFrame {
    Plain {
        stream_id: u32,
        payload: Vec<u8>,
    },
    Recovery {
        stream_id: u32,
        payload: Vec<u8>,
        queued_tx: oneshot::Sender<Result<(), Error>>,
    },
}
pub(crate) type HandlerJob = (Arc<dyn Handler>, Box<dyn crate::message::Message>);

enum WriterCommand {
    Attach {
        gen: u64,
        writer: TransportWriter,
        resume_seq: u64,
    },
    Detach {
        gen: u64,
    },
}

pub(crate) struct PendingConnection {
    connection: Connection,
    reader: TransportReader,
    writer: TransportWriter,
    outbound_rx: mpsc::Receiver<OutboundFrame>,
    writer_cmd_rx: mpsc::UnboundedReceiver<WriterCommand>,
}

#[doc(hidden)]
pub struct ConnectionInner {
    self_ref: OnceLock<Weak<ConnectionInner>>,
    outbound_tx: mpsc::Sender<OutboundFrame>,
    stream_contexts: Mutex<HashMap<u32, StreamContext>>,
    on_new_stream: Option<Arc<dyn Fn(Context) + Send + Sync>>,
    on_close_stream: Option<Arc<dyn Fn(Context) + Send + Sync>>,
    registry: Registry,
    closed: AtomicBool,
    writer_abort: OnceLock<AbortHandle>,
    current_reader_abort: Mutex<Option<(u64, AbortHandle)>>,
    closed_notify: Notify,
    send_notify: Notify,
    next_stream_id: AtomicU32,
    next_send_seq: AtomicU64,
    transport_gen: AtomicU64,
    default_stream: OnceLock<StreamContext>,
    config: OnceLock<ConnConfig>,
    recovery: OnceLock<Arc<RecoveryState>>,
    writer_cmd_tx: mpsc::UnboundedSender<WriterCommand>,
    pub(crate) debug: ConnectionDebugCounters,
}

impl PendingConnection {
    pub(crate) fn connection(&self) -> Connection {
        self.connection.clone()
    }

    pub(crate) fn io_mut(&mut self) -> (&mut TransportReader, &mut TransportWriter) {
        (&mut self.reader, &mut self.writer)
    }

    pub(crate) fn start_with_config(
        self,
        config: HandshakeConfig,
        recovery: Option<Arc<RecoveryState>>,
        peer_last_recv_seq: u64,
    ) -> Result<Connection, Error> {
        let config = self.connection.build_config(config)?;
        let _ = self.connection.config.set(config);
        if let Some(recovery) = recovery {
            let _ = self.connection.recovery.set(recovery);
        }
        let connection = self.connection.clone();
        connection.start(self.outbound_rx, self.writer_cmd_rx);
        connection.attach_transport_parts(
            TransportParts {
                reader: self.reader,
                writer: self.writer,
            },
            peer_last_recv_seq,
        );
        Ok(connection)
    }

    pub(crate) fn into_transport_parts(self) -> TransportParts {
        TransportParts {
            reader: self.reader,
            writer: self.writer,
        }
    }
}

impl ConnectionInner {
    pub(crate) fn new_pending<RW>(
        io: RW,
        registry: Registry,
        on_new_stream: Option<Arc<dyn Fn(Context) + Send + Sync>>,
        on_close_stream: Option<Arc<dyn Fn(Context) + Send + Sync>>,
    ) -> PendingConnection
    where
        RW: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (reader, writer) = tokio::io::split(io);
        Self::new_pending_from_split(reader, writer, registry, on_new_stream, on_close_stream)
    }

    pub(crate) fn new_pending_from_split<R, W>(
        reader: R,
        writer: W,
        registry: Registry,
        on_new_stream: Option<Arc<dyn Fn(Context) + Send + Sync>>,
        on_close_stream: Option<Arc<dyn Fn(Context) + Send + Sync>>,
    ) -> PendingConnection
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (connection, outbound_rx, writer_cmd_rx) =
            Self::new_unstarted(registry, on_new_stream, on_close_stream);
        PendingConnection {
            connection,
            reader: Box::new(reader),
            writer: Box::new(BufWriter::new(writer)),
            outbound_rx,
            writer_cmd_rx,
        }
    }

    fn new_unstarted(
        registry: Registry,
        on_new_stream: Option<Arc<dyn Fn(Context) + Send + Sync>>,
        on_close_stream: Option<Arc<dyn Fn(Context) + Send + Sync>>,
    ) -> (
        Connection,
        mpsc::Receiver<OutboundFrame>,
        mpsc::UnboundedReceiver<WriterCommand>,
    ) {
        let (outbound_tx, outbound_rx) = mpsc::channel::<OutboundFrame>(STREAM_QUEUE_SIZE);
        let (writer_cmd_tx, writer_cmd_rx) = mpsc::unbounded_channel();
        let connection: Connection = Arc::new_cyclic(|weak| {
            let self_ref = {
                let lock = OnceLock::new();
                let _ = lock.set(weak.clone());
                lock
            };
            ConnectionInner {
                self_ref,
                outbound_tx,
                stream_contexts: Mutex::new(HashMap::new()),
                on_new_stream,
                on_close_stream,
                registry,
                closed: AtomicBool::new(false),
                writer_abort: OnceLock::new(),
                current_reader_abort: Mutex::new(None),
                closed_notify: Notify::new(),
                send_notify: Notify::new(),
                next_stream_id: AtomicU32::new(1),
                next_send_seq: AtomicU64::new(0),
                transport_gen: AtomicU64::new(0),
                default_stream: OnceLock::new(),
                config: OnceLock::new(),
                recovery: OnceLock::new(),
                writer_cmd_tx,
                debug: ConnectionDebugCounters::new(),
            }
        });
        (connection, outbound_rx, writer_cmd_rx)
    }

    fn try_shared(&self) -> Option<Connection> {
        self.self_ref.get().and_then(Weak::upgrade)
    }

    fn shared(&self) -> Connection {
        self.try_shared().expect("connection weak ref missing")
    }

    fn build_config(&self, config: HandshakeConfig) -> Result<ConnConfig, Error> {
        let codec =
            codec_by_id(config.codec_id).ok_or(Error::UnsupportedCodec(config.codec_id.0))?;
        Ok(ConnConfig {
            version: config.version,
            codec_id: config.codec_id,
            codec,
            max_frame: config.max_frame,
        })
    }

    fn config(&self) -> &ConnConfig {
        self.config.get().expect("connection config missing")
    }

    fn recovery(&self) -> Option<&Arc<RecoveryState>> {
        self.recovery.get()
    }

    pub(crate) fn recovery_state(&self) -> Option<Arc<RecoveryState>> {
        self.recovery().cloned()
    }

    pub(crate) fn codec_id(&self) -> CodecID {
        self.config().codec_id
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    fn close_internal(&self) {
        self.mark_closed();
    }

    /// Wait until the connection is closed.
    pub async fn wait_closed(&self) {
        if self.closed.load(Ordering::Relaxed) {
            return;
        }
        self.closed_notify.notified().await;
    }

    pub(crate) fn close_all_streams(&self) {
        let stream_contexts = std::mem::take(&mut *self.stream_contexts.lock().unwrap());
        for stream_ctx in stream_contexts.values() {
            stream_ctx.stream.close_channels();
            self.notify_close(stream_ctx);
        }
    }

    /// Open a new logical stream on this connection.
    pub fn new_stream(self: &Arc<Self>) -> Stream {
        let stream_id = self.next_stream_id.fetch_add(1, Ordering::Relaxed);
        Arc::clone(&self.make_stream(stream_id).stream)
    }

    /// Send a message on the default stream.
    pub async fn send<M: crate::message::Message>(self: &Arc<Self>, msg: M) -> Result<(), Error> {
        self.default_stream().send(msg).await
    }

    /// Receive the next message on the default stream.
    pub async fn recv<T: crate::message::MessageDecode + 'static>(
        self: &Arc<Self>,
    ) -> Result<T, Error> {
        self.default_stream().recv::<T>().await
    }

    /// Send a request and wait for a response on the default stream.
    pub async fn send_recv<
        TReq: crate::message::Message,
        TResp: crate::message::MessageDecode + 'static,
    >(
        self: &Arc<Self>,
        msg: TReq,
    ) -> Result<TResp, Error> {
        self.default_stream().send_recv::<TReq, TResp>(msg).await
    }

    /// Peek the next wire name on the default stream without consuming it.
    pub async fn peek_wire(self: &Arc<Self>) -> Result<String, Error> {
        self.default_stream().peek_wire().await
    }

    /// Set the receive timeout on the default stream.
    pub fn set_recv_timeout(self: &Arc<Self>, timeout: Duration) {
        self.default_stream().set_recv_timeout(timeout);
    }

    #[cfg(test)]
    pub(crate) fn close_transport_for_test(self: &Arc<Self>) {
        let gen = self.transport_gen.load(Ordering::Acquire);
        if gen > 0 {
            self.detach_transport(gen, true);
        }
    }

    #[cfg(test)]
    pub(crate) fn transport_generation_for_test(&self) -> u64 {
        self.transport_gen.load(Ordering::Acquire)
    }

    /// Close the connection and all streams.
    pub fn close(self: &Arc<Self>) {
        self.close_internal();
    }

    /// Returns a point-in-time diagnostic snapshot of this connection.
    pub fn debug_state(&self) -> ConnectionDebugState {
        let config = self.config.get();
        let (protocol, codec_id, codec_name, max_frame) = match config {
            Some(c) => (
                c.version,
                c.codec_id.0,
                c.codec_id.name().to_string(),
                c.max_frame,
            ),
            None => (0, 0, String::new(), 0),
        };
        let streams_map = self.stream_contexts.lock().unwrap();
        let stream_states: Vec<_> = streams_map
            .values()
            .map(|ctx| ctx.stream.debug_state())
            .collect();
        let stream_count = streams_map.len();
        drop(streams_map);

        let recovery = self
            .recovery()
            .map(|r| r.debug_state(self.transport_gen.load(Ordering::Relaxed)));

        self.debug.build_state(
            self.closed.load(Ordering::Relaxed),
            protocol,
            codec_id,
            codec_name,
            max_frame,
            stream_count,
            self.next_send_seq.load(Ordering::Relaxed),
            stream_states,
            recovery,
        )
    }

    fn default_stream(self: &Connection) -> Stream {
        Arc::clone(&self.get_stream_ctx(DEFAULT_STREAM_ID).stream)
    }

    fn mark_closed(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(handle) = self.writer_abort.get() {
            handle.abort();
        }
        if let Some((_, handle)) = self.current_reader_abort.lock().unwrap().take() {
            handle.abort();
        }
        self.closed_notify.notify_waiters();
        if let Some(recovery) = self.recovery() {
            if let Some(connection) = self.try_shared() {
                recovery.on_closed(&connection);
            }
        }
        self.close_all_streams();
    }

    pub(crate) fn remove_stream(&self, stream_id: u32) -> Option<StreamContext> {
        let stream_ctx = self.stream_contexts.lock().unwrap().remove(&stream_id);
        if let Some(ref ctx) = stream_ctx {
            // Promote stream failure to connection so it survives stream removal.
            ctx.stream.debug.promote_failure_to(&self.debug);
            ctx.stream.close_channels();
            self.debug
                .streams_closed
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        stream_ctx
    }

    pub(crate) fn notify_close(&self, stream_ctx: &StreamContext) {
        if let Some(ref f) = self.on_close_stream {
            f(Arc::clone(&stream_ctx.context));
        }
    }

    fn lookup_stream_ctx(self: &Connection, stream_id: u32) -> Option<StreamContext> {
        if stream_id == DEFAULT_STREAM_ID {
            if let Some(handle) = self.default_stream.get() {
                return Some(handle.clone());
            }
        }
        self.stream_contexts
            .lock()
            .unwrap()
            .get(&stream_id)
            .cloned()
    }

    fn get_stream_ctx(self: &Connection, stream_id: u32) -> StreamContext {
        if let Some(stream_ctx) = self.lookup_stream_ctx(stream_id) {
            return stream_ctx;
        }
        let stream_ctx = self.make_stream(stream_id);
        if let Some(ref f) = self.on_new_stream {
            f(Arc::clone(&stream_ctx.context));
        }
        stream_ctx
    }

    fn get_stream(self: &Connection, stream_id: u32) -> Stream {
        Arc::clone(&self.get_stream_ctx(stream_id).stream)
    }

    fn start(
        self: &Connection,
        outbound_rx: mpsc::Receiver<OutboundFrame>,
        writer_cmd_rx: mpsc::UnboundedReceiver<WriterCommand>,
    ) -> Connection {
        let writer_task = if self.is_recovery_enabled() {
            self.spawn_recovery_writer(writer_cmd_rx, outbound_rx)
        } else {
            self.spawn_plain_writer(writer_cmd_rx, outbound_rx)
        };
        let _ = self.writer_abort.set(writer_task.abort_handle());
        self.clone()
    }

    fn make_stream(self: &Connection, stream_id: u32) -> StreamContext {
        let (inbox_tx, inbox_rx) = mpsc::channel::<RawMessage>(STREAM_QUEUE_SIZE);
        let (incoming_tx, mut incoming_rx) = mpsc::channel::<Vec<u8>>(STREAM_QUEUE_SIZE);
        let (handler_tx, handler_rx) = if self.registry.has_handlers() {
            let (handler_tx, handler_rx) = mpsc::channel::<HandlerJob>(STREAM_QUEUE_SIZE);
            (Some(handler_tx), Some(tokio::sync::Mutex::new(handler_rx)))
        } else {
            (None, None)
        };
        let connection = self.clone();
        let context = Arc::new(ContextInner::new());
        let stream = Arc::new(StreamInner::new(
            stream_id,
            connection.clone(),
            inbox_rx,
            inbox_tx,
            incoming_tx,
            handler_rx,
            handler_tx,
        ));
        let stream_ctx = Arc::new(StreamContextInner::new(
            Arc::clone(&stream),
            Arc::clone(&context),
        ));
        self.stream_contexts
            .lock()
            .unwrap()
            .insert(stream_id, Arc::clone(&stream_ctx));
        if stream_id == DEFAULT_STREAM_ID {
            let _ = self.default_stream.set(Arc::clone(&stream_ctx));
        }
        self.debug
            .streams_opened
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.spawn_handler_task(Arc::clone(&stream_ctx));
        let _ = tokio::spawn({
            let stream = stream.clone();
            async move {
                loop {
                    let payload = tokio::select! {
                        msg = incoming_rx.recv() => match msg {
                            Some(payload) => payload,
                            None => break,
                        },
                        _ = stream.wait_closed() => break,
                    };
                    let raw = match stream.connection.decode_envelope(&payload) {
                        Ok(raw) => {
                            stream
                                .debug
                                .data_messages_received
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            raw
                        }
                        Err(err) => {
                            stream
                                .debug
                                .decode_errors
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            stream
                                .debug
                                .note_failure(crate::debug::FAILURE_STREAM_DECODE, err.to_string());
                            warn!("decode failed: {err}");
                            stream.protocol_error("codec_error", err.to_string()).await;
                            break;
                        }
                    };
                    stream.connection.dispatch_raw(&stream, raw).await;
                }
            }
        });
        stream_ctx
    }

    fn spawn_handler_task(self: &Connection, stream_ctx: StreamContext) {
        if !self.registry.has_handlers() {
            return;
        }
        let stream = Arc::clone(&stream_ctx.stream);
        tokio::spawn(async move {
            loop {
                let (handler, msg) = match stream.recv_handler_job().await {
                    Ok(job) => {
                        stream
                            .debug
                            .handler_calls
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        job
                    }
                    Err(Error::Closed) => break,
                    Err(err) => {
                        warn!("handler recv error: {err}");
                        break;
                    }
                };
                match handler.handle(msg, Arc::clone(&stream_ctx)).await {
                    Ok(Some(reply)) => {
                        let _ = stream.send_boxed(reply).await;
                    }
                    Ok(None) => {
                        let _ = stream
                            .send_boxed(Box::new(crate::message::OkReply {}))
                            .await;
                    }
                    Err(err) => {
                        stream
                            .debug
                            .handler_errors
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        warn!("handler error: {err}");
                        let _ = stream
                            .send_boxed(Box::new(crate::message::ErrorReply::new(
                                "handler_error",
                                err.to_string(),
                            )))
                            .await;
                    }
                }
            }
        });
    }

    async fn dispatch_raw(&self, stream: &Stream, raw: RawMessage) {
        if let Some(handler) = self.registry.handler(&raw.wire) {
            match crate::raw_message::decode_raw_with_registry(raw, &self.registry) {
                Ok(msg) => {
                    let _ = stream.handler_q(handler, msg).await;
                }
                Err(err) => {
                    stream
                        .debug
                        .decode_errors
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    warn!("handler decode error: {err}");
                    let _ = stream.protocol_error("codec_error", err.to_string()).await;
                }
            }
        } else {
            let _ = stream.inbox_q(raw).await;
        }
    }

    fn decode_envelope(&self, payload: &[u8]) -> Result<RawMessage, Error> {
        let config = self.config();
        let Envelope { wire, body } = config.codec.decode_envelope(payload)?;
        Ok(RawMessage {
            wire,
            codec: config.codec_id,
            body,
        })
    }

    pub(crate) fn encode_message(
        &self,
        msg: &dyn crate::message::Message,
    ) -> Result<Vec<u8>, Error> {
        self.config().codec.encode(msg)
    }

    pub(crate) async fn enqueue_frame(&self, frame: OutboundFrame) -> Result<(), Error> {
        if self.recovery().is_some() {
            let (stream_id, payload) = match frame {
                OutboundFrame::Plain { stream_id, payload } => (stream_id, payload),
                OutboundFrame::Recovery {
                    stream_id, payload, ..
                } => (stream_id, payload),
            };
            let (queued_tx, queued_rx) = oneshot::channel();
            self.outbound_tx
                .send(OutboundFrame::Recovery {
                    stream_id,
                    payload,
                    queued_tx,
                })
                .await
                .map_err(|_| Error::Closed)?;
            self.signal_send();
            return Ok(queued_rx.await.map_err(|_| Error::Closed)??);
        }
        self.outbound_tx
            .send(frame)
            .await
            .map_err(|_| Error::Closed)?;
        self.debug
            .data_messages_sent
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    pub(crate) fn next_outbound_seq(&self) -> u64 {
        if self.config().version != PROTOCOL_VERSION_V3 {
            return 0;
        }
        self.next_send_seq.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub(crate) fn signal_send(&self) {
        self.send_notify.notify_one();
    }

    pub(crate) fn is_recovery_enabled(&self) -> bool {
        self.recovery().is_some() && self.config().version == PROTOCOL_VERSION_V3
    }

    fn spawn_plain_writer(
        self: &Connection,
        mut writer_cmd_rx: mpsc::UnboundedReceiver<WriterCommand>,
        mut outbound_rx: mpsc::Receiver<OutboundFrame>,
    ) -> JoinHandle<()> {
        let connection = self.clone();
        tokio::spawn(async move {
            let mut writer: Option<TransportWriter> = None;
            loop {
                tokio::select! {
                    _ = connection.closed_notify.notified() => return,
                    cmd = writer_cmd_rx.recv() => match cmd {
                        Some(WriterCommand::Attach { writer: next_writer, .. }) => {
                            writer = Some(next_writer);
                            connection.debug.transport_attaches.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        Some(WriterCommand::Detach { .. }) => {
                            writer = None;
                            connection.debug.transport_detaches.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        None => return,
                    },
                    maybe_frame = outbound_rx.recv(), if writer.is_some() => match maybe_frame {
                        Some(frame) => {
                            let writer_ref = writer.as_mut().expect("writer missing");
                            // Write first frame without flushing.
                            if let Err(err) = Self::write_plain_frame_no_flush(&connection, writer_ref, frame).await {
                                connection.debug.note_failure(crate::debug::FAILURE_CONNECTION_WRITER, err.to_string());
                                warn!("write failed: {err}");
                                connection.mark_closed();
                                return;
                            }
                            // Drain any queued frames without blocking.
                            while let Ok(frame) = outbound_rx.try_recv() {
                                if let Err(err) = Self::write_plain_frame_no_flush(&connection, writer_ref, frame).await {
                                    connection.debug.note_failure(crate::debug::FAILURE_CONNECTION_WRITER, err.to_string());
                                    warn!("write failed: {err}");
                                    connection.mark_closed();
                                    return;
                                }
                            }
                            // Flush once for the entire batch.
                            if let Err(err) = writer_ref.flush().await {
                                connection.debug.note_failure(crate::debug::FAILURE_CONNECTION_WRITER, err.to_string());
                                warn!("flush failed: {err}");
                                connection.mark_closed();
                                return;
                            }
                        }
                        None => return,
                    }
                }
            }
        })
    }

    async fn write_plain_frame_no_flush(
        connection: &Connection,
        writer: &mut TransportWriter,
        frame: OutboundFrame,
    ) -> Result<(), Error> {
        let (stream_id, payload) = match frame {
            OutboundFrame::Plain { stream_id, payload } => (stream_id, payload),
            OutboundFrame::Recovery {
                stream_id,
                payload,
                queued_tx,
            } => {
                let _ = queued_tx.send(Ok(()));
                (stream_id, payload)
            }
        };
        write_frame_no_flush(connection.config(), writer, stream_id, 0, &payload).await?;
        connection
            .debug
            .frames_written
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        connection
            .debug
            .bytes_written
            .fetch_add(payload.len() as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    fn spawn_plain_reader(
        self: &Connection,
        gen: u64,
        mut reader: TransportReader,
    ) -> JoinHandle<()> {
        let connection = self.clone();
        tokio::spawn(async move {
            loop {
                match read_frame(connection.config(), &mut reader).await {
                    Ok((stream_id, _, payload)) => {
                        connection
                            .debug
                            .frames_read
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        connection
                            .debug
                            .bytes_read
                            .fetch_add(payload.len() as u64, std::sync::atomic::Ordering::Relaxed);
                        let stream = connection.get_stream(stream_id);
                        let _ = stream.incoming_tx.send(payload).await;
                    }
                    Err(err) => {
                        connection
                            .debug
                            .note_failure(crate::debug::FAILURE_CONNECTION_READER, err.to_string());
                        debug!("read loop ended: {err}");
                        connection.detach_transport(gen, false);
                        connection.mark_closed();
                        return;
                    }
                }
            }
        })
    }
}

impl Drop for ConnectionInner {
    fn drop(&mut self) {
        self.close_internal();
    }
}

async fn write_frame<W>(
    config: &ConnConfig,
    writer: &mut W,
    stream_id: u32,
    seq: u64,
    payload: &[u8],
) -> Result<(), Error>
where
    W: AsyncWrite + Unpin,
{
    write_frame_no_flush(config, writer, stream_id, seq, payload).await?;
    writer.flush().await?;
    Ok(())
}

async fn write_frame_no_flush<W>(
    config: &ConnConfig,
    writer: &mut W,
    stream_id: u32,
    seq: u64,
    payload: &[u8],
) -> Result<(), Error>
where
    W: AsyncWrite + Unpin,
{
    let header = build_header(
        config.version,
        stream_id,
        seq,
        payload.len(),
        config.max_frame,
    )?;
    let header_len = frame_header_len_for_version(config.version)?;
    writer.write_all(&header[..header_len]).await?;
    writer.write_all(payload).await?;
    Ok(())
}

async fn read_frame<R>(config: &ConnConfig, reader: &mut R) -> Result<InboundFrame, Error>
where
    R: AsyncRead + Unpin,
{
    let header_len = frame_header_len_for_version(config.version)?;
    let mut header = [0u8; FRAME_HEADER_LEN_V3];
    reader.read_exact(&mut header[..header_len]).await?;
    let (stream_id, seq, payload_len) = parse_header(&header[..header_len], config.version)?;
    if payload_len as u32 > config.max_frame {
        return Err(Error::FrameTooLarge(payload_len));
    }
    let mut payload = vec![0u8; payload_len];
    reader.read_exact(&mut payload).await?;
    Ok((stream_id, seq, payload))
}
