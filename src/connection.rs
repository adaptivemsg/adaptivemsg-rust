use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::{mpsc, Notify};
use tokio::task::{AbortHandle, JoinHandle};
use tracing::{debug, warn};

use crate::codec::{CodecID, CodecImpl, Envelope};
use crate::codec_registry::codec_by_id;
use crate::context::{Context, ContextInner, StreamContext, StreamContextInner};
use crate::error::Error;
use crate::frame::{build_header, parse_header, FRAME_HEADER_LEN};
use crate::protocol::{handshake_client, handshake_server, HandshakeConfig};
use crate::raw_message::RawMessage;
use crate::registry::{Handler, Registry};
use crate::stream::{Stream, StreamInner};

pub(crate) const STREAM_QUEUE_SIZE: usize = 1024;
const DEFAULT_STREAM_ID: u32 = 0;

pub type Connection = Arc<ConnectionInner>;

#[derive(Clone, Debug)]
pub struct Netconn {
    peer_addr: Option<String>,
}

impl Netconn {
    pub(crate) fn new(peer_addr: Option<String>) -> Self {
        Self { peer_addr }
    }

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

#[doc(hidden)]
pub struct ConnectionInner {
    outbound_tx: mpsc::Sender<OutboundFrame>,
    stream_contexts: Mutex<HashMap<u32, StreamContext>>,
    on_new_stream: Option<Arc<dyn Fn(Context) + Send + Sync>>,
    on_close_stream: Option<Arc<dyn Fn(Context) + Send + Sync>>,
    registry: Registry,
    closed: AtomicBool,
    reader_abort: OnceLock<AbortHandle>,
    writer_abort: OnceLock<AbortHandle>,
    closed_notify: Notify,
    next_stream_id: AtomicU32,
    default_stream: OnceLock<StreamContext>,
    config: OnceLock<ConnConfig>,
}

type InboundFrame = (u32, Vec<u8>);
type OutboundFrame = (u32, Vec<u8>);
pub(crate) type HandlerJob = (Arc<dyn Handler>, RawMessage);

pub(crate) struct PendingConnection<R, W> {
    connection: Connection,
    reader: R,
    writer: W,
    outbound_rx: mpsc::Receiver<OutboundFrame>,
}

impl<R, W> PendingConnection<R, W>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    pub(crate) fn connection(&self) -> Connection {
        self.connection.clone()
    }

    pub(crate) async fn start_client(
        mut self,
        codecs: &[CodecID],
        max_frame: u32,
    ) -> Result<Connection, Error> {
        let config = handshake_client(&mut self.reader, &mut self.writer, codecs, max_frame).await?;
        let config = self.connection.build_config(config)?;
        let _ = self.connection.config.set(config);
        Ok(self
            .connection
            .start(self.reader, self.writer, self.outbound_rx))
    }

    pub(crate) async fn start_server(
        mut self,
        codecs: &[CodecID],
        max_frame: u32,
    ) -> Result<Connection, Error> {
        let config = handshake_server(&mut self.reader, &mut self.writer, codecs, max_frame).await?;
        let config = self.connection.build_config(config)?;
        let _ = self.connection.config.set(config);
        Ok(self
            .connection
            .start(self.reader, self.writer, self.outbound_rx))
    }
}

impl ConnectionInner {
    pub(crate) fn new_pending<RW>(
        io: RW,
        registry: Registry,
        on_new_stream: Option<Arc<dyn Fn(Context) + Send + Sync>>,
        on_close_stream: Option<Arc<dyn Fn(Context) + Send + Sync>>,
    ) -> PendingConnection<ReadHalf<RW>, WriteHalf<RW>>
    where
        RW: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (read_half, write_half) = tokio::io::split(io);
        let (connection, outbound_rx) =
            Self::new_unstarted(registry, on_new_stream, on_close_stream);
        PendingConnection {
            connection,
            reader: read_half,
            writer: write_half,
            outbound_rx,
        }
    }

    #[cfg(feature = "quic")]
    pub(crate) fn new_pending_from_split<R, W>(
        reader: R,
        writer: W,
        registry: Registry,
        on_new_stream: Option<Arc<dyn Fn(Context) + Send + Sync>>,
        on_close_stream: Option<Arc<dyn Fn(Context) + Send + Sync>>,
    ) -> PendingConnection<R, W>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (connection, outbound_rx) =
            Self::new_unstarted(registry, on_new_stream, on_close_stream);
        PendingConnection {
            connection,
            reader,
            writer,
            outbound_rx,
        }
    }

    fn new_unstarted(
        registry: Registry,
        on_new_stream: Option<Arc<dyn Fn(Context) + Send + Sync>>,
        on_close_stream: Option<Arc<dyn Fn(Context) + Send + Sync>>,
    ) -> (Connection, mpsc::Receiver<OutboundFrame>) {
        let (outbound_tx, outbound_rx) = mpsc::channel::<OutboundFrame>(STREAM_QUEUE_SIZE);
        let connection: Connection = Arc::new(ConnectionInner {
            outbound_tx,
            stream_contexts: Mutex::new(HashMap::new()),
            on_new_stream,
            on_close_stream,
            registry,
            closed: AtomicBool::new(false),
            reader_abort: OnceLock::new(),
            writer_abort: OnceLock::new(),
            closed_notify: Notify::new(),
            next_stream_id: AtomicU32::new(1),
            default_stream: OnceLock::new(),
            config: OnceLock::new(),
        });
        (connection, outbound_rx)
    }

    fn build_config(&self, config: HandshakeConfig) -> Result<ConnConfig, Error> {
        let codec = codec_by_id(config.codec_id).ok_or(Error::UnsupportedCodec(config.codec_id.0))?;
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

    fn close_internal(&self) {
        self.mark_closed();
        self.close_all_streams();
        if let Some(handle) = self.reader_abort.get() {
            handle.abort();
        }
        if let Some(handle) = self.writer_abort.get() {
            handle.abort();
        }
    }

    pub async fn wait_closed(&self) {
        if self.closed.load(Ordering::Relaxed) {
            return;
        }
        self.closed_notify.notified().await;
    }

    pub(crate) fn close_all_streams(&self) {
        let stream_contexts = std::mem::take(&mut *self.stream_contexts.lock().unwrap());
        for stream_ctx in stream_contexts.values() {
            self.notify_close(stream_ctx);
        }
    }

    pub fn new_stream(self: &Arc<Self>) -> Stream {
        let stream_id = self.next_stream_id.fetch_add(1, Ordering::Relaxed);
        Arc::clone(&self.make_stream(stream_id).stream)
    }

    pub async fn send<M: crate::message::Message>(self: &Arc<Self>, msg: M) -> Result<(), Error> {
        self.default_stream().send(msg).await
    }

    pub async fn recv<T: crate::message::MessageDecode + 'static>(
        self: &Arc<Self>,
    ) -> Result<T, Error> {
        self.default_stream().recv::<T>().await
    }

    pub async fn send_recv<TReq: crate::message::Message, TResp: crate::message::MessageDecode + 'static>(
        self: &Arc<Self>,
        msg: TReq,
    ) -> Result<TResp, Error> {
        self.default_stream().send_recv::<TReq, TResp>(msg).await
    }

    pub async fn peek_wire(self: &Arc<Self>) -> Result<String, Error> {
        self.default_stream().peek_wire().await
    }

    pub fn set_recv_timeout(self: &Arc<Self>, timeout: std::time::Duration) {
        self.default_stream().set_recv_timeout(timeout);
    }

    pub fn close(self: &Arc<Self>) {
        self.close_internal();
    }


    fn default_stream(self: &Connection) -> Stream {
        Arc::clone(&self.get_stream_ctx(DEFAULT_STREAM_ID).stream)
    }

    fn mark_closed(&self) {
        self.closed.store(true, Ordering::Relaxed);
        self.closed_notify.notify_waiters();
    }

    pub(crate) fn remove_stream(&self, stream_id: u32) -> Option<StreamContext> {
        self.stream_contexts.lock().unwrap().remove(&stream_id)
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

    pub(crate) fn start<R, W>(
        self: &Connection,
        reader: R,
        writer: W,
        outbound_rx: mpsc::Receiver<OutboundFrame>,
    ) -> Connection
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let writer_task = self.spawn_writer(writer, outbound_rx);
        let _ = self.writer_abort.set(writer_task.abort_handle());

        let reader_task = self.spawn_reader(reader);
        let _ = self.reader_abort.set(reader_task.abort_handle());

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
        self.spawn_handler_task(Arc::clone(&stream_ctx));
        let _ = tokio::spawn({
            let stream = stream.clone();
            async move {
                while let Some(payload) = incoming_rx.recv().await {
                    let raw = match stream.connection.decode_envelope(&payload) {
                        Ok(raw) => raw,
                        Err(err) => {
                            warn!("decode failed: {err}");
                            stream.connection.mark_closed();
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
                let (handler, raw) = match stream.recv_handler_job().await {
                    Ok(job) => job,
                    Err(Error::Closed) => break,
                    Err(err) => {
                        warn!("handler recv error: {err}");
                        break;
                    }
                };
                let msg = match crate::raw_message::decode_raw_with_registry(
                    raw,
                    &stream.connection.registry,
                ) {
                    Ok(msg) => msg,
                    Err(err) => {
                        warn!("handler decode error: {err}");
                        stream.protocol_error("codec_error", err.to_string()).await;
                        continue;
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
            let _ = stream.handler_q(handler, raw).await;
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

    pub(crate) fn encode_message(&self, msg: &dyn crate::message::Message) -> Result<Vec<u8>, Error> {
        self.config().codec.encode(msg)
    }

    pub(crate) async fn enqueue_frame(&self, frame: OutboundFrame) -> Result<(), Error> {
        self.outbound_tx
            .send(frame)
            .await
            .map_err(|_| Error::Closed)
    }

    fn spawn_writer<W>(
        self: &Connection,
        mut writer: W,
        mut outbound_rx: mpsc::Receiver<OutboundFrame>,
    ) -> JoinHandle<()>
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let connection = self.clone();
        tokio::spawn(async move {
            while let Some((stream_id, payload)) = outbound_rx.recv().await {
                if let Err(err) = connection.write_frame(&mut writer, stream_id, &payload).await {
                    warn!("write failed: {err}");
                    connection.mark_closed();
                    break;
                }
            }
        })
    }

    fn spawn_reader<R>(self: &Connection, mut reader: R) -> JoinHandle<()>
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        let connection = self.clone();
        tokio::spawn(async move {
            loop {
                let (stream_id, payload) = match connection.read_frame(&mut reader).await {
                    Ok(frame) => frame,
                    Err(err) => {
                        debug!("read loop ended: {err}");
                        connection.mark_closed();
                        break;
                    }
                };
                let stream = connection.get_stream(stream_id);
                let _ = stream.incoming_tx.send(payload).await;
            }
        })
    }

    async fn write_frame<W>(
        &self,
        writer: &mut W,
        stream_id: u32,
        payload: &[u8],
    ) -> Result<(), Error>
    where
        W: AsyncWrite + Unpin,
    {
        let config = self.config();
        let header = build_header(config.version, stream_id, payload.len(), config.max_frame)?;
        writer.write_all(&header).await?;
        writer.write_all(payload).await?;
        writer.flush().await?;
        Ok(())
    }

    async fn read_frame<R>(&self, reader: &mut R) -> Result<InboundFrame, Error>
    where
        R: AsyncRead + Unpin,
    {
        let mut header = [0u8; FRAME_HEADER_LEN];
        reader.read_exact(&mut header).await?;
        let (stream_id, payload_len) = parse_header(header, self.config().version)?;
        if payload_len as u32 > self.config().max_frame {
            return Err(Error::FrameTooLarge(payload_len));
        }
        let mut payload = vec![0u8; payload_len];
        reader.read_exact(&mut payload).await?;
        Ok((stream_id, payload))
    }
}

impl Drop for ConnectionInner {
    fn drop(&mut self) {
        self.close_internal();
    }
}
