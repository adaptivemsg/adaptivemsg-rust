use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::future::Future;
use std::io::{Cursor, ErrorKind};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use rmpv::Value;
use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::{mpsc, Mutex as AsyncMutex, Notify};
use tokio::task::{AbortHandle, JoinHandle};
use tracing::{debug, warn};

use crate::error::Error;
use crate::message::Message;
use crate::registry::{Handler, Registry};

const STREAM_QUEUE_SIZE: usize = 1024;

type StreamId = u32;

const DEFAULT_STREAM_ID: StreamId = 0;
const RECV_TIMEOUT_NONE: u64 = 0;

const PROTOCOL_VERSION: u8 = 1;
const FRAME_HEADER_LEN: usize = 10;
const HANDSHAKE_MAGIC: [u8; 2] = *b"AM";
const HANDSHAKE_CLIENT_LEN: usize = 12;
const HANDSHAKE_SERVER_LEN: usize = 12;
const DEFAULT_MAX_FRAME: u32 = u32::MAX;

fn timeout_to_nanos(timeout: Duration) -> u64 {
    let nanos = timeout.as_nanos();
    if nanos > u64::MAX as u128 {
        u64::MAX
    } else {
        nanos as u64
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Codec {
    Compact,
    Map,
}

impl Codec {
    fn to_u8(self) -> u8 {
        match self {
            Self::Compact => 1,
            Self::Map => 2,
        }
    }

    fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Compact),
            2 => Some(Self::Map),
            _ => None,
        }
    }
}

impl Default for Codec {
    fn default() -> Self {
        Self::Map
    }
}

#[derive(Clone, Copy, Debug)]
struct ConnConfig {
    version: u8,
    codec: Codec,
    max_frame: u32,
}

#[doc(hidden)]
pub struct ConnectionInner {
    outbound_tx: mpsc::Sender<OutboundFrame>,
    stream_contexts: Mutex<HashMap<StreamId, StreamContext>>,
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

    pub(crate) async fn start_client(mut self, codec: Codec, max_frame: u32) -> Result<Connection, Error> {
        let config = handshake_client(&mut self.reader, &mut self.writer, codec, max_frame).await?;
        let _ = self.connection.config.set(config);
        Ok(self
            .connection
            .start(self.reader, self.writer, self.outbound_rx))
    }

    pub(crate) async fn start_server(mut self) -> Result<Connection, Error> {
        let config = handshake_server(&mut self.reader, &mut self.writer, DEFAULT_MAX_FRAME).await?;
        let _ = self.connection.config.set(config);
        Ok(self
            .connection
            .start(self.reader, self.writer, self.outbound_rx))
    }
}

pub type Stream = Arc<StreamInner>;

pub type StreamContext = Arc<StreamContextInner>;
pub type Context = Arc<ContextInner>;

type InboundFrame = (StreamId, Vec<u8>);
type OutboundFrame = (StreamId, Vec<u8>);
type HandlerJob = (Arc<dyn Handler>, Box<dyn Message>);

#[derive(Deserialize)]
struct MapEnvelope<'a> {
    #[serde(rename = "type")]
    #[serde(borrow)]
    r#type: Cow<'a, str>,
    data: Value,
}

#[doc(hidden)]
pub struct StreamInner {
    id: StreamId,
    connection: Connection,
    handler_rx: Option<AsyncMutex<mpsc::Receiver<HandlerJob>>>,
    handler_tx: Option<mpsc::Sender<HandlerJob>>,
    inbox_rx: AsyncMutex<mpsc::Receiver<Box<dyn Message>>>,
    inbox_tx: mpsc::Sender<Box<dyn Message>>,
    incoming_tx: mpsc::Sender<Vec<u8>>,
    recv_timeout_nanos: AtomicU64,
    recv_active: AtomicBool,
}

#[doc(hidden)]
pub struct ContextInner {
    data: Mutex<Option<Arc<dyn Any + Send + Sync>>>,
}

impl ContextInner {
    pub fn set_context<T>(&self, ctx: Arc<T>)
    where
        T: Any + Send + Sync + 'static,
    {
        *self.data.lock().unwrap() = Some(ctx);
    }

    pub fn get_context<T>(&self) -> Option<Arc<T>>
    where
        T: Any + Send + Sync + 'static,
    {
        self.data
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|ctx| Arc::clone(ctx).downcast::<T>().ok())
    }
}

#[doc(hidden)]
pub struct StreamContextInner {
    stream: Stream,
    context: Context,
    handler_task_active: AtomicBool,
}

impl StreamContextInner {
    pub fn set_context<T>(&self, ctx: Arc<T>)
    where
        T: Any + Send + Sync + 'static,
    {
        self.context.set_context(ctx);
    }

    pub fn get_context<T>(&self) -> Option<Arc<T>>
    where
        T: Any + Send + Sync + 'static,
    {
        self.context.get_context::<T>()
    }

    /// Spawn a background task tied to this stream; only one active task is allowed.
    pub fn new_task<F, Fut>(self: &Arc<Self>, f: F) -> Result<JoinHandle<()>, Error>
    where
        F: FnOnce(Stream) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let stream = Arc::clone(&self.stream);
        if self
            .handler_task_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(Error::HandlerTaskBusy);
        }
        let guard = HandlerTaskGuard {
            ctx: Arc::clone(self),
        };
        let handle = tokio::spawn(async move {
            let _guard = guard;
            f(stream).await;
        });
        Ok(handle)
    }
}

struct RecvGuard<'a> {
    flag: &'a AtomicBool,
}

impl Drop for RecvGuard<'_> {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
    }
}

struct HandlerTaskGuard {
    ctx: Arc<StreamContextInner>,
}

impl Drop for HandlerTaskGuard {
    fn drop(&mut self) {
        self.ctx.handler_task_active.store(false, Ordering::Release);
    }
}

impl ConnectionInner {
    fn mark_closed(&self) {
        self.closed.store(true, Ordering::Relaxed);
        self.closed_notify.notify_waiters();
    }

    fn config(&self) -> &ConnConfig {
        self.config.get().expect("connection config missing")
    }

    fn remove_stream(&self, stream_id: StreamId) -> Option<StreamContext> {
        self.stream_contexts.lock().unwrap().remove(&stream_id)
    }

    fn notify_close(&self, stream_ctx: &StreamContext) {
        if let Some(ref f) = self.on_close_stream {
            f(Arc::clone(&stream_ctx.context));
        }
    }

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

    pub(crate) fn close(&self) {
        self.mark_closed();
        if let Some(handle) = self.reader_abort.get() {
            handle.abort();
        }
        if let Some(handle) = self.writer_abort.get() {
            handle.abort();
        }
    }

    pub(crate) fn close_all_streams(&self) {
        let stream_contexts = std::mem::take(&mut *self.stream_contexts.lock().unwrap());
        for stream_ctx in stream_contexts.values() {
            self.notify_close(stream_ctx);
        }
    }

    pub(crate) async fn wait_closed(&self) {
        if self.closed.load(Ordering::Relaxed) {
            return;
        }
        self.closed_notify.notified().await;
    }

    fn default_stream(self: &Connection) -> Stream {
        Arc::clone(&self.get_stream_ctx(DEFAULT_STREAM_ID).stream)
    }

    pub async fn send<M: Message>(self: &Arc<Self>, msg: M) -> Result<(), Error> {
        self.default_stream().send(msg).await
    }

    /// Only one task should call recv()/send_recv() per connection stream at a time.
    pub async fn recv<T: Message + 'static>(self: &Arc<Self>) -> Result<T, Error> {
        self.default_stream().recv::<T>().await
    }

    /// Only one task should call recv()/send_recv() per connection stream at a time.
    pub async fn send_recv<TReq: Message, TResp: Message + 'static>(
        self: &Arc<Self>,
        msg: TReq,
    ) -> Result<TResp, Error> {
        self.default_stream().send_recv::<TReq, TResp>(msg).await
    }

    pub fn new_stream(self: &Arc<Self>) -> Stream {
        let stream_id = self.next_stream_id.fetch_add(1, Ordering::Relaxed);
        Arc::clone(&self.make_stream(stream_id).stream)
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

    fn spawn_handler_task(self: &Connection, stream_ctx: StreamContext) {
        if !self.registry.has_handlers() {
            return;
        }
        let stream = Arc::clone(&stream_ctx.stream);
        tokio::spawn(async move {
            loop {
                let (handler, msg) = match stream.recv_handler_job().await {
                    Ok(job) => job,
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

    async fn dispatch_message(&self, stream: &Stream, msg: Box<dyn Message>) {
        let wire_name = msg.wire_name();
        if let Some(handler) = self.registry.handler(wire_name) {
            let _ = stream.handler_q(handler, msg).await;
        } else {
            let _ = stream.inbox_q(msg).await;
        }
    }

    fn make_stream(self: &Connection, stream_id: StreamId) -> StreamContext {
        let (inbox_tx, inbox_rx) = mpsc::channel::<Box<dyn Message>>(STREAM_QUEUE_SIZE);
        let (incoming_tx, mut incoming_rx) = mpsc::channel::<Vec<u8>>(STREAM_QUEUE_SIZE);
        let (handler_tx, handler_rx) = if self.registry.has_handlers() {
            let (handler_tx, handler_rx) = mpsc::channel::<HandlerJob>(STREAM_QUEUE_SIZE);
            (Some(handler_tx), Some(AsyncMutex::new(handler_rx)))
        } else {
            (None, None)
        };
        let connection = self.clone();
        let context = Arc::new(ContextInner {
            data: Mutex::new(None),
        });
        let stream = Arc::new(StreamInner {
            id: stream_id,
            connection: connection.clone(),
            handler_rx,
            handler_tx,
            inbox_rx: AsyncMutex::new(inbox_rx),
            inbox_tx,
            incoming_tx,
            recv_timeout_nanos: AtomicU64::new(RECV_TIMEOUT_NONE),
            recv_active: AtomicBool::new(false),
        });
        let stream_ctx = Arc::new(StreamContextInner {
            stream: Arc::clone(&stream),
            context: Arc::clone(&context),
            handler_task_active: AtomicBool::new(false),
        });
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
                    let msg = match stream.connection.decode_message(&payload) {
                        Ok(msg) => msg,
                        Err(err) => {
                            warn!("decode failed: {err}");
                            stream.connection.mark_closed();
                            break;
                        }
                    };

                    stream.connection.dispatch_message(&stream, msg).await;
                }
            }
        });
        stream_ctx
    }

    fn lookup_stream_ctx(self: &Connection, stream_id: StreamId) -> Option<StreamContext> {
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

    fn get_stream_ctx(self: &Connection, stream_id: StreamId) -> StreamContext {
        if let Some(stream_ctx) = self.lookup_stream_ctx(stream_id) {
            return stream_ctx;
        }
        let stream_ctx = self.make_stream(stream_id);
        if let Some(ref f) = self.on_new_stream {
            f(Arc::clone(&stream_ctx.context));
        }
        stream_ctx
    }

    fn get_stream(self: &Connection, stream_id: StreamId) -> Stream {
        Arc::clone(&self.get_stream_ctx(stream_id).stream)
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
                    match err {
                        Error::Io(ref io)
                            if matches!(
                                io.kind(),
                                ErrorKind::BrokenPipe
                                    | ErrorKind::ConnectionReset
                                    | ErrorKind::ConnectionAborted
                            ) =>
                        {
                            debug!("writer closed: {err}");
                        }
                        _ => warn!("write failed: {err}"),
                    }
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

    fn encode_message(&self, msg: &dyn Message) -> Result<Vec<u8>, Error> {
        match self.config().codec {
            Codec::Compact => msg.encode_compact(),
            Codec::Map => msg.encode_map(),
        }
    }

    fn decode_message(&self, payload: &[u8]) -> Result<Box<dyn Message>, Error> {
        match self.config().codec {
            Codec::Compact => self.decode_compact(payload),
            Codec::Map => self.decode_map(payload),
        }
    }

    fn decode_map(&self, payload: &[u8]) -> Result<Box<dyn Message>, Error> {
        let env: MapEnvelope<'_> = rmp_serde::from_slice(payload)?;
        let MapEnvelope { r#type: wire_name, data } = env;
        let factory = match self.registry.message(wire_name.as_ref()) {
            Some(factory) => factory,
            None => return Err(Error::UnknownMessage(wire_name.into_owned())),
        };
        factory.decode_map(data)
    }

    fn decode_compact(&self, payload: &[u8]) -> Result<Box<dyn Message>, Error> {
        let mut cursor = Cursor::new(payload);
        let value = rmpv::decode::read_value(&mut cursor)?;
        let values = match value {
            Value::Array(values) if !values.is_empty() => values,
            _ => {
                return Err(Error::Codec(
                    "compact payload must be a non-empty array".to_string(),
                ))
            }
        };
        let mut iter = values.into_iter();
        let name_value = iter.next().unwrap();
        let name = match &name_value {
            Value::String(s) => s
                .as_str()
                .ok_or_else(|| Error::Codec("compact message name must be utf-8".to_string()))?,
            _ => {
                return Err(Error::Codec(
                    "compact message name must be a string".to_string(),
                ))
            }
        };
        let factory = match self.registry.message(name) {
            Some(factory) => factory,
            None => return Err(Error::UnknownMessage(name.to_string())),
        };
        let values = iter.collect::<Vec<_>>();
        factory.decode_compact(values)
    }

    // Frame layout (big endian for multi-byte fields):
    // [0]: version (u8)
    // [1]: flags (u8)
    // [2..6): stream id (u32)
    // [6..10): payload_len (u32)
    // [10..): payload bytes (MessagePack)
    fn build_header(&self, stream_id: StreamId, payload_len: usize) -> Result<[u8; FRAME_HEADER_LEN], Error> {
        if payload_len > u32::MAX as usize {
            return Err(Error::FrameTooLarge(payload_len));
        }
        let config = self.config();
        if payload_len as u32 > config.max_frame {
            return Err(Error::FrameTooLarge(payload_len));
        }

        let mut header = [0u8; FRAME_HEADER_LEN];
        header[0] = config.version;
        header[1] = 0;
        header[2..6].copy_from_slice(&stream_id.to_be_bytes());
        header[6..10].copy_from_slice(&(payload_len as u32).to_be_bytes());
        Ok(header)
    }

    fn parse_header(&self, header: [u8; FRAME_HEADER_LEN]) -> Result<(StreamId, usize), Error> {
        let version = header[0];
        if version != self.config().version {
            return Err(Error::UnsupportedFrameVersion(version));
        }
        let stream_id = u32::from_be_bytes(header[2..6].try_into().unwrap());
        let len = u32::from_be_bytes(header[6..10].try_into().unwrap()) as usize;
        Ok((stream_id, len))
    }

    async fn write_frame<W>(
        &self,
        writer: &mut W,
        stream_id: StreamId,
        payload: &[u8],
    ) -> Result<(), Error>
    where
        W: AsyncWrite + Unpin,
    {
        let header = self.build_header(stream_id, payload.len())?;
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
        let (stream_id, payload_len) = self.parse_header(header)?;
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
        self.close();
    }
}

impl StreamInner {
    pub fn close(self: &Arc<Self>) {
        if let Some(stream_ctx) = self.connection.remove_stream(self.id) {
            self.connection.notify_close(&stream_ctx);
        }
    }

    pub fn set_recv_timeout(&self, timeout: Duration) {
        let nanos = if timeout.is_zero() {
            RECV_TIMEOUT_NONE
        } else {
            timeout_to_nanos(timeout)
        };
        self.recv_timeout_nanos.store(nanos, Ordering::Relaxed);
    }

    pub async fn send<M: Message>(&self, msg: M) -> Result<(), Error> {
        self.send_boxed(Box::new(msg)).await
    }

    pub(crate) async fn send_boxed(&self, msg: Box<dyn Message>) -> Result<(), Error> {
        let payload = self.connection.encode_message(msg.as_ref())?;
        let frame: OutboundFrame = (self.id, payload);
        self.connection
            .outbound_tx
            .send(frame)
            .await
            .map_err(|_| Error::Closed)
    }

    pub(crate) async fn inbox_q(&self, msg: Box<dyn Message>) -> Result<(), Error> {
        // Enqueue for recv()/send_recv() on this stream.
        self.inbox_tx
            .send(msg)
            .await
            .map_err(|_| Error::Closed)
    }

    pub(crate) async fn handler_q(
        &self,
        handler: Arc<dyn Handler>,
        msg: Box<dyn Message>,
    ) -> Result<(), Error> {
        match self.handler_tx.as_ref() {
            Some(tx) => tx.send((handler, msg)).await.map_err(|_| Error::Closed),
            None => Err(Error::Closed),
        }
    }

    /// Only one task should call recv()/send_recv() on a stream at a time.
    pub async fn recv<T: Message + 'static>(&self) -> Result<T, Error> {
        let msg = self.recv_msg().await?;
        let expected = T::wire_name_static();
        let got = msg.wire_name();
        match msg.downcast::<T>() {
            Ok(val) => Ok(*val),
            Err(_) => Err(Error::TypeMismatch { expected, got }),
        }
    }

    pub(crate) async fn recv_handler_job(&self) -> Result<HandlerJob, Error> {
        let Some(handler_rx) = self.handler_rx.as_ref() else {
            return Err(Error::Closed);
        };
        let mut handler_rx = handler_rx.lock().await;
        handler_rx.recv().await.ok_or(Error::Closed)
    }

    /// Only one task should call recv()/send_recv() on a stream at a time.
    pub async fn send_recv<TReq: Message, TResp: Message + 'static>(
        &self,
        msg: TReq,
    ) -> Result<TResp, Error> {
        self.send(msg).await?;
        let msg = self.recv_msg().await?;
        let got = msg.wire_name();
        let msg = match msg.downcast::<crate::message::ErrorReply>() {
            Ok(err) => {
                let (code, message) = err.into_parts();
                return Err(Error::Remote { code, message });
            }
            Err(msg) => msg,
        };
        let expected = TResp::wire_name_static();
        match msg.downcast::<TResp>() {
            Ok(val) => Ok(*val),
            Err(_) => Err(Error::TypeMismatch { expected, got }),
        }
    }

    fn recv_guard(&self) -> RecvGuard<'_> {
        if self
            .recv_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            panic!("concurrent recv on stream {}", self.id);
        }
        RecvGuard {
            flag: &self.recv_active,
        }
    }

    async fn recv_msg(&self) -> Result<Box<dyn Message>, Error> {
        let _guard = self.recv_guard();
        let timeout_nanos = self.recv_timeout_nanos.load(Ordering::Relaxed);
        let mut inbox = self.inbox_rx.lock().await;
        if timeout_nanos == RECV_TIMEOUT_NONE {
            inbox.recv().await.ok_or(Error::Closed)
        } else {
            let timeout = Duration::from_nanos(timeout_nanos);
            match tokio::time::timeout(timeout, inbox.recv()).await {
                Ok(Some(msg)) => Ok(msg),
                Ok(None) => Err(Error::Closed),
                Err(_) => Err(Error::RecvTimeout),
            }
        }
    }
}

async fn handshake_client<R, W>(
    reader: &mut R,
    writer: &mut W,
    codec: Codec,
    max_frame: u32,
) -> Result<ConnConfig, Error>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut request = [0u8; HANDSHAKE_CLIENT_LEN];
    request[0..2].copy_from_slice(&HANDSHAKE_MAGIC);
    request[2] = PROTOCOL_VERSION;
    request[3] = PROTOCOL_VERSION;
    request[4] = codec.to_u8();
    request[5..7].copy_from_slice(&0u16.to_be_bytes());
    request[7] = 0;
    request[8..12].copy_from_slice(&max_frame.to_be_bytes());
    writer.write_all(&request).await?;
    writer.flush().await?;

    let mut response = [0u8; HANDSHAKE_SERVER_LEN];
    reader.read_exact(&mut response).await?;
    if response[0..2] != HANDSHAKE_MAGIC {
        return Err(Error::BadHandshakeMagic);
    }
    let version = response[2];
    let accept = response[3];
    let _server_flags = u16::from_be_bytes([response[4], response[5]]);
    let _server_reserved = u16::from_be_bytes([response[6], response[7]]);
    let server_max = u32::from_be_bytes([response[8], response[9], response[10], response[11]]);
    if accept == 0 {
        return Err(Error::HandshakeRejected);
    }
    if version != PROTOCOL_VERSION {
        return Err(Error::UnsupportedFrameVersion(version));
    }

    Ok(ConnConfig {
        version,
        codec,
        max_frame: server_max,
    })
}

async fn handshake_server<R, W>(
    reader: &mut R,
    writer: &mut W,
    max_frame: u32,
) -> Result<ConnConfig, Error>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut request = [0u8; HANDSHAKE_CLIENT_LEN];
    reader.read_exact(&mut request).await?;
    if request[0..2] != HANDSHAKE_MAGIC {
        return Err(Error::BadHandshakeMagic);
    }

    let client_min = request[2];
    let client_max = request[3];
    let codec_raw = request[4];
    let _client_flags = u16::from_be_bytes([request[5], request[6]]);
    let _client_reserved = request[7];
    let client_max_frame = u32::from_be_bytes([request[8], request[9], request[10], request[11]]);

    let codec = match Codec::from_u8(codec_raw) {
        Some(codec) => codec,
        None => {
            let _ = write_handshake_reply(writer, PROTOCOL_VERSION, 0, 0, 0).await;
            return Err(Error::UnsupportedCodec(codec_raw));
        }
    };

    if client_min > PROTOCOL_VERSION || client_max < PROTOCOL_VERSION {
        let _ = write_handshake_reply(writer, PROTOCOL_VERSION, 0, 0, 0).await;
        return Err(Error::NoCommonVersion {
            client_min,
            client_max,
            server_min: PROTOCOL_VERSION,
            server_max: PROTOCOL_VERSION,
        });
    }

    let negotiated_max = if client_max_frame == 0 {
        0
    } else {
        client_max_frame.min(max_frame)
    };
    write_handshake_reply(writer, PROTOCOL_VERSION, 1, 0, negotiated_max).await?;

    Ok(ConnConfig {
        version: PROTOCOL_VERSION,
        codec,
        max_frame: negotiated_max,
    })
}

async fn write_handshake_reply<W>(
    writer: &mut W,
    version: u8,
    accept: u8,
    flags: u16,
    max_frame: u32,
) -> Result<(), Error>
where
    W: AsyncWrite + Unpin,
{
    let mut response = [0u8; HANDSHAKE_SERVER_LEN];
    response[0..2].copy_from_slice(&HANDSHAKE_MAGIC);
    response[2] = version;
    response[3] = accept;
    response[4..6].copy_from_slice(&flags.to_be_bytes());
    response[6..8].copy_from_slice(&0u16.to_be_bytes());
    response[8..12].copy_from_slice(&max_frame.to_be_bytes());
    writer.write_all(&response).await?;
    writer.flush().await?;
    Ok(())
}
