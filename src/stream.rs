use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::io::ErrorKind;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::{mpsc, Mutex as AsyncMutex, Notify};
use tokio::task::{AbortHandle, JoinHandle};
use tracing::{debug, warn};

use crate::error::Error;
use crate::message::Message;
use crate::registry::Registry;
use serde::{Deserialize, Serialize};

const STREAM_QUEUE_SIZE: usize = 1024;

type StreamId = u16;

const STREAM_ID_XOR: StreamId = 0xA5A5;
const DEFAULT_STREAM_ID: StreamId = 0;
const DEFAULT_STREAM_ID_ENCODED: u32 =
    ((DEFAULT_STREAM_ID as u32) << 16) | ((DEFAULT_STREAM_ID ^ STREAM_ID_XOR) as u32);

const FRAME_VERSION_MAJOR_V1: u8 = 1;
const FRAME_VERSION_MINOR_V1: u8 = 0;
const FRAME_HEADER_LEN_V1: usize = 12;
const RECV_TIMEOUT_NONE: u64 = 0;

fn timeout_to_nanos(timeout: Duration) -> u64 {
    let nanos = timeout.as_nanos();
    if nanos > u64::MAX as u128 {
        u64::MAX
    } else {
        nanos as u64
    }
}

#[doc(hidden)]
pub struct ConnectionInner {
    outbound_tx: mpsc::Sender<OutboundFrame>,
    stream_contexts: Mutex<HashMap<StreamId, StreamContext>>,
    on_new_stream: Option<Arc<dyn Fn(Context) + Send + Sync>>,
    on_close_stream: Option<Arc<dyn Fn(Context) + Send + Sync>>,
    handler_registry: Option<Registry>,
    closed: AtomicBool,
    reader_abort: OnceLock<AbortHandle>,
    writer_abort: OnceLock<AbortHandle>,
    closed_notify: Notify,
    next_stream_id: AtomicU16,
    default_stream: OnceLock<StreamContext>,
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

    pub(crate) fn start(self) -> Connection {
        self.connection
            .start(self.reader, self.writer, self.outbound_rx)
    }
}

pub type Stream = Arc<StreamInner>;

pub type StreamContext = Arc<StreamContextInner>;
pub type Context = Arc<ContextInner>;

type InboundFrame = (StreamId, Vec<u8>);
type OutboundFrame = (StreamId, Envelope);

#[derive(Serialize, Deserialize)]
pub(crate) struct Envelope {
    meta: Meta,
    msg: Box<dyn Message>,
}

impl Envelope {
    pub(crate) fn new(meta: Meta, msg: Box<dyn Message>) -> Self {
        Self { meta, msg }
    }

    pub(crate) fn msg(&self) -> &dyn Message {
        self.msg.as_ref()
    }

    pub(crate) fn into_msg(self) -> Box<dyn Message> {
        self.msg
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(default)]
pub(crate) struct Meta {}

#[doc(hidden)]
pub struct StreamInner {
    id: StreamId,
    connection: Connection,
    handler_rx: Option<AsyncMutex<mpsc::Receiver<Box<dyn Message>>>>,
    handler_tx: Option<mpsc::Sender<Box<dyn Message>>>,
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
        handler_registry: Option<Registry>,
        on_new_stream: Option<Arc<dyn Fn(Context) + Send + Sync>>,
        on_close_stream: Option<Arc<dyn Fn(Context) + Send + Sync>>,
    ) -> PendingConnection<ReadHalf<RW>, WriteHalf<RW>>
    where
        RW: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (read_half, write_half) = tokio::io::split(io);
        let (connection, outbound_rx) = Self::new_unstarted(
            handler_registry,
            on_new_stream,
            on_close_stream,
        );
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
        handler_registry: Option<Registry>,
        on_new_stream: Option<Arc<dyn Fn(Context) + Send + Sync>>,
        on_close_stream: Option<Arc<dyn Fn(Context) + Send + Sync>>,
    ) -> PendingConnection<R, W>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (connection, outbound_rx) = Self::new_unstarted(
            handler_registry,
            on_new_stream,
            on_close_stream,
        );
        PendingConnection {
            connection,
            reader,
            writer,
            outbound_rx,
        }
    }

    fn new_unstarted(
        handler_registry: Option<Registry>,
        on_new_stream: Option<Arc<dyn Fn(Context) + Send + Sync>>,
        on_close_stream: Option<Arc<dyn Fn(Context) + Send + Sync>>,
    ) -> (Connection, mpsc::Receiver<OutboundFrame>) {
        let (outbound_tx, outbound_rx) = mpsc::channel::<OutboundFrame>(STREAM_QUEUE_SIZE);

        let connection: Connection = Arc::new(ConnectionInner {
            outbound_tx,
            stream_contexts: Mutex::new(HashMap::new()),
            on_new_stream,
            on_close_stream,
            handler_registry,
            closed: AtomicBool::new(false),
            reader_abort: OnceLock::new(),
            writer_abort: OnceLock::new(),
            closed_notify: Notify::new(),
            next_stream_id: AtomicU16::new(1),
            default_stream: OnceLock::new(),
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
        let Some(registry) = self.handler_registry.clone() else {
            return;
        };
        let stream = Arc::clone(&stream_ctx.stream);
        tokio::spawn(async move {
            loop {
                let msg = match stream.recv_handler_boxed().await {
                    Ok(msg) => msg,
                    Err(Error::Closed) => break,
                    Err(err) => {
                        warn!("handler recv error: {err}");
                        break;
                    }
                };
                let type_name = msg.type_name();
                let Some(handler) = registry.handler(type_name) else {
                    warn!("no handler for message type: {type_name}");
                    continue;
                };
                match handler.handle(msg, Arc::clone(&stream_ctx)).await {
                    Ok(Some(reply)) => {
                        let _ = stream.send_boxed(reply).await;
                    }
                    Ok(None) => {
                        let _ = stream
                            .send_boxed(Box::new(crate::message::OkReply))
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
                            )
                            .await;
                    }
                }
            }
        });
    }

    async fn dispatch_envelope(&self, stream: &Stream, env: Envelope) {
        if let Some(registry) = self.handler_registry.as_ref() {
            let type_name = env.msg().type_name();
            if registry.handler(type_name).is_some() {
                let _ = stream.handler_q(env.into_msg()).await;
                return;
            }
        }

        let _ = stream.inbox_q(env.into_msg()).await;
    }

    fn make_stream(self: &Connection, stream_id: StreamId) -> StreamContext {
        let (inbox_tx, inbox_rx) = mpsc::channel::<Box<dyn Message>>(STREAM_QUEUE_SIZE);
        let (incoming_tx, mut incoming_rx) = mpsc::channel::<Vec<u8>>(STREAM_QUEUE_SIZE);
        let (handler_tx, handler_rx) = if self.handler_registry.is_some() {
            let (handler_tx, handler_rx) = mpsc::channel::<Box<dyn Message>>(STREAM_QUEUE_SIZE);
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
                    let env = match postcard::from_bytes::<Envelope>(&payload) {
                        Ok(env) => env,
                        Err(err) => {
                            warn!("decode failed: {err}");
                            stream.connection.mark_closed();
                            break;
                        }
                    };

                    stream.connection.dispatch_envelope(&stream, env).await;
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
            while let Some((stream_id, env)) = outbound_rx.recv().await {
                if let Err(err) = Self::write_envelope(&mut writer, stream_id, &env).await {
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
                let (stream_id, payload) = match Self::read_frame(&mut reader).await {
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

    // Frame layout (big endian for multi-byte fields):
    // [0]: version major (u8)
    // [1]: version minor (u8)
    // [2..6): stream id word (u16 part A | u16 part B)
    //   - part A: raw id
    //   - part B: id ^ STREAM_ID_XOR
    // [6..8): reserved (u16)
    // [8..12): payload_len (u32)
    // [12..): payload bytes (postcard-encoded Envelope: meta + msg)
    fn build_header(
        stream_id: StreamId,
        payload_len: usize,
    ) -> Result<[u8; FRAME_HEADER_LEN_V1], Error> {
        if payload_len > u32::MAX as usize {
            return Err(Error::FrameTooLarge(payload_len));
        }

        let mut header = [0u8; FRAME_HEADER_LEN_V1];
        let raw_stream_id = if stream_id == DEFAULT_STREAM_ID {
            DEFAULT_STREAM_ID_ENCODED
        } else {
            ((stream_id as u32) << 16) | ((stream_id ^ STREAM_ID_XOR) as u32)
        };

        header[0] = FRAME_VERSION_MAJOR_V1;
        header[1] = FRAME_VERSION_MINOR_V1;
        header[2..6].copy_from_slice(&raw_stream_id.to_be_bytes());
        header[8..12].copy_from_slice(&(payload_len as u32).to_be_bytes());
        Ok(header)
    }

    fn parse_header(header: [u8; FRAME_HEADER_LEN_V1]) -> Result<(StreamId, usize), Error> {
        let major = header[0];
        let minor = header[1];
        if major != FRAME_VERSION_MAJOR_V1 || minor != FRAME_VERSION_MINOR_V1 {
            return Err(Error::UnsupportedFrameVersion { major, minor });
        }

        let raw_stream_id = u32::from_be_bytes(header[2..6].try_into().unwrap());
        let stream_id = if raw_stream_id == DEFAULT_STREAM_ID_ENCODED {
            DEFAULT_STREAM_ID
        } else {
            let part_a = (raw_stream_id >> 16) as u16;
            let part_b = (raw_stream_id & 0xFFFF) as u16;
            let id_b = part_b ^ STREAM_ID_XOR;
            if part_a != id_b {
                return Err(Error::StreamIdMismatch { a: part_a, b: id_b });
            }
            part_a
        };
        let len = u32::from_be_bytes(header[8..12].try_into().unwrap()) as usize;
        Ok((stream_id, len))
    }

    async fn write_envelope<W>(
        writer: &mut W,
        stream_id: StreamId,
        env: &Envelope,
    ) -> Result<(), Error>
    where
        W: AsyncWrite + Unpin,
    {
        let payload = postcard::to_stdvec(env)?;
        let header = Self::build_header(stream_id, payload.len())?;
        writer.write_all(&header).await?;
        writer.write_all(&payload).await?;
        writer.flush().await?;
        Ok(())
    }

    async fn read_frame<R>(reader: &mut R) -> Result<InboundFrame, Error>
    where
        R: AsyncRead + Unpin,
    {
        let mut header = [0u8; FRAME_HEADER_LEN_V1];
        reader.read_exact(&mut header).await?;
        let (stream_id, payload_len) = Self::parse_header(header)?;
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
        let env = Envelope::new(Meta::default(), msg);
        let frame: OutboundFrame = (self.id, env);
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

    pub(crate) async fn handler_q(&self, msg: Box<dyn Message>) -> Result<(), Error> {
        match self.handler_tx.as_ref() {
            Some(tx) => tx.send(msg).await.map_err(|_| Error::Closed),
            None => Err(Error::Closed),
        }
    }

    /// Only one task should call recv()/send_recv() on a stream at a time.
    pub async fn recv<T: Message + 'static>(&self) -> Result<T, Error> {
        let msg = self.recv_msg().await?;
        let expected = std::any::type_name::<T>();
        let got = msg.type_name();
        match msg.downcast::<T>() {
            Ok(val) => Ok(*val),
            Err(_) => Err(Error::TypeMismatch { expected, got }),
        }
    }

    pub(crate) async fn recv_handler_boxed(&self) -> Result<Box<dyn Message>, Error> {
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
        let got = msg.type_name();
        let msg = match msg.downcast::<crate::message::ErrorReply>() {
            Ok(err) => {
                let (code, message) = err.into_parts();
                return Err(Error::Remote { code, message });
            }
            Err(msg) => msg,
        };
        let expected = std::any::type_name::<TResp>();
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
