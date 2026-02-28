use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::{mpsc, Mutex as AsyncMutex, Notify};
use tokio::task::{AbortHandle, JoinHandle};
use tracing::{debug, warn};

use crate::error::Error;
use crate::message::Message;
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

pub(crate) type DispatchFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
pub(crate) type DispatchFn = Arc<dyn Fn(Stream, Envelope) -> DispatchFuture + Send + Sync>;

#[doc(hidden)]
pub struct ConnectionInner {
    outbound_tx: mpsc::Sender<OutboundFrame>,
    streams: Mutex<HashMap<StreamId, Stream>>,
    peer_addr: Option<String>,
    on_new_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
    on_close_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
    dispatch: DispatchFn,
    closed: AtomicBool,
    reader_abort: OnceLock<AbortHandle>,
    writer_abort: OnceLock<AbortHandle>,
    closed_notify: Notify,
    next_stream_id: AtomicU16,
    default_stream: OnceLock<Stream>,
}

pub type Connection = Arc<ConnectionInner>;

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
    inbox_rx: AsyncMutex<mpsc::Receiver<Box<dyn Message>>>,
    inbox_tx: mpsc::Sender<Box<dyn Message>>,
    incoming_tx: mpsc::Sender<Vec<u8>>,
    context: Mutex<Option<Arc<dyn Any + Send + Sync>>>,
    recv_timeout: Mutex<Option<Duration>>,
}

impl ConnectionInner {
    fn mark_closed(&self) {
        self.closed.store(true, Ordering::Relaxed);
        self.closed_notify.notify_waiters();
    }

    pub(crate) fn new_pending<RW>(
        io: RW,
        peer_addr: Option<String>,
        dispatch: DispatchFn,
        on_new_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
        on_close_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
    ) -> PendingConnection<ReadHalf<RW>, WriteHalf<RW>>
    where
        RW: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (read_half, write_half) = tokio::io::split(io);
        let (connection, outbound_rx) =
            Self::new_unstarted(peer_addr, dispatch, on_new_stream, on_close_stream);
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
        peer_addr: Option<String>,
        dispatch: DispatchFn,
        on_new_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
        on_close_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
    ) -> PendingConnection<R, W>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (connection, outbound_rx) =
            Self::new_unstarted(peer_addr, dispatch, on_new_stream, on_close_stream);
        PendingConnection {
            connection,
            reader,
            writer,
            outbound_rx,
        }
    }

    fn new_unstarted(
        peer_addr: Option<String>,
        dispatch: DispatchFn,
        on_new_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
        on_close_stream: Option<Arc<dyn Fn(&Stream) + Send + Sync>>,
    ) -> (Connection, mpsc::Receiver<OutboundFrame>) {
        let (outbound_tx, outbound_rx) = mpsc::channel::<OutboundFrame>(STREAM_QUEUE_SIZE);

        let connection: Connection = Arc::new(ConnectionInner {
            outbound_tx,
            streams: Mutex::new(HashMap::new()),
            peer_addr,
            on_new_stream,
            on_close_stream,
            dispatch,
            closed: AtomicBool::new(false),
            reader_abort: OnceLock::new(),
            writer_abort: OnceLock::new(),
            closed_notify: Notify::new(),
            next_stream_id: AtomicU16::new(1),
            default_stream: OnceLock::new(),
        });

        (connection, outbound_rx)
    }

    pub fn peer_addr(&self) -> Option<String> {
        self.peer_addr.clone()
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
        let streams: Vec<Stream> = self.streams.lock().unwrap().values().cloned().collect();
        for stream in streams {
            stream.close();
        }
    }

    pub(crate) async fn wait_closed(&self) {
        if self.closed.load(Ordering::Relaxed) {
            return;
        }
        self.closed_notify.notified().await;
    }

    pub fn set_recv_timeout(self: &Arc<Self>, timeout: Duration) {
        self.default_stream().set_recv_timeout(timeout);
    }

    pub async fn send<M: Message>(self: &Arc<Self>, msg: M) -> Result<(), Error> {
        self.default_stream().send(msg).await
    }

    pub async fn recv<T: Message + 'static>(self: &Arc<Self>) -> Result<T, Error> {
        self.default_stream().recv::<T>().await
    }

    pub async fn send_recv<TReq: Message, TResp: Message + 'static>(
        self: &Arc<Self>,
        msg: TReq,
    ) -> Result<TResp, Error> {
        self.default_stream().send_recv::<TReq, TResp>(msg).await
    }

    pub fn new_stream(self: &Arc<Self>) -> Stream {
        let stream_id = self.next_stream_id.fetch_add(1, Ordering::Relaxed);
        self.get_stream(stream_id)
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
}

impl Drop for ConnectionInner {
    fn drop(&mut self) {
        self.close();
    }
}

impl StreamInner {
    pub fn id(self: &Arc<Self>) -> u16 {
        self.id
    }

    pub fn close(self: &Arc<Self>) {
        let on_close = self.connection.on_close_stream.clone();
        let removed = self
            .connection
            .streams
            .lock()
            .unwrap()
            .remove(&self.id)
            .is_some();
        if removed {
            if let Some(ref f) = on_close {
                f(self);
            }
        }
    }

    pub fn set_context<T>(self: &Arc<Self>, ctx: Arc<T>)
    where
        T: Any + Send + Sync + 'static,
    {
        *self.context.lock().unwrap() = Some(ctx);
    }

    pub fn get_context<T>(self: &Arc<Self>) -> Option<Arc<T>>
    where
        T: Any + Send + Sync + 'static,
    {
        self.context
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|ctx| Arc::clone(ctx).downcast::<T>().ok())
    }

    pub fn set_recv_timeout(self: &Arc<Self>, timeout: Duration) {
        let mut guard = self.recv_timeout.lock().unwrap();
        if timeout.is_zero() {
            *guard = None;
        } else {
            *guard = Some(timeout);
        }
    }

    pub async fn send<M: Message>(self: &Arc<Self>, msg: M) -> Result<(), Error> {
        self.send_boxed(Box::new(msg)).await
    }

    pub(crate) async fn send_boxed(
        self: &Arc<Self>,
        msg: Box<dyn Message>,
    ) -> Result<(), Error> {
        let env = Envelope::new(Meta::default(), msg);
        let frame: OutboundFrame = (self.id, env);
        self.connection
            .outbound_tx
            .send(frame)
            .await
            .map_err(|_| Error::Closed)
    }

    pub(crate) async fn inbox_q(self: &Arc<Self>, msg: Box<dyn Message>) -> Result<(), Error> {
        // Enqueue for recv()/send_recv() on this stream.
        self.inbox_tx
            .send(msg)
            .await
            .map_err(|_| Error::Closed)
    }

    pub async fn recv<T: Message + 'static>(self: &Arc<Self>) -> Result<T, Error> {
        let msg = self.recv_msg().await?;
        let expected = std::any::type_name::<T>();
        let got = msg.type_name();
        match msg.downcast::<T>() {
            Ok(val) => Ok(*val),
            Err(_) => Err(Error::TypeMismatch { expected, got }),
        }
    }

    pub async fn send_recv<TReq: Message, TResp: Message + 'static>(
        self: &Arc<Self>,
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

    async fn recv_msg(self: &Arc<Self>) -> Result<Box<dyn Message>, Error> {
        let timeout = *self.recv_timeout.lock().unwrap();
        let mut inbox = self.inbox_rx.lock().await;
        if let Some(timeout) = timeout {
            match tokio::time::timeout(timeout, inbox.recv()).await {
                Ok(Some(msg)) => Ok(msg),
                Ok(None) => Err(Error::Closed),
                Err(_) => Err(Error::RecvTimeout),
            }
        } else {
            inbox.recv().await.ok_or(Error::Closed)
        }
    }
}

impl ConnectionInner {
    fn make_stream(self: &Connection, stream_id: StreamId) -> Stream {
        let (inbox_tx, inbox_rx) = mpsc::channel::<Box<dyn Message>>(STREAM_QUEUE_SIZE);
        let (incoming_tx, incoming_rx) =
            mpsc::channel::<Vec<u8>>(STREAM_QUEUE_SIZE);
        let stream: Stream = Arc::new(StreamInner {
            id: stream_id,
            connection: self.clone(),
            inbox_rx: AsyncMutex::new(inbox_rx),
            inbox_tx,
            incoming_tx,
            context: Mutex::new(None),
            recv_timeout: Mutex::new(None),
        });
        self.streams
            .lock()
            .unwrap()
            .insert(stream_id, stream.clone());
        let dispatch = self.dispatch.clone();
        let stream_task = stream.clone();
        let _ = tokio::spawn(async move {
            let mut incoming_rx = incoming_rx;
            while let Some(payload) = incoming_rx.recv().await {
                let env = match postcard::from_bytes::<Envelope>(&payload) {
                    Ok(env) => env,
                    Err(err) => {
                        warn!("decode failed: {err}");
                        stream_task.connection.mark_closed();
                        break;
                    }
                };

                dispatch(stream_task.clone(), env).await;
            }
        });
        if let Some(ref f) = self.on_new_stream {
            f(&stream);
        }
        stream
    }

    fn default_stream(self: &Connection) -> Stream {
        self.get_stream(DEFAULT_STREAM_ID)
    }

    fn get_stream(self: &Connection, stream_id: StreamId) -> Stream {
        if stream_id == DEFAULT_STREAM_ID {
            return self
                .default_stream
                .get_or_init(|| self.make_stream(DEFAULT_STREAM_ID))
                .clone();
        }
        if let Some(stream) = self.streams.lock().unwrap().get(&stream_id).cloned() {
            return stream;
        }

        self.make_stream(stream_id)
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
